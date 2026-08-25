package service

// serper_gateway_service.go 实现 serper.dev 平台的反向代理转发。
//
// serper.dev 是 Google 搜索 API（https://google.serper.dev），认证方式为
// X-API-KEY header，请求/响应均为普通 JSON（无流式、无模型概念）。
//
// 本服务复用现有账号池（Account）基础设施：
//   - 通过 GatewayService 的调度器选账号（多 key 轮询、优先级、并发）
//   - 通过 RateLimitService 在 429/403 时踢出账号（rate_limited）并重选
//   - 被踢出的账号由管理员在后台“清除限流”手动恢复
//
// 客户端把原本发给 google.serper.dev 的搜索请求发到 /serper/search，
// 携带 sub2api 的 API Key 认证，本服务替换认证头后转发到上游。

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
)

// serperMaxResponseSize 限制上游响应体读取大小，防止异常大响应打爆内存。
const serperMaxResponseSize = 10 * 1024 * 1024 // 10 MiB

const serperSchedulableUpdateTimeout = 3 * time.Second

// SerperGatewayService 负责 serper 平台的账号选择与请求转发。
type SerperGatewayService struct {
	gateway          *GatewayService
	httpUpstream     HTTPUpstream
	rateLimitService *RateLimitService
	// maxAccountSwitches 是单次请求最多尝试的账号数量（踢出后重选）。
	maxAccountSwitches int
}

// NewSerperGatewayService 构造 serper 网关服务。
func NewSerperGatewayService(gateway *GatewayService, httpUpstream HTTPUpstream, rateLimitService *RateLimitService) *SerperGatewayService {
	return &SerperGatewayService{
		gateway:            gateway,
		httpUpstream:       httpUpstream,
		rateLimitService:   rateLimitService,
		maxAccountSwitches: 5,
	}
}

// RecordUsageLog delegates Serper's audit-only usage row to the shared
// GatewayService writer. No billing or quota mutation is performed.
func (s *SerperGatewayService) RecordUsageLog(ctx context.Context, usageLog *UsageLog) error {
	if s == nil || s.gateway == nil {
		return nil
	}
	return s.gateway.RecordUsageLog(ctx, usageLog)
}

// SerperForwardResult 承载一次 serper 转发的结果，供 handler 写回客户端。
type SerperForwardResult struct {
	StatusCode        int
	Header            http.Header
	Body              []byte
	AccountID         int64
	UpstreamAttempted bool
	Duration          time.Duration
}

// SerperCapacityError indicates that a selected account had no available
// concurrency slot. The caller should retry the request instead of treating
// this as an upstream service outage.
type SerperCapacityError struct {
	RetryAfter int
}

func (e *SerperCapacityError) Error() string {
	return "serper: account capacity unavailable"
}

// SerperRateLimitError preserves the final upstream 429 after all eligible
// accounts have been tried. This lets the HTTP handler return the upstream
// rate-limit response instead of converting it to a generic 503.
type SerperRateLimitError struct {
	Result *SerperForwardResult
}

func (e *SerperRateLimitError) Error() string {
	if e == nil || e.Result == nil {
		return "serper: upstream rate limit exhausted"
	}
	return fmt.Sprintf("serper: upstream returned %d after account failover", e.Result.StatusCode)
}

// SerperCreditsExhaustedError preserves the final upstream 400 after all
// eligible accounts reported that their Serper credits were exhausted.
type SerperCreditsExhaustedError struct {
	Result *SerperForwardResult
}

func (e *SerperCreditsExhaustedError) Error() string {
	if e == nil || e.Result == nil {
		return "serper: upstream credits exhausted"
	}
	return fmt.Sprintf("serper: upstream returned %d because credits are exhausted", e.Result.StatusCode)
}

// ForwardSearch 从号池选一个 serper 账号并把搜索请求转发到上游。
// 收到 429/403 时踢出当前账号并换下一个重试，直到成功或耗尽候选。
func (s *SerperGatewayService) ForwardSearch(
	ctx context.Context,
	groupID *int64,
	sub2apiUserID int64,
	rawQuery string,
	reqHeader http.Header,
	body []byte,
) (*SerperForwardResult, error) {
	excluded := make(map[int64]struct{})
	var lastErr error
	var lastRateLimitResult *SerperForwardResult
	var lastCreditsExhaustedResult *SerperForwardResult
	var lastUpstreamResult *SerperForwardResult
	lastSwitchStatus := 0
	startedAt := time.Now()
	finish := func(result *SerperForwardResult) *SerperForwardResult {
		if result != nil {
			result.Duration = time.Since(startedAt)
		}
		return result
	}

	for attempt := 0; attempt < s.maxAccountSwitches; attempt++ {
		selection, err := s.gateway.SelectAccountWithLoadAwareness(ctx, groupID, "", "", excluded, "", sub2apiUserID)
		if err != nil {
			if lastCreditsExhaustedResult != nil {
				return finish(lastCreditsExhaustedResult), &SerperCreditsExhaustedError{Result: lastCreditsExhaustedResult}
			}
			if lastSwitchStatus == http.StatusTooManyRequests && lastRateLimitResult != nil {
				return finish(lastRateLimitResult), &SerperRateLimitError{Result: lastRateLimitResult}
			}
			if lastErr != nil {
				return finish(lastUpstreamResult), lastErr
			}
			return nil, err
		}
		if selection == nil || selection.Account == nil {
			if lastCreditsExhaustedResult != nil {
				return finish(lastCreditsExhaustedResult), &SerperCreditsExhaustedError{Result: lastCreditsExhaustedResult}
			}
			if lastSwitchStatus == http.StatusTooManyRequests && lastRateLimitResult != nil {
				return finish(lastRateLimitResult), &SerperRateLimitError{Result: lastRateLimitResult}
			}
			return finish(lastUpstreamResult), fmt.Errorf("serper: account selection returned no account")
		}
		if !selection.Acquired {
			return finish(lastUpstreamResult), &SerperCapacityError{RetryAfter: 1}
		}

		account := selection.Account
		result, switchAccount, err := func() (*SerperForwardResult, bool, error) {
			if selection.ReleaseFunc != nil {
				defer selection.ReleaseFunc()
			}

			apiKey, tokenType, credentialErr := s.gateway.GetAccessToken(ctx, account)
			if credentialErr != nil {
				return nil, true, fmt.Errorf("serper: get credential for account %d: %w", account.ID, credentialErr)
			}
			if tokenType != "apikey" {
				return nil, true, fmt.Errorf("serper: account %d has unsupported token type %q", account.ID, tokenType)
			}

			return s.forwardToAccount(ctx, account, apiKey, rawQuery, reqHeader, body)
		}()
		if err != nil {
			if !switchAccount {
				return finish(result), err
			}
			excluded[account.ID] = struct{}{}
			lastErr = err
			lastCreditsExhaustedResult = nil
			// A pre-send credential/configuration failure supersedes any earlier
			// upstream 429; only an uninterrupted final 429 chain is preserved.
			lastSwitchStatus = 0
			lastRateLimitResult = nil
			continue
		}
		if switchAccount {
			// 上游返回限流/鉴权错误：踢出该账号（写 rate_limited），换下一个重试。
			excluded[account.ID] = struct{}{}
			lastErr = fmt.Errorf("serper: account %d upstream returned %d", account.ID, result.StatusCode)
			lastSwitchStatus = result.StatusCode
			if isSerperCreditsExhausted(result) {
				lastCreditsExhaustedResult = result
			} else {
				lastCreditsExhaustedResult = nil
			}
			if result.UpstreamAttempted {
				lastUpstreamResult = result
			}
			if result.StatusCode == http.StatusTooManyRequests {
				lastRateLimitResult = result
			}
			continue
		}
		return finish(result), nil
	}

	if lastErr != nil {
		if lastCreditsExhaustedResult != nil {
			return finish(lastCreditsExhaustedResult), &SerperCreditsExhaustedError{Result: lastCreditsExhaustedResult}
		}
		if lastSwitchStatus == http.StatusTooManyRequests && lastRateLimitResult != nil {
			return finish(lastRateLimitResult), &SerperRateLimitError{Result: lastRateLimitResult}
		}
		return finish(lastUpstreamResult), lastErr
	}
	return nil, fmt.Errorf("serper: no available account after %d attempts", s.maxAccountSwitches)
}

// forwardToAccount 用指定账号执行一次上游请求。
// 返回 switchAccount=true 表示凭证/配置失败，或明确收到 429/403，可安全换号。
func (s *SerperGatewayService) forwardToAccount(
	ctx context.Context,
	account *Account,
	apiKey string,
	rawQuery string,
	reqHeader http.Header,
	body []byte,
) (result *SerperForwardResult, switchAccount bool, err error) {
	startedAt := time.Now()
	result = &SerperForwardResult{AccountID: account.ID}
	defer func() {
		if result != nil {
			result.Duration = time.Since(startedAt)
		}
	}()
	baseURL := account.GetBaseURL()
	if baseURL == "" {
		baseURL = "https://google.serper.dev"
	}
	validatedBaseURL, err := s.gateway.validateUpstreamBaseURL(baseURL)
	if err != nil {
		return nil, true, fmt.Errorf("serper: validate account %d base URL: %w", account.ID, err)
	}
	parsedURL, err := url.Parse(validatedBaseURL)
	if err != nil {
		return nil, true, fmt.Errorf("serper: parse account %d base URL: %w", account.ID, err)
	}
	parsedURL.Path = strings.TrimRight(parsedURL.Path, "/") + "/search"
	parsedURL.RawPath = ""
	parsedURL.RawQuery = rawQuery
	parsedURL.Fragment = ""
	upstreamURL := parsedURL.String()

	proxyURL := ""
	if account.ProxyID != nil && account.Proxy != nil {
		proxyURL = account.Proxy.URL()
	}

	logger.LegacyPrintf("service.serper", "[Serper] 转发: account=%d name=%s url=%s proxy=%v",
		account.ID, account.Name, upstreamURL, proxyURL != "")

	forwardCtx := WithHTTPUpstreamProfile(ctx, HTTPUpstreamProfileSerper)
	reqCtx, cancel := context.WithTimeout(forwardCtx, 60*time.Second)
	defer cancel()

	upstreamReq, err := http.NewRequestWithContext(reqCtx, http.MethodPost, upstreamURL, bytes.NewReader(body))
	if err != nil {
		return nil, true, fmt.Errorf("serper: build request: %w", err)
	}
	// 透传客户端的 Content-Type / Accept，注入 serper 认证头。
	if ct := reqHeader.Get("Content-Type"); ct != "" {
		upstreamReq.Header.Set("Content-Type", ct)
	} else {
		upstreamReq.Header.Set("Content-Type", "application/json")
	}
	if ac := reqHeader.Get("Accept"); ac != "" {
		upstreamReq.Header.Set("Accept", ac)
	}
	upstreamReq.Header.Set("X-API-KEY", apiKey)

	result.UpstreamAttempted = true
	resp, err := s.httpUpstream.Do(upstreamReq, proxyURL, account.ID, account.Concurrency)
	if err != nil {
		return result, false, fmt.Errorf("serper: upstream request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	result.StatusCode = resp.StatusCode
	result.Header = resp.Header.Clone()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, serperMaxResponseSize))
	if err != nil {
		return result, false, fmt.Errorf("serper: read upstream body: %w", err)
	}

	// 429（限流）/ 403（鉴权/额度）：踢出账号并换号重试。
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusForbidden {
		if s.rateLimitService != nil {
			s.rateLimitService.HandleUpstreamError(ctx, account, resp.StatusCode, resp.Header, respBody)
		}
		result.Body = respBody
		return result, true, nil
	}

	result.Body = respBody
	if resp.StatusCode == http.StatusBadRequest && isSerperCreditsExhaustedResponse(respBody) {
		s.disableCreditsExhaustedAccount(account.ID)
		return result, true, nil
	}
	return result, false, nil
}

type serperCreditsErrorResponse struct {
	Message    string          `json:"message"`
	StatusCode json.RawMessage `json:"statusCode"`
}

func isSerperCreditsExhausted(result *SerperForwardResult) bool {
	return result != nil && result.StatusCode == http.StatusBadRequest && isSerperCreditsExhaustedResponse(result.Body)
}

func isSerperCreditsExhaustedResponse(body []byte) bool {
	var payload serperCreditsErrorResponse
	if err := json.Unmarshal(body, &payload); err != nil {
		return false
	}
	if payload.Message != "Not enough credits" {
		return false
	}
	if len(payload.StatusCode) == 0 {
		return true
	}
	var statusCode int
	return json.Unmarshal(payload.StatusCode, &statusCode) == nil && statusCode == http.StatusBadRequest
}

func (s *SerperGatewayService) disableCreditsExhaustedAccount(accountID int64) {
	if s == nil || s.gateway == nil || s.gateway.accountRepo == nil || accountID <= 0 {
		return
	}
	updateCtx, cancel := context.WithTimeout(context.Background(), serperSchedulableUpdateTimeout)
	defer cancel()
	if err := s.gateway.accountRepo.SetSchedulable(updateCtx, accountID, false); err != nil {
		slog.Warn("serper_credits_exhausted_set_schedulable_failed", "account_id", accountID, "error", err)
	}
}

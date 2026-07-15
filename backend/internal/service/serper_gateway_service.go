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
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
)

// serperMaxResponseSize 限制上游响应体读取大小，防止异常大响应打爆内存。
const serperMaxResponseSize = 10 * 1024 * 1024 // 10 MiB

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

// SerperForwardResult 承载一次 serper 转发的结果，供 handler 写回客户端。
type SerperForwardResult struct {
	StatusCode int
	Header     http.Header
	Body       []byte
	AccountID  int64
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

	for attempt := 0; attempt < s.maxAccountSwitches; attempt++ {
		selection, err := s.gateway.SelectAccountWithLoadAwareness(ctx, groupID, "", "", excluded, "", sub2apiUserID)
		if err != nil {
			if lastErr != nil {
				return nil, lastErr
			}
			return nil, err
		}
		if selection == nil || selection.Account == nil {
			return nil, fmt.Errorf("serper: account selection returned no account")
		}
		if !selection.Acquired {
			return nil, fmt.Errorf("serper: account capacity unavailable")
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
				return nil, err
			}
			excluded[account.ID] = struct{}{}
			lastErr = err
			continue
		}
		if switchAccount {
			// 上游返回限流/鉴权错误：踢出该账号（写 rate_limited），换下一个重试。
			excluded[account.ID] = struct{}{}
			lastErr = fmt.Errorf("serper: account %d upstream returned %d", account.ID, result.StatusCode)
			continue
		}
		return result, nil
	}

	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("serper: no available account after %d attempts", s.maxAccountSwitches)
}

// forwardToAccount 用指定账号执行一次上游请求。
// 返回 switchAccount=true 表示请求尚未发送，或明确收到 429/403，可安全换号。
func (s *SerperGatewayService) forwardToAccount(
	ctx context.Context,
	account *Account,
	apiKey string,
	rawQuery string,
	reqHeader http.Header,
	body []byte,
) (result *SerperForwardResult, switchAccount bool, err error) {
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

	resp, err := s.httpUpstream.Do(upstreamReq, proxyURL, account.ID, account.Concurrency)
	if err != nil {
		return nil, false, fmt.Errorf("serper: upstream request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, serperMaxResponseSize))
	if err != nil {
		return nil, false, fmt.Errorf("serper: read upstream body: %w", err)
	}

	// 429（限流）/ 403（鉴权/额度）：踢出账号并换号重试。
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusForbidden {
		if s.rateLimitService != nil {
			s.rateLimitService.HandleUpstreamError(ctx, account, resp.StatusCode, resp.Header, respBody)
		}
		return &SerperForwardResult{
			StatusCode: resp.StatusCode,
			Header:     resp.Header.Clone(),
			Body:       respBody,
			AccountID:  account.ID,
		}, true, nil
	}

	return &SerperForwardResult{
		StatusCode: resp.StatusCode,
		Header:     resp.Header.Clone(),
		Body:       respBody,
		AccountID:  account.ID,
	}, false, nil
}

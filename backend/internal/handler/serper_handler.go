package handler

// serper_handler.go 实现 serper.dev 反向代理的 HTTP 入口。
//
// 客户端把原本发给 google.serper.dev 的请求发到 /serper/search，携带 sub2api
// 的 API Key 认证。本 handler 读取请求体，交由 SerperGatewayService 从号池选号
// 并转发到上游，再把上游响应原样写回客户端。

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ip"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"

	"github.com/gin-gonic/gin"
)

// serperMaxRequestBody 限制客户端请求体大小。
const serperMaxRequestBody = 1 * 1024 * 1024 // 1 MiB

// SerperHandler 处理 serper 反代请求。
type SerperHandler struct {
	serperGatewayService  *service.SerperGatewayService
	concurrencyHelper     *ConcurrencyHelper
	billingCacheService   *service.BillingCacheService
	usageRecordWorkerPool *service.UsageRecordWorkerPool
}

// NewSerperHandler 构造 serper handler。
func NewSerperHandler(
	serperGatewayService *service.SerperGatewayService,
	concurrencyHelper *ConcurrencyHelper,
	billingCacheService *service.BillingCacheService,
	usageRecordWorkerPool *service.UsageRecordWorkerPool,
) *SerperHandler {
	return &SerperHandler{
		serperGatewayService:  serperGatewayService,
		concurrencyHelper:     concurrencyHelper,
		billingCacheService:   billingCacheService,
		usageRecordWorkerPool: usageRecordWorkerPool,
	}
}

// Search 反代 POST /serper/search 到 google.serper.dev/search。
func (h *SerperHandler) Search(c *gin.Context) {
	apiKey, ok := middleware2.GetAPIKeyFromContext(c)
	if !ok || apiKey == nil || apiKey.User == nil {
		serperErrorResponse(c, http.StatusUnauthorized, "authentication_error", "Invalid API key")
		return
	}

	subject, ok := middleware2.GetAuthSubjectFromContext(c)
	if !ok {
		serperErrorResponse(c, http.StatusInternalServerError, "api_error", "User context not found")
		return
	}

	// 校验分组平台为 serper，避免非 serper Key 打到此入口。
	if apiKey.Group == nil || apiKey.Group.Platform != service.PlatformSerper {
		serperErrorResponse(c, http.StatusForbidden, "permission_error", "API key is not bound to a serper group")
		return
	}

	if h.concurrencyHelper == nil || h.billingCacheService == nil || h.serperGatewayService == nil {
		serperErrorResponse(c, http.StatusServiceUnavailable, "api_error", "Service temporarily unavailable")
		return
	}

	streamStarted := false
	userReleaseFunc, err := h.concurrencyHelper.AcquireUserSlotWithWait(
		c,
		subject.UserID,
		subject.Concurrency,
		false,
		&streamStarted,
	)
	if err != nil {
		status, code, message := concurrencyErrorResponse(err, "user")
		if status == http.StatusTooManyRequests {
			// A full pending queue is a temporary admission failure. Give clients
			// a short retry hint instead of making them guess a backoff interval.
			c.Header("Retry-After", "1")
		}
		serperErrorResponse(c, status, code, message)
		return
	}
	userReleaseFunc = wrapReleaseOnDone(c.Request.Context(), userReleaseFunc)
	if userReleaseFunc != nil {
		defer userReleaseFunc()
	}

	subscription, _ := middleware2.GetSubscriptionFromContext(c)
	if err := h.billingCacheService.CheckBillingEligibility(
		c.Request.Context(),
		apiKey.User,
		apiKey,
		apiKey.Group,
		subscription,
		service.PlatformSerper,
	); err != nil {
		status, code, message, retryAfter := billingErrorDetails(err)
		if retryAfter > 0 {
			c.Header("Retry-After", strconv.Itoa(retryAfter))
		}
		serperErrorResponse(c, status, code, message)
		return
	}

	body, err := io.ReadAll(io.LimitReader(c.Request.Body, serperMaxRequestBody))
	if err != nil {
		serperErrorResponse(c, http.StatusBadRequest, "invalid_request_error", "failed to read request body")
		return
	}

	result, err := h.serperGatewayService.ForwardSearch(
		c.Request.Context(),
		apiKey.GroupID,
		subject.UserID,
		c.Request.URL.RawQuery,
		c.Request.Header,
		body,
	)
	if result != nil && result.UpstreamAttempted {
		h.recordUsageLog(c, apiKey, subscription, result)
	}
	if err != nil {
		var capacityErr *service.SerperCapacityError
		if errors.As(err, &capacityErr) {
			retryAfter := capacityErr.RetryAfter
			if retryAfter < 1 {
				retryAfter = 1
			}
			c.Header("Retry-After", strconv.Itoa(retryAfter))
			serperErrorResponse(c, http.StatusTooManyRequests, "rate_limit_error", "Serper account capacity is temporarily full")
			return
		}

		var creditsExhaustedErr *service.SerperCreditsExhaustedError
		if errors.As(err, &creditsExhaustedErr) && creditsExhaustedErr.Result != nil {
			if result == nil || !result.UpstreamAttempted {
				h.recordUsageLog(c, apiKey, subscription, creditsExhaustedErr.Result)
			}
			writeSerperForwardResult(c, creditsExhaustedErr.Result)
			return
		}

		var rateLimitErr *service.SerperRateLimitError
		if errors.As(err, &rateLimitErr) && rateLimitErr.Result != nil {
			if result == nil || !result.UpstreamAttempted {
				h.recordUsageLog(c, apiKey, subscription, rateLimitErr.Result)
			}
			writeSerperForwardResult(c, rateLimitErr.Result)
			return
		}

		serperErrorResponse(c, http.StatusServiceUnavailable, "api_error", err.Error())
		return
	}

	writeSerperForwardResult(c, result)
}

func (h *SerperHandler) recordUsageLog(c *gin.Context, apiKey *service.APIKey, subscription *service.UserSubscription, result *service.SerperForwardResult) {
	if h == nil || h.serperGatewayService == nil || c == nil || apiKey == nil || apiKey.User == nil || result == nil || !result.UpstreamAttempted || result.AccountID == 0 {
		return
	}

	model := "serper/search"
	billingMode := string(service.BillingModePerRequest)
	durationMs := int(result.Duration.Milliseconds())
	if durationMs < 0 {
		durationMs = 0
	}
	billingType := service.BillingTypeBalance
	if subscription != nil && apiKey.Group != nil && apiKey.Group.IsSubscriptionType() {
		billingType = service.BillingTypeSubscription
	}
	usageLog := &service.UsageLog{
		UserID:           apiKey.User.ID,
		APIKeyID:         apiKey.ID,
		AccountID:        result.AccountID,
		Model:            model,
		RequestedModel:   model,
		InboundEndpoint:  serperStringPtr("/serper/search"),
		UpstreamEndpoint: serperStringPtr("/search"),
		GroupID:          apiKey.GroupID,
		SubscriptionID:   serperSubscriptionID(subscription),
		BillingType:      billingType,
		BillingMode:      &billingMode,
		RequestType:      service.RequestTypeSync,
		Stream:           false,
		DurationMs:       &durationMs,
		UserAgent:        serperStringPtr(c.GetHeader("User-Agent")),
		IPAddress:        serperStringPtr(ip.GetClientIP(c)),
	}

	task := service.UsageRecordTask(func(ctx context.Context) {
		_ = h.serperGatewayService.RecordUsageLog(ctx, usageLog)
	})
	if h.usageRecordWorkerPool != nil {
		task = wrapUsageRecordTaskContext(c.Request.Context(), task)
		if mode := h.usageRecordWorkerPool.Submit(task); mode != service.UsageRecordSubmitModeDropped {
			return
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	task(usageRecordContext(c.Request.Context(), ctx))
}

func serperSubscriptionID(subscription *service.UserSubscription) *int64 {
	if subscription == nil {
		return nil
	}
	return &subscription.ID
}

func serperStringPtr(value string) *string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return &value
}

func writeSerperForwardResult(c *gin.Context, result *service.SerperForwardResult) {
	if result == nil {
		serperErrorResponse(c, http.StatusServiceUnavailable, "api_error", "Service temporarily unavailable")
		return
	}
	// 透传上游 Content-Type 和 Retry-After，写回状态码与响应体。
	if ct := result.Header.Get("Content-Type"); ct != "" {
		c.Header("Content-Type", ct)
	}
	if retryAfter := result.Header.Get("Retry-After"); retryAfter != "" {
		c.Header("Retry-After", retryAfter)
	} else if result.StatusCode == http.StatusTooManyRequests {
		c.Header("Retry-After", "1")
	}
	c.Status(result.StatusCode)
	_, _ = c.Writer.Write(result.Body)
}

func serperErrorResponse(c *gin.Context, status int, code, message string) {
	c.JSON(status, gin.H{"error": gin.H{"type": code, "message": message}})
}

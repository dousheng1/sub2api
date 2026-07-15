package handler

// serper_handler.go 实现 serper.dev 反向代理的 HTTP 入口。
//
// 客户端把原本发给 google.serper.dev 的请求发到 /serper/search，携带 sub2api
// 的 API Key 认证。本 handler 读取请求体，交由 SerperGatewayService 从号池选号
// 并转发到上游，再把上游响应原样写回客户端。

import (
	"io"
	"net/http"
	"strconv"

	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"

	"github.com/gin-gonic/gin"
)

// serperMaxRequestBody 限制客户端请求体大小。
const serperMaxRequestBody = 1 * 1024 * 1024 // 1 MiB

// SerperHandler 处理 serper 反代请求。
type SerperHandler struct {
	serperGatewayService *service.SerperGatewayService
	concurrencyHelper    *ConcurrencyHelper
	billingCacheService  *service.BillingCacheService
}

// NewSerperHandler 构造 serper handler。
func NewSerperHandler(
	serperGatewayService *service.SerperGatewayService,
	concurrencyHelper *ConcurrencyHelper,
	billingCacheService *service.BillingCacheService,
) *SerperHandler {
	return &SerperHandler{
		serperGatewayService: serperGatewayService,
		concurrencyHelper:    concurrencyHelper,
		billingCacheService:  billingCacheService,
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
	if err != nil {
		serperErrorResponse(c, http.StatusServiceUnavailable, "api_error", err.Error())
		return
	}

	// 透传上游 Content-Type，写回状态码与响应体。
	if ct := result.Header.Get("Content-Type"); ct != "" {
		c.Header("Content-Type", ct)
	}
	c.Status(result.StatusCode)
	_, _ = c.Writer.Write(result.Body)
}

func serperErrorResponse(c *gin.Context, status int, code, message string) {
	c.JSON(status, gin.H{"error": gin.H{"type": code, "message": message}})
}

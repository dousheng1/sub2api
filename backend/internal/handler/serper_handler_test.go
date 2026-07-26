package handler

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type serperHandlerAccountRepoStub struct {
	service.AccountRepository
	account   *service.Account
	listCalls int32
}

func (s *serperHandlerAccountRepoStub) GetByID(context.Context, int64) (*service.Account, error) {
	return s.account, nil
}

func (s *serperHandlerAccountRepoStub) ListSchedulableByGroupIDAndPlatform(context.Context, int64, string) ([]service.Account, error) {
	atomic.AddInt32(&s.listCalls, 1)
	return []service.Account{*s.account}, nil
}

func (s *serperHandlerAccountRepoStub) ListSchedulableByPlatform(context.Context, string) ([]service.Account, error) {
	atomic.AddInt32(&s.listCalls, 1)
	return []service.Account{*s.account}, nil
}

type serperHandlerGroupRepoStub struct {
	service.GroupRepository
	group *service.Group
}

func (s *serperHandlerGroupRepoStub) GetByID(context.Context, int64) (*service.Group, error) {
	return s.group, nil
}

func (s *serperHandlerGroupRepoStub) GetByIDLite(context.Context, int64) (*service.Group, error) {
	return s.group, nil
}

type serperHandlerUpstreamStub struct {
	calls int32
}

func (s *serperHandlerUpstreamStub) Do(*http.Request, string, int64, int) (*http.Response, error) {
	atomic.AddInt32(&s.calls, 1)
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"ok":true}`)),
	}, nil
}

func (s *serperHandlerUpstreamStub) DoWithTLS(*http.Request, string, int64, int, *tlsfingerprint.Profile) (*http.Response, error) {
	panic("unexpected DoWithTLS call")
}

type serperHandlerBalanceCacheStub struct {
	service.BillingCache
	balance         float64
	apiKeyRateLimit *service.APIKeyRateLimitCacheData
}

func (s *serperHandlerBalanceCacheStub) GetUserBalance(context.Context, int64) (float64, error) {
	return s.balance, nil
}

func (s *serperHandlerBalanceCacheStub) GetAPIKeyRateLimit(context.Context, int64) (*service.APIKeyRateLimitCacheData, error) {
	return s.apiKeyRateLimit, nil
}

func (s *serperHandlerBalanceCacheStub) InvalidateAPIKeyRateLimit(context.Context, int64) error {
	return nil
}

type serperHandlerRPMCacheStub struct {
	service.UserRPMCache
	groupCount int
	userCount  int
}

func (s *serperHandlerRPMCacheStub) IncrementUserGroupRPM(context.Context, int64, int64) (int, error) {
	return s.groupCount, nil
}

func (s *serperHandlerRPMCacheStub) IncrementUserRPM(context.Context, int64) (int, error) {
	return s.userCount, nil
}

func (s *serperHandlerRPMCacheStub) GetUserGroupRPM(context.Context, int64, int64) (int, error) {
	return 0, nil
}

func (s *serperHandlerRPMCacheStub) GetUserRPM(context.Context, int64) (int, error) {
	return 0, nil
}

type serperHandlerQueueFullCacheStub struct {
	service.ConcurrencyCache
	acquireCalls int32
}

func (s *serperHandlerQueueFullCacheStub) AcquireUserSlot(context.Context, int64, int, string) (bool, error) {
	atomic.AddInt32(&s.acquireCalls, 1)
	return false, nil
}

func (s *serperHandlerQueueFullCacheStub) IncrementWaitCount(context.Context, int64, int) (bool, error) {
	return false, nil
}

type serperHandlerFixture struct {
	handler          *SerperHandler
	apiKey           *service.APIKey
	concurrencyCache *concurrencyCacheMock
	accountRepo      *serperHandlerAccountRepoStub
	upstream         *serperHandlerUpstreamStub
	billing          *service.BillingCacheService
}

func newSerperHandlerFixture(t *testing.T, cfg *config.Config, billingCache service.BillingCache, rpmCache service.UserRPMCache) *serperHandlerFixture {
	t.Helper()

	groupID := int64(71)
	user := &service.User{ID: 701, Concurrency: 2, Balance: 10, Status: service.StatusActive}
	group := &service.Group{
		ID:       groupID,
		Platform: service.PlatformSerper,
		Status:   service.StatusActive,
		Hydrated: true,
	}
	account := &service.Account{
		ID:          801,
		Name:        "serper",
		Platform:    service.PlatformSerper,
		Type:        service.AccountTypeAPIKey,
		Credentials: map[string]any{"api_key": "secret"},
		Status:      service.StatusActive,
		Schedulable: true,
		Concurrency: 2,
	}
	accountRepo := &serperHandlerAccountRepoStub{account: account}
	groupRepo := &serperHandlerGroupRepoStub{group: group}
	concurrencyCache := &concurrencyCacheMock{
		acquireUserSlotFn: func(context.Context, int64, int, string) (bool, error) {
			return true, nil
		},
		acquireAccountSlotFn: func(context.Context, int64, int, string) (bool, error) {
			return true, nil
		},
	}
	concurrencyService := service.NewConcurrencyService(concurrencyCache)
	upstream := &serperHandlerUpstreamStub{}
	billing := service.NewBillingCacheService(billingCache, nil, nil, nil, rpmCache, nil, cfg, nil)
	t.Cleanup(billing.Stop)

	gateway := service.NewGatewayService(
		accountRepo,
		groupRepo,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		cfg,
		nil,
		concurrencyService,
		nil,
		nil,
		billing,
		nil,
		upstream,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	serperGateway := service.NewSerperGatewayService(gateway, upstream, nil)
	concurrencyHelper := NewConcurrencyHelper(concurrencyService, SSEPingFormatNone, 0)

	return &serperHandlerFixture{
		handler:          NewSerperHandler(serperGateway, concurrencyHelper, billing),
		apiKey:           &service.APIKey{ID: 901, UserID: user.ID, GroupID: &groupID, User: user, Group: group},
		concurrencyCache: concurrencyCache,
		accountRepo:      accountRepo,
		upstream:         upstream,
		billing:          billing,
	}
}

func performSerperSearch(t *testing.T, fixture *serperHandlerFixture) *httptest.ResponseRecorder {
	t.Helper()

	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/serper/search?gl=us", strings.NewReader(`{"q":"golang"}`))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Set(string(middleware2.ContextKeyAPIKey), fixture.apiKey)
	c.Set(string(middleware2.ContextKeyUser), middleware2.AuthSubject{
		UserID:      fixture.apiKey.User.ID,
		Concurrency: fixture.apiKey.User.Concurrency,
	})

	fixture.handler.Search(c)
	return recorder
}

func TestSerperHandlerSearchAcquiresAndReleasesUserSlot(t *testing.T) {
	cfg := &config.Config{RunMode: config.RunModeSimple}
	cfg.Gateway.Scheduling.LoadBatchEnabled = false
	cfg.Security.URLAllowlist.Enabled = true
	cfg.Security.URLAllowlist.UpstreamHosts = []string{"google.serper.dev"}
	fixture := newSerperHandlerFixture(t, cfg, nil, nil)

	recorder := performSerperSearch(t, fixture)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, int32(1), atomic.LoadInt32(&fixture.concurrencyCache.releaseUserCalled))
	require.Equal(t, int32(1), atomic.LoadInt32(&fixture.concurrencyCache.releaseAccountCalled))
	require.Equal(t, int32(1), atomic.LoadInt32(&fixture.upstream.calls))
}

func TestSerperHandlerRPMRejectionDoesNotReachUpstream(t *testing.T) {
	cfg := &config.Config{RunMode: config.RunModeStandard}
	cfg.Gateway.Scheduling.LoadBatchEnabled = false
	cfg.Billing.MinimumBalanceReserve = 0.01
	fixture := newSerperHandlerFixture(
		t,
		cfg,
		&serperHandlerBalanceCacheStub{balance: 10},
		&serperHandlerRPMCacheStub{groupCount: 2},
	)
	fixture.apiKey.Group.RPMLimit = 1

	recorder := performSerperSearch(t, fixture)

	require.Equal(t, http.StatusTooManyRequests, recorder.Code)
	require.NotEmpty(t, recorder.Header().Get("Retry-After"))
	require.Contains(t, recorder.Body.String(), "rate_limit_exceeded")
	require.Equal(t, int32(1), atomic.LoadInt32(&fixture.concurrencyCache.releaseUserCalled))
	require.Zero(t, atomic.LoadInt32(&fixture.accountRepo.listCalls))
	require.Zero(t, atomic.LoadInt32(&fixture.upstream.calls))
}

func TestSerperHandlerBillingRejectionDoesNotReachUpstream(t *testing.T) {
	cfg := &config.Config{RunMode: config.RunModeStandard}
	cfg.Gateway.Scheduling.LoadBatchEnabled = false
	cfg.Billing.MinimumBalanceReserve = 0.01
	fixture := newSerperHandlerFixture(t, cfg, &serperHandlerBalanceCacheStub{balance: 0.001}, nil)

	recorder := performSerperSearch(t, fixture)

	require.Equal(t, http.StatusForbidden, recorder.Code)
	require.Contains(t, recorder.Body.String(), "billing_error")
	require.Equal(t, int32(1), atomic.LoadInt32(&fixture.concurrencyCache.releaseUserCalled))
	require.Zero(t, atomic.LoadInt32(&fixture.accountRepo.listCalls))
	require.Zero(t, atomic.LoadInt32(&fixture.upstream.calls))
}

func TestSerperHandlerUserConcurrencyQueueFullDoesNotReachUpstream(t *testing.T) {
	cfg := &config.Config{RunMode: config.RunModeSimple}
	fixture := newSerperHandlerFixture(t, cfg, nil, nil)
	queueFullCache := &serperHandlerQueueFullCacheStub{}
	fixture.handler.concurrencyHelper = NewConcurrencyHelper(
		service.NewConcurrencyService(queueFullCache),
		SSEPingFormatNone,
		0,
	)

	recorder := performSerperSearch(t, fixture)

	require.Equal(t, http.StatusTooManyRequests, recorder.Code)
	require.Contains(t, recorder.Body.String(), "Too many pending requests")
	require.Equal(t, int32(1), atomic.LoadInt32(&queueFullCache.acquireCalls))
	require.Zero(t, atomic.LoadInt32(&fixture.accountRepo.listCalls))
	require.Zero(t, atomic.LoadInt32(&fixture.upstream.calls))
}

func TestSerperHandlerUserRPMRejectionDoesNotReachUpstream(t *testing.T) {
	cfg := &config.Config{RunMode: config.RunModeStandard}
	cfg.Billing.MinimumBalanceReserve = 0.01
	fixture := newSerperHandlerFixture(
		t,
		cfg,
		&serperHandlerBalanceCacheStub{balance: 10},
		&serperHandlerRPMCacheStub{userCount: 2},
	)
	fixture.apiKey.User.RPMLimit = 1

	recorder := performSerperSearch(t, fixture)

	require.Equal(t, http.StatusTooManyRequests, recorder.Code)
	require.NotEmpty(t, recorder.Header().Get("Retry-After"))
	require.Contains(t, recorder.Body.String(), "user requests-per-minute limit exceeded")
	require.Equal(t, int32(1), atomic.LoadInt32(&fixture.concurrencyCache.releaseUserCalled))
	require.Zero(t, atomic.LoadInt32(&fixture.accountRepo.listCalls))
	require.Zero(t, atomic.LoadInt32(&fixture.upstream.calls))
}

func TestSerperHandlerAPIKeyRollingLimitRejectionDoesNotReachUpstream(t *testing.T) {
	cfg := &config.Config{RunMode: config.RunModeStandard}
	cfg.Billing.MinimumBalanceReserve = 0.01
	fixture := newSerperHandlerFixture(t, cfg, &serperHandlerBalanceCacheStub{
		balance: 10,
		apiKeyRateLimit: &service.APIKeyRateLimitCacheData{
			Usage5h:  1,
			Window5h: time.Now().Unix(),
		},
	}, nil)
	fixture.apiKey.RateLimit5h = 1

	recorder := performSerperSearch(t, fixture)

	require.Equal(t, http.StatusTooManyRequests, recorder.Code)
	require.Contains(t, recorder.Body.String(), "api key 5小时限额已用完")
	require.Equal(t, int32(1), atomic.LoadInt32(&fixture.concurrencyCache.releaseUserCalled))
	require.Zero(t, atomic.LoadInt32(&fixture.accountRepo.listCalls))
	require.Zero(t, atomic.LoadInt32(&fixture.upstream.calls))
}

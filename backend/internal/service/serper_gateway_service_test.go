package service

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type serperConcurrencyCacheStub struct {
	ConcurrencyCache
	acquireResults map[int64]bool
	acquireCalls   []int64
	releaseCalls   map[int64]int
}

func (s *serperConcurrencyCacheStub) AcquireAccountSlot(_ context.Context, accountID int64, _ int, _ string) (bool, error) {
	s.acquireCalls = append(s.acquireCalls, accountID)
	if acquired, ok := s.acquireResults[accountID]; ok {
		return acquired, nil
	}
	return true, nil
}

func (s *serperConcurrencyCacheStub) ReleaseAccountSlot(_ context.Context, accountID int64, _ string) error {
	if s.releaseCalls == nil {
		s.releaseCalls = make(map[int64]int)
	}
	s.releaseCalls[accountID]++
	return nil
}

func (s *serperConcurrencyCacheStub) GetAccountWaitingCount(context.Context, int64) (int, error) {
	return 0, nil
}

type serperHTTPUpstreamStub struct {
	do          func(req *http.Request, accountID int64) (*http.Response, error)
	calls       int
	accountIDs  []int64
	methods     []string
	requestURLs []string
	bodies      []string
	headers     []http.Header
}

func (s *serperHTTPUpstreamStub) Do(req *http.Request, _ string, accountID int64, _ int) (*http.Response, error) {
	s.calls++
	s.accountIDs = append(s.accountIDs, accountID)
	s.methods = append(s.methods, req.Method)
	s.requestURLs = append(s.requestURLs, req.URL.String())
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	s.bodies = append(s.bodies, string(body))
	s.headers = append(s.headers, req.Header.Clone())
	return s.do(req, accountID)
}

func (s *serperHTTPUpstreamStub) DoWithTLS(*http.Request, string, int64, int, *tlsfingerprint.Profile) (*http.Response, error) {
	panic("unexpected DoWithTLS call")
}

type serperUnexpectedEOFBody struct{}

func (serperUnexpectedEOFBody) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
func (serperUnexpectedEOFBody) Close() error             { return nil }

type serperHydrationSchedulerCacheStub struct {
	SchedulerCache
}

func (s *serperHydrationSchedulerCacheStub) GetAccount(context.Context, int64) (*Account, error) {
	return nil, nil
}

type serperHydrationAccountRepoStub struct {
	AccountRepository
	err error
}

func (s *serperHydrationAccountRepoStub) GetByID(context.Context, int64) (*Account, error) {
	return nil, s.err
}

type serperAccountRepoStub struct {
	AccountRepository
	accounts     []Account
	accountsByID map[int64]*Account
}

func (s *serperAccountRepoStub) GetByID(_ context.Context, id int64) (*Account, error) {
	account, ok := s.accountsByID[id]
	if !ok {
		return nil, ErrNoAvailableAccounts
	}
	return account, nil
}

func (s *serperAccountRepoStub) ListSchedulableByGroupIDAndPlatform(_ context.Context, groupID int64, platform string) ([]Account, error) {
	accounts := make([]Account, 0, len(s.accounts))
	for _, account := range s.accounts {
		if account.Platform == platform && account.IsSchedulable() {
			accounts = append(accounts, account)
		}
	}
	return accounts, nil
}

func (s *serperAccountRepoStub) SetError(context.Context, int64, string) error { return nil }

func (s *serperAccountRepoStub) SetRateLimited(context.Context, int64, time.Time) error {
	return nil
}

type serperGroupRepoStub struct {
	GroupRepository
	group *Group
}

func (s *serperGroupRepoStub) GetByID(context.Context, int64) (*Group, error) {
	return s.group, nil
}

func (s *serperGroupRepoStub) GetByIDLite(context.Context, int64) (*Group, error) {
	return s.group, nil
}

func newSerperGatewayServiceForTest(
	t *testing.T,
	accounts []Account,
	concurrencyCache *serperConcurrencyCacheStub,
	upstream HTTPUpstream,
) (*SerperGatewayService, *config.Config, *serperAccountRepoStub, int64) {
	t.Helper()

	groupID := int64(91)
	repo := &serperAccountRepoStub{
		accounts:     accounts,
		accountsByID: make(map[int64]*Account, len(accounts)),
	}
	for i := range repo.accounts {
		repo.accounts[i].AccountGroups = []AccountGroup{{GroupID: groupID}}
		repo.accountsByID[repo.accounts[i].ID] = &repo.accounts[i]
	}

	cfg := &config.Config{RunMode: config.RunModeStandard}
	cfg.Gateway.Scheduling.LoadBatchEnabled = false
	cfg.Security.URLAllowlist.Enabled = true
	cfg.Security.URLAllowlist.UpstreamHosts = []string{"google.serper.dev"}
	cfg.Security.URLAllowlist.AllowPrivateHosts = false

	groupRepo := &serperGroupRepoStub{group: &Group{
		ID:       groupID,
		Platform: PlatformSerper,
		Status:   StatusActive,
		Hydrated: true,
	}}
	gateway := &GatewayService{
		accountRepo:        repo,
		groupRepo:          groupRepo,
		cfg:                cfg,
		concurrencyService: NewConcurrencyService(concurrencyCache),
	}
	rateLimitService := NewRateLimitService(repo, nil, cfg, nil, nil)
	return NewSerperGatewayService(gateway, upstream, rateLimitService), cfg, repo, groupID
}

func serperTestAccount(id int64, priority int, baseURL string) Account {
	credentials := map[string]any{"api_key": "secret-key"}
	if baseURL != "" {
		credentials["base_url"] = baseURL
	}
	return Account{
		ID:          id,
		Name:        "serper",
		Platform:    PlatformSerper,
		Type:        AccountTypeAPIKey,
		Credentials: credentials,
		Priority:    priority,
		Status:      StatusActive,
		Schedulable: true,
		Concurrency: 1,
	}
}

func serperResponse(status int, body io.ReadCloser) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       body,
	}
}

func TestSerperGatewayForwardSearchUsesFixedEndpointAndReleasesSuccessSlot(t *testing.T) {
	concurrencyCache := &serperConcurrencyCacheStub{}
	upstream := &serperHTTPUpstreamStub{do: func(*http.Request, int64) (*http.Response, error) {
		return serperResponse(http.StatusOK, io.NopCloser(strings.NewReader(`{"ok":true}`))), nil
	}}
	svc, _, _, groupID := newSerperGatewayServiceForTest(t, []Account{serperTestAccount(11, 1, "")}, concurrencyCache, upstream)

	result, err := svc.ForwardSearch(
		context.Background(),
		&groupID,
		501,
		"gl=us&hl=en",
		http.Header{"Content-Type": []string{"application/json"}, "Accept": []string{"application/json"}},
		[]byte(`{"q":"golang"}`),
	)

	require.NoError(t, err)
	require.Equal(t, http.StatusOK, result.StatusCode)
	require.Equal(t, []string{http.MethodPost}, upstream.methods)
	require.Equal(t, []string{"https://google.serper.dev/search?gl=us&hl=en"}, upstream.requestURLs)
	require.Equal(t, []string{`{"q":"golang"}`}, upstream.bodies)
	require.Equal(t, "secret-key", upstream.headers[0].Get("X-API-KEY"))
	require.Equal(t, 1, concurrencyCache.releaseCalls[11])
}

func TestSerperGatewayRejectsDisallowedBaseURLBeforeDo(t *testing.T) {
	concurrencyCache := &serperConcurrencyCacheStub{}
	upstream := &serperHTTPUpstreamStub{do: func(*http.Request, int64) (*http.Response, error) {
		return nil, errors.New("must not be called")
	}}
	svc, _, _, groupID := newSerperGatewayServiceForTest(t, []Account{
		serperTestAccount(12, 1, "http://169.254.169.254/latest/meta-data"),
	}, concurrencyCache, upstream)

	_, err := svc.ForwardSearch(context.Background(), &groupID, 502, "", nil, []byte(`{"q":"x"}`))

	require.Error(t, err)
	require.Zero(t, upstream.calls)
	require.Equal(t, 1, concurrencyCache.releaseCalls[12])
}

func TestSerperGatewayTransportErrorDoesNotReplay(t *testing.T) {
	concurrencyCache := &serperConcurrencyCacheStub{}
	upstream := &serperHTTPUpstreamStub{do: func(*http.Request, int64) (*http.Response, error) {
		return nil, io.ErrUnexpectedEOF
	}}
	svc, _, _, groupID := newSerperGatewayServiceForTest(t, []Account{
		serperTestAccount(13, 1, ""),
		serperTestAccount(14, 2, ""),
	}, concurrencyCache, upstream)

	_, err := svc.ForwardSearch(context.Background(), &groupID, 503, "", nil, []byte(`{"q":"x"}`))

	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.Equal(t, 1, upstream.calls)
	require.Equal(t, []int64{13}, upstream.accountIDs)
	require.Equal(t, 1, concurrencyCache.releaseCalls[13])
	require.Zero(t, concurrencyCache.releaseCalls[14])
}

func TestSerperGatewayResponseBodyErrorDoesNotReplay(t *testing.T) {
	concurrencyCache := &serperConcurrencyCacheStub{}
	upstream := &serperHTTPUpstreamStub{do: func(*http.Request, int64) (*http.Response, error) {
		return serperResponse(http.StatusOK, serperUnexpectedEOFBody{}), nil
	}}
	svc, _, _, groupID := newSerperGatewayServiceForTest(t, []Account{
		serperTestAccount(15, 1, ""),
		serperTestAccount(16, 2, ""),
	}, concurrencyCache, upstream)

	_, err := svc.ForwardSearch(context.Background(), &groupID, 504, "", nil, []byte(`{"q":"x"}`))

	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.Equal(t, 1, upstream.calls)
	require.Equal(t, []int64{15}, upstream.accountIDs)
	require.Equal(t, 1, concurrencyCache.releaseCalls[15])
	require.Zero(t, concurrencyCache.releaseCalls[16])
}

func TestSerperGateway403SwitchesAccountAndReleasesBothSlots(t *testing.T) {
	concurrencyCache := &serperConcurrencyCacheStub{}
	upstream := &serperHTTPUpstreamStub{do: func(_ *http.Request, accountID int64) (*http.Response, error) {
		if accountID == 17 {
			return serperResponse(http.StatusForbidden, io.NopCloser(strings.NewReader(`{"error":"forbidden"}`))), nil
		}
		return serperResponse(http.StatusOK, io.NopCloser(strings.NewReader(`{"ok":true}`))), nil
	}}
	svc, _, _, groupID := newSerperGatewayServiceForTest(t, []Account{
		serperTestAccount(17, 1, ""),
		serperTestAccount(18, 2, ""),
	}, concurrencyCache, upstream)

	result, err := svc.ForwardSearch(context.Background(), &groupID, 505, "", nil, []byte(`{"q":"x"}`))

	require.NoError(t, err)
	require.Equal(t, int64(18), result.AccountID)
	require.Equal(t, []int64{17, 18}, upstream.accountIDs)
	require.Equal(t, 1, concurrencyCache.releaseCalls[17])
	require.Equal(t, 1, concurrencyCache.releaseCalls[18])
}

func TestSerperGateway429SwitchesAccountAndReleasesBothSlots(t *testing.T) {
	concurrencyCache := &serperConcurrencyCacheStub{}
	upstream := &serperHTTPUpstreamStub{do: func(_ *http.Request, accountID int64) (*http.Response, error) {
		if accountID == 21 {
			return serperResponse(http.StatusTooManyRequests, io.NopCloser(strings.NewReader(`{"error":"rate limited"}`))), nil
		}
		return serperResponse(http.StatusOK, io.NopCloser(strings.NewReader(`{"ok":true}`))), nil
	}}
	svc, _, _, groupID := newSerperGatewayServiceForTest(t, []Account{
		serperTestAccount(21, 1, ""),
		serperTestAccount(22, 2, ""),
	}, concurrencyCache, upstream)

	result, err := svc.ForwardSearch(context.Background(), &groupID, 507, "", nil, []byte(`{"q":"x"}`))

	require.NoError(t, err)
	require.Equal(t, int64(22), result.AccountID)
	require.Equal(t, []int64{21, 22}, upstream.accountIDs)
	require.Equal(t, 1, concurrencyCache.releaseCalls[21])
	require.Equal(t, 1, concurrencyCache.releaseCalls[22])
}

func TestSerperGatewayMissingCredentialSwitchesBeforeDoAndReleasesSlot(t *testing.T) {
	concurrencyCache := &serperConcurrencyCacheStub{}
	upstream := &serperHTTPUpstreamStub{do: func(*http.Request, int64) (*http.Response, error) {
		return serperResponse(http.StatusOK, io.NopCloser(strings.NewReader(`{"ok":true}`))), nil
	}}
	missingCredential := serperTestAccount(23, 1, "")
	delete(missingCredential.Credentials, "api_key")
	svc, _, _, groupID := newSerperGatewayServiceForTest(t, []Account{
		missingCredential,
		serperTestAccount(24, 2, ""),
	}, concurrencyCache, upstream)

	result, err := svc.ForwardSearch(context.Background(), &groupID, 508, "", nil, []byte(`{"q":"x"}`))

	require.NoError(t, err)
	require.Equal(t, int64(24), result.AccountID)
	require.Equal(t, 1, upstream.calls)
	require.Equal(t, []int64{24}, upstream.accountIDs)
	require.Equal(t, 1, concurrencyCache.releaseCalls[23])
	require.Equal(t, 1, concurrencyCache.releaseCalls[24])
}

func TestSerperGatewayCapacityWaitPlanDoesNotSendUpstream(t *testing.T) {
	concurrencyCache := &serperConcurrencyCacheStub{acquireResults: map[int64]bool{19: false}}
	upstream := &serperHTTPUpstreamStub{do: func(*http.Request, int64) (*http.Response, error) {
		return nil, errors.New("must not be called")
	}}
	svc, _, _, groupID := newSerperGatewayServiceForTest(t, []Account{serperTestAccount(19, 1, "")}, concurrencyCache, upstream)

	_, err := svc.ForwardSearch(context.Background(), &groupID, 506, "", nil, []byte(`{"q":"x"}`))

	require.Error(t, err)
	require.Zero(t, upstream.calls)
	require.Zero(t, concurrencyCache.releaseCalls[19])
}

func TestSerperAccountConnectionRejectsDisallowedBaseURLBeforeDo(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := &serperHTTPUpstreamStub{do: func(*http.Request, int64) (*http.Response, error) {
		return nil, errors.New("must not be called")
	}}
	cfg := &config.Config{}
	cfg.Security.URLAllowlist.Enabled = true
	cfg.Security.URLAllowlist.UpstreamHosts = []string{"google.serper.dev"}
	cfg.Security.URLAllowlist.AllowPrivateHosts = false
	svc := NewAccountTestService(nil, nil, nil, nil, nil, upstream, cfg, nil)
	account := serperTestAccount(20, 1, "http://169.254.169.254/latest/meta-data")
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/admin/accounts/20/test", nil)

	err := svc.testSerperAccountConnection(c, &account)

	require.Error(t, err)
	require.Zero(t, upstream.calls)
}

func TestSerperAccountConnectionAppendsSearchToPathAndClearsQueryFragment(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := &serperHTTPUpstreamStub{do: func(*http.Request, int64) (*http.Response, error) {
		return serperResponse(http.StatusOK, io.NopCloser(strings.NewReader(`{"ok":true}`))), nil
	}}
	cfg := &config.Config{}
	cfg.Security.URLAllowlist.Enabled = true
	cfg.Security.URLAllowlist.UpstreamHosts = []string{"google.serper.dev"}
	svc := NewAccountTestService(nil, nil, nil, nil, nil, upstream, cfg, nil)
	account := serperTestAccount(25, 1, "https://google.serper.dev/base?region=us#frag")
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/admin/accounts/25/test", nil)

	err := svc.testSerperAccountConnection(c, &account)

	require.NoError(t, err)
	require.Equal(t, []string{"https://google.serper.dev/base/search"}, upstream.requestURLs)
}

func TestGatewayNewSelectionResultReleasesOnlyAcquiredSlotWhenHydrationFails(t *testing.T) {
	hydrationErr := errors.New("hydrate failed")
	svc := &GatewayService{schedulerSnapshot: NewSchedulerSnapshotService(
		&serperHydrationSchedulerCacheStub{},
		nil,
		&serperHydrationAccountRepoStub{err: hydrationErr},
		nil,
		nil,
	)}

	for _, tc := range []struct {
		name             string
		acquired         bool
		hasRelease       bool
		wantReleaseCalls int
	}{
		{name: "acquired", acquired: true, hasRelease: true, wantReleaseCalls: 1},
		{name: "not acquired", acquired: false, hasRelease: true, wantReleaseCalls: 0},
		{name: "nil release", acquired: true, hasRelease: false, wantReleaseCalls: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			releaseCalls := 0
			var release func()
			if tc.hasRelease {
				release = func() { releaseCalls++ }
			}

			selection, err := svc.newSelectionResult(
				context.Background(),
				&Account{ID: 1001},
				tc.acquired,
				release,
				nil,
			)

			require.ErrorIs(t, err, hydrationErr)
			require.Nil(t, selection)
			require.Equal(t, tc.wantReleaseCalls, releaseCalls)
		})
	}
}

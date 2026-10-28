package webui

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/reconcile"
)

func newTestServer(t *testing.T) (*Server, Config) {
	t.Helper()
	c := Config{Brain: t.TempDir(), Home: t.TempDir(), Executable: "/Users/test/factory", Version: "factory test test"}
	s, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	s.BootstrapURL("127.0.0.1:43210")
	t.Cleanup(s.Close)
	return s, c
}
func request(s *Server, method, route, body string, cookie *http.Cookie, csrf string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, "http://127.0.0.1:43210"+route, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	if method == http.MethodPost {
		r.Header.Set("Origin", "http://127.0.0.1:43210")
	}
	if cookie != nil {
		r.AddCookie(cookie)
	}
	if csrf != "" {
		r.Header.Set("X-CSRF-Token", csrf)
	}
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	return w
}
func login(t *testing.T, s *Server) (*http.Cookie, string) {
	t.Helper()
	w := request(s, "POST", "/session", `{"token":"`+s.bootstrap+`"}`, nil, "")
	if w.Code != 200 {
		t.Fatalf("session %d: %s", w.Code, w.Body)
	}
	var result struct {
		CSRF string `json:"csrf"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	cookies := w.Result().Cookies()
	if len(cookies) != 1 || !cookies[0].HttpOnly || cookies[0].SameSite != http.SameSiteStrictMode {
		t.Fatalf("cookie %+v", cookies)
	}
	return cookies[0], result.CSRF
}
func TestSessionAuthenticationOriginHostAndCSRF(t *testing.T) {
	s, _ := newTestServer(t)
	token := s.bootstrap
	if w := request(s, "GET", base+"/overview", "", nil, ""); w.Code != 401 {
		t.Fatalf("unauthenticated %d", w.Code)
	}
	cookie, csrf := login(t, s)
	if w := request(s, "POST", "/session", `{"token":"`+token+`"}`, nil, ""); w.Code != 401 {
		t.Fatalf("bootstrap reused: %d", w.Code)
	}
	if w := request(s, "GET", "/session", "", cookie, ""); w.Code != 200 || !strings.Contains(w.Body.String(), csrf) {
		t.Fatalf("reload session %d %s", w.Code, w.Body)
	}
	for _, tc := range []struct {
		name, host, origin, csrf string
		cookie                   *http.Cookie
	}{
		{"host", "attacker.example", "http://127.0.0.1:43210", csrf, cookie},
		{"origin", "127.0.0.1:43210", "https://evil.example", csrf, cookie},
		{"missing origin", "127.0.0.1:43210", "", csrf, cookie},
		{"csrf", "127.0.0.1:43210", "http://127.0.0.1:43210", "", cookie},
		{"cookie", "127.0.0.1:43210", "http://127.0.0.1:43210", csrf, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("POST", "http://127.0.0.1:43210"+base+"/actions", strings.NewReader(`{}`))
			r.Host = tc.host
			r.Header.Set("Origin", tc.origin)
			r.Header.Set("X-CSRF-Token", tc.csrf)
			r.Header.Set("Content-Type", "application/json")
			if tc.cookie != nil {
				r.AddCookie(tc.cookie)
			}
			w := httptest.NewRecorder()
			s.ServeHTTP(w, r)
			if w.Code != 403 && w.Code != 401 {
				t.Fatalf("accepted attack %d %s", w.Code, w.Body)
			}
		})
	}
	w := request(s, "GET", base+"/overview", "", cookie, "")
	if w.Code != 200 {
		t.Fatalf("authorized %d: %s", w.Code, w.Body)
	}
	for _, name := range []string{"Content-Security-Policy", "X-Content-Type-Options", "Cache-Control", "Referrer-Policy"} {
		if w.Header().Get(name) == "" {
			t.Errorf("missing %s", name)
		}
	}
	if w.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Error("CORS unexpectedly enabled")
	}
}
func TestReadOnlyAndStrictBounds(t *testing.T) {
	s, _ := newTestServer(t)
	s.cfg.ReadOnly = true
	cookie, csrf := login(t, s)
	if w := request(s, "POST", base+"/uploads", `{"name":"p.yaml","text":"plan"}`, cookie, csrf); w.Code != 403 {
		t.Fatalf("read only upload %d", w.Code)
	}
	s.cfg.ReadOnly = false
	for _, route := range []string{base + "/jobs?limit=1000", base + "/jobs?offset=-1", base + "/jobs?q=" + strings.Repeat("a", 257), base + "/entities/e1/events?after_seq=-1", base + "/entities/e1/events?limit=1000", base + "/entities/../inspection", base + "/logs/../../secrets", "/api/v1/instances/other/overview"} {
		w := request(s, "GET", route, "", cookie, "")
		if w.Code == 200 {
			t.Errorf("invalid route accepted %s", route)
		}
	}
	for _, body := range []string{`{"name":"p","text":"x","path":"/tmp/secret"}`, `{"name":"p","text":"` + strings.Repeat("x", bodyLimit) + `"}`, "{\"name\":\"p\",\"text\":\"\xff\"}"} {
		if w := request(s, "POST", base+"/uploads", body, cookie, csrf); w.Code != 400 {
			t.Fatalf("invalid body %d", w.Code)
		}
	}
}
func TestReceiptsSerializeDeduplicateAndSurviveLostResponse(t *testing.T) {
	s, c := newTestServer(t)
	var calls atomic.Int32
	var wg sync.WaitGroup
	body := map[string]string{"type": "synthetic", "request_id": "request-1234"}
	fn := func(context.Context) (reconcile.UIActionResult, error) {
		calls.Add(1)
		return reconcile.UIActionResult{EntityID: "e1", State: "applied", Message: "created"}, nil
	}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := httptest.NewRecorder()
			s.perform(w, "request-1234", body, fn)
			if w.Code != 200 {
				t.Errorf("duplicate %d", w.Code)
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("executions %d", calls.Load())
	}
	w := httptest.NewRecorder()
	s.perform(w, "request-1234", map[string]string{"different": "body"}, fn)
	if w.Code != 409 {
		t.Fatalf("body reuse %d", w.Code)
	}
	s.Close()
	restarted, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	w = httptest.NewRecorder()
	restarted.perform(w, "request-1234", body, fn)
	if w.Code != 200 || calls.Load() != 1 {
		t.Fatalf("lost response replayed %d executions %d", w.Code, calls.Load())
	}
	st, err := os.Stat(filepath.Join(c.Brain, "factory/ui/receipt-request-1234.json"))
	if err != nil || st.Mode().Perm() != 0600 {
		t.Fatalf("receipt mode %v %v", st, err)
	}
}
func TestIncompleteIntentNeverReplaysAfterRestart(t *testing.T) {
	s, c := newTestServer(t)
	body := map[string]string{"type": "test"}
	if err := s.saveReceipt(Receipt{RequestID: "request-pending", State: "pending", Hash: canonicalHash(body), UpdatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	next, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close()
	w := httptest.NewRecorder()
	next.perform(w, "request-pending", body, func(context.Context) (reconcile.UIActionResult, error) {
		t.Fatal("replayed ambiguous intent")
		return reconcile.UIActionResult{}, nil
	})
	if !strings.Contains(w.Body.String(), "outcome_unknown") {
		t.Fatalf("receipt %s", w.Body)
	}
}
func TestOneUIOwnsSelectedBrainAndNoRuntimeNeeded(t *testing.T) {
	s, c := newTestServer(t)
	if second, err := New(c); err == nil {
		second.Close()
		t.Fatal("second UI acquired same brain")
	}
	cookie, _ := login(t, s)
	w := request(s, "GET", base+"/health", "", cookie, "")
	if w.Code != 200 {
		t.Fatalf("health %d %s", w.Code, w.Body)
	}
	if !strings.Contains(w.Body.String(), c.Brain) || !strings.Contains(w.Body.String(), c.Executable) {
		t.Fatalf("selected binding hidden: %s", w.Body)
	}
	if strings.Contains(w.Body.String(), `"answer_supported":true`) {
		t.Fatalf("old manager enabled answers: %s", w.Body)
	}
}
func TestLogsAndAllPrivateSerializationsAreRedacted(t *testing.T) {
	s, c := newTestServer(t)
	logs := filepath.Join(c.Home, ".factory/logs")
	if err := os.MkdirAll(logs, 0700); err != nil {
		t.Fatal(err)
	}
	secret := "ghp_abcdefghijklmnopqrstuvwxyz0123456789"
	if err := os.WriteFile(filepath.Join(logs, "tick.log"), []byte("Authorization: Bearer "+secret+"\n/home/private/brain/secret\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cookie, _ := login(t, s)
	w := request(s, "GET", base+"/logs/tick", "", cookie, "")
	if w.Code != 200 || strings.Contains(w.Body.String(), secret) || strings.Contains(w.Body.String(), "/home/private") {
		t.Fatalf("unsafe log %d %s", w.Code, w.Body)
	}
	for _, kind := range []string{"default", "debug", "error", "export", "receipt"} {
		w := httptest.NewRecorder()
		s.json(w, 200, map[string]any{kind: map[string]string{"message": "GH_TOKEN=" + secret}})
		if strings.Contains(w.Body.String(), secret) {
			t.Errorf("%s leaked", kind)
		}
	}
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, []byte("outside marker"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(logs, "repo-worker.log")); err != nil {
		t.Fatal(err)
	}
	if w := request(s, "GET", base+"/logs/repo-worker", "", cookie, ""); w.Code == 200 {
		t.Fatal("log symlink escaped source root")
	}
}
func TestUploadIsDurableOpaqueAndRejectsEscapedPrivateRoot(t *testing.T) {
	s, c := newTestServer(t)
	cookie, csrf := login(t, s)
	w := request(s, "POST", base+"/uploads", `{"name":"../../input.yaml","text":"items: []\n"}`, cookie, csrf)
	if w.Code != 201 {
		t.Fatalf("upload %d %s", w.Code, w.Body)
	}
	var upload uploadRecord
	if err := json.Unmarshal(w.Body.Bytes(), &upload); err != nil {
		t.Fatal(err)
	}
	if !opaqueID.MatchString(upload.UploadID) || upload.SHA256 != canonicalTextHash("items: []\n") {
		t.Fatalf("upload %+v", upload)
	}
	var disk uploadRecord
	if err := s.readPrivate("upload-"+upload.UploadID+".json", &disk, bodyLimit); err != nil || disk.Text != "items: []\n" {
		t.Fatalf("upload not persisted %+v %v", disk, err)
	}
	s.Close()
	evil := t.TempDir()
	brain := t.TempDir()
	if err := os.Mkdir(filepath.Join(brain, "factory"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(evil, filepath.Join(brain, "factory/ui")); err != nil {
		t.Fatal(err)
	}
	c.Brain = brain
	if escaped, err := New(c); err == nil {
		escaped.Close()
		t.Fatal("private UI directory escaped selected brain")
	}
}
func TestServeRejectsNonLoopbackListener(t *testing.T) {
	s, _ := newTestServer(t)
	if err := s.Serve(context.Background(), fakeListener{}); err == nil {
		t.Fatal("non-loopback listener accepted")
	}
}

type fakeListener struct{}

func (fakeListener) Accept() (net.Conn, error) { return nil, io.EOF }
func (fakeListener) Close() error              { return nil }
func (fakeListener) Addr() net.Addr            { return &net.TCPAddr{IP: net.ParseIP("0.0.0.0"), Port: 43210} }

func TestActionTimeoutAndPanicRemainUnknownWithoutReplay(t *testing.T) {
	for _, mode := range []string{"timeout", "panic"} {
		t.Run(mode, func(t *testing.T) {
			s, _ := newTestServer(t)
			calls := 0
			body := map[string]string{"action": mode}
			fn := func(context.Context) (reconcile.UIActionResult, error) {
				calls++
				if mode == "panic" {
					panic("GH_TOKEN=not-for-the-browser")
				}
				return reconcile.UIActionResult{EntityID: "e-created"}, context.DeadlineExceeded
			}
			w := httptest.NewRecorder()
			s.perform(w, "ambiguous-"+mode, body, fn)
			if w.Code != 200 || !strings.Contains(w.Body.String(), "outcome_unknown") || strings.Contains(w.Body.String(), "not-for-the-browser") {
				t.Fatalf("ambiguous %d %s", w.Code, w.Body)
			}
			s.perform(httptest.NewRecorder(), "ambiguous-"+mode, body, fn)
			if calls != 1 {
				t.Fatalf("replayed %d times", calls)
			}
		})
	}
}
func TestLogDirectorySymlinkCannotEscapeHome(t *testing.T) {
	s, _ := newTestServer(t)
	home := t.TempDir()
	outside := t.TempDir()
	if err := os.Mkdir(filepath.Join(home, ".factory"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outside, "tick.log"), []byte("outside marker"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(home, ".factory/logs")); err != nil {
		t.Fatal(err)
	}
	s.cfg.Home = home
	cookie, _ := login(t, s)
	if w := request(s, "GET", base+"/logs/tick", "", cookie, ""); w.Code == 200 {
		t.Fatalf("log directory escaped: %s", w.Body)
	}
}
func TestCanonicalReceiptBodyIgnoresJSONKeyOrder(t *testing.T) {
	s, _ := newTestServer(t)
	cookie, csrf := login(t, s)
	first := `{"request_id":"same-body-123","type":"message","target_id":"e1","expected":"old","value":"hello"}`
	second := `{"value":"hello","expected":"old","target_id":"e1","type":"message","request_id":"same-body-123"}`
	a := request(s, "POST", base+"/actions", first, cookie, csrf)
	b := request(s, "POST", base+"/actions", second, cookie, csrf)
	if a.Code != 200 || b.Code != 200 || a.Body.String() != b.Body.String() {
		t.Fatalf("canonical reuse %d %d: %s / %s", a.Code, b.Code, a.Body, b.Body)
	}
}

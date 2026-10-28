// Package webui serves the selected local factory without owning its manager.
package webui

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/kingpinXD/factory/internal/reconcile"
	"github.com/kingpinXD/factory/internal/store"
)

//go:embed assets/*
var assets embed.FS

const base = "/api/v1/instances/local"
const bodyLimit = 1 << 20

var opaqueID = regexp.MustCompile(`^[A-Za-z0-9_-]{8,100}$`)

type Config struct {
	Brain, Home, Executable, Version string
	ReadOnly                         bool
	Deps                             func(io.Writer) (reconcile.Deps, error)
	Now                              func() time.Time
}
type Server struct {
	cfg                            Config
	root                           *os.Root
	unlock                         func() error
	mu                             sync.Mutex
	bootstrap, session, csrf, host string
	cacheAt                        time.Time
	cache                          reconcile.UIViewReport
	cacheErr                       error
	writeMu                        sync.Mutex
	receipts                       map[string]Receipt
	http                           *http.Server
	closeOnce                      sync.Once
}
type Receipt struct {
	RequestID string                 `json:"request_id"`
	State     string                 `json:"state"`
	EntityID  string                 `json:"entity_id"`
	Events    []reconcile.UIEventRef `json:"events"`
	Message   string                 `json:"message"`
	UpdatedAt time.Time              `json:"updated_at"`
	Hash      string                 `json:"-"`
}
type receiptDisk struct {
	Receipt
	BodyHash string `json:"body_hash"`
}

func randomToken() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}
func New(c Config) (*Server, error) {
	var err error
	c.Brain, err = filepath.Abs(c.Brain)
	if err != nil {
		return nil, err
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	if c.Home == "" {
		c.Home, _ = os.UserHomeDir()
	}
	brain, err := os.OpenRoot(c.Brain)
	if err != nil {
		return nil, errors.New("cannot open selected brain")
	}
	defer brain.Close()
	if err := brain.MkdirAll("factory/ui", 0700); err != nil {
		return nil, err
	}
	root, err := brain.OpenRoot("factory/ui")
	if err != nil {
		return nil, err
	}
	if err := root.Chmod(".", 0700); err != nil {
		root.Close()
		return nil, err
	}
	unlock, err := store.Lock(c.Brain, "ui")
	if err != nil {
		root.Close()
		return nil, fmt.Errorf("another UI owns this brain: %w", err)
	}
	s := &Server{cfg: c, root: root, unlock: unlock, bootstrap: randomToken(), session: randomToken(), csrf: randomToken(), receipts: map[string]Receipt{}}
	if err := s.recoverReceipts(); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}
func (s *Server) BootstrapURL(host string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.host = host
	return "http://" + host + "/#" + s.bootstrap
}
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	tcp, ok := ln.Addr().(*net.TCPAddr)
	if !ok || tcp.IP == nil || !tcp.IP.IsLoopback() {
		ln.Close()
		return errors.New("UI listener must bind a numeric loopback address")
	}
	s.mu.Lock()
	s.host = ln.Addr().String()
	s.mu.Unlock()
	s.http = &http.Server{Handler: s, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 3 * time.Minute, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10}
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = s.http.Shutdown(shutdown)
		case <-done:
		}
	}()
	err := s.http.Serve(ln)
	close(done)
	s.Close()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
func (s *Server) Close() {
	s.closeOnce.Do(func() { s.writeMu.Lock(); defer s.writeMu.Unlock(); s.root.Close(); _ = s.unlock() })
}
func same(a, b string) bool { return a != "" && subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1 }
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self'; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Cross-Origin-Resource-Policy", "same-origin")
	s.mu.Lock()
	host := s.host
	s.mu.Unlock()
	if host == "" || r.Host != host {
		s.error(w, 403, "unrecognized local host")
		return
	}
	origin := "http://" + host
	if r.Header.Get("Origin") != "" && r.Header.Get("Origin") != origin {
		s.error(w, 403, "cross-origin request refused")
		return
	}
	if r.Header.Get("Sec-Fetch-Site") == "cross-site" {
		s.error(w, 403, "cross-site request refused")
		return
	}
	if r.URL.Path == "/session" {
		s.sessionRoute(w, r, origin)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/api/") {
		cookie, err := r.Cookie("factory_ui")
		if err != nil || !same(cookie.Value, s.session) {
			s.error(w, 401, "local session required")
			return
		}
		if r.Method == http.MethodPost {
			if r.Header.Get("Origin") != origin || !same(r.Header.Get("X-CSRF-Token"), s.csrf) {
				s.error(w, 403, "CSRF check failed")
				return
			}
			if s.cfg.ReadOnly {
				s.error(w, 403, "this UI is read-only")
				return
			}
		} else if r.Method != http.MethodGet {
			s.error(w, 405, "method not allowed")
			return
		}
		s.api(w, r)
		return
	}
	if r.Method != http.MethodGet {
		s.error(w, 405, "method not allowed")
		return
	}
	asset := strings.TrimPrefix(r.URL.Path, "/")
	if asset == "" {
		asset = "index.html"
	}
	if strings.Contains(asset, "/") || strings.Contains(asset, "..") {
		s.error(w, 404, "not found")
		return
	}
	b, err := assets.ReadFile("assets/" + asset)
	if err != nil {
		s.error(w, 404, "not found")
		return
	}
	typ := mime.TypeByExtension(filepath.Ext(asset))
	if filepath.Ext(asset) == ".mjs" {
		typ = "text/javascript; charset=utf-8"
	}
	w.Header().Set("Content-Type", typ)
	_, _ = w.Write(b)
}
func (s *Server) sessionRoute(w http.ResponseWriter, r *http.Request, origin string) {
	switch r.Method {
	case http.MethodPost:
		if r.Header.Get("Origin") != origin {
			s.error(w, 403, "origin required")
			return
		}
		var in struct {
			Token string `json:"token"`
		}
		if !s.decode(w, r, &in) {
			return
		}
		s.mu.Lock()
		valid := same(in.Token, s.bootstrap)
		if valid {
			s.bootstrap = ""
		}
		s.mu.Unlock()
		if !valid {
			s.error(w, 401, "bootstrap token invalid or already used")
			return
		}
		http.SetCookie(w, &http.Cookie{Name: "factory_ui", Value: s.session, Path: "/", HttpOnly: true, SameSite: http.SameSiteStrictMode})
	case http.MethodGet:
		c, err := r.Cookie("factory_ui")
		if err != nil || !same(c.Value, s.session) {
			s.error(w, 401, "local session required")
			return
		}
	default:
		s.error(w, 405, "method not allowed")
		return
	}
	s.json(w, 200, map[string]string{"csrf": s.csrf})
}
func (s *Server) decode(w http.ResponseWriter, r *http.Request, v any) bool {
	mediaType, _, mediaErr := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if mediaErr != nil || mediaType != "application/json" {
		s.error(w, 415, "application/json is required")
		return false
	}
	raw, readErr := io.ReadAll(http.MaxBytesReader(w, r.Body, bodyLimit))
	if readErr != nil || !utf8.Valid(raw) {
		s.error(w, 400, "invalid or oversized UTF-8 body")
		return false
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		s.error(w, 400, "invalid or oversized JSON body")
		return false
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		s.error(w, 400, "one JSON value is required")
		return false
	}
	return true
}
func redactValue(v any) any {
	switch x := v.(type) {
	case string:
		return reconcile.UIRedact(x)
	case []any:
		for i, v := range x {
			x[i] = redactValue(v)
		}
	case map[string]any:
		for k, v := range x {
			x[k] = redactValue(v)
		}
	}
	return v
}
func (s *Server) json(w http.ResponseWriter, code int, v any) { s.writeJSON(w, code, v, false) }
func (s *Server) writeJSON(w http.ResponseWriter, code int, v any, binding bool) {
	b, err := json.Marshal(v)
	if err != nil {
		http.Error(w, "response unavailable", 500)
		return
	}
	var safe any
	if err = json.Unmarshal(b, &safe); err != nil {
		http.Error(w, "response unavailable", 500)
		return
	}
	safe = redactValue(safe)
	if binding {
		if body, ok := safe.(map[string]any); ok {
			body["brain"] = s.cfg.Brain
			body["executable"] = s.cfg.Executable
		}
	}
	b, err = json.Marshal(safe)
	if err != nil {
		http.Error(w, "response unavailable", 500)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_, _ = w.Write(append(b, '\n'))
}
func (s *Server) error(w http.ResponseWriter, code int, msg string) {
	s.json(w, code, map[string]string{"error": msg})
}
func (s *Server) options() reconcile.UIOptions {
	return reconcile.UIOptions{Brain: s.cfg.Brain, Home: s.cfg.Home, Now: s.cfg.Now}
}
func (s *Server) overview(ctx context.Context) (reconcile.UIViewReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cacheAt.IsZero() || s.cfg.Now().Sub(s.cacheAt) > 2*time.Second {
		v, err := reconcile.UIView(ctx, s.options())
		s.cacheAt = s.cfg.Now()
		s.cacheErr = err
		if err == nil {
			s.cache = v
		}
	}
	return s.cache, s.cacheErr
}
func boundedQuery(r *http.Request, name string, def, min, max int) (int, error) {
	text := r.URL.Query().Get(name)
	if text == "" {
		return def, nil
	}
	n, err := strconv.Atoi(text)
	if err != nil || n < min || n > max {
		return 0, fmt.Errorf("invalid %s", name)
	}
	return n, nil
}
func (s *Server) api(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, base+"/") {
		s.error(w, 404, "unknown instance or endpoint")
		return
	}
	suffix := strings.TrimPrefix(r.URL.Path, base+"/")
	if r.Method == http.MethodPost {
		switch suffix {
		case "jobs":
			s.submit(w, r)
		case "actions":
			s.action(w, r)
		case "uploads":
			s.upload(w, r)
		default:
			s.error(w, 404, "unknown endpoint")
		}
		return
	}
	var out any
	var err error
	switch {
	case suffix == "overview":
		out, err = s.overview(r.Context())
	case suffix == "jobs":
		var offset, limit int
		offset, err = boundedQuery(r, "offset", 0, 0, 1000000)
		if err != nil {
			break
		}
		limit, err = boundedQuery(r, "limit", 50, 1, 100)
		if err != nil {
			break
		}
		var view reconcile.UIViewReport
		view, err = s.overview(r.Context())
		if err != nil {
			break
		}
		jobs := []reconcile.UIJobView{}
		q := strings.ToLower(r.URL.Query().Get("q"))
		state := r.URL.Query().Get("state")
		if len(q) > 256 || state != "" && state != "active" && state != "completed" {
			err = errors.New("invalid job filter")
			break
		}
		for _, job := range view.Jobs {
			if state == "active" && job.End || state == "completed" && !job.End || q != "" && !strings.Contains(strings.ToLower(job.ID+" "+job.Title), q) {
				continue
			}
			jobs = append(jobs, job)
		}
		total := len(jobs)
		offset = min(offset, total)
		end := min(offset+limit, total)
		var next any
		if end < total {
			next = end
		}
		out = map[string]any{"jobs": jobs[offset:end], "next_offset": next, "total": total}
	case strings.HasPrefix(suffix, "jobs/"):
		id := strings.TrimPrefix(suffix, "jobs/")
		out, err = reconcile.UIJob(r.Context(), s.options(), id)
	case suffix == "health":
		s.writeJSON(w, 200, s.health(r.Context()), true)
		return
	case strings.HasPrefix(suffix, "actions/"):
		s.writeMu.Lock()
		out, err = s.getReceipt(r.Context(), strings.TrimPrefix(suffix, "actions/"))
		s.writeMu.Unlock()
	case strings.HasPrefix(suffix, "logs/"):
		out, err = s.logs(strings.TrimPrefix(suffix, "logs/"))
	case strings.HasPrefix(suffix, "entities/"):
		parts := strings.Split(suffix, "/")
		if len(parts) < 3 || store.CheckID(parts[1]) != nil {
			err = errors.New("invalid entity")
			break
		}
		id := parts[1]
		switch {
		case len(parts) == 3 && parts[2] == "events":
			var after, limit int
			after, err = boundedQuery(r, "after_seq", 0, 0, 1000000000)
			if err != nil {
				break
			}
			limit, err = boundedQuery(r, "limit", 100, 1, 200)
			if err == nil {
				out, err = reconcile.UIActivity(r.Context(), s.options(), id, after, limit)
			}
		case len(parts) == 3 && parts[2] == "inspection":
			out, err = reconcile.UIInspect(r.Context(), s.options(), id)
		case len(parts) == 3 && parts[2] == "cancel-preview":
			out, err = reconcile.UICancelPreview(r.Context(), s.options(), id)
		case len(parts) == 4 && parts[2] == "artifacts":
			out, err = reconcile.UIArtifact(r.Context(), s.options(), id, parts[3])
		default:
			err = errors.New("unknown entity endpoint")
		}
	default:
		s.error(w, 404, "unknown endpoint")
		return
	}
	if err != nil {
		s.error(w, 400, err.Error())
		return
	}
	s.json(w, 200, out)
}
func (s *Server) health(ctx context.Context) any {
	type prerequisite struct {
		Name      string `json:"name"`
		Available bool   `json:"available"`
		Detail    string `json:"detail"`
	}
	checks := []prerequisite{}
	for _, name := range []string{"claude", "gh"} {
		_, err := exec.LookPath(name)
		detail := "executable found; authentication has not been checked"
		if err != nil {
			detail = "executable missing; read-only browsing is available"
		}
		checks = append(checks, prerequisite{name, err == nil, detail})
	}
	root, err := os.OpenRoot(s.cfg.Brain)
	if err == nil {
		defer root.Close()
	}
	for _, name := range []string{"factory/blueprint.yaml", "repos"} {
		ok := false
		if err == nil {
			f, e := root.Open(name)
			if e == nil {
				ok = true
				f.Close()
			}
		}
		checks = append(checks, prerequisite{name, ok, "passive selected-brain file observation"})
	}
	mutationErr := reconcile.UIMutationsSupported(ctx, s.options())
	reason := ""
	if mutationErr != nil {
		reason = mutationErr.Error()
	}
	return map[string]any{"instance_id": "local", "brain": s.cfg.Brain, "executable": s.cfg.Executable, "version": s.cfg.Version, "read_only": s.cfg.ReadOnly || mutationErr != nil, "mutation_reason": reason, "answer_supported": !s.cfg.ReadOnly && mutationErr == nil && reconcile.UIAnswerSupported(ctx, s.options()), "prerequisites": checks}
}
func (s *Server) logs(source string) (any, error) {
	if source != "tick" && source != "repo-worker" {
		return nil, errors.New("unknown log source")
	}
	homeRoot, err := os.OpenRoot(s.cfg.Home)
	if err != nil {
		return nil, errors.New("log source is unavailable")
	}
	defer homeRoot.Close()
	root, err := homeRoot.OpenRoot(filepath.Join(".factory", "logs"))
	if err != nil {
		return nil, errors.New("log source is unavailable")
	}
	defer root.Close()
	f, err := root.Open(source + ".log")
	if err != nil {
		return nil, errors.New("log source is unavailable")
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() {
		return nil, errors.New("log source is not a regular file")
	}
	const limit = 128 << 10
	truncated := st.Size() > limit
	if truncated {
		if _, err = f.Seek(-limit, io.SeekEnd); err != nil {
			return nil, err
		}
	}
	b, err := io.ReadAll(io.LimitReader(f, limit))
	if err != nil {
		return nil, err
	}
	if !utf8.Valid(b) {
		b = []byte(strings.ToValidUTF8(string(b), "�"))
	}
	return map[string]any{"source": source, "text": string(b), "truncated": truncated, "source_at": st.ModTime().UTC()}, nil
}

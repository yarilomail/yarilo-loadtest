package jmapdrv_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yarilomail/yarilo-loadtest/internal/jmapdrv"
	"github.com/yarilomail/yarilo-loadtest/internal/stats"
)

// fakeJMAP answers enough of RFC 8620 to drive the client, and records what it
// was asked so a test can assert on the requests rather than on the driver's
// own account of them.
type fakeJMAP struct {
	mu sync.Mutex
	// sessionHits counts GETs of the well-known resource.
	sessionHits int
	// envelopes holds every decoded request envelope, in order.
	envelopes []envelope
	auth      []string
	conns     map[string]bool

	// apiPath is where the session says the API lives. Deliberately not the
	// conventional one, so a driver that assembles the URL itself is caught.
	apiPath string
	// methodError makes every method answer with an "error" response under an
	// HTTP 200, which is legal JMAP and the shape a naive client reads as
	// success.
	methodError bool
}

type envelope struct {
	Using       []string            `json:"using"`
	MethodCalls [][]json.RawMessage `json:"methodCalls"`
}

func newFake() *fakeJMAP {
	return &fakeJMAP{apiPath: "/jmap/api/", conns: map[string]bool{}}
}

func (f *fakeJMAP) start(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return srv.URL
}

func (f *fakeJMAP) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.auth = append(f.auth, r.Header.Get("Authorization"))
	f.conns[r.RemoteAddr] = true
	f.mu.Unlock()

	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/.well-known/jmap":
		f.mu.Lock()
		f.sessionHits++
		api := f.apiPath
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{
			"apiUrl": %q,
			"downloadUrl": "/jmap/download/{accountId}/{blobId}/{name}",
			"accounts": {"acct-1": {"name": "u1", "isPersonal": true}},
			"primaryAccounts": {"urn:ietf:params:jmap:mail": "acct-1"},
			"username": "u1", "state": "s0"
		}`, api)

	case r.Method == http.MethodPost && r.URL.Path == f.apiPath:
		var env envelope
		if err := json.NewDecoder(r.Body).Decode(&env); err != nil {
			http.Error(w, "bad envelope", http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		f.envelopes = append(f.envelopes, env)
		methodError := f.methodError
		f.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		if methodError {
			// HTTP 200 with a method-level error: RFC 8620 §3.6.2.
			fmt.Fprint(w, `{"methodResponses":[["error",{"type":"accountNotFound"},"c0"]]}`)
			return
		}
		var out []string
		for _, call := range env.MethodCalls {
			var name, id string
			json.Unmarshal(call[0], &name) //nolint:errcheck
			json.Unmarshal(call[2], &id)   //nolint:errcheck
			switch name {
			case "Email/query":
				out = append(out, fmt.Sprintf(`["Email/query",{"ids":["e1","e2"]},%q]`, id))
			case "Email/get":
				out = append(out, fmt.Sprintf(`["Email/get",{"list":[{"id":"e1","threadId":"t1"}]},%q]`, id))
			default:
				out = append(out, fmt.Sprintf(`[%q,{"list":[]},%q]`, name, id))
			}
		}
		fmt.Fprintf(w, `{"methodResponses":[%s]}`, strings.Join(out, ","))

	default:
		http.NotFound(w, r)
	}
}

func (f *fakeJMAP) snapshot() (sessions int, envs []envelope, auth []string, conns int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sessionHits, append([]envelope(nil), f.envelopes...), append([]string(nil), f.auth...), len(f.conns)
}

func testDriver(t *testing.T, base string, cfg jmapdrv.Config) *jmapdrv.Driver {
	t.Helper()
	cfg.BaseURL = base
	if cfg.Users == nil {
		cfg.Users = []string{"u1@example.com"}
	}
	if cfg.Password == "" {
		cfg.Password = "pw"
	}
	if cfg.Window == 0 {
		cfg.Window = 20
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 5 * time.Second
	}
	d, err := jmapdrv.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return d
}

// methodNames returns the names in one envelope, in order.
func methodNames(env envelope) []string {
	var out []string
	for _, call := range env.MethodCalls {
		var name string
		json.Unmarshal(call[0], &name) //nolint:errcheck
		out = append(out, name)
	}
	return out
}

// The session resource is discovered once and reused. A client that re-fetches
// it per iteration measures discovery, which no real client repeats and no
// operator cares about.
func TestSessionIsFetchedOncePerClient(t *testing.T) {
	srv := newFake()
	d := testDriver(t, srv.start(t), jmapdrv.Config{})
	c := d.NewClient()

	for i := 0; i < 5; i++ {
		if err := d.Iterate(context.Background(), c, stats.New()); err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
	}

	sessions, _, _, _ := srv.snapshot()
	if sessions != 1 {
		t.Errorf("the session was fetched %d times over 5 iterations, want 1", sessions)
	}
}

// The API endpoint comes from the session, never from the client's idea of
// where it should be. The fake serves it somewhere deliberately unconventional:
// a driver that assembles the path fails here and would pass against a server
// that happens to agree with it.
func TestAPIEndpointComesFromTheSession(t *testing.T) {
	srv := newFake()
	srv.apiPath = "/somewhere/else/api"
	d := testDriver(t, srv.start(t), jmapdrv.Config{})

	if err := d.Iterate(context.Background(), d.NewClient(), stats.New()); err != nil {
		t.Fatalf("Iterate: %v", err)
	}
	if _, envs, _, _ := srv.snapshot(); len(envs) == 0 {
		t.Error("no request reached the API URL the session advertised")
	}
}

// The query and the fetch travel as one request, joined by a back-reference.
// Saving that round trip is what JMAP is for, so a driver that issues them
// separately measures a client nobody writes — and would report the server as
// slower than it is.
func TestQueryAndGetTravelInOneRequest(t *testing.T) {
	srv := newFake()
	d := testDriver(t, srv.start(t), jmapdrv.Config{})

	if err := d.Iterate(context.Background(), d.NewClient(), stats.New()); err != nil {
		t.Fatalf("Iterate: %v", err)
	}

	_, envs, _, _ := srv.snapshot()
	var chained bool
	for _, env := range envs {
		names := methodNames(env)
		if len(names) >= 2 && names[0] == "Email/query" && names[1] == "Email/get" {
			chained = true
			// And the get must reference the query rather than carry ids.
			var args map[string]any
			json.Unmarshal(env.MethodCalls[1][1], &args) //nolint:errcheck
			ref, ok := args["#ids"].(map[string]any)
			if !ok {
				t.Fatalf("Email/get carries no #ids back-reference: %v", args)
			}
			if ref["resultOf"] != "q0" || ref["name"] != "Email/query" {
				t.Errorf("back-reference points at %v, not the query in the same envelope", ref)
			}
		}
	}
	if !chained {
		t.Errorf("Email/query and Email/get were not sent together: %v", envs)
	}
}

// A JMAP server answers 200 for a request whose methods all failed. A driver
// that stops at the status code reports a server refusing everything as a
// healthy one — the failure mode this whole repository keeps running into.
func TestMethodLevelErrorUnderHTTP200IsReported(t *testing.T) {
	srv := newFake()
	srv.methodError = true
	d := testDriver(t, srv.start(t), jmapdrv.Config{})

	c := stats.New()
	err := d.Iterate(context.Background(), d.NewClient(), c)
	if err == nil {
		t.Fatal("a request whose every method failed was reported as a success")
	}
	if !strings.Contains(err.Error(), "accountNotFound") {
		t.Errorf("the error does not carry what the server said: %v", err)
	}
	if got := c.Summary().Errors; got == 0 {
		t.Error("the failure was not counted in the statistics")
	}
}

// The query is bounded. An unbounded one costs more as the mailbox grows, so
// the same operation is cheap early in a run and expensive late.
func TestQueryCarriesTheConfiguredWindow(t *testing.T) {
	srv := newFake()
	d := testDriver(t, srv.start(t), jmapdrv.Config{Window: 7})

	if err := d.Iterate(context.Background(), d.NewClient(), stats.New()); err != nil {
		t.Fatalf("Iterate: %v", err)
	}

	_, envs, _, _ := srv.snapshot()
	for _, env := range envs {
		for i, name := range methodNames(env) {
			if name != "Email/query" {
				continue
			}
			var args map[string]any
			json.Unmarshal(env.MethodCalls[i][1], &args) //nolint:errcheck
			if args["limit"] != float64(7) {
				t.Errorf("Email/query limit = %v, want 7", args["limit"])
			}
			return
		}
	}
	t.Error("no Email/query was sent")
}

func TestNewRejectsAnUnboundedWindow(t *testing.T) {
	if _, err := jmapdrv.New(jmapdrv.Config{
		BaseURL: "http://x", Users: []string{"u"}, Window: 0,
	}); err == nil {
		t.Error("a run with no query window was accepted; its cost would grow with the mailbox")
	}
}

// Credentials ride on every request, since JMAP holds no session state. If one
// went out unauthenticated the server would answer 401 and the run would report
// the server as broken.
func TestEveryRequestCarriesCredentials(t *testing.T) {
	srv := newFake()
	d := testDriver(t, srv.start(t), jmapdrv.Config{Users: []string{"u1@example.com"}, Password: "s3cret"})
	c := d.NewClient()
	for i := 0; i < 3; i++ {
		if err := d.Iterate(context.Background(), c, stats.New()); err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
	}

	want := "Basic " + base64.StdEncoding.EncodeToString([]byte("u1@example.com:s3cret"))
	_, _, auth, _ := srv.snapshot()
	if len(auth) < 4 {
		t.Fatalf("only %d requests reached the server", len(auth))
	}
	for i, got := range auth {
		if got != want {
			t.Errorf("request %d carried %q, want %q", i, got, want)
		}
	}
}

// The connection is what a JMAP client keeps, so it has to be kept. Without
// reuse every request pays a handshake and the run measures connection setup
// rather than mail.
func TestOneClientUsesOneConnection(t *testing.T) {
	srv := newFake()
	d := testDriver(t, srv.start(t), jmapdrv.Config{})
	c := d.NewClient()
	for i := 0; i < 6; i++ {
		if err := d.Iterate(context.Background(), c, stats.New()); err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
	}
	if _, _, _, conns := srv.snapshot(); conns != 1 {
		t.Errorf("one client opened %d connections over 6 iterations; keep-alive is not in effect", conns)
	}
}

// Accounts advance per client, so a run touches as many as it has clients —
// the same rule the other drivers here follow, and the one the LMTP driver got
// wrong once.
func TestClientsSpreadAcrossTheAccountSet(t *testing.T) {
	srv := newFake()
	users := []string{"u1@example.com", "u2@example.com", "u3@example.com"}
	d := testDriver(t, srv.start(t), jmapdrv.Config{Users: users})

	for i := 0; i < len(users); i++ {
		if err := d.Iterate(context.Background(), d.NewClient(), stats.New()); err != nil {
			t.Fatalf("client %d: %v", i, err)
		}
	}

	seen := map[string]bool{}
	_, _, auth, _ := srv.snapshot()
	for _, a := range auth {
		raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(a, "Basic "))
		if err != nil {
			continue
		}
		seen[strings.SplitN(string(raw), ":", 2)[0]] = true
	}
	if len(seen) != len(users) {
		t.Errorf("%d clients reached %d of %d accounts: %v", len(users), len(seen), len(users), seen)
	}
}

// Package jmapdrv is the JMAP driver: session discovery once per client, then
// the request chain a mail client actually issues — Mailbox/get, Email/query,
// Email/get — over a kept-alive HTTP connection.
//
// JMAP has no session state on the wire: credentials ride on every request, so
// there is nothing to hold open the way IMAP holds a selected mailbox. What
// there is to hold is the TCP connection and the session resource, and a
// generator that re-fetches /.well-known/jmap per operation measures discovery
// instead of mail.
package jmapdrv

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/yarilomail/yarilo-loadtest/internal/stats"
)

const (
	capCore = "urn:ietf:params:jmap:core"
	capMail = "urn:ietf:params:jmap:mail"
	// sessionPath is the well-known endpoint of RFC 8620 §2.2. Everything else
	// a client talks to is read out of what it returns, never assembled by the
	// client — a driver that hard-codes the API path is testing its own guess.
	sessionPath = "/.well-known/jmap"
)

// Config describes a JMAP run.
type Config struct {
	// BaseURL is the origin the session resource is fetched from, e.g.
	// "https://mail.example.com".
	BaseURL  string
	Users    []string
	Password string
	Insecure bool
	Timeout  time.Duration

	// Window bounds Email/query and the Email/get that follows it. Bounded for
	// the reason every other driver here bounds its fetch: an unbounded query
	// costs more as the mailbox grows, so the same operation is cheap early in
	// a run and expensive late, and no two measurements compare.
	Window int

	// FetchBodies asks Email/get for bodyValues rather than metadata alone.
	// It is the difference between reading a mailbox listing and reading mail,
	// and on the server it is the difference between an index lookup and going
	// to the message store.
	FetchBodies bool

	// Threads adds a Thread/get for the threads the query returned, which is
	// what a client rendering a conversation view does.
	Threads bool
}

// Driver runs JMAP request chains.
type Driver struct {
	cfg Config
	// seq advances once per iteration rather than once per client, so which
	// accounts a run touches is a property of the run.
	seq atomic.Uint64
}

func New(cfg Config) (*Driver, error) {
	if len(cfg.Users) == 0 {
		return nil, fmt.Errorf("jmap: no users configured")
	}
	if cfg.BaseURL == "" {
		return nil, fmt.Errorf("jmap: no base URL configured")
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	if cfg.Window <= 0 {
		return nil, fmt.Errorf("jmap: -window must be positive; an unbounded query grows with the mailbox")
	}
	cfg.BaseURL = strings.TrimSuffix(cfg.BaseURL, "/")
	return &Driver{cfg: cfg}, nil
}

// session is what a client learns once and then reuses.
type session struct {
	APIURL      string             `json:"apiUrl"`
	DownloadURL string             `json:"downloadUrl"`
	Accounts    map[string]account `json:"accounts"`
	Primary     map[string]string  `json:"primaryAccounts"`
	Username    string             `json:"username"`
}

type account struct {
	Name string `json:"name"`
}

// accountID resolves the account to work in: the primary mail account, which is
// what the session says rather than what the client would like it to be.
func (s *session) accountID() (string, error) {
	if id := s.Primary[capMail]; id != "" {
		return id, nil
	}
	for id := range s.Accounts {
		return id, nil
	}
	return "", fmt.Errorf("jmap: session names no account for %s", capMail)
}

// Client is one JMAP client: an HTTP connection pool, credentials, and the
// session it discovered.
type Client struct {
	http    *http.Client
	base    string
	user    string
	pass    string
	sess    *session
	account string
}

// Iterate runs one full chain for one account, discovering the session on the
// first call and reusing it afterwards.
func (d *Driver) Iterate(ctx context.Context, c *Client, col *stats.Collector) error {
	if c.sess == nil {
		if err := c.discover(ctx, col); err != nil {
			return err
		}
	}

	if _, err := c.call(ctx, col, "Mailbox/get", []any{
		[]any{"Mailbox/get", map[string]any{
			"accountId": c.account,
			"ids":       nil,
		}, "m0"},
	}); err != nil {
		return err
	}

	// Query and fetch as one request, joined by a back-reference (RFC 8620
	// §3.7): "#ids" tells the server to feed the query's result into the get
	// without the client seeing it. That round trip is the one JMAP is designed
	// to save, so a driver that issues them separately measures a client nobody
	// writes.
	get := map[string]any{
		"accountId": c.account,
		"#ids": map[string]any{
			"resultOf": "q0",
			"name":     "Email/query",
			"path":     "/ids",
		},
		"properties": []string{"id", "subject", "from", "receivedAt", "threadId"},
	}
	if d.cfg.FetchBodies {
		get["fetchTextBodyValues"] = true
		get["properties"] = []string{"id", "subject", "from", "receivedAt", "threadId", "bodyValues", "textBody"}
	}
	calls := []any{
		[]any{"Email/query", map[string]any{
			"accountId": c.account,
			"sort":      []any{map[string]any{"property": "receivedAt", "isAscending": false}},
			"limit":     d.cfg.Window,
		}, "q0"},
		[]any{"Email/get", get, "g0"},
	}
	if d.cfg.Threads {
		calls = append(calls, []any{"Thread/get", map[string]any{
			"accountId": c.account,
			"#ids": map[string]any{
				"resultOf": "g0",
				"name":     "Email/get",
				"path":     "/list/*/threadId",
			},
		}, "t0"})
	}

	name := "Email/query+get"
	if d.cfg.Threads {
		name = "Email/query+get+Thread/get"
	}
	_, err := c.call(ctx, col, name, calls)
	return err
}

// NewClient builds a client for the next account in the set.
func (d *Driver) NewClient() *Client {
	user := d.cfg.Users[int(d.seq.Add(1)-1)%len(d.cfg.Users)]
	return &Client{
		http: &http.Client{
			Timeout: d.cfg.Timeout,
			Transport: &http.Transport{
				// Keep-alive is the whole of what a JMAP client holds open, so
				// a pool of one per client is the shape being measured. Without
				// it every request pays a handshake and the run measures TLS.
				MaxIdleConnsPerHost: 1,
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: d.cfg.Insecure, //nolint:gosec // opt-in, for sandbox certs
					MinVersion:         tls.VersionTLS12,
				},
			},
		},
		base: d.cfg.BaseURL,
		user: user,
		pass: d.cfg.Password,
	}
}

// discover fetches the session resource and resolves the account.
func (c *Client) discover(ctx context.Context, col *stats.Collector) error {
	t0 := time.Now()
	body, err := c.do(ctx, http.MethodGet, c.base+sessionPath, nil)
	col.Observe("session", time.Since(t0), err)
	if err != nil {
		return err
	}
	var s session
	if err := json.Unmarshal(body, &s); err != nil {
		return fmt.Errorf("jmap: session is not JSON: %w", err)
	}
	if s.APIURL == "" {
		return fmt.Errorf("jmap: session carries no apiUrl")
	}
	id, err := s.accountID()
	if err != nil {
		return err
	}
	c.sess, c.account = &s, id
	return nil
}

// call issues one JMAP request envelope and times it under name.
func (c *Client) call(ctx context.Context, col *stats.Collector, name string, calls []any) ([]byte, error) {
	env := map[string]any{
		"using":       []string{capCore, capMail},
		"methodCalls": calls,
	}
	payload, err := json.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("jmap: encode %s: %w", name, err)
	}
	t0 := time.Now()
	body, err := c.do(ctx, http.MethodPost, c.apiURL(), payload)
	if err == nil {
		err = checkMethodErrors(body)
	}
	col.Observe(name, time.Since(t0), err)
	return body, err
}

func (c *Client) apiURL() string {
	if strings.HasPrefix(c.sess.APIURL, "http") {
		return c.sess.APIURL
	}
	return c.base + c.sess.APIURL
}

// do performs one HTTP request with Basic credentials and reads the whole body.
//
// The body is read to completion even when it is discarded: a client that
// abandons it cannot reuse the connection, so the next request would pay a new
// handshake and the run would measure a keep-alive that never happened.
func (c *Client) do(ctx context.Context, method, url string, payload []byte) ([]byte, error) {
	var rdr io.Reader
	if payload != nil {
		rdr = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, rdr)
	if err != nil {
		return nil, fmt.Errorf("jmap: build %s %s: %w", method, url, err)
	}
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(c.user+":"+c.pass)))
	req.Header.Set("Accept", "application/json")
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("jmap: %s %s: %w", method, url, err)
	}
	defer resp.Body.Close() //nolint:errcheck
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("jmap: read %s %s: %w", method, url, err)
	}
	if resp.StatusCode != http.StatusOK {
		return body, fmt.Errorf("jmap: %s %s: %s: %s", method, url, resp.Status, snippet(body))
	}
	return body, nil
}

// methodResponse is one entry of the responses array.
type envelopeResponse struct {
	MethodResponses []json.RawMessage `json:"methodResponses"`
}

// checkMethodErrors turns a per-method "error" response into a failure.
//
// JMAP answers 200 for a request whose methods all failed (RFC 8620 §3.6.2), so
// a driver that stops at the status code reports a server that refused every
// call as a healthy one — the exact shape of "the tool says it is fine".
func checkMethodErrors(body []byte) error {
	var env envelopeResponse
	if err := json.Unmarshal(body, &env); err != nil {
		return fmt.Errorf("jmap: response is not a JMAP envelope: %w", err)
	}
	if len(env.MethodResponses) == 0 {
		return fmt.Errorf("jmap: response carries no methodResponses")
	}
	for _, raw := range env.MethodResponses {
		var triple []json.RawMessage
		if err := json.Unmarshal(raw, &triple); err != nil || len(triple) < 2 {
			return fmt.Errorf("jmap: malformed method response %s", snippet(raw))
		}
		var name string
		if err := json.Unmarshal(triple[0], &name); err != nil {
			return fmt.Errorf("jmap: method response has no name: %s", snippet(raw))
		}
		if name == "error" {
			return fmt.Errorf("jmap: method failed: %s", snippet(triple[1]))
		}
	}
	return nil
}

// snippet trims a body for an error message: enough to identify the failure,
// not enough to bury it.
func snippet(b []byte) string {
	const max = 200
	s := strings.TrimSpace(string(b))
	if len(s) > max {
		return s[:max] + "…"
	}
	return s
}

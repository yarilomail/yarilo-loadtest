// Package stallwatch snapshots the backends the moment imaptest reports a
// stalled command.
//
// It runs inside the cluster so that a broken link to an operator's machine
// cannot end the watch: it reads the run's log through the API server with its
// own service account, and writes every capture to a volume that outlives the
// pod.
//
// Why by event and not on a timer: the stall it hunts lasted minutes but
// appeared twice in a day, and three captures taken on a timer all landed on a
// healthy server. A dump taken after the run says nothing -- the parked
// goroutine is gone by then (#1517).
//
// Per trigger it writes, for each backend, the goroutine dump at debug=2 (which
// names the parked stack), a metrics snapshot, the container's cgroup cpu.stat
// when the node's cgroupfs is mounted, and one CPU profile. Captures are
// rate-limited so a burst of stalls cannot bury the first one.
//
// The dump and the profile need telemetry.pprof.enabled on the backends. With
// it off the metrics snapshot still arrives and those two files are simply
// absent, which reads as a quiet server rather than as a switch nobody turned
// on -- so check it first.
package stallwatch

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	apiServer  = "https://kubernetes.default.svc"
	saDir      = "/var/run/secrets/kubernetes.io/serviceaccount"
	backendPfx = "yarilo-backend-"
)

type backend struct {
	name string
	ip   string
	uid  string
}

type watcher struct {
	ns        string
	target    string
	out       string
	cooldown  time.Duration
	profile   time.Duration
	telemetry string
	pattern   *regexp.Regexp

	token string
	api   *http.Client
	plain *http.Client
}

// Run watches the target job and returns when its log ends.
func Run() error {
	w, err := newWatcher()
	if err != nil {
		return err
	}
	return w.run()
}

func newWatcher() (*watcher, error) {
	target := os.Getenv("TARGET")
	if target == "" {
		return nil, fmt.Errorf("TARGET is required: the job-name of the run to watch")
	}
	token, err := os.ReadFile(filepath.Join(saDir, "token"))
	if err != nil {
		return nil, fmt.Errorf("read service account token: %w", err)
	}
	ca, err := os.ReadFile(filepath.Join(saDir, "ca.crt"))
	if err != nil {
		return nil, fmt.Errorf("read service account CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, fmt.Errorf("service account CA is not a PEM bundle")
	}
	w := &watcher{
		ns:        env("NS", "yarilo-sb"),
		target:    target,
		out:       env("OUT", "/caps"),
		cooldown:  time.Duration(envInt("COOLDOWN", 60)) * time.Second,
		profile:   time.Duration(envInt("PROFILE_SECS", 30)) * time.Second,
		telemetry: env("TELEMETRY_PORT", "8080"),
		token:     strings.TrimSpace(string(token)),
		// No timeout on the API client: it follows a log stream for the length
		// of the run.
		api: &http.Client{Transport: &http.Transport{
			TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
		}},
		plain: &http.Client{},
	}
	w.pattern, err = regexp.Compile(env("PATTERN", `stalled (>3s|for \d+ secs)`))
	if err != nil {
		return nil, fmt.Errorf("PATTERN: %w", err)
	}
	return w, os.MkdirAll(w.out, 0o755)
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(k)); err == nil && v > 0 {
		return v
	}
	return def
}

func (w *watcher) logf(format string, args ...any) {
	line := time.Now().Format("15:04:05") + " " + fmt.Sprintf(format, args...)
	fmt.Println(line)
	f, err := os.OpenFile(filepath.Join(w.out, "watch.log"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer f.Close() //nolint:errcheck
	_, _ = f.WriteString(line + "\n")
}

// apiGet returns the response body for a cluster API path. The caller closes it.
func (w *watcher) apiGet(path string) (io.ReadCloser, error) {
	req, err := http.NewRequest(http.MethodGet, apiServer+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+w.token)
	resp, err := w.api.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		resp.Body.Close() //nolint:errcheck
		return nil, fmt.Errorf("GET %s: %s: %s", path, resp.Status, strings.TrimSpace(string(body)))
	}
	return resp.Body, nil
}

type podList struct {
	Items []struct {
		Metadata struct {
			Name string `json:"name"`
			UID  string `json:"uid"`
		} `json:"metadata"`
		Status struct {
			PodIP string `json:"podIP"`
			Phase string `json:"phase"`
		} `json:"status"`
	} `json:"items"`
}

func (w *watcher) pods(query string) (*podList, error) {
	body, err := w.apiGet("/api/v1/namespaces/" + w.ns + "/pods" + query)
	if err != nil {
		return nil, err
	}
	defer body.Close() //nolint:errcheck
	var list podList
	if err := json.NewDecoder(body).Decode(&list); err != nil {
		return nil, fmt.Errorf("decode pod list: %w", err)
	}
	return &list, nil
}

func (w *watcher) backends() ([]backend, error) {
	list, err := w.pods("")
	if err != nil {
		return nil, err
	}
	var out []backend
	for _, p := range list.Items {
		if strings.HasPrefix(p.Metadata.Name, backendPfx) && p.Status.PodIP != "" {
			out = append(out, backend{p.Metadata.Name, p.Status.PodIP, p.Metadata.UID})
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no %s* pods with an address in %s", backendPfx, w.ns)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out, nil
}

// targetPod waits for the run to be scheduled: the watch is usually started
// before the job it watches.
func (w *watcher) targetPod() (string, error) {
	deadline := time.Now().Add(10 * time.Minute)
	for {
		list, err := w.pods("?labelSelector=job-name%3D" + w.target)
		if err != nil {
			return "", err
		}
		for _, p := range list.Items {
			if p.Status.Phase == "Running" || p.Status.Phase == "Succeeded" {
				return p.Metadata.Name, nil
			}
		}
		if time.Now().After(deadline) {
			return "", fmt.Errorf("the run %s never started", w.target)
		}
		time.Sleep(5 * time.Second)
	}
}

// save writes what url returns to path. A failed capture is logged and does not
// end the watch: one missing file is worth less than the rest of the run.
func (w *watcher) save(url, path string, timeout time.Duration) {
	client := *w.plain
	client.Timeout = timeout
	resp, err := client.Get(url)
	if err != nil {
		w.logf("  %s: %v", url, err)
		return
	}
	defer resp.Body.Close() //nolint:errcheck
	f, err := os.Create(path)
	if err != nil {
		w.logf("  %s: %v", path, err)
		return
	}
	defer f.Close() //nolint:errcheck
	if _, err := io.Copy(f, resp.Body); err != nil {
		w.logf("  %s: %v", path, err)
	}
}

// cpuStat copies the container's cgroup cpu.stat, when the node's cgroupfs is
// mounted read-only into this pod. The slice directory carries the pod UID,
// with dashes turned into underscores on systemd-managed nodes.
func (w *watcher) cpuStat(uid, path string) {
	for _, pattern := range []string{
		"/host/sys/fs/cgroup/kubepods.slice/*/*pod%s.slice/*/cpu.stat",
		"/host/sys/fs/cgroup/kubepods/*/pod%s/*/cpu.stat",
	} {
		for _, id := range []string{strings.ReplaceAll(uid, "-", "_"), uid} {
			hits, _ := filepath.Glob(fmt.Sprintf(pattern, id))
			for _, hit := range hits {
				if b, err := os.ReadFile(hit); err == nil {
					_ = os.WriteFile(path, b, 0o644)
					return
				}
			}
		}
	}
}

func (w *watcher) capture(tag string, backends []backend) {
	for _, b := range backends {
		base := filepath.Join(w.out, tag+"-"+b.name)
		w.save(fmt.Sprintf("http://%s:%s/debug/pprof/goroutine?debug=2", b.ip, w.telemetry),
			base+"-goroutine.txt", time.Minute)
		w.save(fmt.Sprintf("http://%s:%s/metrics", b.ip, w.telemetry), base+"-metrics.txt", time.Minute)
		w.cpuStat(b.uid, base+"-cpu.stat")
	}
	first := backends[0]
	w.save(fmt.Sprintf("http://%s:%s/debug/pprof/profile?seconds=%d", first.ip, w.telemetry, int(w.profile.Seconds())),
		filepath.Join(w.out, tag+"-"+first.name+"-cpu.pb.gz"), w.profile+30*time.Second)
	w.logf("captured %s", tag)
}

func (w *watcher) run() error {
	backends, err := w.backends()
	if err != nil {
		return err
	}
	pod, err := w.targetPod()
	if err != nil {
		return err
	}
	names := make([]string, len(backends))
	for i, b := range backends {
		names[i] = b.name
	}
	w.logf("watching %s (pod %s), backends: %s", w.target, pod, strings.Join(names, ", "))

	stream, err := w.apiGet(fmt.Sprintf("/api/v1/namespaces/%s/pods/%s/log?follow=true&sinceSeconds=3600", w.ns, pod))
	if err != nil {
		return err
	}
	defer stream.Close() //nolint:errcheck

	scanner := bufio.NewScanner(stream)
	scanner.Buffer(make([]byte, 0, 64<<10), 1<<20)
	var last time.Time
	hits := 0
	for scanner.Scan() {
		line := scanner.Text()
		if !w.pattern.MatchString(line) {
			continue
		}
		hits++
		if time.Since(last) < w.cooldown {
			continue
		}
		last = time.Now()
		w.logf("EVENT: %s", truncate(strings.TrimSpace(line), 200))
		w.capture("s"+strconv.FormatInt(last.Unix(), 10), backends)
	}
	w.logf("run ended; %d stall lines seen", hits)
	return scanner.Err()
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

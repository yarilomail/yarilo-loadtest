package toolchain

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// go.mod and the base image must name one toolchain: a scan reads go.mod, the
// image ships the base. They disagreed here for months -- go.mod on 1.26.2, the
// Dockerfile on a floating 1.26 -- for the same reason the product repository
// did (yarilo#1497), and with nobody watching either.
func TestGoModAndDockerfileNameTheSameToolchain(t *testing.T) {
	root := filepath.Join("..", "..")

	gomod := readFile(t, filepath.Join(root, "go.mod"))
	m := regexp.MustCompile(`(?m)^go (\d+\.\d+\.\d+)$`).FindStringSubmatch(gomod)
	if m == nil {
		t.Fatal("go.mod has no `go X.Y.Z` line; an unpinned patch version is what this guards against")
	}
	want := m[1]

	dockerfile := readFile(t, filepath.Join(root, "Dockerfile"))
	// golang:<version>-alpine<release>@sha256:<digest>: the version is what
	// go.mod must name, and the digest keeps the base from moving under a tag.
	stages := regexp.MustCompile(`(?m)^FROM golang:(\S+)`).FindAllStringSubmatch(dockerfile, -1)
	if len(stages) == 0 {
		t.Fatal("no golang base image in the Dockerfile; this guard is watching a file that moved")
	}
	version := regexp.MustCompile(`^(\d+\.\d+\.\d+)-alpine[0-9.]*@sha256:[0-9a-f]{64}$`)
	for _, s := range stages {
		v := version.FindStringSubmatch(s[1])
		if v == nil {
			t.Errorf("golang:%s is not <version>-alpine pinned by digest", s[1])
			continue
		}
		if v[1] != want {
			t.Errorf("the image builds on golang:%s, go.mod says %s", s[1], want)
		}
	}
	if !strings.Contains(dockerfile, "ENV GOTOOLCHAIN=local") {
		t.Error("GOTOOLCHAIN is not local, so a newer go.mod fetches a toolchain instead of failing the build")
	}

	// And CI must take the version from go.mod rather than from a literal of
	// its own. It did not: with GOTOOLCHAIN=local and a literal one patch
	// behind, every job failed with "go.mod requires go >= 1.26.7 (running go
	// 1.26.2)" -- a third place naming the toolchain is a third place to
	// forget.
	ci := readFile(t, filepath.Join(root, ".github", "workflows", "ci.yml"))
	if strings.Contains(ci, "go-version:") {
		t.Error("ci.yml pins a Go version of its own; it must use go-version-file: go.mod")
	}
	if !strings.Contains(ci, "go-version-file: go.mod") {
		t.Error("ci.yml does not read the Go version from go.mod")
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

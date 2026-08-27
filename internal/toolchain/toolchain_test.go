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
	stages := regexp.MustCompile(`(?m)^FROM golang:(\S+)`).FindAllStringSubmatch(dockerfile, -1)
	if len(stages) == 0 {
		t.Fatal("no golang base image in the Dockerfile; this guard is watching a file that moved")
	}
	for _, s := range stages {
		if got := strings.TrimSuffix(s[1], "-alpine"); got != want {
			t.Errorf("the image builds on golang:%s, go.mod says %s", s[1], want)
		}
	}
	if !strings.Contains(dockerfile, "ENV GOTOOLCHAIN=local") {
		t.Error("GOTOOLCHAIN is not local, so a newer go.mod fetches a toolchain instead of failing the build")
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

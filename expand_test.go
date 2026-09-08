package main

import (
	"strings"
	"testing"
)

// The number the local part ends in is where the run starts: read as a prefix
// numbered from one, u51@d:100 became u511..u51100 (#34).
func TestRecipientsRunFromTheNumberTheyName(t *testing.T) {
	for _, tc := range []struct {
		spec  string
		first string
		last  string
		count int
	}{
		{spec: "u1@d.test:150", first: "u1@d.test", last: "u150@d.test", count: 150},
		{spec: "u51@d.test:50", first: "u51@d.test", last: "u100@d.test", count: 50},
		{spec: "u@d.test:3", first: "u1@d.test", last: "u3@d.test", count: 3},
		{spec: "u101@d.test:2", first: "u101@d.test", last: "u102@d.test", count: 2},
		{spec: "alice@d.test", first: "alice@d.test", last: "alice@d.test", count: 1},
		{spec: "a@d.test,b@d.test", first: "a@d.test", last: "b@d.test", count: 2},
	} {
		t.Run(tc.spec, func(t *testing.T) {
			got, err := expandRecipients("-recipients", tc.spec)
			if err != nil {
				t.Fatalf("%q was refused: %v", tc.spec, err)
			}
			if len(got) != tc.count {
				t.Fatalf("%q expanded to %d addresses, want %d: %s",
					tc.spec, len(got), tc.count, strings.Join(got, ","))
			}
			if got[0] != tc.first {
				t.Errorf("first is %q, want %q", got[0], tc.first)
			}
			if got[len(got)-1] != tc.last {
				t.Errorf("last is %q, want %q", got[len(got)-1], tc.last)
			}
		})
	}
}

// A form with two readings is refused, and the refusal names the flag: picking
// one silently is how a run reaches addresses nobody asked for (#34).
func TestAnAmbiguousRunIsRefused(t *testing.T) {
	for _, tc := range []struct{ spec, names string }{
		{spec: "u0@d.test:3", names: "start at zero"},
		{spec: "u051@d.test:3", names: "zero-padded"},
		{spec: "u00@d.test:2", names: "start at zero"},
	} {
		t.Run(tc.spec, func(t *testing.T) {
			got, err := expandRecipients("-recipients", tc.spec)
			if err == nil {
				t.Fatalf("%q expanded to %s", tc.spec, strings.Join(got, ","))
			}
			if !strings.Contains(err.Error(), tc.names) {
				t.Errorf("said %q, want it to name %q", err, tc.names)
			}
			if !strings.Contains(err.Error(), "-recipients") {
				t.Errorf("said %q, want it to name the flag", err)
			}
		})
	}
}

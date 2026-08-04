package main

import (
	"flag"
	"testing"
)

// Some flag defaults are the safety of the tool rather than a preference, and
// the damage from changing one is silent: the run reports success either way,
// and the cost shows up later as an emptied mailbox or a measurement nobody can
// compare.
//
// Driver-level tests cannot guard these. They exercise a config the test itself
// supplies, so flipping a default in this file leaves them green — which is
// exactly what happened to TestDeleteIsOffByDefault, a test whose name promised
// this and whose body checked something adjacent.
//
// DefValue is read rather than the parsed variable, so the assertion is about
// what a run gets when nobody passes the flag.
func TestDefaultsThatProtectTheCorpus(t *testing.T) {
	for _, tc := range []struct {
		flag string
		want string
		why  string
	}{
		{
			flag: "delete",
			want: "false",
			why:  "a run that deletes consumes the mailboxes every other measurement is taken against",
		},
		{
			flag: "retr",
			want: "10",
			why:  "an unbounded retrieval walks the whole maildrop, so its cost grows with the mailbox and no two measurements in one run are comparable",
		},
		{
			flag: "window",
			want: "20",
			why:  "an unbounded JMAP query grows with the mailbox, like every other fetch here",
		},
		{
			flag: "concurrency",
			want: "10",
			why:  "a default in the hundreds points a load test at a shared environment before anyone has chosen to",
		},
		{
			flag: "duration",
			want: "0s",
			why:  "an unbounded run against a shared environment must be asked for, not arrived at by omission",
		},
		{
			flag: "iterations",
			want: "0",
			why:  "same as duration: one of the two bounds has to be set deliberately",
		},
		{
			flag: "stop-on-error",
			want: "false",
			why:  "a run that stops at the first blip reports less than one that counts them",
		},
	} {
		t.Run(tc.flag, func(t *testing.T) {
			f := flag.Lookup(tc.flag)
			if f == nil {
				t.Fatalf("-%s no longer exists; this guard is now checking nothing", tc.flag)
			}
			if f.DefValue != tc.want {
				t.Errorf("-%s defaults to %q, want %q — %s", tc.flag, f.DefValue, tc.want, tc.why)
			}
		})
	}
}

// A guard over a list of names stops guarding the moment a name is wrong, and
// says nothing while it does. Every entry above must resolve.
func TestGuardedFlagsAllExist(t *testing.T) {
	const guarded = 7
	var found int
	for _, name := range []string{"delete", "retr", "window", "concurrency", "duration", "iterations", "stop-on-error"} {
		if flag.Lookup(name) != nil {
			found++
		}
	}
	if found != guarded {
		t.Errorf("%d of %d guarded flags resolve; the list above has drifted from the flags", found, guarded)
	}
}

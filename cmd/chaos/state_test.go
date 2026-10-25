package main

import "testing"

func pair(r1 string, e1 int64, r2 string, e2 int64) pairState {
	return pairState{Pods: []roleInfo{{"a", r1, e1}, {"b", r2, e2}}}
}

func TestKind(t *testing.T) {
	cases := map[string]pairState{
		"healthy":     pair("primary", 6, "backup", 6),
		"split-brain": pair("primary", 6, "primary", 7),
		"no-primary":  pair("backup", 6, "backup", 6),
		"unsettled":   pair("primary", 6, "starting", 0),
		"unknown":     {Pods: []roleInfo{{"a", "primary", 1}}},
	}
	for want, st := range cases {
		if got := st.kind(); got != want {
			t.Errorf("%s: got %s", want, got)
		}
	}
}

func TestStalePicksLowerEpoch(t *testing.T) {
	if p, ok := pair("primary", 6, "primary", 7).stale(); !ok || p != "a" {
		t.Fatalf("want a (epoch 6), got %q %v", p, ok)
	}
	if _, ok := pair("primary", 7, "primary", 7).stale(); ok {
		t.Fatal("a tie must not be guessed")
	}
	if p := pair("backup", 6, "primary", 7).primary(); p != "b" {
		t.Fatalf("primary() = %q", p)
	}
}

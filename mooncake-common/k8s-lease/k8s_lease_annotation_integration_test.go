//go:build integration

package main

import (
	"testing"
	"time"
)

func electedState(t *testing.T, ns, lease string) *electionState {
	t.Helper()
	electionMutex.Lock()
	state := elections[electionKey(ns, lease)]
	electionMutex.Unlock()
	select {
	case <-state.elected:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for election")
	}
	return state
}

// The replication record may be written only by the holder of the term it
// names; a renewal refreshes the time the leader may still answer until.
func TestAnnotationCasIsFencedByHolderAndTerm(t *testing.T) {
	ns, lease, identity := "default", "annotation-cas-test", "node-a:50051#1"
	if err := runElection(ns, lease, identity, 5, 3, 1); err != nil {
		t.Fatalf("runElection: %v", err)
	}
	state := electedState(t, ns, lease)
	defer state.cancel()
	_, transitions, err := getHolder(ns, lease)
	if err != nil {
		t.Fatalf("getHolder: %v", err)
	}

	if ok, err := casAnnotation(ns, lease, identity, transitions, "k", "v1"); !ok || err != nil {
		t.Fatalf("holder's write: ok=%v err=%v", ok, err)
	}
	if value, found, err := getAnnotation(ns, lease, "k"); err != nil || !found || value != "v1" {
		t.Fatalf("read back: %q found=%v err=%v", value, found, err)
	}
	if ok, _ := casAnnotation(ns, lease, "node-b:50051#2", transitions, "k", "v2"); ok {
		t.Fatal("another identity wrote the record")
	}
	if ok, _ := casAnnotation(ns, lease, identity, transitions+1, "k", "v3"); ok {
		t.Fatal("a write for another term went through")
	}
	if value, _, _ := getAnnotation(ns, lease, "k"); value != "v1" {
		t.Fatalf("record changed to %q", value)
	}

	// Renewals keep the leader's deadline moving, the annotation writes
	// notwithstanding (they bump the resourceVersion under client-go).
	first := state.lastRenew.Load()
	if first <= 0 {
		t.Fatalf("no renewal recorded: %d", first)
	}
	time.Sleep(2500 * time.Millisecond)
	if later := state.lastRenew.Load(); later <= first {
		t.Fatalf("renewal time did not advance: %d -> %d", first, later)
	}
	if _, found, _ := getAnnotation(ns, "no-such-lease", "k"); found {
		t.Fatal("annotation of a missing lease")
	}
}

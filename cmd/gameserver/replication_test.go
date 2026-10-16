package main

import (
	"context"
	"errors"
	"testing"

	"github.com/CSC3104-ARCADE/pkg/replication"
	"github.com/CSC3104-ARCADE/pkg/uno"
	"github.com/alicebob/miniredis/v2"
)

func TestStartPrimaryFreshThenResume(t *testing.T) {
	ctx := context.Background()
	store, err := connectStore(ctx, miniredis.RunT(t).Addr())
	if err != nil {
		t.Fatalf("connectStore: %v", err)
	}
	newGame := func() (*uno.Game, error) { return uno.NewGameEngine("m1", []string{"a", "b"}, 1) }

	var fenced *replication.FencedError
	first, err := startPrimary(ctx, store, "m1", replication.FormatJSON, newGame,
		func(fe *replication.FencedError) { fenced = fe })
	if err != nil {
		t.Fatalf("first startPrimary: %v", err)
	}
	if _, err := first.DrawCard(first.CurrentPlayer()); err != nil {
		t.Fatalf("move on first primary: %v", err)
	}

	// A restarted primary resumes the replicated game and fences the first one.
	second, err := startPrimary(ctx, store, "m1", replication.FormatJSON, newGame, nil)
	if err != nil {
		t.Fatalf("second startPrimary: %v", err)
	}
	if second.Sequence() != first.Sequence() {
		t.Fatalf("resumed at seq %d, want %d", second.Sequence(), first.Sequence())
	}

	_, err = first.DrawCard(first.CurrentPlayer())
	if !errors.Is(err, replication.ErrFenced) || fenced == nil {
		t.Fatalf("old primary move err = %v (onFenced %v), want ErrFenced", err, fenced)
	}
}

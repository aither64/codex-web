package codex

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSubmissionDatabaseConcurrentFirstOpen(t *testing.T) {
	dir := t.TempDir()
	options := ClientOptions{SubmissionDatabasePath: filepath.Join(dir, "attempts.sqlite"), SubmissionLedgerPath: filepath.Join(dir, "attempts.json")}
	start := make(chan struct{})
	results := make(chan error, 16)
	for range 16 {
		client := NewWithOptions("", options)
		t.Cleanup(client.Close)
		go func() { <-start; results <- client.openSubmissionDatabaseLocked(context.Background()) }()
	}
	close(start)
	for range 16 {
		if err := <-results; err != nil {
			t.Error(err)
		}
	}
}

func TestSubmissionDatabaseInitializationLockCannotBePredecessor(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		path := filepath.Join(t.TempDir(), "attempts.sqlite")
		options := ClientOptions{SubmissionDatabasePath: path, SubmissionLedgerPath: path + ".json"}
		if legacy {
			options.LegacySubmissionLedgerPath = path + ".lock"
		} else {
			options.SubmissionLedgerPath = path + ".lock"
		}
		client := NewWithOptions("", options)
		defer client.Close()
		if err := client.openSubmissionDatabaseLocked(context.Background()); err == nil {
			t.Fatal("initialization lock used as a JSON predecessor")
		}
	}
}

func TestSubmissionInitializationReleasesGateBeforePredecessorWait(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	dir := t.TempDir()
	path := filepath.Join(dir, "attempts.sqlite")
	ledger := filepath.Join(dir, "attempts.json")
	releasePredecessor, err := lockSubmissionPredecessors(ctx, []string{ledger})
	if err != nil {
		t.Fatal(err)
	}
	defer releasePredecessor()
	client := NewWithOptions("", ClientOptions{SubmissionDatabasePath: path, SubmissionLedgerPath: ledger})
	defer client.Close()
	result := make(chan error, 1)
	go func() { result <- client.openSubmissionDatabaseLocked(ctx) }()
	joined := false
	defer func() {
		cancel()
		if !joined {
			<-result
		}
	}()
	for {
		if info, e := os.Stat(path); e == nil && info.Size() > 0 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	// The client is waiting for the independently held JSON predecessor. The
	// init gate must already be available; retaining both would invert ordering.
	releaseInitialization, err := lockSubmissionPredecessors(ctx, []string{path})
	if err != nil {
		t.Fatal("initialization gate retained during predecessor wait", err)
	}
	releaseInitialization()
	cancel()
	err = <-result
	joined = true
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestSubmissionDatabaseInitializationGateCancellationAndExistingStore(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "attempts.sqlite")
	options := ClientOptions{SubmissionDatabasePath: path, SubmissionLedgerPath: filepath.Join(dir, "attempts.json")}
	release, err := lockSubmissionPredecessors(ctx, []string{path})
	if err != nil {
		t.Fatal(err)
	}
	client := NewWithOptions("", options)
	t.Cleanup(client.Close)
	wait, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	err = client.openSubmissionDatabaseLocked(wait)
	release()
	if !errors.Is(err, context.DeadlineExceeded) || client.submission != nil {
		t.Fatal("canceled initialization gained connection", err)
	}
	if err = client.openSubmissionDatabaseLocked(ctx); err != nil {
		t.Fatal(err)
	}
	// A retained initialized connection does not need the bootstrap gate again.
	release, err = lockSubmissionPredecessors(ctx, []string{path})
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if err = client.openSubmissionDatabaseLocked(ctx); err != nil {
		t.Fatal(err)
	}
}

package codex

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestSubmissionLedgerAcceptsExactSizeLimitAndRejectsOverflow(t *testing.T) {
	client := newTestClient(filepath.Join(t.TempDir(), "app-server.sock"))
	defer client.Close()
	if err := client.recordQueueAttempt("initial-thread", "initial-id", "initial text"); err != nil {
		t.Fatal(err)
	}
	initialInfo, err := os.Stat(client.queueLedgerPath)
	if err != nil {
		t.Fatal(err)
	}

	// A single valid, long thread key makes the serialized boundary exact.
	digest := queueTextDigest("body")
	sample, err := json.Marshal(queueAttemptLedger{
		Schema: 3, Attempts: map[string]map[string]string{"t": {"id": digest}},
	})
	if err != nil {
		t.Fatal(err)
	}
	threadID := "t" + strings.Repeat("x", queueLedgerMaxSize-len(sample)-1)
	if err := queueLedgerAction(client, func() error {
		previous := client.queueAttempts
		client.queueAttempts = map[string]map[string]string{threadID: {"id": digest}}
		if err := client.writeQueueLedgerLocked(); err != nil {
			client.queueAttempts = previous
			return err
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(client.queueLedgerPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(before) != queueLedgerMaxSize || len(before) <= 1024*1024 {
		t.Fatalf("ledger size = %d, want %d", len(before), queueLedgerMaxSize)
	}
	fullInfo, err := os.Stat(client.queueLedgerPath)
	if err != nil || fullInfo.Mode().Perm() != 0o600 || os.SameFile(initialInfo, fullInfo) {
		t.Fatalf("ledger replacement mode or inode = %v, %v", fullInfo, err)
	}
	reopened := newTestClient(client.socket)
	defer reopened.Close()
	if found, err := reopened.queueAttempt(threadID, "id", "body"); err != nil || !found {
		t.Fatalf("read at exact limit = %t, %v", found, err)
	}
	if err := reopened.recordQueueAttempt(threadID, "extra", "more text"); !errors.Is(err, errQueueLedgerTooLarge) {
		t.Fatalf("write over limit error = %v", err)
	}
	after, err := os.ReadFile(client.queueLedgerPath)
	if err != nil || !bytes.Equal(after, before) {
		t.Fatalf("refused write changed ledger bytes: %v", err)
	}
	afterInfo, err := os.Stat(client.queueLedgerPath)
	if err != nil || !os.SameFile(fullInfo, afterInfo) || afterInfo.Mode().Perm() != 0o600 {
		t.Fatalf("refused write replaced ledger or changed mode: %v, %v", afterInfo, err)
	}

	// Padding is valid JSON whitespace. The file must fail at the size check.
	oversized := append(bytes.Clone(before), ' ')
	if err := os.WriteFile(client.queueLedgerPath, oversized, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.queueAttempt(threadID, "id", "body"); !errors.Is(err, errQueueLedgerTooLarge) {
		t.Fatalf("read over limit error = %v", err)
	}
	after, err = os.ReadFile(client.queueLedgerPath)
	if err != nil || !bytes.Equal(after, oversized) {
		t.Fatalf("refused read changed ledger bytes: %v", err)
	}
	afterInfo, err = os.Stat(client.queueLedgerPath)
	if err != nil || afterInfo.Mode().Perm() != 0o600 {
		t.Fatalf("refused read changed ledger mode: %v, %v", afterInfo, err)
	}
}

func TestCompactAcceptedSendOptionsPreservesRetryIdentityAndOtherContexts(t *testing.T) {
	client := newTestClient(filepath.Join(t.TempDir(), "app-server.sock"))
	defer client.Close()
	options := testTurnOptions()
	for _, entry := range []struct {
		thread, id, action, state string
		steered                   bool
	}{
		{"member-thread", "team-accepted", "team:assign:one", "accepted", true},
		{"other-thread", "other-team-accepted", "team:assign:two", "accepted", false},
		{"member-thread", "browser-accepted", "browser:message", "accepted", false},
		{"member-thread", "other-accepted", "teammate:message", "accepted", false},
		{"member-thread", "team-prepared", "team:assign:three", "prepared", false},
		{"member-thread", "team-submitting", "team:assign:four", "submitting", false},
	} {
		if err := client.PrepareSendWithOptions(
			entry.thread, "message", entry.id, entry.action, entry.steered, options,
		); err != nil {
			t.Fatal(err)
		}
		if entry.state != "prepared" {
			if err := client.markSendSubmitting(entry.thread, entry.id); err != nil {
				t.Fatal(err)
			}
		}
		if entry.state == "accepted" {
			if err := client.markSendAccepted(entry.thread, entry.id, SendReceipt{
				TurnID: "turn-" + entry.id, ClientUserMessageID: entry.id, Steered: entry.steered,
			}); err != nil {
				t.Fatal(err)
			}
		}
	}
	before, err := os.ReadFile(client.queueLedgerPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.CompactAcceptedSendOptions(""); err == nil {
		t.Fatal("empty context prefix was accepted")
	}
	afterEmpty, err := os.ReadFile(client.queueLedgerPath)
	if err != nil || !bytes.Equal(afterEmpty, before) {
		t.Fatalf("empty prefix changed ledger bytes: %v", err)
	}
	if err := client.CompactAcceptedSendOptions("team:"); err != nil {
		t.Fatal(err)
	}
	firstInfo, err := os.Stat(client.queueLedgerPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range []struct {
		thread, id, action string
		compacted          bool
	}{
		{"member-thread", "team-accepted", "team:assign:one", true},
		{"other-thread", "other-team-accepted", "team:assign:two", true},
		{"member-thread", "browser-accepted", "browser:message", false},
		{"member-thread", "other-accepted", "teammate:message", false},
		{"member-thread", "team-prepared", "team:assign:three", false},
		{"member-thread", "team-submitting", "team:assign:four", false},
	} {
		attempt, found, err := client.sendAttemptWithOptions(
			entry.thread, entry.id, "message", entry.action, mustTurnOptionsDigest(t, options),
		)
		if err != nil || !found || (attempt.Options == nil) != entry.compacted ||
			attempt.OptionsDigest == "" || attempt.Digest != queueTextDigest("message") ||
			attempt.Context != entry.action {
			t.Fatalf("%s after compaction = %#v, %t, %v", entry.id, attempt, found, err)
		}
		if !entry.compacted {
			original, found, err := client.OriginalSendOptions(entry.thread, "message", entry.id, entry.action)
			if err != nil || !found || !reflect.DeepEqual(original, options) {
				t.Fatalf("%s original options = %#v, %t, %v", entry.id, original, found, err)
			}
		}
	}
	if _, _, err := client.OriginalSendOptions("member-thread", "message", "team-accepted", "team:assign:one"); err == nil || !strings.Contains(err.Error(), "unavailable") {
		t.Fatalf("compacted original options error = %v", err)
	}
	if err := client.CompactAcceptedSendOptions("team:"); err != nil {
		t.Fatal(err)
	}
	secondInfo, err := os.Stat(client.queueLedgerPath)
	if err != nil || !os.SameFile(firstInfo, secondInfo) {
		t.Fatalf("idempotent compaction rewrote ledger: %v, %v", secondInfo, err)
	}

	restarted := newTestClient(client.socket)
	defer restarted.Close()
	receipt, err := restarted.SendWithOptions(
		context.Background(), "member-thread", "message", "team-accepted", "team:assign:one", options,
	)
	if err != nil || receipt != (SendReceipt{
		TurnID: "turn-team-accepted", ClientUserMessageID: "team-accepted", Steered: true,
	}) {
		t.Fatalf("accepted retry = %#v, %v", receipt, err)
	}
	changed := options
	changed.Model = "different"
	for _, retry := range []struct {
		text, action string
		options      TurnOptions
	}{
		{"different message", "team:assign:one", options},
		{"message", "team:assign:one", changed},
		{"message", "team:assign:different", options},
	} {
		if _, err := restarted.SendWithOptions(
			context.Background(), "member-thread", retry.text, "team-accepted", retry.action, retry.options,
		); err == nil || !strings.Contains(err.Error(), "another action") {
			t.Fatalf("changed retry error = %v", err)
		}
	}
}

func mustTurnOptionsDigest(t *testing.T, options TurnOptions) string {
	t.Helper()
	normalized, err := normalizeTurnOptions(options)
	if err != nil {
		t.Fatal(err)
	}
	return normalized.Digest
}

func TestClearRetiredThreadAttemptsPreservesOtherThreadAndOperationMarkers(t *testing.T) {
	client := newTestClient(filepath.Join(t.TempDir(), "app-server.sock"))
	defer client.Close()
	if err := client.recordQueueAttempt("member-thread", "queued", "queued text"); err != nil {
		t.Fatal(err)
	}
	if err := client.PrepareSend("member-thread", "sent text", "sent", "team:assign", false); err != nil {
		t.Fatal(err)
	}
	if _, err := client.recordQueueDeletionAttempt("member-thread", QueueEntry{
		ID: "deleting", ClientUserMessageID: "queued", Text: "queued text",
	}); err != nil {
		t.Fatal(err)
	}
	if err := client.recordQueueAttempt("root-thread", "root-queued", "root text"); err != nil {
		t.Fatal(err)
	}
	for _, marker := range []struct{ directory, thread string }{
		{"/workspace/root", "root-thread"},
		{"/workspace/member", "member-thread"},
	} {
		if err := client.RecordThreadOperationAttempt(marker.directory, marker.thread); err != nil {
			t.Fatal(err)
		}
	}
	if err := client.ClearRetiredThreadAttempts(""); err == nil {
		t.Fatal("empty thread ID was accepted")
	}
	if err := client.ClearRetiredThreadAttempts("member-thread"); err != nil {
		t.Fatal(err)
	}
	firstInfo, err := os.Stat(client.queueLedgerPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.ClearRetiredThreadAttempts("member-thread"); err != nil {
		t.Fatal(err)
	}
	secondInfo, err := os.Stat(client.queueLedgerPath)
	if err != nil || !os.SameFile(firstInfo, secondInfo) {
		t.Fatalf("idempotent cleanup rewrote ledger: %v, %v", secondInfo, err)
	}
	restarted := newTestClient(client.socket)
	defer restarted.Close()
	if err := queueLedgerAction(restarted, func() error {
		if _, found := restarted.queueAttempts["member-thread"]; found {
			t.Error("retired queue attempts remain")
		}
		if _, found := restarted.sendAttempts["member-thread"]; found {
			t.Error("retired send attempts remain")
		}
		if _, found := restarted.queueDeletions["member-thread"]; found {
			t.Error("retired deletion attempts remain")
		}
		if len(restarted.queueAttempts["root-thread"]) != 1 {
			t.Error("root queue attempt was removed")
		}
		if restarted.operations["/workspace/root"] != "root-thread" ||
			restarted.operations["/workspace/member"] != "member-thread" {
			t.Error("operation marker was removed")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

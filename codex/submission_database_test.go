package codex

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func databaseTestClient(t *testing.T) *Client {
	t.Helper()
	dir := t.TempDir()
	c := NewWithOptions(filepath.Join(dir, "absent.sock"), ClientOptions{SubmissionLedgerPath: filepath.Join(dir, "attempts.json"), SubmissionDatabasePath: filepath.Join(dir, "attempts.sqlite")})
	t.Cleanup(c.Close)
	return c
}

func TestSubmissionDatabaseImportsExactStateAndFencesOldWriters(t *testing.T) {
	dir := t.TempDir()
	socket := filepath.Join(dir, "absent.sock")
	ledger := filepath.Join(dir, "attempts.json")
	old := NewWithOptions(socket, ClientOptions{SubmissionLedgerPath: ledger})
	defer old.Close()
	for _, state := range []string{"prepared", "submitting", "accepted"} {
		if err := old.recordSendAttempt("thread", state, "text", "plan:original", false); err != nil {
			t.Fatal(err)
		}
		if state != "prepared" {
			if err := old.markSendSubmitting("thread", state); err != nil {
				t.Fatal(err)
			}
		}
		if state == "accepted" {
			if err := old.markSendAccepted("thread", state, SendReceipt{TurnID: "turn", ClientUserMessageID: state}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := old.recordQueueAttempt("other", "queue", "queued text"); err != nil {
		t.Fatal(err)
	}
	if _, err := old.recordQueueDeletionAttempt("other", QueueEntry{ID: "queued-entry", ClientUserMessageID: "queue", Text: "queued text"}); err != nil {
		t.Fatal(err)
	}
	if err := old.RecordThreadOperationAttempt("/retired", "retired-thread"); err != nil {
		t.Fatal(err)
	}
	before, err := old.ExportSubmissionAttempts("thread")
	if err != nil {
		t.Fatal(err)
	}
	options := ClientOptions{SubmissionLedgerPath: ledger, SubmissionDatabasePath: filepath.Join(dir, "attempts.sqlite")}
	c := NewWithOptions(socket, options)
	defer c.Close()
	after, err := c.ExportSubmissionAttempts("thread")
	if err != nil {
		t.Fatal(err)
	}
	var a, b queueAttemptLedger
	if json.Unmarshal(before, &a) != nil || json.Unmarshal(after, &b) != nil {
		t.Fatal("invalid snapshot")
	}
	// JSON exports are explicitly scoped, including on the legacy backend.
	if !reflect.DeepEqual(a.Sends["thread"], b.Sends["thread"]) {
		t.Fatalf("send states changed: %#v", b)
	}
	if bound, err := c.ThreadOperationAttempt("/retired"); err != nil || bound != "retired-thread" {
		t.Fatalf("retirement lost: %s %v", bound, err)
	}
	if queued, err := c.queueAttempt("other", "queue", "queued text"); err != nil || !queued {
		t.Fatalf("queue lost: %t %v", queued, err)
	}
	if deletion, found, err := c.queueDeletionAttempt("other", "queued-entry"); err != nil || !found || deletion.ClientUserMessageID != "queue" {
		t.Fatalf("deletion lost: %#v %t %v", deletion, found, err)
	}
	if err := old.recordQueueAttempt("third", "new", "must refuse"); err == nil {
		t.Fatal("old writer accepted migrated state")
	}
	c.Close()
	restarted := NewWithOptions(socket, options)
	defer restarted.Close()
	if _, found, err := restarted.sendAttempt("thread", "prepared", "text", "plan:original"); err != nil || !found {
		t.Fatalf("restart lost import: %t %v", found, err)
	}
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if info, err := os.Stat(options.SubmissionDatabasePath + suffix); err == nil && info.Mode().Perm() != 0600 {
			t.Fatalf("database permissions %s: %v", suffix, info.Mode())
		}
	}
}

func TestSubmissionDatabaseResumesInterruptedFencePublication(t *testing.T) {
	c := databaseTestClient(t)
	if err := c.recordSendAttempt("thread", "id", "message", "", false); err != nil {
		t.Fatal(err)
	}
	if _, err := c.submission.db.Exec("UPDATE submission_metadata SET value='imported' WHERE key='migration_phase'"); err != nil {
		t.Fatal(err)
	}
	// Simulate a crash after the database commit but before publishing the fence
	// for a predecessor that was originally absent.
	if err := os.Remove(c.queueLedgerPath); err != nil {
		t.Fatal(err)
	}
	c.Close()
	restarted := NewWithOptions(c.socket, c.options)
	defer restarted.Close()
	if _, found, err := restarted.sendAttempt("thread", "id", "message", ""); err != nil || !found {
		t.Fatalf("interrupted import did not resume: %t %v", found, err)
	}
}

func TestSubmissionDatabaseRejectsCorruptImportWithoutFencing(t *testing.T) {
	c := databaseTestClient(t)
	data := []byte(`{"schema":3,"attempts":{},"sends":{"thread":{"id":{"digest":"bad","state":"accepted","steered":false}}}}`)
	if err := os.WriteFile(c.queueLedgerPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.sendAttempt("thread", "id", "text", ""); err == nil {
		t.Fatal("corrupt legacy state accepted")
	}
	after, err := os.ReadFile(c.queueLedgerPath)
	if err != nil || !bytes.Equal(data, after) {
		t.Fatal("failed import changed predecessor")
	}
}

func TestSubmissionDatabaseScopesReadsAndExceedsFormerCapacity(t *testing.T) {
	c := databaseTestClient(t)
	if err := c.recordSendAttempt("target", "target", "text", "", false); err != nil {
		t.Fatal(err)
	}
	seedRetainedSubmissionRows(t, c, 100000)
	before := c.submission.rowsRead.Load()
	if _, found, err := c.sendAttempt("target", "target", "text", ""); err != nil || !found {
		t.Fatalf("large-store lookup: %t %v", found, err)
	}
	if read := c.submission.rowsRead.Load() - before; read != 1 {
		t.Fatalf("one attempt loaded %d records", read)
	}
	if err := c.recordSendAttempt("target", "second", "second text", "", false); err != nil {
		t.Fatal(err)
	}
	var bytes int64
	if err := c.submission.db.QueryRow("SELECT sum(length(payload)) FROM submission_records").Scan(&bytes); err != nil {
		t.Fatal(err)
	}
	if bytes <= queueLedgerMaxSize {
		t.Fatalf("fixture did not exceed former capacity: %d", bytes)
	}
	snapshot, err := c.ExportSubmissionAttempts("target")
	if err != nil {
		t.Fatal(err)
	}
	var exported queueAttemptLedger
	if err := json.Unmarshal(snapshot, &exported); err != nil {
		t.Fatal(err)
	}
	if len(exported.Sends) != 1 || len(exported.Sends["target"]) != 2 {
		t.Fatalf("export leaked unrelated threads: %#v", exported.Sends)
	}
}

func TestSubmissionDatabaseKeepsConcurrentAttempts(t *testing.T) {
	c := databaseTestClient(t)
	if err := c.recordSendAttempt("thread", "initial", "text", "", false); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 8)
	for i := 0; i < 8; i++ {
		go func(i int) {
			peer := NewWithOptions(c.socket, c.options)
			defer peer.Close()
			done <- peer.recordSendAttempt("thread", fmt.Sprint(i), "text", "", false)
		}(i)
	}
	for i := 0; i < 8; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := c.ExportSubmissionAttempts("thread")
	if err != nil {
		t.Fatal(err)
	}
	var exported queueAttemptLedger
	if json.Unmarshal(snapshot, &exported) != nil || len(exported.Sends["thread"]) != 9 {
		t.Fatal("concurrent write lost an attempt")
	}
}

func TestSubmissionDatabaseUnwrittenRecoveryRequiresOriginalClaim(t *testing.T) {
	c := databaseTestClient(t)
	if err := c.recordSendAttempt("thread", "id", "text", "", false); err != nil {
		t.Fatal(err)
	}
	first, err := c.claimSendSubmission("thread", "id")
	if err != nil {
		t.Fatal(err)
	}
	if err := c.restoreRejectedSendClaim("thread", "id", first); err != nil {
		t.Fatal(err)
	}
	second, err := c.claimSendSubmission("thread", "id")
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("admission reused its claim")
	}
	if err := c.restoreRejectedSendClaim("thread", "id", first); err == nil {
		t.Fatal("stale admission reset a newer claim")
	}
	if err := c.restoreRejectedSendClaim("thread", "id", second); err != nil {
		t.Fatal(err)
	}
}

func TestSubmissionAdmissionRechecksExactMessage(t *testing.T) {
	c := databaseTestClient(t)
	if err := c.recordSendAttempt("thread", "id", "replacement", "new-action", false); err != nil {
		t.Fatal(err)
	}
	if _, err := c.claimSendSubmissionExact("thread", "id", "original", "old-action", zeroTurnOptionsDigest(), false); err == nil {
		t.Fatal("stale sender claimed replacement input")
	}
	attempt, found, err := c.sendAttempt("thread", "id", "replacement", "new-action")
	if err != nil || !found || attempt.State != "prepared" {
		t.Fatalf("failed claim changed replacement: %#v %v", attempt, err)
	}
}

func seedRetainedSubmissionRows(t *testing.T, c *Client, count int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tx, err := c.submission.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	statement, err := tx.PrepareContext(ctx, `INSERT INTO submission_records(kind,thread_id,attempt_id,payload,state,action,has_options) VALUES('sends',?,?,?,'accepted','',0)`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Close()
	data, _ := json.Marshal(sendAttempt{Digest: queueTextDigest("historical"), State: "accepted", TurnID: "turn", Context: strings.Repeat("retained-", 16), Steered: false})
	for i := 0; i < count; i++ {
		if _, err := statement.ExecContext(ctx, fmt.Sprintf("thread-%d", i%64), fmt.Sprint(i), data); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestSubmissionDatabaseDoesNotRecreateMissingReceiptTable(t *testing.T) {
	c := databaseTestClient(t)
	if err := c.recordSendAttempt("thread", "id", "message", "", false); err != nil {
		t.Fatal(err)
	}
	if _, err := c.submission.db.Exec("DROP TABLE submission_records"); err != nil {
		t.Fatal(err)
	}
	c.Close()
	restarted := NewWithOptions(c.socket, c.options)
	defer restarted.Close()
	if _, _, err := restarted.sendAttempt("thread", "id", "message", ""); err == nil {
		t.Fatal("missing receipts appeared as an empty database")
	}
}

func TestSubmissionDatabaseRecoversVolatilePredecessorAcrossRestart(t *testing.T) {
	for _, item := range []struct {
		name, phase, backup string
		changed, fails      bool
	}{
		{name: "ready", phase: "ready"},
		{name: "ready-without-old-backup", phase: "ready", backup: "remove"},
		{name: "imported", phase: "imported"},
		{name: "imported-corrupt-backup", phase: "imported", backup: "corrupt", fails: true},
		{name: "changed-predecessor", phase: "ready", changed: true, fails: true},
	} {
		t.Run(item.name, func(t *testing.T) {
			root := t.TempDir()
			volatile := filepath.Join(root, "runtime")
			if err := os.MkdirAll(volatile, 0700); err != nil {
				t.Fatal(err)
			}
			legacy := filepath.Join(volatile, "attempts.json")
			socket := filepath.Join(volatile, "absent.sock")
			old := NewWithOptions(socket, ClientOptions{SubmissionLedgerPath: legacy})
			if err := old.recordSendAttempt("thread", "id", "original", "plan:original", false); err != nil {
				t.Fatal(err)
			}
			if err := old.markSendSubmitting("thread", "id"); err != nil {
				t.Fatal(err)
			}
			old.Close()
			persistent := filepath.Join(root, "persistent")
			options := ClientOptions{SubmissionLedgerPath: filepath.Join(persistent, "attempts.json"), LegacySubmissionLedgerPath: legacy, SubmissionDatabasePath: filepath.Join(persistent, "attempts.sqlite")}
			client := NewWithOptions(socket, options)
			if _, found, err := client.sendAttempt("thread", "id", "original", "plan:original"); err != nil || !found {
				t.Fatalf("initial import: found=%t %v", found, err)
			}
			encoded, err := submissionMetadata(context.Background(), client.submission.db, "import_sources")
			if err != nil {
				t.Fatal(err)
			}
			var sources []submissionImportSource
			if err := json.Unmarshal([]byte(encoded), &sources); err != nil {
				t.Fatal(err)
			}
			backup := sources[1].Backup
			if filepath.Dir(backup) != persistent || sources[1].Digest == "" {
				t.Fatalf("volatile backup is not persistent: %s", backup)
			}
			if info, err := os.Stat(backup); err != nil || info.Mode().Perm() != 0600 {
				t.Fatalf("backup unavailable or not private: %v", err)
			}
			if _, err := client.submission.db.Exec("UPDATE submission_metadata SET value=? WHERE key='migration_phase'", item.phase); err != nil {
				t.Fatal(err)
			}
			client.Close()
			if err := os.RemoveAll(volatile); err != nil {
				t.Fatal(err)
			}
			if item.backup == "remove" {
				if err := os.Remove(backup); err != nil {
					t.Fatal(err)
				}
			} else if item.backup == "corrupt" {
				if err := os.WriteFile(backup, []byte("changed"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			changed := []byte(`{"schema":3,"attempts":{}}`)
			if item.changed {
				if err := os.MkdirAll(volatile, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(legacy, changed, 0600); err != nil {
					t.Fatal(err)
				}
			}
			restarted := NewWithOptions(socket, options)
			defer restarted.Close()
			attempt, found, err := restarted.sendAttempt("thread", "id", "original", "plan:original")
			if item.fails {
				if err == nil {
					t.Fatal("changed migration evidence was accepted")
				}
				if item.changed {
					data, err := os.ReadFile(legacy)
					if err != nil || !bytes.Equal(data, changed) {
						t.Fatal("changed predecessor was overwritten")
					}
				} else if _, err := os.Stat(legacy); !os.IsNotExist(err) {
					t.Fatal("unverified predecessor was fenced")
				}
			} else {
				if err != nil || !found || attempt.State != "submitting" {
					t.Fatalf("restart changed unresolved receipt: %#v %t %v", attempt, found, err)
				}
				for _, path := range []string{options.SubmissionLedgerPath, legacy} {
					data, err := os.ReadFile(path)
					var fence submissionFence
					if err != nil || json.Unmarshal(data, &fence) != nil || fence.Schema != 4 || fence.Database != options.SubmissionDatabasePath {
						t.Fatalf("missing restored fence: %s %v", path, err)
					}
				}
			}
			db, err := sql.Open("sqlite", options.SubmissionDatabasePath)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var payload []byte
			if err := db.QueryRow("SELECT payload FROM submission_records WHERE kind='sends' AND thread_id='thread' AND attempt_id='id'").Scan(&payload); err != nil {
				t.Fatal(err)
			}
			var saved sendAttempt
			if err := json.Unmarshal(payload, &saved); err != nil || saved.State != "submitting" || saved.Digest != queueTextDigest("original") {
				t.Fatal("persisted original receipt was lost or reset")
			}
		})
	}
}

func TestSubmissionDatabaseRefusesOrphanedDurableBackup(t *testing.T) {
	root := t.TempDir()
	volatile := filepath.Join(root, "runtime")
	if err := os.MkdirAll(volatile, 0700); err != nil {
		t.Fatal(err)
	}
	legacy := filepath.Join(volatile, "attempts.json")
	socket := filepath.Join(volatile, "absent.sock")
	old := NewWithOptions(socket, ClientOptions{SubmissionLedgerPath: legacy})
	if err := old.recordSendAttempt("thread", "id", "unresolved", "", false); err != nil {
		t.Fatal(err)
	}
	if err := old.markSendSubmitting("thread", "id"); err != nil {
		t.Fatal(err)
	}
	old.Close()
	original, err := os.ReadFile(legacy)
	if err != nil {
		t.Fatal(err)
	}
	options := ClientOptions{SubmissionLedgerPath: filepath.Join(root, "attempts.json"), LegacySubmissionLedgerPath: legacy, SubmissionDatabasePath: filepath.Join(root, "attempts.sqlite")}
	client := NewWithOptions(socket, options)
	if _, found, err := client.sendAttempt("thread", "id", "unresolved", ""); err != nil || !found {
		t.Fatalf("initial import: %t %v", found, err)
	}
	// Reconstruct a crash after the durable backup but before the data/journal
	// transaction committed. Atomic schema bootstrap is already durable.
	if _, err := client.submission.db.Exec("DELETE FROM submission_records; DELETE FROM submission_metadata;"); err != nil {
		t.Fatal(err)
	}
	client.Close()
	if err := os.Remove(options.SubmissionLedgerPath); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(volatile); err != nil {
		t.Fatal(err)
	}
	restarted := NewWithOptions(socket, options)
	defer restarted.Close()
	if _, _, err := restarted.sendAttempt("thread", "id", "unresolved", ""); err == nil {
		t.Fatal("orphaned durable evidence was accepted as an empty store")
	}
	for _, path := range []string{options.SubmissionLedgerPath, legacy} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("ambiguous predecessor was fenced: %s", path)
		}
	}
	backups, err := filepath.Glob(options.SubmissionDatabasePath + ".schema3-backup-*")
	if err != nil || len(backups) != 1 {
		t.Fatalf("durable original backup lost: %v %v", backups, err)
	}
	if data, err := os.ReadFile(backups[0]); err != nil || !bytes.Equal(data, original) {
		t.Fatal("durable original backup changed")
	}
	db, err := sql.Open("sqlite", options.SubmissionDatabasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var count int
	if err := db.QueryRow("SELECT count(*) FROM submission_metadata").Scan(&count); err != nil || count != 0 {
		t.Fatalf("ambiguous migration published an authoritative journal: %d %v", count, err)
	}
}

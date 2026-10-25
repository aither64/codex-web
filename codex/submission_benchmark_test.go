package codex

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func BenchmarkSubmissionStoreLookup(b *testing.B) {
	// Match the occupied host's record count and approximate file size without
	// copying conversation content or application policies into the fixture.
	ledger := queueAttemptLedger{Schema: 3, Attempts: map[string]map[string]string{}, Sends: map[string]map[string]sendAttempt{}}
	action := strings.Repeat("synthetic-", 52)
	for i := 0; i < 22000; i++ {
		thread := fmt.Sprintf("thread-%d", i%164)
		if ledger.Sends[thread] == nil {
			ledger.Sends[thread] = map[string]sendAttempt{}
		}
		ledger.Sends[thread][fmt.Sprint(i)] = sendAttempt{State: "accepted", Digest: queueTextDigest("message"), Context: action, TurnID: "turn"}
	}
	data, err := json.Marshal(ledger)
	if err != nil {
		b.Fatal(err)
	}
	if len(data) > queueLedgerMaxSize {
		b.Fatal("benchmark exceeds the legacy reader contract")
	}
	for _, sqlite := range []bool{false, true} {
		name := "json"
		if sqlite {
			name = "sqlite"
		}
		b.Run(name, func(b *testing.B) {
			dir := b.TempDir()
			path := filepath.Join(dir, "submissions.json")
			if err := os.WriteFile(path, data, 0600); err != nil {
				b.Fatal(err)
			}
			options := ClientOptions{SubmissionLedgerPath: path}
			if sqlite {
				options.SubmissionDatabasePath = filepath.Join(dir, "submissions.sqlite")
			}
			client := NewWithOptions(filepath.Join(dir, "absent.sock"), options)
			defer client.Close()
			if _, _, err := client.sendAttempt("thread-0", "0", "message", action); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ReportMetric(float64(len(data)), "fixture-bytes")
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, found, err := client.sendAttempt("thread-0", "0", "message", action); err != nil || !found {
					b.Fatalf("lookup: %t %v", found, err)
				}
			}
		})
	}
}

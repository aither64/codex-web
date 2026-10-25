package codex

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

type submissionFence struct {
	Schema   int    `json:"schema"`
	Storage  string `json:"storage"`
	Database string `json:"database"`
}

type submissionImportSource struct {
	Path   string `json:"path"`
	Digest string `json:"digest,omitempty"`
	Backup string `json:"backup,omitempty"`
}

func lockSubmissionFile(ctx context.Context, fd int) error {
	for {
		err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// Bootstrap is atomic: an interrupted schema creation leaves an empty version-0
// database. A version-1 database must retain its tables; never recreate missing
// tables and silently present lost receipts as an empty store.
func createSubmissionSchema(ctx context.Context, db *sql.DB) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err = conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return err
	}
	defer conn.ExecContext(context.Background(), "ROLLBACK")
	var version int
	if err = conn.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return err
	}
	if version == 0 {
		_, err = conn.ExecContext(ctx, `CREATE TABLE submission_metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
		CREATE TABLE submission_records(
		 kind TEXT NOT NULL,thread_id TEXT NOT NULL,attempt_id TEXT NOT NULL,payload BLOB NOT NULL,
		 state TEXT NOT NULL,action TEXT NOT NULL,has_options INTEGER NOT NULL,claim TEXT NOT NULL DEFAULT '',
		 PRIMARY KEY(kind,thread_id,attempt_id));
		CREATE INDEX submission_threads ON submission_records(thread_id,attempt_id);
		CREATE INDEX submission_option_compaction ON submission_records(kind,state,has_options,action);
		PRAGMA user_version=1;`)
		if err != nil {
			return err
		}
	} else if version != 1 {
		return fmt.Errorf("unsupported submission database schema %d", version)
	}
	_, err = conn.ExecContext(ctx, "COMMIT")
	return err
}

func lockSubmissionPredecessors(ctx context.Context, paths []string) (func(), error) {
	lockPaths := append([]string(nil), paths...)
	sort.Strings(lockPaths)
	var locks []*os.File
	release := func() {
		for i := len(locks) - 1; i >= 0; i-- {
			_ = locks[i].Close()
		}
	}
	fail := func(err error) (func(), error) { release(); return nil, err }
	for _, path := range lockPaths {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return fail(errors.New("submission import path must be canonical and absolute"))
		}
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			return fail(err)
		}
		fd, err := unix.Open(path+".lock", unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0600)
		if err != nil {
			return fail(err)
		}
		locks = append(locks, os.NewFile(uintptr(fd), path+".lock"))
		if err := lockSubmissionFile(ctx, fd); err != nil {
			return fail(err)
		}
	}
	return release, nil
}

func initializeSubmissionDatabase(ctx context.Context, store *submissionDatabase, primary, legacy, database string) error {
	if primary == database || legacy == database {
		return errors.New("submission database must differ from predecessor paths")
	}
	var version int
	if err := store.db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return err
	}
	if version != 0 && version != 1 {
		return fmt.Errorf("unsupported submission database schema %d", version)
	}
	if version == 0 {
		if err := createSubmissionSchema(ctx, store.db); err != nil {
			return err
		}
	}
	phase, err := submissionMetadata(ctx, store.db, "migration_phase")
	if err != nil {
		return err
	}
	if phase == "ready" {
		return verifySubmissionFences(ctx, store.db, database, primary, legacy)
	}
	paths := []string{primary}
	if legacy != "" && legacy != primary {
		paths = append(paths, legacy)
	}
	release, err := lockSubmissionPredecessors(ctx, paths)
	if err != nil {
		return err
	}
	defer release()
	phase, err = submissionMetadata(ctx, store.db, "migration_phase")
	if err != nil {
		return err
	}
	if phase == "ready" {
		return verifySubmissionFencesLocked(ctx, store.db, database, primary, legacy)
	}
	var sources []submissionImportSource
	if phase == "imported" {
		encoded, err := submissionMetadata(ctx, store.db, "import_sources")
		if err != nil {
			return err
		}
		if err = json.Unmarshal([]byte(encoded), &sources); err != nil || len(sources) == 0 {
			return errors.New("submission migration journal is invalid")
		}
	} else if phase == "" {
		var chosen []byte
		for _, path := range paths {
			source := submissionImportSource{Path: path}
			info, statErr := os.Lstat(path)
			if statErr == nil && (!info.Mode().IsRegular() || info.Mode().Perm() != 0600) {
				return errors.New("submission import must be a mode-0600 regular file")
			}
			if statErr == nil && info.Size() > queueLedgerMaxSize {
				return errQueueLedgerTooLarge
			}
			if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
				return statErr
			}
			data, err := os.ReadFile(path)
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
			if err == nil {
				info, err := os.Stat(path)
				if err != nil {
					return err
				}
				if !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
					return errors.New("submission import must be a mode-0600 regular file")
				}
				var fence submissionFence
				if json.Unmarshal(data, &fence) == nil && fence.Schema == 4 {
					return errors.New("submission fence exists without its migration journal")
				}
				source.Digest = fmt.Sprintf("%x", sha256.Sum256(data))
				source.Backup = database + ".schema3-backup-" + source.Digest
				if err := preserveSubmissionBackup(source.Backup, data); err != nil {
					return err
				}
				if chosen == nil {
					chosen = data
				}
			}
			sources = append(sources, source)
		}
		if chosen == nil {
			entries, err := os.ReadDir(filepath.Dir(database))
			if err != nil {
				return err
			}
			for _, entry := range entries {
				if strings.HasPrefix(entry.Name(), filepath.Base(database)+".schema3-backup-") {
					return errors.New("submission sources are missing while migration backups remain; restore the authoritative predecessor before retrying")
				}
			}
		}
		ledger := queueAttemptLedger{Schema: 3, Attempts: map[string]map[string]string{}}
		if chosen != nil {
			if len(chosen) > queueLedgerMaxSize {
				return errQueueLedgerTooLarge
			}
			decoder := json.NewDecoder(bytes.NewReader(chosen))
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(&ledger); err != nil {
				return err
			}
			var extra any
			if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
				return errors.New("submission import contains trailing data")
			}
			if ledger.Schema != 3 || ledger.Attempts == nil {
				return errors.New("submission import has an invalid schema")
			}
		}
		temporary := &Client{}
		temporary.resetSubmissionMaps()
		for thread, entries := range ledger.Attempts {
			if thread == "" || entries == nil {
				return errors.New("invalid queued submission group")
			}
			for id, value := range entries {
				data, _ := json.Marshal(value)
				if err := temporary.installSubmissionRecord(submissionKey{"attempts", thread, id}, data); err != nil {
					return err
				}
			}
		}
		for thread, entries := range ledger.Sends {
			if thread == "" || entries == nil {
				return errors.New("invalid send submission group")
			}
			for id, value := range entries {
				data, _ := json.Marshal(value)
				if err := temporary.installSubmissionRecord(submissionKey{"sends", thread, id}, data); err != nil {
					return err
				}
			}
		}
		for thread, entries := range ledger.Deletions {
			if thread == "" || entries == nil {
				return errors.New("invalid deletion submission group")
			}
			for id, value := range entries {
				data, _ := json.Marshal(value)
				if err := temporary.installSubmissionRecord(submissionKey{"deletions", thread, id}, data); err != nil {
					return err
				}
			}
		}
		for directory, thread := range ledger.Operations {
			data, _ := json.Marshal(thread)
			if err := temporary.installSubmissionRecord(submissionKey{"retirements", "", directory}, data); err != nil {
				return err
			}
		}
		records, err := temporary.submissionRecords()
		if err != nil {
			return err
		}
		tx, err := store.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		var existing int
		if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM submission_records").Scan(&existing); err != nil {
			return err
		}
		if existing != 0 {
			return errors.New("uninitialized submission database contains records")
		}
		for key, data := range records {
			if err = writeSubmissionRecord(ctx, tx, key, data); err != nil {
				return err
			}
		}
		encoded, err := json.Marshal(sources)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO submission_metadata(key,value) VALUES('import_sources',?),('migration_phase','imported')`, string(encoded)); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "PRAGMA user_version=1"); err != nil {
			return err
		}
		if err = tx.Commit(); err != nil {
			return err
		}
	} else {
		return errors.New("unknown submission migration phase")
	}
	if len(sources) != len(paths) {
		return errors.New("submission import sources changed")
	}
	fence, _ := json.Marshal(submissionFence{Schema: 4, Storage: "sqlite", Database: database})
	for i, source := range sources {
		if source.Path != paths[i] {
			return errors.New("submission import source identity changed")
		}
		data, err := os.ReadFile(source.Path)
		if err == nil && bytes.Equal(data, fence) {
			continue
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err == nil {
			if source.Digest == "" || fmt.Sprintf("%x", sha256.Sum256(data)) != source.Digest {
				return errors.New("submission source changed during interrupted migration")
			}
		} else if source.Digest != "" {
			// A volatile predecessor can disappear across reboot after import.
			// Its persistent, checksummed backup proves the committed source.
			if source.Backup != database+".schema3-backup-"+source.Digest {
				return errors.New("submission backup identity changed")
			}
			backup, backupErr := os.ReadFile(source.Backup)
			if backupErr != nil || fmt.Sprintf("%x", sha256.Sum256(backup)) != source.Digest {
				return errors.New("submission backup missing or changed during interrupted migration")
			}
		}
		if err := atomicSubmissionFile(source.Path, fence); err != nil {
			return err
		}
	}
	_, err = store.db.ExecContext(ctx, "UPDATE submission_metadata SET value='ready' WHERE key='migration_phase'")
	return err
}

func submissionMetadata(ctx context.Context, db *sql.DB, key string) (string, error) {
	var value string
	err := db.QueryRowContext(ctx, "SELECT value FROM submission_metadata WHERE key=?", key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return value, err
}

func verifySubmissionFences(ctx context.Context, db *sql.DB, database, primary, legacy string) error {
	paths := []string{primary}
	if legacy != "" && legacy != primary {
		paths = append(paths, legacy)
	}
	release, err := lockSubmissionPredecessors(ctx, paths)
	if err != nil {
		return err
	}
	defer release()
	return verifySubmissionFencesLocked(ctx, db, database, primary, legacy)
}

func verifySubmissionFencesLocked(ctx context.Context, db *sql.DB, database, primary, legacy string) error {
	encoded, err := submissionMetadata(ctx, db, "import_sources")
	if err != nil {
		return err
	}
	var sources []submissionImportSource
	if json.Unmarshal([]byte(encoded), &sources) != nil || len(sources) == 0 {
		return errors.New("submission import journal is missing")
	}
	paths := []string{primary}
	if legacy != "" && legacy != primary {
		paths = append(paths, legacy)
	}
	if len(paths) != len(sources) {
		return errors.New("submission database import identities changed")
	}
	var missing []string
	for i, source := range sources {
		if source.Path != paths[i] {
			return errors.New("submission database import identities changed")
		}
		data, err := os.ReadFile(source.Path)
		if errors.Is(err, os.ErrNotExist) {
			missing = append(missing, source.Path)
			continue
		}
		if err != nil {
			return err
		}
		var fence submissionFence
		if json.Unmarshal(data, &fence) != nil || fence.Schema != 4 || fence.Storage != "sqlite" || fence.Database != database {
			return errors.New("submission database predecessor fence changed")
		}
	}
	// Ready database records are authoritative. Restore disposable fences only
	// after every present predecessor and configured identity has been checked.
	fence, _ := json.Marshal(submissionFence{Schema: 4, Storage: "sqlite", Database: database})
	for _, path := range missing {
		if err := atomicSubmissionFile(path, fence); err != nil {
			return err
		}
	}
	return nil
}

func preserveSubmissionBackup(path string, data []byte) error {
	previous, err := os.ReadFile(path)
	if err == nil {
		if !bytes.Equal(previous, data) {
			return errors.New("submission migration backup changed")
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return atomicSubmissionFile(path, data)
}

func atomicSubmissionFile(path string, data []byte) error {
	file, err := os.CreateTemp(filepath.Dir(path), ".submission-migration-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if err = file.Chmod(0600); err == nil {
		_, err = file.Write(data)
	}
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = os.Rename(file.Name(), path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

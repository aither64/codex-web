package codex

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"
)

// A scope bounds every ordinary transaction to the requested message or thread.
// The maps used by the legacy state machine contain only this transaction's rows.
type submissionScope struct {
	thread, directory string
	ids               []string
	all, write        bool
}

func attemptScope(thread string, ids ...string) submissionScope {
	return submissionScope{thread: thread, ids: ids}
}
func threadScope(thread string) submissionScope       { return submissionScope{thread: thread} }
func operationScope(directory string) submissionScope { return submissionScope{directory: directory} }
func conversationScope(thread, directory string) submissionScope {
	return submissionScope{thread: thread, directory: directory}
}
func (s submissionScope) writable() submissionScope { s.write = true; return s }

type submissionKey struct{ kind, thread, id string }
type submissionDatabase struct {
	db       *sql.DB
	conn     *sql.Conn
	ctx      context.Context
	before   map[submissionKey][]byte
	rowsRead atomic.Uint64
}

func (s *submissionDatabase) close() {
	if s.db != nil {
		_ = s.db.Close()
	}
}

func submissionAction(c *Client, scope submissionScope, action func() error) error {
	_, err := submissionTransaction(c, scope.writable(), func() (struct{}, error) { return struct{}{}, action() })
	return err
}

func submissionTransaction[T any](c *Client, scope submissionScope, transaction func() (T, error)) (value T, err error) {
	if c.options.SubmissionDatabasePath == "" {
		return queueLedgerTransaction(c, transaction)
	}
	started := time.Now()
	defer func() {
		identity := deliveryIdentity{thread: scope.thread}
		if len(scope.ids) == 1 {
			identity.client = scope.ids[0]
		}
		ctx := context.WithValue(context.Background(), deliveryIdentityKey{}, identity)
		c.observeDelivery(ctx, "submission-transaction", started, err)
	}()
	lockContext, stopLock := context.WithTimeout(context.Background(), 10*time.Second)
	defer stopLock()
	if err = c.queueMu.LockContext(lockContext); err != nil {
		return value, err
	}
	defer c.queueMu.Unlock()
	c.connectionMu.Lock()
	closed := c.closed
	c.connectionMu.Unlock()
	if closed {
		return value, errors.New("Codex client is closed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err = c.openSubmissionDatabaseLocked(ctx); err != nil {
		return value, err
	}
	store := c.submission
	conn, err := store.db.Conn(ctx)
	if err != nil {
		return value, err
	}
	defer conn.Close()
	begin := "BEGIN"
	if scope.write {
		begin = "BEGIN IMMEDIATE"
	}
	if _, err = conn.ExecContext(ctx, begin); err != nil {
		return value, err
	}
	store.conn, store.ctx = conn, ctx
	defer func() { _, _ = conn.ExecContext(context.Background(), "ROLLBACK"); store.conn = nil; store.ctx = nil }()
	if err = c.readSubmissionRecordsLocked(scope); err != nil {
		return value, err
	}
	value, err = transaction()
	if err != nil {
		return value, err
	}
	_, err = conn.ExecContext(ctx, "COMMIT")
	return value, err
}

func (c *Client) openSubmissionDatabaseLocked(ctx context.Context) error {
	if c.submission != nil {
		return nil
	}
	path := c.options.SubmissionDatabasePath
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return errors.New("submission database path must be canonical and absolute")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	info, statErr := file.Stat()
	_ = file.Close()
	if statErr != nil {
		return statErr
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		return errors.New("submission database must be a mode-0600 regular file")
	}
	dsn := &url.URL{Scheme: "file", Path: path}
	query := dsn.Query()
	for _, pragma := range []string{"busy_timeout(5000)", "journal_mode(WAL)", "synchronous(FULL)"} {
		query.Add("_pragma", pragma)
	}
	dsn.RawQuery = query.Encode()
	db, err := sql.Open("sqlite", dsn.String())
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(1)
	store := &submissionDatabase{db: db}
	if err = initializeSubmissionDatabase(ctx, store, c.queueLedgerPath, c.options.LegacySubmissionLedgerPath, path); err != nil {
		db.Close()
		return err
	}
	c.submission = store
	return nil
}

func (c *Client) resetSubmissionMaps() {
	c.queueAttempts = make(map[string]map[string]string)
	c.sendAttempts = make(map[string]map[string]sendAttempt)
	c.queueDeletions = make(map[string]map[string]queueDeletionAttempt)
	c.operations = make(map[string]string)
}

func (c *Client) readSubmissionRecordsLocked(scope submissionScope) error {
	c.resetSubmissionMaps()
	store := c.submission
	store.before = make(map[submissionKey][]byte)
	where := []string{}
	args := []any{}
	if scope.all {
		where = append(where, "1")
	}
	if scope.thread != "" {
		clause := "(thread_id = ?"
		args = append(args, scope.thread)
		if len(scope.ids) != 0 {
			clause += " AND attempt_id IN (" + strings.TrimSuffix(strings.Repeat("?,", len(scope.ids)), ",") + ")"
			for _, id := range scope.ids {
				args = append(args, id)
			}
		}
		where = append(where, clause+")")
	}
	if scope.directory != "" {
		where = append(where, "(kind = 'retirements' AND thread_id = '' AND attempt_id = ?)")
		args = append(args, scope.directory)
	}
	if len(where) == 0 {
		return errors.New("submission transaction has no scope")
	}
	rows, err := store.conn.QueryContext(store.ctx, "SELECT kind, thread_id, attempt_id, payload,claim FROM submission_records WHERE "+strings.Join(where, " OR "), args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var key submissionKey
		var data []byte
		var claim string
		if err := rows.Scan(&key.kind, &key.thread, &key.id, &data, &claim); err != nil {
			return err
		}
		if err := c.installSubmissionRecord(key, data); err != nil {
			return err
		}
		if key.kind == "sends" {
			attempt := c.sendAttempts[key.thread][key.id]
			attempt.admissionID = claim
			c.sendAttempts[key.thread][key.id] = attempt
		}
		store.before[key] = data
		store.rowsRead.Add(1)
	}
	return rows.Err()
}

func (c *Client) installSubmissionRecord(key submissionKey, data []byte) error {
	decode := func(target any) error {
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		return decoder.Decode(target)
	}
	if key.id == "" {
		return errors.New("submission has no attempt identity")
	}
	switch key.kind {
	case "attempts":
		var digest string
		if err := decode(&digest); err != nil {
			return err
		}
		if key.thread == "" || !validSubmissionDigest(digest) {
			return errors.New("invalid queued submission")
		}
		if c.queueAttempts[key.thread] == nil {
			c.queueAttempts[key.thread] = make(map[string]string)
		}
		c.queueAttempts[key.thread][key.id] = digest
	case "sends":
		var attempt sendAttempt
		if err := decode(&attempt); err != nil {
			return err
		}
		if key.thread == "" || !validSubmissionDigest(attempt.Digest) || !validSubmissionDigest(effectiveTurnOptionsDigest(attempt.OptionsDigest)) || !validSendAttemptState(attempt) {
			return errors.New("invalid send attempt")
		}
		if attempt.Options != nil {
			normalized, err := normalizeTurnOptions(*attempt.Options)
			if err != nil || normalized.Digest != effectiveTurnOptionsDigest(attempt.OptionsDigest) {
				return errors.New("invalid stored send options")
			}
		}
		attempt.OptionsDigest = persistedTurnOptionsDigest(effectiveTurnOptionsDigest(attempt.OptionsDigest))
		if c.sendAttempts[key.thread] == nil {
			c.sendAttempts[key.thread] = make(map[string]sendAttempt)
		}
		c.sendAttempts[key.thread][key.id] = attempt
	case "deletions":
		var attempt queueDeletionAttempt
		if err := decode(&attempt); err != nil {
			return err
		}
		if key.thread == "" || attempt.ClientUserMessageID == "" || !validSubmissionDigest(attempt.Digest) {
			return errors.New("invalid deletion intent")
		}
		if c.queueDeletions[key.thread] == nil {
			c.queueDeletions[key.thread] = make(map[string]queueDeletionAttempt)
		}
		c.queueDeletions[key.thread][key.id] = attempt
	case "retirements":
		var thread string
		if err := decode(&thread); err != nil {
			return err
		}
		if key.thread != "" || !filepath.IsAbs(key.id) || thread == "" {
			return errors.New("invalid retirement marker")
		}
		c.operations[key.id] = thread
	default:
		return fmt.Errorf("unknown submission record kind %q", key.kind)
	}
	return nil
}

func (c *Client) submissionRecords() (map[submissionKey][]byte, error) {
	records := make(map[submissionKey][]byte)
	put := func(kind, thread, id string, value any) error {
		encoded, err := json.Marshal(value)
		if err == nil {
			records[submissionKey{kind, thread, id}] = encoded
		}
		return err
	}
	for thread, entries := range c.queueAttempts {
		for id, value := range entries {
			if err := put("attempts", thread, id, value); err != nil {
				return nil, err
			}
		}
	}
	for thread, entries := range c.sendAttempts {
		for id, value := range entries {
			if err := put("sends", thread, id, value); err != nil {
				return nil, err
			}
		}
	}
	for thread, entries := range c.queueDeletions {
		for id, value := range entries {
			if err := put("deletions", thread, id, value); err != nil {
				return nil, err
			}
		}
	}
	for directory, thread := range c.operations {
		if err := put("retirements", "", directory, thread); err != nil {
			return nil, err
		}
	}
	return records, nil
}

func (c *Client) writeSubmissionRecordsLocked() error {
	store := c.submission
	records, err := c.submissionRecords()
	if err != nil {
		return err
	}
	for key := range store.before {
		if _, exists := records[key]; exists {
			continue
		}
		if _, err := store.conn.ExecContext(store.ctx, "DELETE FROM submission_records WHERE kind=? AND thread_id=? AND attempt_id=?", key.kind, key.thread, key.id); err != nil {
			return err
		}
	}
	for key, data := range records {
		if bytes.Equal(store.before[key], data) {
			continue
		}
		if err := writeSubmissionRecord(store.ctx, store.conn, key, data, c.sendAttempts[key.thread][key.id].admissionID); err != nil {
			return err
		}
	}
	store.before = records
	return nil
}

type submissionExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func writeSubmissionRecord(ctx context.Context, exec submissionExecutor, key submissionKey, data []byte, claims ...string) error {
	claim := ""
	if len(claims) > 0 {
		claim = claims[0]
	}
	state, action, hasOptions := "", "", false
	if key.kind == "sends" {
		var attempt sendAttempt
		if err := json.Unmarshal(data, &attempt); err != nil {
			return err
		}
		state, action, hasOptions = attempt.State, attempt.Context, attempt.Options != nil
	}
	_, err := exec.ExecContext(ctx, `INSERT INTO submission_records(kind,thread_id,attempt_id,payload,state,action,has_options,claim)
	VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(kind,thread_id,attempt_id) DO UPDATE SET payload=excluded.payload,state=excluded.state,action=excluded.action,has_options=excluded.has_options,claim=excluded.claim`, key.kind, key.thread, key.id, data, state, action, hasOptions, claim)
	return err
}

func (c *Client) compactSubmissionOptions(prefix string) error {
	c.queueMu.Lock()
	defer c.queueMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.openSubmissionDatabaseLocked(ctx); err != nil {
		return err
	}
	_, err := c.submission.db.ExecContext(ctx, `UPDATE submission_records SET payload=json_remove(payload,'$.options'),has_options=0
	WHERE kind='sends' AND state='accepted' AND has_options=1 AND action>=? AND action<?`, prefix, prefix+"\U0010ffff")
	return err
}

// ExportSubmissionAttempts returns a consistent, thread-scoped schema-3 snapshot
// for recovery consumers. It does not expose the physical storage format.
func (c *Client) ExportSubmissionAttempts(threadID string) (json.RawMessage, error) {
	if strings.TrimSpace(threadID) == "" {
		return nil, errors.New("submission export requires a thread")
	}
	return submissionTransaction(c, threadScope(threadID), func() (json.RawMessage, error) {
		ledger := queueAttemptLedger{Schema: 3, Attempts: map[string]map[string]string{}}
		if entries := c.queueAttempts[threadID]; len(entries) > 0 {
			ledger.Attempts[threadID] = entries
		}
		if entries := c.sendAttempts[threadID]; len(entries) > 0 {
			ledger.Sends = map[string]map[string]sendAttempt{threadID: entries}
		}
		if entries := c.queueDeletions[threadID]; len(entries) > 0 {
			ledger.Deletions = map[string]map[string]queueDeletionAttempt{threadID: entries}
		}
		return json.Marshal(ledger)
	})
}

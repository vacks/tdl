package download

import (
	"encoding/base64"
	"errors"
	"strconv"
	"strings"
)

// InboxAttemptLimit is inboxAttemptLimit for readers outside this package. The
// Bot renders an event's progress as "3/20" so a person can see how close it is
// to being left failed for good.
const InboxAttemptLimit = inboxAttemptLimit

// ListenerEvent is one row of the listener queues, collapsed across the two
// inbox tables so a reader can see them as a single stream.
//
// The two tables have independent identity sequences, so an ID is only
// meaningful together with its Source. Nothing here is resolved Telegram state:
// the columns are the ones the queues already carry, so listing events costs no
// RPC and works while the account is offline.
type ListenerEvent struct {
	// Source is "message" for chat_message_inbox and "reaction" for
	// reaction_inbox.
	Source        string
	ID            int64
	DialogName    string
	DialogKey     string
	MessageID     int
	Emoji         string
	Status        string
	Attempts      int
	NextAttemptAt string
	Error         string
	CreatedAt     string
}

const (
	// openEventSourceMessage and openEventSourceReaction name the two arms of
	// the merged list. They are also the values a caller switches on.
	openEventSourceMessage  = "message"
	openEventSourceReaction = "reaction"
	// eventClearBatchSize bounds one DELETE. Each batch is its own autocommit
	// statement, so the lock it takes on the queue is short and a claim or a
	// heartbeat never waits behind the whole clear.
	eventClearBatchSize = cleanupBatchSize
	// eventClearBatchLimit bounds one call. Clearing runs inside a Bot command
	// handler, which holds one of a small number of worker slots, so it must
	// finish in a bounded time even if the queue holds far more failed rows
	// than a person would ever clear by hand.
	eventClearBatchLimit = 100
	// openEventStatuses is the one definition of "an event someone can still act
	// on", and it is shared with the partial indexes that serve these reads
	// (postgres_schema.go) so the two cannot drift.
	//
	// It is written as a list of the open statuses rather than as "not done"
	// because the negation is what went wrong. When 'skipped' was added as a
	// second terminal status, every "not done" predicate silently started
	// treating an answered event as work still waiting - it reappeared in the
	// Bot's list and in the count above it. A positive list makes a status
	// visible only once someone has decided it belongs here.
	openEventStatuses = `status IN ('pending', 'processing', 'failed')`
	// eventRetentionStatuses is the set of states the retention sweep may
	// delete, shared with the partial indexes that serve that sweep so a status
	// added to one and not the other cannot make the sweep scan the whole queue
	// it is supposed to be trimming. It overlaps openEventStatuses on 'failed'
	// on purpose: a failed event is still listed while it can be retried, and
	// is discarded once it is older than the retention window either way.
	eventRetentionStatuses = `status IN ('done', 'failed', 'skipped')`
)

var errInvalidEventCursor = errors.New("invalid event cursor")

// ListListenerEvents returns one page of events that are still open - pending,
// processing or failed - newest first, together with the total number of open
// events and the cursor for the next page.
//
// Settled events are excluded deliberately, both completed ones and the
// 'skipped' ones that record a request which was answered with "nothing to
// download". They are the overwhelming majority of both tables and they are
// permanent history, so listing them would bury the handful of events a person
// can act on, and both the page query and the total would grow with the whole
// table rather than with the queue.
//
// The two inbox tables cannot be ordered as one sequence by ID, so rather than
// a UNION ALL that has to sort, each table is read separately through its own
// index and the two ordered arms are merged here. Each arm continues from its
// own keyset position, which is what the opaque cursor carries.
func (m *Manager) ListListenerEvents(cursor string, pageSize int) ([]ListenerEvent, int, string, error) {
	if pageSize < 1 {
		pageSize = 10
	}
	if pageSize > 50 {
		pageSize = 50
	}
	messageCursor, reactionCursor, err := decodeEventCursor(cursor)
	if err != nil {
		return nil, 0, "", err
	}
	total, err := m.openListenerEventTotal()
	if err != nil {
		return nil, 0, "", err
	}
	// One extra row per arm is enough to learn whether the merged page has a
	// successor: the page can only overflow if one of the arms did.
	messages, err := m.listMessageEvents(messageCursor, pageSize+1)
	if err != nil {
		return nil, 0, "", err
	}
	reactions, err := m.listReactionEvents(reactionCursor, pageSize+1)
	if err != nil {
		return nil, 0, "", err
	}
	merged := mergeListenerEvents(messages, reactions)
	next := ""
	if len(merged) > pageSize {
		merged = merged[:pageSize]
		// The page is a prefix of each arm independently, so an arm's next
		// position is its last row on this page. An arm that contributed no row
		// keeps the cursor it came in with.
		messageNext, reactionNext := messageCursor, reactionCursor
		for _, event := range merged {
			switch event.Source {
			case openEventSourceMessage:
				messageNext = formatArmCursor(event.CreatedAt, event.ID)
			case openEventSourceReaction:
				reactionNext = formatArmCursor(event.CreatedAt, event.ID)
			}
		}
		next = encodeEventCursor(messageNext, reactionNext)
	}
	return merged, total, next, nil
}

// openListenerEventTotal counts the events the list can show. The predicate
// matches the partial indexes on both tables, so this is an index-only scan of
// the queues rather than of a permanent history.
func (m *Manager) openListenerEventTotal() (int, error) {
	var total int
	err := m.db.QueryRow(`SELECT
 (SELECT COUNT(1) FROM chat_message_inbox WHERE ` + openEventStatuses + `) +
 (SELECT COUNT(1) FROM reaction_inbox WHERE ` + openEventStatuses + `)`).Scan(&total)
	if err != nil {
		return 0, err
	}
	return total, nil
}

func (m *Manager) listMessageEvents(cursor string, limit int) ([]ListenerEvent, error) {
	where := `WHERE ` + openEventStatuses
	args := make([]any, 0, 4)
	if cursor != "" {
		createdAt, id, err := parseArmCursor(cursor)
		if err != nil {
			return nil, err
		}
		// The row comparison is the same predicate as the OR form the task list
		// uses, but the planner can push it into the index as one seek. Written
		// as an OR it is a filter applied after scanning from the top of the
		// index, so page N costs N pages of work: measured at 200k rows deep on
		// a 2.4M row table, 39.5ms against 0.15ms.
		where += ` AND (created_at, id) < (?, ?)`
		args = append(args, createdAt, id)
	}
	args = append(args, limit)
	rows, err := m.db.Query(`SELECT id, dialog_name, dialog_key, message_id, status, attempts, next_attempt_at, error, created_at
FROM chat_message_inbox `+where+` ORDER BY created_at DESC, id DESC LIMIT ?`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	events := make([]ListenerEvent, 0, limit)
	for rows.Next() {
		var event ListenerEvent
		if err := rows.Scan(&event.ID, &event.DialogName, &event.DialogKey, &event.MessageID, &event.Status, &event.Attempts, &event.NextAttemptAt, &event.Error, &event.CreatedAt); err != nil {
			return nil, err
		}
		event.Source = openEventSourceMessage
		events = append(events, event)
	}
	return events, rows.Err()
}

func (m *Manager) listReactionEvents(cursor string, limit int) ([]ListenerEvent, error) {
	where := `WHERE ` + openEventStatuses
	args := make([]any, 0, 4)
	if cursor != "" {
		createdAt, id, err := parseArmCursor(cursor)
		if err != nil {
			return nil, err
		}
		// The row comparison is the same predicate as the OR form the task list
		// uses, but the planner can push it into the index as one seek. Written
		// as an OR it is a filter applied after scanning from the top of the
		// index, so page N costs N pages of work: measured at 200k rows deep on
		// a 2.4M row table, 39.5ms against 0.15ms.
		where += ` AND (created_at, id) < (?, ?)`
		args = append(args, createdAt, id)
	}
	args = append(args, limit)
	rows, err := m.db.Query(`SELECT id, dialog_name, dialog_key, message_id, emoji, status, attempts, next_attempt_at, error, created_at
FROM reaction_inbox `+where+` ORDER BY created_at DESC, id DESC LIMIT ?`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	events := make([]ListenerEvent, 0, limit)
	for rows.Next() {
		var event ListenerEvent
		if err := rows.Scan(&event.ID, &event.DialogName, &event.DialogKey, &event.MessageID, &event.Emoji, &event.Status, &event.Attempts, &event.NextAttemptAt, &event.Error, &event.CreatedAt); err != nil {
			return nil, err
		}
		event.Source = openEventSourceReaction
		events = append(events, event)
	}
	return events, rows.Err()
}

// mergeListenerEvents interleaves two arms that are already newest first. The
// ID and source comparisons only ever decide between rows that share a
// created_at, but they make the order total, so no row can be skipped or shown
// twice by a cursor that landed inside a tie.
func mergeListenerEvents(messages, reactions []ListenerEvent) []ListenerEvent {
	merged := make([]ListenerEvent, 0, len(messages)+len(reactions))
	left, right := 0, 0
	for left < len(messages) && right < len(reactions) {
		if listenerEventAfter(messages[left], reactions[right]) {
			merged = append(merged, messages[left])
			left++
			continue
		}
		merged = append(merged, reactions[right])
		right++
	}
	merged = append(merged, messages[left:]...)
	merged = append(merged, reactions[right:]...)
	return merged
}

func listenerEventAfter(a, b ListenerEvent) bool {
	if a.CreatedAt != b.CreatedAt {
		// created_at is RFC3339Nano text in UTC, which sorts chronologically as
		// text. The existing task list orders download_jobs the same way.
		return a.CreatedAt > b.CreatedAt
	}
	if a.ID != b.ID {
		return a.ID > b.ID
	}
	return a.Source < b.Source
}

// StoppedEventCounts reports how many events have stopped being retried, and
// how many of those are only resting: an event below the attempt limit is
// failed on the fast cadence and is offered to a worker again by the hourly
// revive. Saying so before a clear matters, because those are the ones a person
// destroys without meaning to.
//
// Only 'failed' counts. A 'skipped' row also stopped, but nothing is wrong with
// it and there is nothing to act on, so counting it here would inflate the one
// figure the Bot presents as a problem signal.
func (m *Manager) StoppedEventCounts() (stopped, revivable int64, err error) {
	err = m.db.QueryRow(`SELECT
 (SELECT COUNT(1) FROM chat_message_inbox WHERE status = 'failed') + (SELECT COUNT(1) FROM reaction_inbox WHERE status = 'failed'),
 (SELECT COUNT(1) FROM chat_message_inbox WHERE status = 'failed' AND attempts < ?) + (SELECT COUNT(1) FROM reaction_inbox WHERE status = 'failed' AND attempts < ?)`,
		inboxAttemptLimit, inboxAttemptLimit).Scan(&stopped, &revivable)
	if err != nil {
		return 0, 0, err
	}
	return stopped, revivable, nil
}

// ClearStoppedEvents deletes every event that stopped being retried. It is the
// only way to remove a failed event other than waiting out the retention sweep,
// which is what makes a queue that keeps giving up actionable rather than
// merely observable.
//
// Returns how many rows went from each table. A caller that gets a full batch
// limit back should read StoppedEventCounts again: the remainder is still
// there and one more call clears it.
func (m *Manager) ClearStoppedEvents() (messages, reactions int64, err error) {
	if messages, err = m.clearStoppedEvents(`DELETE FROM chat_message_inbox WHERE id IN (SELECT id FROM chat_message_inbox WHERE status = 'failed' ORDER BY updated_at, id LIMIT ?)`); err != nil {
		return 0, 0, err
	}
	if reactions, err = m.clearStoppedEvents(`DELETE FROM reaction_inbox WHERE id IN (SELECT id FROM reaction_inbox WHERE status = 'failed' ORDER BY updated_at, id LIMIT ?)`); err != nil {
		return messages, 0, err
	}
	return messages, reactions, nil
}

// clearStoppedEvents runs one table's delete in batches. The subquery reads in
// the order of the existing (status, updated_at, id) index, so each batch is an
// index range scan and not a sort of every failed row.
func (m *Manager) clearStoppedEvents(query string) (int64, error) {
	var deleted int64
	for batch := 0; batch < eventClearBatchLimit; batch++ {
		result, err := m.db.Exec(query, eventClearBatchSize)
		if err != nil {
			return deleted, err
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return deleted, err
		}
		deleted += changed
		if changed < eventClearBatchSize {
			return deleted, nil
		}
	}
	return deleted, nil
}

// formatArmCursor and parseArmCursor encode one table's keyset position. The
// same createdAt-and-ID pair the task list uses, kept as raw text here because
// the pair is wrapped and base64-encoded once for the whole merged cursor
// rather than once per arm.
func formatArmCursor(createdAt string, id int64) string {
	return createdAt + "\x00" + strconv.FormatInt(id, 10)
}

func parseArmCursor(value string) (string, int64, error) {
	createdAt, rawID, ok := strings.Cut(value, "\x00")
	if !ok || createdAt == "" || rawID == "" {
		return "", 0, errInvalidEventCursor
	}
	// Parsed here rather than left as text so that a cursor that will not bind
	// as an ID is rejected as malformed instead of reaching the database.
	id, err := strconv.ParseInt(rawID, 10, 64)
	if err != nil {
		return "", 0, errInvalidEventCursor
	}
	return createdAt, id, nil
}

// encodeEventCursor packs both arms into one opaque string. The Bot stores it
// as a single cursor and hands it back unchanged, so the fact that a page is
// two reads stays inside this package.
func encodeEventCursor(message, reaction string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(message + "\x01" + reaction))
}

func decodeEventCursor(value string) (message, reaction string, err error) {
	if value == "" {
		return "", "", nil
	}
	data, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return "", "", errInvalidEventCursor
	}
	message, reaction, ok := strings.Cut(string(data), "\x01")
	if !ok {
		return "", "", errInvalidEventCursor
	}
	for _, arm := range []string{message, reaction} {
		if arm == "" {
			continue
		}
		if _, _, err := parseArmCursor(arm); err != nil {
			return "", "", err
		}
	}
	return message, reaction, nil
}

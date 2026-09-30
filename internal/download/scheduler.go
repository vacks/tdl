package download

import "time"

// hasPriorityMessageTask returns true for a message task that is either ready
// to start or already transferring. Keeping the reservation until the message
// task leaves those states gives the message class a durable share of global
// concurrency instead of immediately handing its slot back to chat batches.
//
// Both halves are indexed probes. The 'running' half is a seek: it fixes one
// status and asks nothing else. The 'queued' half cannot be, because it also
// asks that the task have a queued file - that is a seek into the queued set
// with one indexed probe per candidate, and it is bounded by the queue rather
// than by the history. What matters is that neither reads the history, which
// holds millions of chat tasks.
//
// A task whose account is inside a Telegram cooldown is excluded. The
// reservation it would otherwise hold is not protecting interactive work: the
// task cannot start, and the scheduler that claims tasks already skips blocked
// accounts, so counting it here reserved chat capacity for a task nothing was
// going to run. At the default concurrency of one that is total - the chat
// share is floor(1/2), so session downloads could not start at all until the
// cooldown expired, which for a flood wait can be hours.
// The two halves are written as two EXISTS rather than one OR over them. The
// planner cannot prove that a disjunction implies either index's predicate, so
// the OR form was answered by scanning download_jobs and hashing the queued
// files of every row in it - the whole task history, to answer "is there one
// task". Split, each half reads only what its own status and index cover. This
// runs on every session batch, so its cost is paid per batch, not per task.
func (m *Manager) hasPriorityMessageTask() (bool, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	var found bool
	err := m.db.QueryRow(`SELECT EXISTS (
  SELECT 1
  FROM download_jobs j
  WHERE j.status = 'running'
    AND NOT EXISTS (
      SELECT 1 FROM telegram_rate_limits l
      WHERE l.account_id = j.account_id AND l.blocked_until::timestamptz > ?::timestamptz)
) OR EXISTS (
  SELECT 1
  FROM download_jobs j
  WHERE j.status = 'queued'
    AND EXISTS (SELECT 1 FROM download_items i WHERE i.job_id = j.id AND i.status = 'queued')
    AND NOT EXISTS (
      SELECT 1 FROM telegram_rate_limits l
      WHERE l.account_id = j.account_id AND l.blocked_until::timestamptz > ?::timestamptz)
)`, now, now).Scan(&found)
	return found, err
}

package download

import "time"

// hasPriorityMessageTask returns true for a message task that is either ready
// to start or already transferring. Keeping the reservation until the message
// task leaves those states gives the message class a durable share of global
// concurrency instead of immediately handing its slot back to chat batches.
// It is intentionally a single indexed EXISTS query, so historical chat
// indexes with millions of rows do not need to be read.
//
// A task whose account is inside a Telegram cooldown is excluded. The
// reservation it would otherwise hold is not protecting interactive work: the
// task cannot start, and the scheduler that claims tasks already skips blocked
// accounts, so counting it here reserved chat capacity for a task nothing was
// going to run. At the default concurrency of one that is total - the chat
// share is floor(1/2), so session downloads could not start at all until the
// cooldown expired, which for a flood wait can be hours.
func (m *Manager) hasPriorityMessageTask() (bool, error) {
	var found bool
	err := m.db.QueryRow(`SELECT EXISTS (
  SELECT 1
  FROM download_jobs j
  WHERE (j.status = 'running'
      OR (j.status = 'queued'
          AND EXISTS (SELECT 1 FROM download_items i WHERE i.job_id = j.id AND i.status = 'queued')))
    AND NOT EXISTS (
      SELECT 1 FROM telegram_rate_limits l
      WHERE l.account_id = j.account_id AND l.blocked_until::timestamptz > ?::timestamptz)
)`, time.Now().UTC().Format(time.RFC3339Nano)).Scan(&found)
	return found, err
}

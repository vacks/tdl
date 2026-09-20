package download

// hasPriorityMessageTask returns true for a message task that is either ready
// to start or already transferring. Keeping the reservation until the message
// task leaves those states gives the message class a durable share of global
// concurrency instead of immediately handing its slot back to chat batches.
// It is intentionally a single indexed EXISTS query, so historical chat
// indexes with millions of rows do not need to be read.
func (m *Manager) hasPriorityMessageTask() (bool, error) {
	var found bool
	err := m.db.QueryRow(`SELECT EXISTS (
  SELECT 1
  FROM download_jobs j
  WHERE j.status = 'running'
     OR (j.status = 'queued'
         AND EXISTS (SELECT 1 FROM download_items i WHERE i.job_id = j.id AND i.status = 'queued'))
)`).Scan(&found)
	return found, err
}

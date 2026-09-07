package download

import (
	"fmt"
	"sync"
	"time"

	upstreamDL "github.com/iyear/tdl/app/dl"
)

// FileProgress is live, non-persistent download state. It intentionally stays
// out of SQLite: upstream invokes the callback for every completed chunk.
type FileProgress struct {
	DialogID   int64   `json:"dialogId"`
	MessageID  int     `json:"messageId"`
	Downloaded int64   `json:"downloaded"`
	Total      int64   `json:"total"`
	SpeedBPS   float64 `json:"speedBps"`
	UpdatedAt  string  `json:"updatedAt"`
}

type liveFileProgress struct {
	FileProgress
	jobID            string
	lastMeasuredAt   time.Time
	lastMeasuredByte int64
}

type progressStore struct {
	mu    sync.RWMutex
	files map[string]liveFileProgress
}

func newProgressStore() *progressStore {
	return &progressStore{files: make(map[string]liveFileProgress)}
}

func progressKey(dialogID int64, messageID int) string {
	return fmt.Sprintf("%d:%d", dialogID, messageID)
}

func (p *progressStore) Update(jobID string, update upstreamDL.ProgressUpdate) (started, completed bool) {
	now := time.Now()
	key := progressKey(update.DialogID, update.MessageID)
	p.mu.Lock()
	defer p.mu.Unlock()
	previous, exists := p.files[key]
	if !exists || previous.jobID != jobID || update.Downloaded < previous.Downloaded {
		previous = liveFileProgress{jobID: jobID, lastMeasuredAt: now, lastMeasuredByte: update.Downloaded}
		started = true
	}

	// The upstream callback can run once per completed concurrent chunk. Using
	// neighbouring callbacks therefore turns a few milliseconds into a wildly
	// inflated "instant" rate. Measure at most once a second instead.
	speed := previous.SpeedBPS
	if elapsed := now.Sub(previous.lastMeasuredAt).Seconds(); elapsed >= 1 {
		measured := float64(update.Downloaded-previous.lastMeasuredByte) / elapsed
		if speed == 0 {
			speed = measured
		} else {
			speed = speed*0.7 + measured*0.3
		}
		previous.lastMeasuredAt = now
		previous.lastMeasuredByte = update.Downloaded
	}
	previous.FileProgress = FileProgress{DialogID: update.DialogID, MessageID: update.MessageID, Downloaded: update.Downloaded, Total: update.Total, SpeedBPS: speed, UpdatedAt: now.UTC().Format(time.RFC3339Nano)}
	p.files[key] = previous
	return started, update.Completed
}

func (p *progressStore) Snapshot() []FileProgress {
	p.mu.RLock()
	defer p.mu.RUnlock()
	files := make([]FileProgress, 0, len(p.files))
	for _, state := range p.files {
		files = append(files, state.FileProgress)
	}
	return files
}

func (p *progressStore) ClearJob(jobID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for key, state := range p.files {
		if state.jobID == jobID {
			delete(p.files, key)
		}
	}
}

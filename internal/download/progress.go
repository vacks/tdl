package download

import (
	"fmt"
	"sync"
	"time"

	upstreamDL "github.com/iyear/tdl/app/dl"
)

// FileProgress is live, non-persistent download state. It intentionally stays
// out of PostgreSQL: upstream invokes the callback for every completed chunk.
type FileProgress struct {
	DialogType string  `json:"dialogType"`
	DialogKey  string  `json:"dialogKey"`
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

// liveJobProgress measures bytes received by the whole visible task. A chat
// task commonly contains many small files, so summing per-file rates is both
// misleading and prone to retaining rates for files that have already ended.
type liveJobProgress struct {
	lastSampleAt time.Time
	lastActivity time.Time
	windowBytes  int64
	speedBPS     float64
}

type progressStore struct {
	mu    sync.RWMutex
	files map[string]liveFileProgress
	jobs  map[string]liveJobProgress
}

func newProgressStore() *progressStore {
	return &progressStore{files: make(map[string]liveFileProgress), jobs: make(map[string]liveJobProgress)}
}

func progressKey(dialogKey string, messageID int) string {
	return fmt.Sprintf("%s:%d", dialogKey, messageID)
}

func (p *progressStore) Update(jobID string, item Item, update upstreamDL.ProgressUpdate) (started, completed bool) {
	now := time.Now()
	key := progressKey(item.DialogKey, update.MessageID)
	p.mu.Lock()
	defer p.mu.Unlock()
	previous, exists := p.files[key]
	previousBytes := int64(0)
	if exists && previous.jobID == jobID {
		previousBytes = previous.Downloaded
	}
	if !exists || previous.jobID != jobID || update.Downloaded < previous.Downloaded {
		previous = liveFileProgress{jobID: jobID, lastMeasuredAt: now, lastMeasuredByte: update.Downloaded}
		started = true
		if update.Downloaded < previousBytes {
			previousBytes = 0
		}
	}
	// Maintain a task-wide rolling sample independently of individual files.
	// Unlike a sum of per-file speeds, this remains meaningful when many small
	// files start and finish within the one-second per-file sample interval.
	job := p.jobs[jobID]
	if job.lastSampleAt.IsZero() {
		job.lastSampleAt = now
	}
	if delta := update.Downloaded - previousBytes; delta > 0 {
		job.windowBytes += delta
	}
	job.lastActivity = now
	if elapsed := now.Sub(job.lastSampleAt).Seconds(); elapsed >= 1 {
		job.speedBPS = float64(job.windowBytes) / elapsed
		job.windowBytes = 0
		job.lastSampleAt = now
	}
	p.jobs[jobID] = job

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
	previous.FileProgress = FileProgress{DialogType: item.DialogType, DialogKey: item.DialogKey, DialogID: update.DialogID, MessageID: update.MessageID, Downloaded: update.Downloaded, Total: update.Total, SpeedBPS: speed, UpdatedAt: now.UTC().Format(time.RFC3339Nano)}
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
	delete(p.jobs, jobID)
}

// ClearItem removes a finished file from the live set immediately. Its bytes
// remain part of the task-wide rolling window, but its last per-file rate can
// no longer inflate a chat task's aggregate rate.
func (p *progressStore) ClearItem(jobID string, item Item) {
	p.mu.Lock()
	defer p.mu.Unlock()
	key := progressKey(item.DialogKey, item.MessageID)
	if state, ok := p.files[key]; ok && state.jobID == jobID {
		delete(p.files, key)
	}
}

func (p *progressStore) Aggregate(jobID string) (files int, speed float64) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, state := range p.files {
		if state.jobID == jobID {
			files++
		}
	}
	job, ok := p.jobs[jobID]
	if !ok || time.Since(job.lastActivity) > 3*time.Second {
		return files, 0
	}
	return files, job.speedBPS
}

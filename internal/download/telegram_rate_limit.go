package download

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"sync"
	"time"
)

// Telegram metadata requests are deliberately kept separate from file byte
// transfer. The upstream downloader owns the latter; this gate covers every
// application-level resolve, history, search and discussion request.
const (
	telegramRPCBurst = 8.0
	telegramRPCRate  = 4.0
)

var telegramWaitPattern = regexp.MustCompile(`(?i)(?:FLOOD_WAIT|SLOWMODE_WAIT)[_: ]*([0-9]+)`)

type telegramRPCState struct {
	tokens       float64
	lastRefill   time.Time
	blockedUntil time.Time
}

func telegramWaitDuration(err error) time.Duration {
	if err == nil {
		return 0
	}
	match := telegramWaitPattern.FindStringSubmatch(err.Error())
	if len(match) != 2 {
		return 0
	}
	seconds, parseErr := strconv.ParseInt(match[1], 10, 64)
	if parseErr != nil || seconds <= 0 {
		return 0
	}
	return time.Duration(seconds) * time.Second
}

func (m *Manager) loadTelegramRateLimits() error {
	rows, err := m.db.Query(`SELECT account_id, blocked_until FROM telegram_rate_limits WHERE blocked_until > ?`, time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return err
	}
	defer rows.Close()
	m.rpcMu.Lock()
	defer m.rpcMu.Unlock()
	for rows.Next() {
		var accountID, raw string
		if err := rows.Scan(&accountID, &raw); err != nil {
			return err
		}
		until, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			continue
		}
		m.rpcState[accountID] = &telegramRPCState{tokens: telegramRPCBurst, lastRefill: time.Now(), blockedUntil: until}
	}
	return rows.Err()
}

func (m *Manager) awaitTelegramRPC(ctx context.Context, accountID string) error {
	if accountID == "" {
		return nil
	}
	for {
		now := time.Now()
		m.rpcMu.Lock()
		state := m.rpcState[accountID]
		if state == nil {
			state = &telegramRPCState{tokens: telegramRPCBurst, lastRefill: now}
			m.rpcState[accountID] = state
		}
		if elapsed := now.Sub(state.lastRefill).Seconds(); elapsed > 0 {
			state.tokens = minFloat(telegramRPCBurst, state.tokens+elapsed*telegramRPCRate)
			state.lastRefill = now
		}
		var wait time.Duration
		if state.blockedUntil.After(now) {
			wait = time.Until(state.blockedUntil)
		} else if state.tokens < 1 {
			wait = time.Duration((1 - state.tokens) / telegramRPCRate * float64(time.Second))
		} else {
			state.tokens--
			m.rpcMu.Unlock()
			return nil
		}
		m.rpcMu.Unlock()
		if wait < 10*time.Millisecond {
			wait = 10 * time.Millisecond
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func minFloat(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

// recordTelegramRPCError persists a server-mandated wait. It is intentionally
// account-wide so a reaction, a history scan and a Bot-created task cannot
// take turns violating the same FLOOD_WAIT window.
func (m *Manager) recordTelegramRPCError(accountID string, err error) bool {
	wait := telegramWaitDuration(err)
	if wait == 0 || accountID == "" {
		return false
	}
	until := time.Now().UTC().Add(wait + 250*time.Millisecond)
	m.rpcMu.Lock()
	state := m.rpcState[accountID]
	if state == nil {
		state = &telegramRPCState{tokens: telegramRPCBurst, lastRefill: time.Now()}
		m.rpcState[accountID] = state
	}
	if until.After(state.blockedUntil) {
		state.blockedUntil = until
	}
	m.rpcMu.Unlock()
	if _, dbErr := m.db.Exec(`INSERT INTO telegram_rate_limits(account_id, blocked_until, reason, updated_at) VALUES (?, ?, ?, ?) ON CONFLICT(account_id) DO UPDATE SET blocked_until = GREATEST(telegram_rate_limits.blocked_until, EXCLUDED.blocked_until), reason = EXCLUDED.reason, updated_at = EXCLUDED.updated_at`, accountID, until.Format(time.RFC3339Nano), err.Error(), time.Now().UTC().Format(time.RFC3339Nano)); dbErr != nil {
		return true
	}
	return true
}

func (m *Manager) telegramRateLimitError(accountID string, err error) error {
	if m.recordTelegramRPCError(accountID, err) {
		return fmt.Errorf("Telegram 限流等待: %w", err)
	}
	return err
}

type transferKind uint8

const (
	transferMessage transferKind = iota
	transferChat
)

// chatTransferLimit leaves a share of the global slots for interactive message
// tasks only while such tasks are waiting. With no queued message work, chat
// tasks can use the full configured concurrency. For an odd limit, the extra
// slot is reserved for messages: e.g. 3 becomes 1 chat + 2 message slots.
func chatTransferLimit(global int, messageQueued bool) int {
	if !messageQueued {
		return global
	}
	return global / 2
}

// transferPermit is shared by ordinary links and chat batches. The configured
// concurrent-jobs value remains the sole global cap and permits several tasks
// for one account. While a message task is waiting, chat batches are capped at
// half of that global capacity; finished batches naturally yield those slots
// without interrupting an upstream file transfer. Telegram API request pacing
// and FLOOD_WAIT cooldowns remain account-scoped.
func (m *Manager) tryAcquireTransfer(kind transferKind, messageQueued bool) (func(), bool) {
	m.slotMu.Lock()
	limit := m.settings.Get().Download.ConcurrentJobs
	if limit < 1 || m.activeJobs >= limit || (kind == transferChat && m.activeChats >= chatTransferLimit(limit, messageQueued)) {
		m.slotMu.Unlock()
		return nil, false
	}
	m.activeJobs++
	if kind == transferChat {
		m.activeChats++
	}
	m.slotMu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			m.slotMu.Lock()
			m.activeJobs--
			if kind == transferChat {
				m.activeChats--
			}
			m.slotMu.Unlock()
			select {
			case m.slotWake <- struct{}{}:
			default:
			}
		})
	}, true
}

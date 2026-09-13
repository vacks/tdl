package monitor

import (
	"bufio"
	"context"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

// Sample is an in-memory point for the dashboard; it is intentionally never
// persisted and therefore has no impact on download database activity.
type Sample struct {
	At          string  `json:"at"`
	CPUPercent  float64 `json:"cpuPercent"`
	MemoryUsed  uint64  `json:"memoryUsed"`
	MemoryTotal uint64  `json:"memoryTotal"`
	ReceiveBPS  float64 `json:"receiveBps"`
	TransmitBPS float64 `json:"transmitBps"`
	DiskUsed    uint64  `json:"diskUsed"`
	DiskTotal   uint64  `json:"diskTotal"`
}

type Monitor struct {
	root    string
	mu      sync.RWMutex
	points  []Sample
	lastCPU cpuStat
	lastNet netStat
	lastAt  time.Time
	cancel  context.CancelFunc
	done    chan struct{}
	stop    sync.Once
}
type cpuStat struct{ total, idle uint64 }
type netStat struct{ receive, transmit uint64 }

func New(downloadDir string) *Monitor {
	ctx, cancel := context.WithCancel(context.Background())
	m := &Monitor{root: downloadDir, cancel: cancel, done: make(chan struct{})}
	m.sample()
	go func() {
		defer close(m.done)
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.sample()
			}
		}
	}()
	return m
}

// Stop ends the in-memory sampler. It is safe to call more than once and is
// used by HTTP server shutdown and isolated tests alike.
func (m *Monitor) Stop() {
	if m == nil {
		return
	}
	m.stop.Do(func() {
		m.cancel()
		<-m.done
	})
}

func (m *Monitor) Snapshot() (Sample, []Sample) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	points := append([]Sample(nil), m.points...)
	if len(points) == 0 {
		return Sample{}, points
	}
	return points[len(points)-1], points
}

func (m *Monitor) sample() {
	now := time.Now()
	cpu, cpuOK := readCPU()
	net, netOK := readNetwork()
	memUsed, memTotal := readMemory()
	diskUsed, diskTotal := diskUsage(m.root)
	m.mu.Lock()
	defer m.mu.Unlock()
	s := Sample{At: now.UTC().Format(time.RFC3339), MemoryUsed: memUsed, MemoryTotal: memTotal, DiskUsed: diskUsed, DiskTotal: diskTotal}
	if cpuOK && m.lastCPU.total > 0 && cpu.total > m.lastCPU.total {
		total := cpu.total - m.lastCPU.total
		idle := cpu.idle - m.lastCPU.idle
		if total > 0 {
			s.CPUPercent = float64(total-idle) * 100 / float64(total)
		}
	}
	if netOK && !m.lastAt.IsZero() {
		seconds := now.Sub(m.lastAt).Seconds()
		if seconds > 0 && net.receive >= m.lastNet.receive && net.transmit >= m.lastNet.transmit {
			s.ReceiveBPS = float64(net.receive-m.lastNet.receive) / seconds
			s.TransmitBPS = float64(net.transmit-m.lastNet.transmit) / seconds
		}
	}
	if cpuOK {
		m.lastCPU = cpu
	}
	if netOK {
		m.lastNet = net
	}
	m.lastAt = now
	m.points = append(m.points, s)
	if len(m.points) > 30 {
		m.points = m.points[len(m.points)-30:]
	}
}

func readCPU() (cpuStat, bool) {
	f, err := os.Open("/proc/stat")
	if err != nil {
		return cpuStat{}, false
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	if !scanner.Scan() {
		return cpuStat{}, false
	}
	fields := strings.Fields(scanner.Text())
	if len(fields) < 5 || fields[0] != "cpu" {
		return cpuStat{}, false
	}
	var total uint64
	for _, field := range fields[1:] {
		value, err := strconv.ParseUint(field, 10, 64)
		if err != nil {
			return cpuStat{}, false
		}
		total += value
	}
	idle, _ := strconv.ParseUint(fields[4], 10, 64)
	if len(fields) > 5 {
		wait, _ := strconv.ParseUint(fields[5], 10, 64)
		idle += wait
	}
	return cpuStat{total, idle}, true
}
func readMemory() (uint64, uint64) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, 0
	}
	defer f.Close()
	values := map[string]uint64{}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 2 {
			values[strings.TrimSuffix(fields[0], ":")], _ = strconv.ParseUint(fields[1], 10, 64)
		}
	}
	total := values["MemTotal"] * 1024
	available := values["MemAvailable"] * 1024
	if available > total {
		available = total
	}
	return total - available, total
}
func readNetwork() (netStat, bool) {
	f, err := os.Open("/proc/net/dev")
	if err != nil {
		return netStat{}, false
	}
	defer f.Close()
	var result netStat
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.Contains(line, ":") {
			continue
		}
		pair := strings.SplitN(line, ":", 2)
		if strings.TrimSpace(pair[0]) == "lo" {
			continue
		}
		fields := strings.Fields(pair[1])
		if len(fields) < 9 {
			continue
		}
		rx, _ := strconv.ParseUint(fields[0], 10, 64)
		tx, _ := strconv.ParseUint(fields[8], 10, 64)
		result.receive += rx
		result.transmit += tx
	}
	return result, true
}
func diskUsage(path string) (uint64, uint64) {
	var stat unix.Statfs_t
	if unix.Statfs(path, &stat) != nil {
		return 0, 0
	}
	total := stat.Blocks * uint64(stat.Bsize)
	available := stat.Bavail * uint64(stat.Bsize)
	return total - available, total
}

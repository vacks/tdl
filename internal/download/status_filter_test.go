package download

import "testing"

func TestNormalizeTaskStatusFilter(t *testing.T) {
	for raw, want := range map[string]string{
		"": "", "全部": "", "排队中": "queued",
		"下载中": "running", "已暂停": "paused", "已完成": "completed",
		"部分完成": "partial", "失败": "failed", "已取消": "cancelled",
	} {
		got, err := NormalizeTaskStatusFilter(raw)
		if err != nil || got != want {
			t.Fatalf("NormalizeTaskStatusFilter(%q) = %q, %v; want %q, nil", raw, got, err, want)
		}
	}
	if _, err := NormalizeTaskStatusFilter("不存在"); err == nil {
		t.Fatal("invalid status accepted")
	}
}

// "等待中" was offered by the Web UI and the Bot, but it mapped to
// download_jobs.status = 'waiting', which nothing ever writes: a task waits
// when its files are held by another active task, and that is an item property.
// The filter could only ever return an empty page, so it was removed rather
// than redefined, and a caller still sending it must be told so instead of
// silently getting a different result.
func TestTaskStatusFilterRejectsWaiting(t *testing.T) {
	for _, raw := range []string{"等待中", "waiting", "WAITING"} {
		if got, err := NormalizeTaskStatusFilter(raw); err == nil {
			t.Fatalf("NormalizeTaskStatusFilter(%q) = %q, want an error: no task can ever have that status", raw, got)
		}
	}
	if label := TaskStatusFilterLabel("waiting"); label != "" {
		t.Fatalf("TaskStatusFilterLabel(waiting) = %q, want empty: it is not a task filter", label)
	}
	// The remaining task filters keep their labels. The per-file "等待中" label is
	// a separate map in the Bot and the Web UI and is untouched by this.
	if label := TaskStatusFilterLabel("queued"); label != "排队中" {
		t.Fatalf("unrelated labels must be unaffected, got %q", label)
	}
}

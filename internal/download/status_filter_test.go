package download

import "testing"

func TestNormalizeTaskStatusFilter(t *testing.T) {
	for raw, want := range map[string]string{
		"": "", "全部": "", "排队中": "queued", "waiting": "waiting",
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

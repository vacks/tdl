package download

import (
	"fmt"
	"strings"
)

// NormalizeTaskStatusFilter converts the user-facing task status used by the
// Web UI and Bot into the durable download_jobs status. An empty value means
// all visible tasks. Keeping this mapping in the download package ensures all
// entry points apply exactly the same filter.
func NormalizeTaskStatusFilter(value string) (string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	switch value {
	case "", "全部", "所有", "all":
		return "", nil
	case "排队中", "queued":
		return "queued", nil
	case "等待中", "waiting":
		return "waiting", nil
	case "下载中", "running":
		return "running", nil
	case "已暂停", "暂停", "paused":
		return "paused", nil
	case "已完成", "完成", "completed":
		return "completed", nil
	case "部分完成", "partial":
		return "partial", nil
	case "失败", "已失败", "failed":
		return "failed", nil
	case "已取消", "取消", "cancelled":
		return "cancelled", nil
	default:
		return "", fmt.Errorf("不支持的任务状态：%s", value)
	}
}

func TaskStatusFilterLabel(status string) string {
	return map[string]string{
		"queued":    "排队中",
		"waiting":   "等待中",
		"running":   "下载中",
		"paused":    "已暂停",
		"completed": "已完成",
		"partial":   "部分完成",
		"failed":    "失败",
		"cancelled": "已取消",
	}[status]
}

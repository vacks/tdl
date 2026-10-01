<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import { api } from '@/api'
import { projectVersion } from '@/buildInfo'

type ConnectionStatus = { telegram?: { status?: string }; database?: { status?: string; error?: string } }

const status = ref<ConnectionStatus>()
let stream: EventSource | undefined
let disposed = false
// Set by the first pushed frame. The read below describes the instant the
// server answered it, so on a slow link it can arrive after a change has
// already been pushed - and applying it then would put the older state back on
// screen with no further change coming to correct it.
let pushed = false

// The page asks once, when it appears, because a stream has to be established
// before it can say anything and the sidebar must not sit empty until then.
// From that point on the server is the one that speaks: it watches the two
// states and sends them when they change, so the page has no poll of its own.
// Both paths assign the same value, so a fetched state and a pushed one can
// never be shown at the same time.
async function load() {
  try {
    const snapshot = await api<ConnectionStatus>('/api/status')
    if (!disposed && !pushed) status.value = snapshot
  } catch {
    // A failed read leaves whatever is already on screen. The stream still
    // carries every change from here on, and it reconnects on its own.
  }
}

onMounted(() => {
  void load()
  stream = new EventSource('/api/status/events')
  stream.onmessage = (message) => {
    try { status.value = JSON.parse(message.data) as ConnectionStatus; pushed = true } catch { /* an unreadable frame is not worth a broken sidebar */ }
  }
})

onBeforeUnmount(() => {
  disposed = true
  stream?.close()
  stream = undefined
})

const telegram = computed(() => status.value?.telegram?.status || '')
const database = computed(() => status.value?.database?.status || '')
const telegramText = computed(() => telegram.value === 'connected' ? '已连接' : telegram.value === 'not_connected' ? '尚未登录' : '检测中')
const telegramHint = computed(() => telegram.value === 'connected' ? '当前服务可访问 Telegram' : telegram.value === 'not_connected' ? '请先到登录管理页添加并登录账户' : '正在读取状态')
const databaseText = computed(() => database.value === 'connected' ? '已连接' : database.value ? '连接异常' : '检测中')
const databaseHint = computed(() => database.value === 'connected' ? 'PostgreSQL 服务正常' : status.value?.database?.error || (database.value ? '正在自动重连' : '正在读取状态'))
// An empty status is the one that has not been read yet, and it gets the
// neutral tone: "检测中" is not a failure, and a red dot before the first answer
// arrives would report an outage that has not been observed.
const telegramTone = computed(() => telegram.value === 'connected' ? 'is-ok' : telegram.value === 'not_connected' ? 'is-warn' : 'is-idle')
const databaseTone = computed(() => database.value === 'connected' ? 'is-ok' : database.value ? 'is-bad' : 'is-idle')
</script>

<template>
  <div class="sidebar-status">
    <p class="sidebar-status-title">服务状态</p>
    <div class="status-item" :class="telegramTone" :title="telegramHint"><i class="status-dot"></i><span class="status-label">Telegram 账户</span><span class="status-value">{{ telegramText }}</span></div>
    <div class="status-item" :class="databaseTone" :title="databaseHint"><i class="status-dot"></i><span class="status-label">数据库</span><span class="status-value">{{ databaseText }}</span></div>
    <p class="sidebar-version"><span>TDL 版本</span><b>{{ projectVersion }}</b></p>
  </div>
</template>

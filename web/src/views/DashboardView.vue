<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import { useRouter } from 'vue-router'
import { APIError, api } from '@/api'
import SidebarStatus from '@/components/SidebarStatus.vue'

type Point = { at: string; cpuPercent: number; memoryUsed: number; memoryTotal: number; receiveBps: number; transmitBps: number; diskUsed: number; diskTotal: number }
// The two connection states the dashboard page used to carry now live in the
// sidebar, where every page can see them; what is left here is the resource
// trend this page alone draws.
type Dashboard = { system: { current: Point; trend: Point[] } }
const router = useRouter(); const dashboard = ref<Dashboard>(); const username = ref(''); let timer: number | undefined; let disposed = false
async function load() { try { dashboard.value = await api<Dashboard>('/api/dashboard') } catch (error) { if (error instanceof APIError && error.status === 401) { if (timer) window.clearInterval(timer); timer = undefined; await router.replace('/login') } } }
// Both the session probe and the first load are awaited, so the interval is
// created one tick later at the earliest. Without the disposed check a page
// left before they resolved started a three second poll that nothing could ever
// stop: onBeforeUnmount runs synchronously, when `timer` is still undefined.
onMounted(async () => { try { const session = await api<{ authenticated: boolean; username?: string }>('/api/auth/session'); if (disposed) return; if (!session.authenticated) return void router.replace('/login'); username.value = session.username || ''; await load(); if (disposed) return; timer = window.setInterval(() => void load(), 3000) } catch { if (!disposed) await router.replace('/login') } })
onBeforeUnmount(() => { disposed = true; if (timer) window.clearInterval(timer) })
async function logout() { if (timer) window.clearInterval(timer); timer = undefined; await api('/api/auth/logout', { method: 'POST' }); await router.replace('/login') }
const current = computed(() => dashboard.value?.system.current)
const trend = computed(() => dashboard.value?.system.trend || [])
function percent(a = 0, b = 0) { return b > 0 ? a * 100 / b : 0 }
function fixed(value = 0) { return value.toFixed(value >= 10 ? 0 : 1) }
function bytes(value = 0) { const units = ['B', 'KB', 'MB', 'GB', 'TB']; let n = value; let i = 0; while (n >= 1024 && i < units.length - 1) { n /= 1024; i++ }; return `${n.toFixed(i === 0 ? 0 : n >= 10 ? 1 : 2)} ${units[i]}` }
function line(values: number[], ceiling?: number) { if (!values.length) return ''; const max = ceiling || Math.max(...values, 1); return values.map((value, index) => `${values.length === 1 ? 0 : index * 100 / (values.length - 1)},${66 - Math.min(66, value / max * 62)}`).join(' ') }
const cpuLine = computed(() => line(trend.value.map(p => p.cpuPercent), 100))
const memoryLine = computed(() => line(trend.value.map(p => percent(p.memoryUsed, p.memoryTotal)), 100))
const netMax = computed(() => Math.max(...trend.value.map(p => Math.max(p.receiveBps, p.transmitBps)), 1))
const downLine = computed(() => line(trend.value.map(p => p.receiveBps), netMax.value))
const upLine = computed(() => line(trend.value.map(p => p.transmitBps), netMax.value))
</script>

<template>
  <el-container class="shell"><el-aside width="244px"><div class="brand">TDL 管理</div><el-menu default-active="/" router><el-menu-item index="/">仪表盘</el-menu-item><el-menu-item index="/accounts">登录管理</el-menu-item><el-menu-item index="/downloads">下载管理</el-menu-item><el-menu-item index="/settings">配置管理</el-menu-item></el-menu><SidebarStatus /></el-aside>
    <el-container><el-header><span>仪表盘</span><div><span class="subtle">{{ username }}</span><el-button text @click="logout">退出</el-button></div></el-header><el-main><main class="dashboard-page"><section class="dashboard-hero"><div><p class="eyebrow">OVERVIEW</p><h1>运行概览</h1><p class="subtle">查看服务资源与下载状态。</p></div><div class="live-pill"><i></i> 实时监控</div></section>
      <section class="monitor-grid"><article class="chart-card"><header><div><span>CPU</span><strong>{{ fixed(current?.cpuPercent) }}%</strong></div><small>最近 60 秒</small></header><svg viewBox="0 0 100 70" preserveAspectRatio="none"><path d="M0 67H100" class="grid-line"/><polyline :points="cpuLine" class="chart-line cpu"/></svg></article><article class="chart-card"><header><div><span>内存</span><strong>{{ fixed(percent(current?.memoryUsed, current?.memoryTotal)) }}%</strong></div><small>{{ bytes(current?.memoryUsed) }} / {{ bytes(current?.memoryTotal) }}</small></header><svg viewBox="0 0 100 70" preserveAspectRatio="none"><path d="M0 67H100" class="grid-line"/><polyline :points="memoryLine" class="chart-line memory"/></svg></article><article class="chart-card network-card"><header><div><span>网络</span><strong>{{ bytes(current?.receiveBps) }}/s</strong></div><small><b class="down-dot"></b>接收　<b class="up-dot"></b>发送 {{ bytes(current?.transmitBps) }}/s</small></header><svg viewBox="0 0 100 70" preserveAspectRatio="none"><path d="M0 67H100" class="grid-line"/><polyline :points="downLine" class="chart-line down"/><polyline :points="upLine" class="chart-line up"/></svg></article></section>
    </main></el-main></el-container>
  </el-container>
</template>

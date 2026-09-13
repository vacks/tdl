<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import { useRouter } from 'vue-router'
import { APIError, api } from '@/api'

type Point = { at: string; cpuPercent: number; memoryUsed: number; memoryTotal: number; receiveBps: number; transmitBps: number; diskUsed: number; diskTotal: number }
type Dashboard = { telegram: { status: string }; database: { status: string; error?: string }; downloads: { active: number; completedItems: number; failedItems: number }; upstreamVersion: string; system: { current: Point; trend: Point[] } }
const router = useRouter(); const dashboard = ref<Dashboard>(); const username = ref(''); let timer: number | undefined
async function load() { try { dashboard.value = await api<Dashboard>('/api/dashboard') } catch (error) { if (error instanceof APIError && error.status === 401) { if (timer) window.clearInterval(timer); timer = undefined; await router.replace('/login') } } }
onMounted(async () => { try { const session = await api<{ authenticated: boolean; username?: string }>('/api/auth/session'); if (!session.authenticated) return void router.replace('/login'); username.value = session.username || ''; await load(); timer = window.setInterval(() => void load(), 3000) } catch { await router.replace('/login') } })
onBeforeUnmount(() => { if (timer) window.clearInterval(timer) })
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
  <el-container class="shell"><el-aside width="244px"><div class="brand">TDL 管理</div><el-menu default-active="/" router><el-menu-item index="/">仪表盘</el-menu-item><el-menu-item index="/accounts">登录管理</el-menu-item><el-menu-item index="/downloads">下载管理</el-menu-item><el-menu-item index="/settings">配置管理</el-menu-item></el-menu><div class="sidebar-meta"><span>上游 tdl <b>v0.20.4</b></span><span>项目版本 <b>v0.1.0</b></span></div></el-aside>
    <el-container><el-header><span>仪表盘</span><div><span class="subtle">{{ username }}</span><el-button text @click="logout">退出</el-button></div></el-header><el-main><main class="dashboard-page"><section class="dashboard-hero"><div><p class="eyebrow">OVERVIEW</p><h1>运行概览</h1><p class="subtle">查看服务资源与下载状态。</p></div><div class="live-pill"><i></i> 实时监控</div></section>
      <section class="overview-grid"><article class="overview-card accent-blue"><span>Telegram 账户</span><strong>{{ dashboard?.telegram.status === 'not_connected' ? '尚未登录' : '已连接' }}</strong><small>{{ dashboard?.telegram.status === 'not_connected' ? '请先在账户页完成登录' : '当前服务可访问 Telegram' }}</small></article><article class="overview-card" :class="dashboard?.database.status === 'connected' ? 'accent-green' : 'accent-orange'"><span>数据库</span><strong>{{ dashboard?.database.status === 'connected' ? '已连接' : '连接异常' }}</strong><small>{{ dashboard?.database.status === 'connected' ? 'PostgreSQL 服务正常' : (dashboard?.database.error || '正在自动重连') }}</small></article><article class="overview-card accent-violet"><span>进行中下载</span><strong>{{ dashboard?.downloads.active ?? 0 }}</strong><small>排队、下载或暂停中</small></article><article class="overview-card accent-green"><span>累计完成</span><strong>{{ dashboard?.downloads.completedItems ?? 0 }}</strong><small>已完成下载</small></article><article class="overview-card accent-orange"><span>下载失败</span><strong>{{ dashboard?.downloads.failedItems ?? 0 }}</strong><small>可在下载管理中查看和重试</small></article></section>
      <section class="monitor-grid"><article class="chart-card"><header><div><span>CPU</span><strong>{{ fixed(current?.cpuPercent) }}%</strong></div><small>最近 60 秒</small></header><svg viewBox="0 0 100 70" preserveAspectRatio="none"><path d="M0 67H100" class="grid-line"/><polyline :points="cpuLine" class="chart-line cpu"/></svg></article><article class="chart-card"><header><div><span>内存</span><strong>{{ fixed(percent(current?.memoryUsed, current?.memoryTotal)) }}%</strong></div><small>{{ bytes(current?.memoryUsed) }} / {{ bytes(current?.memoryTotal) }}</small></header><svg viewBox="0 0 100 70" preserveAspectRatio="none"><path d="M0 67H100" class="grid-line"/><polyline :points="memoryLine" class="chart-line memory"/></svg></article><article class="chart-card network-card"><header><div><span>网络</span><strong>{{ bytes(current?.receiveBps) }}/s</strong></div><small><b class="down-dot"></b>接收　<b class="up-dot"></b>发送 {{ bytes(current?.transmitBps) }}/s</small></header><svg viewBox="0 0 100 70" preserveAspectRatio="none"><path d="M0 67H100" class="grid-line"/><polyline :points="downLine" class="chart-line down"/><polyline :points="upLine" class="chart-line up"/></svg></article></section>
    </main></el-main></el-container>
  </el-container>
</template>

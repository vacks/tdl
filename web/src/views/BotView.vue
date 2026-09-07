<script setup lang="ts">
import { onMounted, reactive, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { useRouter } from 'vue-router'
import { api } from '@/api'

type Settings = { proxyUrl: string; download: { threads: number; taskLimit: number; poolSize: number; delayMs: number; tempFilenameTemplate: string; finalFilenameTemplate: string }; bot: { enabled: boolean; token: string; controlUserIds: number[]; notifications: { taskCreated: boolean; taskCompleted: boolean; taskPartial: boolean; taskFailed: boolean } } }
const router = useRouter(); const saving = ref(false); const userIDs = ref<string[]>([]); const userIDInput = ref('')
const form = reactive<Settings>({ proxyUrl: '', download: { threads: 4, taskLimit: 2, poolSize: 8, delayMs: 0, tempFilenameTemplate: '', finalFilenameTemplate: '' }, bot: { enabled: false, token: '', controlUserIds: [], notifications: { taskCreated: true, taskCompleted: true, taskPartial: true, taskFailed: true } } })
async function load() { const result = await api<{ settings: Settings }>('/api/config'); Object.assign(form, result.settings); Object.assign(form.download, result.settings.download); Object.assign(form.bot, result.settings.bot); Object.assign(form.bot.notifications, result.settings.bot.notifications); userIDs.value = (result.settings.bot.controlUserIds || []).map(String) }
function parsedUserIDs(value: unknown): number[] {
  const entries = Array.isArray(value) ? value : [value]
  return [...new Set(entries.flatMap((entry) => String(entry ?? '').split(/[\s,]+/)).map((entry) => Number(entry.trim())).filter((entry) => Number.isSafeInteger(entry) && entry > 0))]
}
function addUserIDs() { const added = userIDInput.value.split(/[\s,]+/).map((value) => value.trim()).filter(Boolean); if (added.length === 0) return; if (added.some((value) => !/^\d+$/.test(value) || !Number.isSafeInteger(Number(value)) || Number(value) <= 0)) return void ElMessage.error('控制用户 ID 必须是正整数'); userIDs.value = [...new Set([...userIDs.value, ...added])]; userIDInput.value = '' }
function removeUserID(id: string) { userIDs.value = userIDs.value.filter((value) => value !== id) }
async function save() { try { if (userIDInput.value.trim()) return void ElMessage.error('请先添加当前控制用户 ID'); form.bot.controlUserIds = parsedUserIDs(userIDs.value); if (form.bot.enabled && form.bot.controlUserIds.length === 0) return void ElMessage.error('请至少添加一个控制用户 ID'); saving.value = true; await api('/api/config', { method: 'PUT', body: JSON.stringify(form) }); userIDs.value = form.bot.controlUserIds.map(String); ElMessage.success(form.bot.enabled ? 'Bot 控制已保存并启用' : 'Bot 控制已关闭') } catch (error) { ElMessage.error(error instanceof Error ? error.message : '保存失败') } finally { saving.value = false } }
async function logout() { await api('/api/auth/logout', { method: 'POST' }); await router.replace('/login') }
onMounted(async () => { try { const session = await api<{ authenticated: boolean }>('/api/auth/session'); if (!session.authenticated) return void router.replace('/login'); await load() } catch { await router.replace('/login') } })
</script>

<template>
  <el-container class="shell">
    <el-aside width="244px"><div class="brand">tdl <span>WEB</span></div><el-menu default-active="/bot" router><el-menu-item index="/">仪表盘</el-menu-item><el-menu-item index="/accounts">Telegram 账户</el-menu-item><el-menu-item index="/downloads">下载管理</el-menu-item><el-menu-item index="/bot">Bot 控制</el-menu-item><el-menu-item index="/settings">功能配置</el-menu-item></el-menu><div class="sidebar-meta"><span>上游 tdl <b>v0.20.4</b></span><span>项目版本 <b>v0.1.0</b></span></div></el-aside>
    <el-container><el-header><span>Bot 控制</span><el-button text @click="logout">退出管理台</el-button></el-header><el-main class="settings-page"><section class="page-heading"><div><p class="eyebrow">BOT CONTROL</p><h1>Bot 控制</h1><p class="subtle">Bot 仅接受指定 Telegram 用户 ID 的消息和按钮操作。</p></div><el-button type="primary" :loading="saving" @click="save">保存配置</el-button></section>
      <el-card shadow="never" class="settings-card"><el-form label-position="top" @submit.prevent><el-form-item label="启用 Bot 控制"><div style="display:flex;align-items:center;gap:12px"><el-switch v-model="form.bot.enabled" /><span>{{ form.bot.enabled ? '已启用' : '已关闭' }}</span></div></el-form-item><el-form-item label="Bot Token"><el-input v-model="form.bot.token" type="password" show-password placeholder="123456:ABC..." autocomplete="off" /></el-form-item><el-form-item label="控制用户 ID"><div class="id-tag-input"><el-tag v-for="id in userIDs" :key="id" closable @close="removeUserID(id)">{{ id }}</el-tag><el-input v-model="userIDInput" placeholder="添加 Telegram 用户 ID" @keydown.enter.prevent="addUserIDs" /></div></el-form-item><p class="field-help">填写允许控制 Bot 的 Telegram 数字用户 ID；可点击标签右侧的 × 删除。可向 <code>@userinfobot</code> 发送 <code>/start</code> 查询自己的 ID。</p><el-form-item label="通知类型"><div style="display:flex;flex-wrap:wrap;gap:24px"><el-checkbox v-model="form.bot.notifications.taskCreated">任务创建</el-checkbox><el-checkbox v-model="form.bot.notifications.taskCompleted">下载完成</el-checkbox><el-checkbox v-model="form.bot.notifications.taskPartial">部分完成</el-checkbox><el-checkbox v-model="form.bot.notifications.taskFailed">下载失败</el-checkbox></div></el-form-item><el-alert type="info" :closable="false" title="可用命令：/help、/tasks、/task 任务ID；也可直接发送 Telegram 消息链接创建任务。活动任务详情会自动更新。" /></el-form></el-card>
    </el-main></el-container>
  </el-container>
</template>

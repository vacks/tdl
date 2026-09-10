<script setup lang="ts">
import { onMounted, reactive, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { useRouter } from 'vue-router'
import { api } from '@/api'

type Settings = {
  proxyUrl: string
  download: { threads: number; taskLimit: number; concurrentJobs: number; poolSize: number; delayMs: number; tempFilenameTemplate: string; finalFilenameTemplate: string }
  bot: { enabled: boolean; token: string; controlUserIds: number[]; notifications: { taskCreated: boolean; taskCompleted: boolean; taskPartial: boolean; taskFailed: boolean } }
  reaction: { enabled: boolean; emojis: string[] }
  cleanup: { retentionDays: number }
}
type ConfigResponse = { settings: Settings; downloadDir: string; upstreamVersion: string }
const router = useRouter()
const form = reactive<Settings>({ proxyUrl: '', download: { threads: 4, taskLimit: 2, concurrentJobs: 1, poolSize: 8, delayMs: 0, tempFilenameTemplate: '{{ .DialogID }}_{{ .MessageID }}_{{ filenamify .FileName }}', finalFilenameTemplate: '{{ .DialogID }}_{{ .MessageID }}_{{ if .MessageText }}{{ .MessageText }}_{{ end }}{{ .FileName }}' }, bot: { enabled: false, token: '', controlUserIds: [], notifications: { taskCreated: true, taskCompleted: true, taskPartial: true, taskFailed: true } }, reaction: { enabled: false, emojis: ['👍'] }, cleanup: { retentionDays: 90 } })
const downloadDir = ref('')
const saving = ref(false)
const userIDs = ref<string[]>([])
const userIDInput = ref('')
const reactionEmojis = ref<string[]>([])
const reactionEmojiInput = ref('')

async function load() {
  const data = await api<ConfigResponse>('/api/config')
  Object.assign(form, data.settings)
  Object.assign(form.download, data.settings.download)
  Object.assign(form.bot, data.settings.bot)
  Object.assign(form.bot.notifications, data.settings.bot.notifications)
  Object.assign(form.reaction, data.settings.reaction || { enabled: false, emojis: ['👍'] })
  Object.assign(form.cleanup, data.settings.cleanup || { retentionDays: 90 })
  userIDs.value = (form.bot.controlUserIds || []).map(String)
  reactionEmojis.value = [...(form.reaction.emojis || [])]
  downloadDir.value = data.downloadDir
}
async function save() {
  if (userIDInput.value.trim()) return void ElMessage.error('请先添加当前控制用户 ID')
  if (reactionEmojiInput.value.trim()) return void ElMessage.error('请先添加当前触发表情')
  form.bot.controlUserIds = parsedUserIDs(userIDs.value)
  if (form.bot.enabled && form.bot.controlUserIds.length === 0) return void ElMessage.error('请至少添加一个控制用户 ID')
  form.reaction.emojis = [...reactionEmojis.value]
  saving.value = true
  try { await api('/api/config', { method: 'PUT', body: JSON.stringify(form) }); userIDs.value = form.bot.controlUserIds.map(String); ElMessage.success('配置已保存，新建 Telegram 登录和后续下载任务将使用这些设置') }
  catch (error) { ElMessage.error(error instanceof Error ? error.message : '保存失败') } finally { saving.value = false }
}
async function cleanupNow() { try { const result = await api<{ jobs: number; chatJobs: number; requests: number; events: number; reactionEvents: number }>('/api/maintenance/cleanup', { method: 'POST', body: JSON.stringify({ retentionDays: form.cleanup.retentionDays }) }); ElMessage.success(`已清理 ${result.jobs} 条消息任务、${result.chatJobs || 0} 条会话任务、${result.events} 条状态事件`) } catch (error) { ElMessage.error(error instanceof Error ? error.message : '清理失败') } }
function parsedUserIDs(value: unknown): number[] { const entries = Array.isArray(value) ? value : [value]; return [...new Set(entries.flatMap((entry) => String(entry ?? '').split(/[\s,]+/)).map((entry) => Number(entry.trim())).filter((entry) => Number.isSafeInteger(entry) && entry > 0))] }
function addUserIDs() { const added = userIDInput.value.split(/[\s,]+/).map((value) => value.trim()).filter(Boolean); if (added.length === 0) return; if (added.some((value) => !/^\d+$/.test(value) || !Number.isSafeInteger(Number(value)) || Number(value) <= 0)) return void ElMessage.error('控制用户 ID 必须是正整数'); userIDs.value = [...new Set([...userIDs.value, ...added])]; userIDInput.value = '' }
function removeUserID(id: string) { userIDs.value = userIDs.value.filter((value) => value !== id) }
function validReactionEmoji(value: string) { return !/\s/.test(value) && /^[\p{Extended_Pictographic}\p{Emoji_Modifier}\u200d\ufe0f\u20e3#*0-9]+$/u.test(value) && (/\p{Extended_Pictographic}/u.test(value) || value.includes('\u20e3')) }
function addReactionEmojis() { const added = reactionEmojiInput.value.split(/[\s,]+/).map((value) => value.trim()).filter(Boolean); if (added.length === 0) return; if (added.some((value) => !validReactionEmoji(value))) return void ElMessage.error('仅支持普通 Unicode 表情，不支持文字或自定义 Premium 表情'); reactionEmojis.value = [...new Set([...reactionEmojis.value, ...added])]; reactionEmojiInput.value = '' }
function removeReactionEmoji(emoji: string) { reactionEmojis.value = reactionEmojis.value.filter((value) => value !== emoji) }
async function logout() { await api('/api/auth/logout', { method: 'POST' }); await router.replace('/login') }
onMounted(async () => { try { const session = await api<{ authenticated: boolean }>('/api/auth/session'); if (!session.authenticated) return void router.replace('/login'); await load() } catch { await router.replace('/login') } })
</script>

<template>
  <el-container class="shell">
    <el-aside width="244px"><div class="brand">TDL 管理</div><el-menu default-active="/settings" router><el-menu-item index="/">仪表盘</el-menu-item><el-menu-item index="/accounts">登录管理</el-menu-item><el-menu-item index="/downloads">下载管理</el-menu-item><el-menu-item index="/settings">配置管理</el-menu-item></el-menu><div class="sidebar-meta"><span>上游 tdl <b>v0.20.4</b></span><span>项目版本 <b>v0.1.0</b></span></div></el-aside>
    <el-container><el-header><span>配置管理</span><el-button text @click="logout">退出管理台</el-button></el-header>
      <el-main class="settings-page"><section class="page-heading"><div><p class="eyebrow">SETTINGS</p><h1>配置管理</h1><p class="subtle">管理代理、下载与自动下载偏好。</p></div><el-button type="primary" :loading="saving" @click="save">保存配置</el-button></section>
        <el-card shadow="never" class="settings-card"><template #header><strong>网络代理</strong></template><el-form label-position="top" @submit.prevent><el-form-item label="代理地址"><el-input v-model="form.proxyUrl" placeholder="例如 socks5://127.0.0.1:1080 或 http://user:pass@host:port" clearable /></el-form-item><p class="field-help">留空表示直连。支持 HTTP、HTTPS、SOCKS5 和 SOCKS5H；保存后，对新发起的 Telegram 登录立即生效。</p></el-form></el-card>
        <el-card shadow="never" class="settings-card"><template #header><strong>Bot 控制</strong></template><el-form label-position="top" @submit.prevent><el-form-item label="启用 Bot 控制"><div style="display:flex;align-items:center;gap:12px"><el-switch v-model="form.bot.enabled" /><span>{{ form.bot.enabled ? '已启用' : '已关闭' }}</span></div></el-form-item><el-form-item label="Bot Token"><el-input v-model="form.bot.token" type="password" show-password placeholder="123456:ABC..." autocomplete="off" /></el-form-item><el-form-item label="控制用户 ID"><div class="id-tag-input"><el-tag v-for="id in userIDs" :key="id" closable @close="removeUserID(id)">{{ id }}</el-tag><el-input v-model="userIDInput" placeholder="添加 Telegram 用户 ID" @keydown.enter.prevent="addUserIDs" /></div></el-form-item><p class="field-help">填写允许控制 Bot 的 Telegram 数字用户 ID；可点击标签右侧的 × 删除。可向 <code>@userinfobot</code> 发送 <code>/start</code> 查询自己的 ID。</p><el-form-item label="通知类型"><div style="display:flex;flex-wrap:wrap;gap:24px"><el-checkbox v-model="form.bot.notifications.taskCreated">任务创建</el-checkbox><el-checkbox v-model="form.bot.notifications.taskCompleted">下载完成</el-checkbox><el-checkbox v-model="form.bot.notifications.taskPartial">部分完成</el-checkbox><el-checkbox v-model="form.bot.notifications.taskFailed">下载失败</el-checkbox></div></el-form-item><el-alert type="info" :closable="false" title="可用命令：/help、/list、/status、/config、/restart；也可直接发送 Telegram 消息链接创建任务。" /></el-form></el-card>
        <el-card shadow="never" class="settings-card"><template #header><strong>表情触发下载</strong></template><el-form label-position="top" @submit.prevent><el-form-item label="启用监听"><div style="display:flex;align-items:center;gap:12px"><el-switch v-model="form.reaction.enabled" /><span>{{ form.reaction.enabled ? '已启用' : '已关闭' }}</span></div></el-form-item><el-form-item label="触发表情"><div class="id-tag-input"><el-tag v-for="emoji in reactionEmojis" :key="emoji" closable @close="removeReactionEmoji(emoji)">{{ emoji }}</el-tag><el-input v-model="reactionEmojiInput" placeholder="添加触发表情" @keydown.enter.prevent="addReactionEmojis" /></div></el-form-item><p class="field-help">添加需要触发下载的常用表情；可点击标签右侧的 × 删除。对带媒体的消息作出匹配表情后，会下载该消息的媒体；相册会作为一组下载。</p></el-form></el-card>
        <el-card shadow="never" class="settings-card"><template #header><strong>TDL 下载配置</strong></template><el-form label-position="top" @submit.prevent><el-row :gutter="16"><el-col :xs="24" :sm="12" :md="6"><el-form-item label="单任务线程数"><el-input-number v-model="form.download.threads" :min="1" :max="32" /></el-form-item></el-col><el-col :xs="24" :sm="12" :md="6"><el-form-item label="单任务文件并发数"><el-input-number v-model="form.download.taskLimit" :min="1" :max="16" /></el-form-item></el-col><el-col :xs="24" :sm="12" :md="6"><el-form-item label="同时执行下载任务数"><el-input-number v-model="form.download.concurrentJobs" :min="1" :max="16" /></el-form-item></el-col><el-col :xs="24" :sm="12" :md="6"><el-form-item label="连接池大小"><el-input-number v-model="form.download.poolSize" :min="0" :max="64" /></el-form-item></el-col><el-col :xs="24" :sm="12" :md="6"><el-form-item label="任务间隔（毫秒）"><el-input-number v-model="form.download.delayMs" :min="0" :max="60000" /></el-form-item></el-col></el-row>
          <el-form-item label="临时文件命名模板（上游 tdl）"><el-input v-model="form.download.tempFilenameTemplate" /></el-form-item>
          <div class="field-help template-help" v-pre>
            <p>仅用于上游 tdl 写入私有临时目录。可用变量：</p>
            <ul><li><code>.DialogID</code>：Telegram 对话 ID</li><li><code>.MessageID</code>：媒体所在消息 ID</li><li><code>.MessageDate</code>：消息发送时间的 Unix 时间戳（秒）</li><li><code>.FileName</code>：Telegram 媒体原始文件名</li><li><code>.FileCaption</code>：该媒体消息附带的文字</li><li><code>.FileSize</code>：上游格式化后的文件大小</li><li><code>.DownloadDate</code>：命名时的 Unix 时间戳（秒）</li></ul>
            <p>可继续使用上游函数，例如 <code>filenamify .FileName</code>、<code>formatDate .MessageDate "2006-01-02"</code>、<code>upper .FileName</code>。</p>
          </div>
          <el-form-item label="最终文件命名模板"><el-input v-model="form.download.finalFilenameTemplate" /></el-form-item>
          <div class="field-help template-help" v-pre>
            <p>临时下载完成后由本项目移动至最终目录。可用变量：</p>
            <ul><li><code>.DialogID</code>：Telegram 对话 ID</li><li><code>.DialogName</code>：对话显示名称</li><li><code>.MessageID</code>：媒体所在消息 ID</li><li><code>.GroupedID</code>：Telegram 媒体组 ID；非媒体组消息为 <code>0</code></li><li><code>.MessageText</code>：按 Telegram 客户端显示逻辑取得的消息正文</li><li><code>.FileName</code>：Telegram 媒体原始文件名</li><li><code>.FileExt</code>：从原始文件名提取的扩展名，包含点，例如 <code>.mp4</code></li><li><code>.DownloadDate</code>：生成最终路径时的当前 Unix 时间戳（秒），可写成 <code>formatDate .DownloadDate "2006-01-02"</code> 取得指定格式的日期</li></ul>
            <p v-pre><strong>条件语法 <code>if</code>：</strong><code>{{ if .MessageText }}{{ .MessageText }}_{{ end }}</code> 是一个完整区块：消息正文不为空时输出“正文和下划线”，为空时整段不输出。也可写成 <code>{{ if .MessageText }}有正文{{ else }}无正文{{ end }}</code>。</p>
          </div>
        </el-form></el-card>
        <el-card shadow="never" class="settings-card"><template #header><strong>历史数据清理</strong></template><el-form label-position="top" @submit.prevent><el-form-item label="历史保留天数"><el-input-number v-model="form.cleanup.retentionDays" :min="1" :max="3650" /></el-form-item><p class="field-help">清理完成、失败、部分完成或已取消的任务记录，以及对应请求、状态事件和表情收件箱历史；不会删除已下载的最终文件。保存后策略生效，可随时立即执行一次清理。</p><el-button @click="cleanupNow">立即清理</el-button></el-form></el-card>
      </el-main>
    </el-container>
  </el-container>
</template>

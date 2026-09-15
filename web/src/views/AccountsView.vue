<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { Loading } from '@element-plus/icons-vue'
import { useRouter } from 'vue-router'
import { APIError, api } from '@/api'
import { projectVersion } from '@/buildInfo'

type Account = {
  id: string; telegramId?: number; firstName?: string; lastName?: string; username?: string
  state: 'starting' | 'waiting_for_qr' | 'waiting_for_2fa' | 'authorized' | 'expired' | 'error' | 'stopped'
  error?: string; qrCode?: string; createdAt: string; checkedAt?: string
}
type AccountsResponse = { accounts: Account[]; currentId: string }

const router = useRouter()
const accounts = ref<Account[]>([])
const currentId = ref('')
const loading = ref(false)
const loginAccountId = ref('')
const password = ref('')
const passwordLoading = ref(false)
let timer: number | undefined

const loginAccount = computed(() => accounts.value.find((item) => item.id === loginAccountId.value))
const needsPolling = computed(() => accounts.value.some((item) => ['starting', 'waiting_for_qr', 'waiting_for_2fa'].includes(item.state)))

async function load() {
  try {
    const data = await api<AccountsResponse>('/api/telegram/accounts')
    accounts.value = data.accounts || []
    currentId.value = data.currentId
    if (loginAccount.value?.state === 'authorized') closeLogin()
  } catch (error) {
    if (error instanceof Error && error.message.includes('登录')) await router.replace('/login')
  }
}

async function startLogin() {
  loading.value = true
  try {
    const account = await api<Account>('/api/telegram/accounts', { method: 'POST' })
    loginAccountId.value = account.id
    await load()
  } catch (error) { ElMessage.error(error instanceof Error ? error.message : '无法开始登录') } finally { loading.value = false }
}

async function selectAccount(id: string) {
  try { await api(`/api/telegram/accounts/${id}/select`, { method: 'POST' }); await load(); ElMessage.success('已切换当前账户') }
  catch (error) { ElMessage.error(error instanceof Error ? error.message : '切换失败') }
}

async function submitPassword() {
  if (!loginAccount.value) return
  passwordLoading.value = true
  try {
    await api(`/api/telegram/accounts/${loginAccount.value.id}/password`, { method: 'POST', body: JSON.stringify({ password: password.value }) })
    password.value = ''
    ElMessage.success('正在验证两步验证密码')
  } catch (error) { ElMessage.error(error instanceof Error ? error.message : '提交失败') } finally { passwordLoading.value = false }
}

async function removeAccount(account: Account) {
  try {
    await ElMessageBox.confirm(`确定移除 ${displayName(account)} 吗？这会删除本服务保存的登录会话。`, '移除 Telegram 账户', { type: 'warning' })
    await api(`/api/telegram/accounts/${account.id}`, { method: 'DELETE' })
    if (loginAccountId.value === account.id) closeLogin()
    await load()
  } catch (error) {
    // Element Plus uses a non-Error rejection for a user cancellation; actual
    // API failures (including incomplete sensitive-file cleanup) must remain
    // visible to the administrator.
    if (error instanceof APIError || error instanceof Error) ElMessage.error(error.message)
  }
}
async function renewAccount(account: Account) {
  try {
    await ElMessageBox.confirm(`将移除 ${displayName(account)} 的失效会话，并开始新的二维码登录。是否继续？`, '重新登录 Telegram', { type: 'warning' })
    loading.value = true
    await api(`/api/telegram/accounts/${account.id}`, { method: 'DELETE' })
    const next = await api<Account>('/api/telegram/accounts', { method: 'POST' })
    loginAccountId.value = next.id
    await load()
    ElMessage.success('请扫描新的登录二维码')
  } catch (error) {
    if (error instanceof Error) ElMessage.error(error.message)
  } finally { loading.value = false }
}

function displayName(account: Account) {
  const name = [account.firstName, account.lastName].filter(Boolean).join(' ')
  return name || (account.username ? `@${account.username}` : '未完成登录的账户')
}
function stateText(state: Account['state']) {
  return ({ starting: '正在准备', waiting_for_qr: '等待扫码', waiting_for_2fa: '等待两步验证', authorized: '已登录', expired: '会话失效', error: '登录失败', stopped: '已停止' })[state]
}
function stateType(state: Account['state']) { return state === 'authorized' ? 'success' : state === 'expired' || state === 'error' ? 'danger' : state === 'stopped' ? 'info' : 'warning' }
function closeLogin() { loginAccountId.value = ''; password.value = '' }
async function logout() { await api('/api/auth/logout', { method: 'POST' }); await router.replace('/login') }

onMounted(async () => {
  const session = await api<{ authenticated: boolean }>('/api/auth/session')
  if (!session.authenticated) return void router.replace('/login')
  await load()
  timer = window.setInterval(() => { if (needsPolling.value) void load() }, 2500)
})
onBeforeUnmount(() => { if (timer) window.clearInterval(timer) })
</script>

<template>
  <el-container class="shell">
    <el-aside width="244px"><div class="brand">TDL 管理</div><el-menu default-active="/accounts" router><el-menu-item index="/">仪表盘</el-menu-item><el-menu-item index="/accounts">登录管理</el-menu-item><el-menu-item index="/downloads">下载管理</el-menu-item><el-menu-item index="/settings">配置管理</el-menu-item></el-menu><div class="sidebar-meta"><span>上游 tdl <b>v0.20.4</b></span><span>项目版本 <b>{{ projectVersion }}</b></span></div></el-aside>
    <el-container><el-header><span>登录管理</span><el-button text @click="logout">退出管理台</el-button></el-header>
      <el-main class="accounts-page">
        <section class="page-heading"><div><p class="eyebrow">ACCOUNT MANAGEMENT</p><h1>Telegram 登录管理</h1><p class="subtle">每个账户独立保存会话；切换当前账户会影响后续从 Web 或 Bot 链接新建的下载任务。</p></div><el-button type="primary" :loading="loading" @click="startLogin">添加 Telegram 账户</el-button></section>
        <el-alert title="请用已登录的 Telegram 手机客户端扫描二维码。" type="info" show-icon :closable="false" />
        <el-empty v-if="!accounts.length" description="还没有 Telegram 账户，点击右上角添加。" />
        <section v-else class="account-list"><el-card v-for="account in accounts" :key="account.id" shadow="never" class="account-card"><div class="account-row"><div><strong>{{ displayName(account) }}</strong><p v-if="account.username" class="subtle">@{{ account.username }}</p><p v-if="account.telegramId" class="subtle">ID: {{ account.telegramId }}</p><p v-if="account.checkedAt" class="subtle">会话最近检查：{{ account.checkedAt }}</p><p v-if="account.error" class="account-error">{{ account.error }}</p></div><div class="account-actions"><el-tag :type="stateType(account.state) as any">{{ stateText(account.state) }}</el-tag><el-tag v-if="currentId === account.id" type="primary">当前账户</el-tag><el-button v-if="account.state === 'expired'" type="primary" plain @click="renewAccount(account)">重新登录</el-button><el-button v-else-if="account.state !== 'authorized' && account.state !== 'stopped'" text type="primary" @click="loginAccountId = account.id">继续登录</el-button><el-button v-if="account.state === 'authorized' && currentId !== account.id" type="primary" plain @click="selectAccount(account.id)">切换为当前账户</el-button><el-button type="danger" text @click="removeAccount(account)">移除</el-button></div></div></el-card></section>
      </el-main>
    </el-container>
  </el-container>
  <el-dialog :model-value="Boolean(loginAccountId)" title="登录 Telegram 账户" width="420px" :close-on-click-modal="false" @update:model-value="(open: boolean) => { if (!open) closeLogin() }">
    <template v-if="loginAccount"><div v-if="loginAccount.state === 'starting'" class="login-progress"><el-icon class="is-loading"><Loading /></el-icon><p>正在连接 Telegram，请稍候…</p></div><template v-else-if="loginAccount.state === 'waiting_for_qr'"><p class="subtle">在 Telegram 手机客户端中打开「设置 → 设备 → 扫描二维码」，扫描下方二维码。</p><img v-if="loginAccount.qrCode" :src="loginAccount.qrCode" class="qr-code" alt="Telegram 登录二维码" /><el-skeleton v-else :rows="6" animated /></template><template v-else-if="loginAccount.state === 'waiting_for_2fa'"><p class="subtle">此账户启用了两步验证，请输入 Telegram 两步验证密码。</p><el-alert v-if="loginAccount.error" :title="loginAccount.error" type="error" :closable="false" class="notice" /><el-input v-model="password" type="password" show-password placeholder="两步验证密码" @keyup.enter="submitPassword" /><el-button type="primary" class="full" :loading="passwordLoading" @click="submitPassword">验证并登录</el-button></template><template v-else-if="loginAccount.state === 'error'"><el-result icon="error" title="登录未完成" :sub-title="loginAccount.error"><template #extra><el-button type="primary" @click="closeLogin">关闭</el-button></template></el-result></template><template v-else-if="loginAccount.state === 'authorized'"><el-result icon="success" title="登录成功" /></template></template>
  </el-dialog>
</template>

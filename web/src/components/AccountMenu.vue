<script setup lang="ts">
import { ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { api } from '../api'

const route = useRoute()
const router = useRouter()
const username = ref('')
const visible = ref(false)
const saving = ref(false)
const currentPassword = ref('')
const newPassword = ref('')
const confirmation = ref('')

async function loadSession() {
  try {
    const session = await api<{ authenticated: boolean; username?: string }>('/api/auth/session')
    username.value = session.authenticated ? (session.username || '') : ''
  } catch {
    username.value = ''
  }
}

function resetForm() {
  currentPassword.value = ''
  newPassword.value = ''
  confirmation.value = ''
}

async function changePassword() {
  if (!currentPassword.value || !newPassword.value || !confirmation.value) {
    ElMessage.error('请填写全部密码字段')
    return
  }
  if (newPassword.value !== confirmation.value) {
    ElMessage.error('两次输入的新密码不一致')
    return
  }
  saving.value = true
  try {
    await api('/api/auth/password', { method: 'POST', body: JSON.stringify({ currentPassword: currentPassword.value, newPassword: newPassword.value }) })
    ElMessage.success('密码已修改')
    visible.value = false
    resetForm()
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : '密码修改失败')
  } finally {
    saving.value = false
  }
}

async function logout() {
  await api('/api/auth/logout', { method: 'POST' })
  username.value = ''
  await router.replace('/login')
}

watch(() => route.fullPath, () => { void loadSession() }, { immediate: true })
</script>

<template>
  <div v-if="username" class="account-menu">
    <el-button text class="account-name" @click="visible = true">{{ username }}</el-button>
    <el-button text class="account-logout" @click="logout">退出</el-button>
  </div>

  <el-dialog v-model="visible" title="修改管理员密码" width="400px" :close-on-click-modal="false" @closed="resetForm">
    <el-form label-position="top" @submit.prevent="changePassword">
      <el-form-item label="当前密码"><el-input v-model="currentPassword" type="password" show-password autocomplete="current-password" @keyup.enter="changePassword" /></el-form-item>
      <el-form-item label="新密码"><el-input v-model="newPassword" type="password" show-password autocomplete="new-password" @keyup.enter="changePassword" /></el-form-item>
      <el-form-item label="确认新密码"><el-input v-model="confirmation" type="password" show-password autocomplete="new-password" @keyup.enter="changePassword" /></el-form-item>
    </el-form>
    <template #footer><el-button @click="visible = false">取消</el-button><el-button type="primary" :loading="saving" @click="changePassword">确认修改</el-button></template>
  </el-dialog>
</template>

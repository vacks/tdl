<script setup lang="ts">
import { ref } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { api } from '@/api'

const router = useRouter()
const username = ref('admin')
const password = ref('')
const loading = ref(false)

async function submit() {
  if (loading.value) return
  loading.value = true
  try {
    await api('/api/auth/login', { method: 'POST', body: JSON.stringify({ username: username.value, password: password.value }) })
    await router.push('/')
  } catch (err) { ElMessage.error(err instanceof Error ? err.message : '登录失败') } finally { loading.value = false }
}
</script>

<template>
  <main class="login-page">
    <el-card class="login-card" shadow="never">
      <p class="eyebrow">TDL WEB</p>
      <h1>管理台登录</h1>
      <p class="subtle">使用你在部署配置中设置的管理员账号。</p>
      <el-form label-position="top" @submit.prevent="submit">
        <el-form-item label="用户名"><el-input v-model="username" autocomplete="username" /></el-form-item>
        <el-form-item label="密码"><el-input v-model="password" type="password" show-password autocomplete="current-password" /></el-form-item>
        <el-button type="primary" native-type="submit" :loading="loading" class="full">登录</el-button>
      </el-form>
    </el-card>
  </main>
</template>

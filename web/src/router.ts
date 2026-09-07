import { createRouter, createWebHistory } from 'vue-router'
import LoginView from './views/LoginView.vue'
import DashboardView from './views/DashboardView.vue'
import AccountsView from './views/AccountsView.vue'
import SettingsView from './views/SettingsView.vue'
import DownloadsView from './views/DownloadsView.vue'

export default createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/login', component: LoginView },
    { path: '/', component: DashboardView },
    { path: '/accounts', component: AccountsView },
    { path: '/settings', component: SettingsView },
    { path: '/downloads', component: DownloadsView },
    { path: '/bot', redirect: '/settings' },
    { path: '/:pathMatch(.*)*', redirect: '/' }
  ]
})

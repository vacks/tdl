import { createRouter, createWebHistory } from 'vue-router'

export default createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/login', component: () => import('./views/LoginView.vue') },
    { path: '/', component: () => import('./views/DashboardView.vue') },
    { path: '/accounts', component: () => import('./views/AccountsView.vue') },
    { path: '/settings', component: () => import('./views/SettingsView.vue') },
    { path: '/downloads', component: () => import('./views/DownloadsView.vue') },
    { path: '/:pathMatch(.*)*', redirect: '/' }
  ]
})

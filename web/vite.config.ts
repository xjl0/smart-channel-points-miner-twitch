import path from 'node:path'
import tailwindcss from '@tailwindcss/vite'
import react from '@vitejs/plugin-react'
import { defineConfig, loadEnv } from 'vite'

// Dev-сервер проксирует вызовы Connect RPC и SSE на Go-бэкенд, поэтому
// админ-панель использует относительные URL и в dev, и в продакшене.
// Логин админки берётся из ../.env (MINER_ADMIN_USER / MINER_ADMIN_PASSWORD)
// и подставляется прокси сам, чтобы в dev не вводить пароль.
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, path.resolve(import.meta.dirname, '..'), 'MINER_')
  const headers: Record<string, string> = {}
  if (env.MINER_ADMIN_PASSWORD) {
    const user = env.MINER_ADMIN_USER || 'admin'
    headers.Authorization = 'Basic ' + Buffer.from(`${user}:${env.MINER_ADMIN_PASSWORD}`).toString('base64')
  }
  const backend = { target: 'http://localhost:8080', headers }
  return {
    plugins: [react(), tailwindcss()],
    resolve: {
      alias: { '@': path.resolve(import.meta.dirname, './src') },
    },
    // Одно небольшое админ-приложение: одного бандла достаточно.
    build: { chunkSizeWarningLimit: 800 },
    server: {
      proxy: {
        '/miner.v1.': backend,
        '/live': backend, // SSE-поток
      },
    },
  }
})

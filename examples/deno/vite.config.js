import { defineConfig } from 'vite'
import deno from '@deno/vite-plugin'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [deno(), react()],
  server: {
    headers: {
      "Cross-Origin-Opener-Policy": "same-origin",
      "Cross-Origin-Opener-Policy-Report-Only": "same-origin",
      "Cross-Origin-Embedder-Policy": "require-corp"
    }
  }
})

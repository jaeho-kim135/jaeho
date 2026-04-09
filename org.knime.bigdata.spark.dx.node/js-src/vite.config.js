import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

export default defineConfig({
  plugins: [vue()],
  base: './',
  build: {
    outDir: 'dist',
    emptyOutDir: true,
    rollupOptions: {
      input: {
        'spark-expression': 'spark-expression.html',
        'spark-ruleengine': 'spark-ruleengine.html',
        'spark-concatenate': 'spark-concatenate.html',
        'spark-editcolumn': 'spark-editcolumn.html'
      },
      output: {
        entryFileNames: 'assets/[name].js',
        chunkFileNames: 'assets/[name].js',
        assetFileNames: 'assets/[name].[ext]',
        // Force all shared code into the 'index' chunk
        // so each dialog only needs to load: entry.js, entry.css, index.js
        manualChunks(id) {
          if (id.includes('node_modules')) return 'index'
          if (id.includes('/components/')) return 'index'
          if (id.includes('knimeService')) return 'index'
        }
      }
    }
  }
})

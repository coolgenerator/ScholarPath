import { defineConfig } from 'vite'
import path from 'path'
import fs from 'fs'
import tailwindcss from '@tailwindcss/vite'
import react from '@vitejs/plugin-react'

const REACT_VENDOR_PACKAGES = new Set(['react', 'react-dom', 'react-router', 'scheduler', 'cookie', 'set-cookie-parser'])
const MOTION_VENDOR_PACKAGES = new Set(['motion', 'framer-motion', 'motion-dom'])
const MARKDOWN_VENDOR_PREFIXES = ['remark-', 'rehype-', 'micromark', 'mdast-util-', 'hast-util-', 'unist-util-']
const MARKDOWN_VENDOR_PACKAGES = new Set([
  'react-markdown',
  'vfile',
  'property-information',
  'decode-named-character-reference',
  'space-separated-tokens',
  'comma-separated-tokens',
  'web-namespaces',
  'zwitch',
  'devlop',
  'trim-lines',
  'markdown-table',
])
const UI_VENDOR_PACKAGES = new Set(['lucide-react', 'clsx', 'tailwind-merge', 'sonner', 'vaul'])

function getPackageName(id: string) {
  const nodeModulesIndex = id.lastIndexOf('/node_modules/')

  if (nodeModulesIndex === -1) {
    return null
  }

  const packagePath = id.slice(nodeModulesIndex + '/node_modules/'.length)
  const [first, second] = packagePath.split('/')

  if (!first) {
    return null
  }

  if (first.startsWith('@') && second) {
    return `${first}/${second}`
  }

  return first
}

function isMarkdownPackage(packageName: string) {
  return MARKDOWN_VENDOR_PACKAGES.has(packageName) || MARKDOWN_VENDOR_PREFIXES.some((prefix) => packageName.startsWith(prefix))
}

function copyUiShotsPlugin() {
  return {
    name: 'copy-ui-shots',
    closeBundle() {
      const sourceDir = path.resolve(__dirname, 'output/ui-shots')
      const targetDir = path.resolve(__dirname, 'dist/output/ui-shots')

      if (!fs.existsSync(sourceDir)) {
        return
      }

      fs.mkdirSync(targetDir, { recursive: true })
      fs.cpSync(sourceDir, targetDir, { recursive: true, force: true })
    },
  }
}

function getMimeType(filePath: string) {
  const extension = path.extname(filePath)

  if (extension === '.png') return 'image/png'
  if (extension === '.jpg' || extension === '.jpeg') return 'image/jpeg'
  if (extension === '.webp') return 'image/webp'
  if (extension === '.svg') return 'image/svg+xml'

  return 'application/octet-stream'
}

function serveUiShotsPlugin() {
  const sourceDir = path.resolve(__dirname, 'output/ui-shots')

  return {
    name: 'serve-ui-shots',
    configureServer(server: import('vite').ViteDevServer) {
      server.middlewares.use('/output/ui-shots', (req, res, next) => {
        const requestPath = decodeURIComponent((req.url ?? '/').split('?')[0]).replace(/^\/+/, '')
        const filePath = path.resolve(sourceDir, requestPath)

        if (!filePath.startsWith(sourceDir)) {
          res.statusCode = 403
          res.end('Forbidden')
          return
        }

        if (!fs.existsSync(filePath) || !fs.statSync(filePath).isFile()) {
          next()
          return
        }

        res.setHeader('Content-Type', getMimeType(filePath))
        fs.createReadStream(filePath).pipe(res)
      })
    },
  }
}

export default defineConfig({
  plugins: [
    // The React and Tailwind plugins are both required for Make, even if
    // Tailwind is not being actively used – do not remove them
    react(),
    tailwindcss(),
    serveUiShotsPlugin(),
    copyUiShotsPlugin(),
  ],
  resolve: {
    alias: {
      // Alias @ to the src directory
      '@': path.resolve(__dirname, './src'),
    },
  },

  // File types to support raw imports. Never add .css, .tsx, or .ts files to this.
  assetsInclude: ['**/*.svg', '**/*.csv'],

  build: {
    rollupOptions: {
      output: {
        manualChunks(id) {
          const packageName = getPackageName(id)

          if (!packageName) {
            return undefined
          }

          if (REACT_VENDOR_PACKAGES.has(packageName)) {
            return 'react-vendor'
          }

          if (MOTION_VENDOR_PACKAGES.has(packageName)) {
            return 'motion-vendor'
          }

          if (packageName === 'd3' || packageName.startsWith('d3-') || packageName === 'internmap' || packageName === 'delaunator' || packageName === 'robust-predicates') {
            return 'd3-vendor'
          }

          if (isMarkdownPackage(packageName)) {
            return 'markdown-vendor'
          }

          if (packageName.startsWith('@radix-ui/') || UI_VENDOR_PACKAGES.has(packageName)) {
            return 'ui-vendor'
          }

          return undefined
        },
      },
    },
  },

  server: {
    proxy: {
      '/api': {
        // In Docker: 'app' service; locally: 'localhost'
        target: process.env.API_TARGET || 'http://localhost:8000',
        changeOrigin: true,
        ws: true,
      },
    },
  },
})

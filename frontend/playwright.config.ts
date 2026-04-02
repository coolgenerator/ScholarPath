import { defineConfig } from '@playwright/test';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const baseURL = process.env.PLAYWRIGHT_BASE_URL ?? 'http://127.0.0.1:4173';
const skipWebServer = process.env.PLAYWRIGHT_SKIP_WEBSERVER === '1';
const configDir = path.dirname(fileURLToPath(import.meta.url));

export default defineConfig({
  testDir: './e2e',
  fullyParallel: false,
  workers: 1,
  retries: process.env.CI ? 1 : 0,
  timeout: 90_000,
  expect: {
    timeout: 20_000,
  },
  outputDir: 'output/playwright/results',
  reporter: [
    ['list'],
    ['html', { open: 'never', outputFolder: 'output/playwright/report' }],
  ],
  use: {
    baseURL,
    headless: true,
    actionTimeout: 15_000,
    navigationTimeout: 30_000,
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
    video: 'retain-on-failure',
  },
  webServer: skipWebServer
    ? undefined
    : {
        command: 'npm run dev -- --host 127.0.0.1 --port 4173',
        cwd: configDir,
        port: 4173,
        reuseExistingServer: !process.env.CI,
        timeout: 120_000,
      },
  projects: [
    {
      name: 'contract',
      testMatch: /.*\.contract\.spec\.ts/,
    },
    {
      name: 'live',
      testMatch: /.*\.live\.spec\.ts/,
    },
  ],
});

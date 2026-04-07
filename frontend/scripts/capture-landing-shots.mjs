import { execFileSync } from 'node:child_process';
import crypto from 'node:crypto';
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { chromium, devices } from '@playwright/test';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(__dirname, '../..');
const frontendRoot = path.resolve(__dirname, '..');
const outputDir = path.resolve(frontendRoot, 'output/ui-shots');

const FRONTEND_URL = process.env.LANDING_FRONTEND_URL || 'http://127.0.0.1:5173';
const BACKEND_URL = process.env.LANDING_BACKEND_URL || 'http://127.0.0.1:8000';

const OUTPUT_FILES = {
  schoolListDesktop: path.join(outputDir, 'landing-school-list-desktop.png'),
  schoolListMobile: path.join(outputDir, 'landing-school-list-mobile.png'),
  offersDesktop: path.join(outputDir, 'landing-offers-desktop.png'),
  offersMobile: path.join(outputDir, 'landing-offers-mobile.png'),
  decisionsDesktop: path.join(outputDir, 'landing-decisions-desktop.png'),
  decisionsMobile: path.join(outputDir, 'landing-decisions-mobile.png'),
};

const FREEZE_STYLES = `
  *, *::before, *::after {
    animation-duration: 0s !important;
    animation-delay: 0s !important;
    transition-duration: 0s !important;
    transition-delay: 0s !important;
    caret-color: transparent !important;
  }
`;

function makeSessionId() {
  if (typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID();
  }
  return `landing-${Date.now()}-${Math.random().toString(36).slice(2)}`;
}

async function fetchWithTimeout(url, options = {}, timeoutMs = 6000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}

async function probeService(url, label) {
  try {
    const response = await fetchWithTimeout(url, {}, 5000);
    if (!response.ok) {
      throw new Error(`${label} responded with ${response.status}`);
    }
  } catch (error) {
    throw new Error(
      `${label} is not reachable at ${url}: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
}

async function postJson(url, label) {
  const response = await fetchWithTimeout(
    url,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
    },
    10000,
  );

  if (!response.ok) {
    throw new Error(`${label} failed with ${response.status}: ${await response.text()}`);
  }

  const payload = await response.json();
  if (payload?.error) {
    throw new Error(`${label} returned an error: ${payload.error}`);
  }

  return payload;
}

function resolvePythonCommand() {
  const candidates = [process.env.PYTHON_BIN, 'python3', 'python'].filter(Boolean);
  for (const candidate of candidates) {
    try {
      execFileSync(candidate, ['--version'], {
        cwd: repoRoot,
        stdio: 'ignore',
      });
      return candidate;
    } catch {
      continue;
    }
  }
  throw new Error('Could not find a usable Python interpreter. Set PYTHON_BIN or install python3.');
}

function runFixtureSeeder() {
  const python = resolvePythonCommand();
  try {
    const stdout = execFileSync(
      python,
      ['-m', 'scholarpath.scripts.seed_landing_capture_fixture'],
      {
        cwd: repoRoot,
        encoding: 'utf8',
      },
    ).trim();
    return JSON.parse(stdout);
  } catch (error) {
    if (error instanceof Error && 'stdout' in error) {
      const stdout = String(error.stdout || '').trim();
      const stderr = String(error.stderr || '').trim();
      throw new Error(`Fixture seeder failed.\nstdout: ${stdout}\nstderr: ${stderr}`);
    }
    throw error;
  }
}

async function settlePage(page) {
  await page.waitForLoadState('domcontentloaded');
  await page.waitForLoadState('networkidle', { timeout: 6000 }).catch(() => {});
  await page.addStyleTag({ content: FREEZE_STYLES }).catch(() => {});
  await page.waitForTimeout(250);
}

async function buildContext(browser, fixture, mode) {
  const sessionId = makeSessionId();
  const contextOptions =
    mode === 'mobile'
      ? {
          ...devices['iPhone 14'],
          locale: 'zh-CN',
          colorScheme: 'light',
          reducedMotion: 'reduce',
        }
      : {
          viewport: { width: 1560, height: 980 },
          deviceScaleFactor: 2,
          isMobile: false,
          hasTouch: false,
          locale: 'zh-CN',
          colorScheme: 'light',
          reducedMotion: 'reduce',
        };

  const context = await browser.newContext(contextOptions);
  await context.addInitScript(
    ({ studentId, studentName, currentSessionId }) => {
      localStorage.setItem('sp_student_id', studentId);
      localStorage.setItem('sp_student_name', studentName);
      localStorage.setItem('sp_session_id', currentSessionId);
      localStorage.setItem('sp_locale', 'zh');
    },
    {
      studentId: fixture.student_id,
      studentName: fixture.student_name,
      currentSessionId: sessionId,
    },
  );

  return { context, page: await context.newPage(), sessionId };
}

async function openWorkspace(page, sessionId, nav) {
  await page.goto(`${FRONTEND_URL}/s/${sessionId}/${nav}`, {
    waitUntil: 'domcontentloaded',
  });
  await settlePage(page);
}

async function captureSchoolList(browser, fixture, mode, outputPath) {
  const { context, page, sessionId } = await buildContext(browser, fixture, mode);
  try {
    await openWorkspace(page, sessionId, 'school-list');
    await page.getByRole('button', { name: 'AI推荐' }).waitFor({ timeout: 15000 });
    await page.getByText('排序: 匹配分数').waitFor({ timeout: 15000 });
    await page.waitForTimeout(1200);
    await page.screenshot({ path: outputPath, type: 'png' });
  } finally {
    await context.close();
  }
}

async function captureOffers(browser, fixture, mode, outputPath) {
  const { context, page, sessionId } = await buildContext(browser, fixture, mode);
  try {
    await openWorkspace(page, sessionId, 'offers');
    const compareButton = page.getByRole('button', { name: '对比录取' });
    await compareButton.waitFor({ timeout: 15000 });
    await compareButton.click();
    await page.getByText('Offer 横向比较').first().waitFor({ timeout: 15000 });
    await page.waitForTimeout(250);
    await page.screenshot({ path: outputPath, type: 'png' });
  } finally {
    await context.close();
  }
}

async function captureDecisions(browser, fixture, mode, outputPath) {
  const { context, page, sessionId } = await buildContext(browser, fixture, mode);
  try {
    await openWorkspace(page, sessionId, 'decisions');
    const presetButton = page.getByText('科研导向').first();
    await presetButton.waitFor({ timeout: 15000 });
    await presetButton.click();
    await page.getByText('综合匹配').first().waitFor({ timeout: 15000 });
    await page.waitForTimeout(300);
    await page.screenshot({ path: outputPath, type: 'png' });
  } finally {
    await context.close();
  }
}

async function main() {
  await fs.mkdir(outputDir, { recursive: true });

  await probeService(`${FRONTEND_URL}/`, 'Frontend dev server');
  await probeService(`${BACKEND_URL}/openapi.json`, 'Backend API');

  await postJson(`${BACKEND_URL}/api/seed/schools`, 'Seed schools');
  await postJson(`${BACKEND_URL}/api/seed/demo-student`, 'Seed demo student');
  await postJson(`${BACKEND_URL}/api/seed/demo-evaluations`, 'Seed demo evaluations');
  await postJson(`${BACKEND_URL}/api/seed/demo-offers`, 'Seed demo offers');

  const fixture = runFixtureSeeder();
  if (!fixture?.student_id || !fixture?.student_name) {
    throw new Error(`Fixture seeder returned an incomplete payload: ${JSON.stringify(fixture)}`);
  }

  let browser;
  try {
    browser = await chromium.launch({ headless: true });
  } catch (error) {
    throw new Error(
      `Playwright Chromium is not available. Run "cd frontend && npx playwright install chromium" first.\n${error instanceof Error ? error.message : String(error)}`,
    );
  }

  try {
    await captureSchoolList(browser, fixture, 'desktop', OUTPUT_FILES.schoolListDesktop);
    await captureSchoolList(browser, fixture, 'mobile', OUTPUT_FILES.schoolListMobile);
    await captureOffers(browser, fixture, 'desktop', OUTPUT_FILES.offersDesktop);
    await captureOffers(browser, fixture, 'mobile', OUTPUT_FILES.offersMobile);
    await captureDecisions(browser, fixture, 'desktop', OUTPUT_FILES.decisionsDesktop);
    await captureDecisions(browser, fixture, 'mobile', OUTPUT_FILES.decisionsMobile);
  } finally {
    await browser.close();
  }

  console.log(JSON.stringify({ ok: true, files: OUTPUT_FILES }, null, 2));
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});

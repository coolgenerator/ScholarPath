import { expect, test } from '@playwright/test';

function parseAdvisorFrame(raw: string): Record<string, unknown> | null {
  try {
    const payload = JSON.parse(raw) as Record<string, unknown>;
    if (!payload || typeof payload !== 'object') {
      return null;
    }
    if (!('assistant_text' in payload)) {
      return null;
    }
    return payload;
  } catch {
    return null;
  }
}

function parseJsonFrame(raw: string): Record<string, unknown> | null {
  try {
    const payload = JSON.parse(raw) as Record<string, unknown>;
    if (!payload || typeof payload !== 'object') {
      return null;
    }
    return payload;
  } catch {
    return null;
  }
}

test('@live-smoke advisor live smoke returns structured contract payload', async ({ page }) => {
  const advisorFrames: string[] = [];

  page.on('websocket', (ws) => {
    if (!ws.url().includes('/api/advisor/v1/sessions/') || !ws.url().includes('/stream')) {
      return;
    }
    ws.on('framereceived', (event) => {
      if (typeof event.payload === 'string') {
        advisorFrames.push(event.payload);
      }
    });
  });

  await page.goto('/s/live-contract/advisor');
  await expect(page.getByTestId('advisor-panel')).toBeVisible();

  await page.getByTestId('advisor-input').fill('给我一个简短的本科建议');
  await page.getByTestId('advisor-send').click();

  await expect.poll(() => {
    return advisorFrames.some((frame) => parseAdvisorFrame(frame) !== null);
  }, { timeout: 60_000 }).toBe(true);

  const response = advisorFrames
    .map((frame) => parseAdvisorFrame(frame))
    .find((payload): payload is Record<string, unknown> => payload !== null);

  expect(response).toBeTruthy();
  expect(Array.isArray(response?.done)).toBe(true);
  expect(Array.isArray(response?.pending)).toBe(true);
  expect(Array.isArray(response?.next_actions)).toBe(true);
  expect(Array.isArray(response?.artifacts)).toBe(true);
});

test('@live-smoke full app smoke navigation: Profile -> Advisor -> School List -> Offers -> Decisions -> History', async ({ page }) => {
  await page.goto('/s/live-nav/advisor');
  await expect(page.getByTestId('advisor-panel')).toBeVisible();

  await page.getByTestId('advisor-input').fill('hello');
  await page.getByTestId('advisor-send').click();

  await page.getByTestId('topbar-avatar-toggle').click();
  await page.getByTestId('topbar-profile').click();
  await expect(page.getByTestId('profile-panel')).toBeVisible();

  await page.getByTestId('nav-advisor').click();
  await expect(page.getByTestId('advisor-panel')).toBeVisible();

  await page.getByTestId('nav-school-list').click();
  await expect(page.getByTestId('school-list-panel')).toBeVisible();

  await page.getByTestId('nav-offers').click();
  await expect(page.getByTestId('offers-panel')).toBeVisible();

  await page.getByTestId('nav-decisions').click();
  await expect(page.getByTestId('decisions-panel')).toBeVisible();

  await page.getByTestId('nav-history').click();
  await expect(page.getByTestId('history-panel')).toBeVisible();

  const sessionCount = await page.getByTestId('history-session-item').count();
  if (sessionCount > 0) {
    await page.getByTestId('history-session-item').first().click();
    await expect(page.getByTestId('advisor-panel')).toBeVisible();
  }
});

test('@live-smoke advisor live re-edit user message sends overwrite payload and regenerates', async ({ page }) => {
  const sentFrames: string[] = [];

  page.on('websocket', (ws) => {
    if (!ws.url().includes('/api/advisor/v1/sessions/') || !ws.url().includes('/stream')) {
      return;
    }
    ws.on('framesent', (event) => {
      if (typeof event.payload === 'string') {
        sentFrames.push(event.payload);
      }
    });
  });

  await page.goto('/s/live-edit/advisor');
  await expect(page.getByTestId('advisor-panel')).toBeVisible();

  await page.getByTestId('advisor-input').fill('请先给我一个本科建议');
  await page.getByTestId('advisor-send').click();

  await expect(page.getByTestId('advisor-user-edit').first()).toBeVisible({ timeout: 60_000 });
  await page.getByTestId('advisor-user-edit').first().click();
  await expect(page.getByTestId('advisor-edit-input')).toBeVisible();

  const editedText = '改成：我更关注预算和城市偏好';
  await page.getByTestId('advisor-edit-input').fill(editedText);
  await page.getByTestId('advisor-edit-save').click();

  await expect.poll(() => {
    return sentFrames
      .map((raw) => parseJsonFrame(raw))
      .filter((payload): payload is Record<string, unknown> => payload !== null)
      .some((payload) => {
        const edit = payload.edit;
        if (!edit || typeof edit !== 'object') {
          return false;
        }
        const editObj = edit as Record<string, unknown>;
        return (
          editObj.mode === 'overwrite'
          && typeof editObj.target_turn_id === 'string'
          && typeof payload.message === 'string'
          && String(payload.message).includes('预算和城市偏好')
        );
      });
  }, { timeout: 60_000 }).toBe(true);

  await expect(page.getByText(editedText).first()).toBeVisible({ timeout: 60_000 });
  await expect(page.getByText('Edited').first()).toBeVisible();
});

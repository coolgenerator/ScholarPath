import { expect, test } from '@playwright/test';

test.beforeEach(async ({ page }) => {
  await page.addInitScript(() => {
    const globalAny = window as unknown as Record<string, unknown>;
    globalAny.__advisorSentPayloads = [];

    const OriginalWebSocket = window.WebSocket;

    type Listener = (event: { type: string; [key: string]: unknown }) => void;

    class AdvisorMockSocket {
      public url: string;
      public readyState = 0;
      public onopen: Listener | null = null;
      public onclose: Listener | null = null;
      public onerror: Listener | null = null;
      public onmessage: Listener | null = null;
      private listeners: Record<string, Listener[]> = {
        open: [],
        close: [],
        error: [],
        message: [],
      };

      constructor(url: string) {
        this.url = url;
        setTimeout(() => {
          this.readyState = 1;
          this.emit('open', { type: 'open' });
        }, 0);
      }

      addEventListener(type: string, listener: Listener): void {
        if (!this.listeners[type]) {
          this.listeners[type] = [];
        }
        this.listeners[type].push(listener);
      }

      removeEventListener(type: string, listener: Listener): void {
        const pool = this.listeners[type] || [];
        this.listeners[type] = pool.filter((fn) => fn !== listener);
      }

      send(raw: string): void {
        let payload: Record<string, unknown> = {};
        try {
          payload = JSON.parse(raw) as Record<string, unknown>;
        } catch {
          payload = {};
        }

        const sent = globalAny.__advisorSentPayloads as Record<string, unknown>[];
        sent.push(payload);

        const message = typeof payload.message === 'string' ? payload.message : '';

        if (message.includes('badjson')) {
          setTimeout(() => {
            this.emit('message', { type: 'message', data: '{bad-json' });
          }, 5);
          return;
        }

        if (message.includes('disconnect')) {
          setTimeout(() => {
            this.readyState = 3;
            this.emit('close', { type: 'close', code: 1011, reason: 'forced', wasClean: false });
          }, 5);
          return;
        }

        const clientContext =
          typeof payload.client_context === 'object' && payload.client_context !== null
            ? (payload.client_context as Record<string, unknown>)
            : {};
        const trigger = typeof clientContext.trigger === 'string' ? clientContext.trigger : '';
        const editPayload =
          typeof payload.edit === 'object' && payload.edit !== null
            ? (payload.edit as Record<string, unknown>)
            : null;

        if (editPayload && typeof editPayload.target_turn_id === 'string') {
          const response = {
            turn_id: editPayload.target_turn_id,
            domain: 'undergrad',
            capability: 'undergrad.school.recommend',
            assistant_text: '已按编辑内容重新生成后续回复。',
            artifacts: [],
            actions: [],
            done: [
              {
                capability: 'undergrad.school.recommend',
                status: 'succeeded',
                message: 'Regenerated after edit',
              },
            ],
            pending: [],
            next_actions: [],
            route_meta: {
              domain_confidence: 0.95,
              capability_confidence: 0.92,
              router_model: 'contract-mock',
              latency_ms: 6,
              fallback_used: false,
              guard_result: 'pass',
              guard_reason: 'none',
              executed_count: 1,
              pending_count: 0,
            },
            error: null,
          };
          setTimeout(() => {
            this.emit('message', {
              type: 'message',
              data: JSON.stringify(response),
            });
          }, 5);
          return;
        }

        const response = trigger === 'queue.run_pending'
          ? {
              turn_id: payload.turn_id ?? 'turn-2',
              domain: 'offer',
              capability: 'offer.compare',
              assistant_text: '待办 offer 对比已执行。',
              artifacts: [
                {
                  type: 'info_card',
                  title: 'Offer Compare',
                  summary: 'Executed from pending queue',
                  data: { source: 'contract-second' },
                },
              ],
              actions: [],
              done: [
                {
                  capability: 'offer.compare',
                  status: 'succeeded',
                  message: 'Offer compare completed',
                },
              ],
              pending: [],
              next_actions: [
                {
                  action_id: 'route.clarify',
                  label: '继续澄清目标',
                  payload: {
                    domain_hint: 'undergrad',
                    capability_hint: 'undergrad.strategy.plan',
                    client_context: { trigger: 'route.clarify' },
                  },
                },
              ],
              route_meta: {
                domain_confidence: 0.93,
                capability_confidence: 0.9,
                router_model: 'contract-mock',
                latency_ms: 9,
                fallback_used: false,
                guard_result: 'pass',
                guard_reason: 'none',
                executed_count: 1,
                pending_count: 0,
              },
              error: null,
            }
          : {
              turn_id: payload.turn_id ?? 'turn-1',
              domain: 'undergrad',
              capability: 'undergrad.school.recommend',
              assistant_text: '已完成本科推荐并保留后续任务。',
              artifacts: [
                {
                  type: 'info_card',
                  title: 'Mock Recommendation',
                  summary: 'Contract suite deterministic artifact',
                  data: { source: 'contract-first' },
                },
              ],
              actions: [
                {
                  action_id: 'compat.display_only',
                  label: 'Display only action',
                  payload: {},
                },
              ],
              done: [
                {
                  capability: 'undergrad.school.recommend',
                  status: 'succeeded',
                  message: 'Recommendation finished',
                },
              ],
              pending: [
                {
                  capability: 'offer.compare',
                  reason: 'over_limit',
                  message: 'Queued for explicit trigger',
                },
              ],
              next_actions: [
                {
                  action_id: 'queue.run_pending',
                  label: '执行待办 Offer 对比',
                  payload: {
                    domain_hint: 'offer',
                    capability_hint: 'offer.compare',
                    client_context: {
                      trigger: 'queue.run_pending',
                      source: 'contract',
                    },
                  },
                },
                {
                  action_id: 'route.clarify',
                  label: '先澄清',
                  payload: {
                    client_context: {
                      trigger: 'route.clarify',
                    },
                  },
                },
              ],
              route_meta: {
                domain_confidence: 0.91,
                capability_confidence: 0.88,
                router_model: 'contract-mock',
                latency_ms: 8,
                fallback_used: false,
                guard_result: 'pass',
                guard_reason: 'none',
                executed_count: 1,
                pending_count: 1,
              },
              error: {
                code: 'CAPABILITY_FAILED',
                message: 'Mock recoverable error for UI rendering audit.',
                retriable: true,
              },
            };

        setTimeout(() => {
          this.emit('message', {
            type: 'message',
            data: JSON.stringify(response),
          });
        }, 5);
      }

      close(code = 1000, reason = ''): void {
        this.readyState = 3;
        this.emit('close', { type: 'close', code, reason, wasClean: code === 1000 });
      }

      private emit(type: string, event: { type: string; [key: string]: unknown }): void {
        const handler = this[`on${type}` as keyof AdvisorMockSocket] as Listener | null;
        if (typeof handler === 'function') {
          handler(event);
        }
        const pool = this.listeners[type] || [];
        pool.forEach((listener) => listener(event));
      }
    }

    function PatchedWebSocket(url: string | URL, protocols?: string | string[]) {
      const wsUrl = String(url);
      if (wsUrl.includes('/api/advisor/v1/sessions/') && wsUrl.includes('/stream')) {
        return new AdvisorMockSocket(wsUrl);
      }
      if (protocols !== undefined) {
        return new OriginalWebSocket(url, protocols);
      }
      return new OriginalWebSocket(url);
    }

    const patched = PatchedWebSocket as unknown as typeof WebSocket;
    patched.prototype = OriginalWebSocket.prototype;
    Object.defineProperty(patched, 'CONNECTING', { value: 0 });
    Object.defineProperty(patched, 'OPEN', { value: 1 });
    Object.defineProperty(patched, 'CLOSING', { value: 2 });
    Object.defineProperty(patched, 'CLOSED', { value: 3 });

    window.WebSocket = patched;
  });
});

test('advisor contract rendering and next action payload loop', async ({ page }) => {
  await page.goto('/s/contract-session/advisor');

  await expect(page.getByTestId('advisor-panel')).toBeVisible();
  await page.getByTestId('advisor-input').fill('请推荐学校并准备 offer 对比');
  await page.getByTestId('advisor-send').click();

  await expect(page.getByTestId('advisor-done')).toBeVisible();
  await expect(page.getByTestId('advisor-pending')).toBeVisible();
  await expect(page.getByTestId('advisor-next-actions')).toBeVisible();
  await expect(page.getByTestId('advisor-artifact')).toHaveCount(1);
  await expect(page.getByTestId('advisor-error')).toBeVisible();

  await page.getByTestId('advisor-next-action-queue-run_pending-0').click();

  await expect.poll(async () => {
    return page.evaluate(() => {
      const sent = (window as unknown as { __advisorSentPayloads?: unknown[] }).__advisorSentPayloads ?? [];
      return sent.length;
    });
  }).toBe(2);

  const latestPayload = await page.evaluate(() => {
    const sent = (window as unknown as { __advisorSentPayloads?: Record<string, unknown>[] }).__advisorSentPayloads ?? [];
    return sent[sent.length - 1] ?? null;
  });

  expect(latestPayload).not.toBeNull();
  expect(latestPayload?.capability_hint).toBe('offer.compare');
  expect(latestPayload?.domain_hint).toBe('offer');
  expect((latestPayload?.client_context as Record<string, unknown>)?.trigger).toBe('queue.run_pending');
});

test('advisor contract handles malformed JSON and websocket disconnect visibly', async ({ page }) => {
  await page.goto('/s/contract-errors/advisor');

  await expect(page.getByTestId('advisor-panel')).toBeVisible();

  await page.getByTestId('advisor-input').fill('badjson');
  await page.getByTestId('advisor-send').click();
  await expect(page.getByText('Received malformed advisor response. Please try again.').first()).toBeVisible();

  await page.getByTestId('advisor-input').fill('disconnect');
  await page.getByTestId('advisor-send').click();
  await expect(page.getByText('Advisor connection closed. Please retry your request.').first()).toBeVisible();
});

test('advisor user message re-edit sends overwrite payload and regenerates', async ({ page }) => {
  await page.goto('/s/contract-edit/advisor');
  await expect(page.getByTestId('advisor-panel')).toBeVisible();

  await page.getByTestId('advisor-input').fill('原始问题');
  await page.getByTestId('advisor-send').click();

  await expect(page.getByTestId('advisor-user-edit').first()).toBeVisible();
  await page.getByTestId('advisor-user-edit').first().click();
  await expect(page.getByTestId('advisor-edit-input')).toBeVisible();
  await page.getByTestId('advisor-edit-input').fill('编辑后的问题');
  await page.getByTestId('advisor-edit-save').click();

  await expect.poll(async () => {
    return page.evaluate(() => {
      const sent = (window as unknown as { __advisorSentPayloads?: Record<string, unknown>[] }).__advisorSentPayloads ?? [];
      return sent.length;
    });
  }).toBe(2);

  const latestPayload = await page.evaluate(() => {
    const sent = (window as unknown as { __advisorSentPayloads?: Record<string, unknown>[] }).__advisorSentPayloads ?? [];
    return sent[sent.length - 1] ?? null;
  });
  expect(latestPayload).not.toBeNull();
  expect(typeof latestPayload?.edit).toBe('object');
  expect((latestPayload?.edit as Record<string, unknown>)?.mode).toBe('overwrite');
  expect(typeof (latestPayload?.edit as Record<string, unknown>)?.target_turn_id).toBe('string');

  await expect(page.getByText('已按编辑内容重新生成后续回复。').first()).toBeVisible();
});

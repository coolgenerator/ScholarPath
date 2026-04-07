import { act, renderHook, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { useChat } from './useChat';

const apiGetMock = vi.fn();

vi.mock('../lib/api', () => ({
  api: {
    get: (...args: unknown[]) => apiGetMock(...args),
  },
  wsUrl: (path: string) => `ws://localhost${path}`,
}));

class FakeWebSocket {
  static instances: FakeWebSocket[] = [];
  static CONNECTING = 0;
  static OPEN = 1;
  static CLOSING = 2;
  static CLOSED = 3;

  readyState = FakeWebSocket.CONNECTING;
  onopen: ((event: Event) => void) | null = null;
  onclose: ((event: CloseEvent) => void) | null = null;
  onerror: ((event: Event) => void) | null = null;
  onmessage: ((event: MessageEvent) => void) | null = null;
  sent: string[] = [];
  url: string;

  constructor(url: string) {
    this.url = url;
    FakeWebSocket.instances.push(this);
  }

  send(message: string) {
    this.sent.push(message);
  }

  close() {
    this.readyState = FakeWebSocket.CLOSED;
    this.onclose?.(new Event('close') as CloseEvent);
  }

  open() {
    this.readyState = FakeWebSocket.OPEN;
    this.onopen?.(new Event('open'));
  }

  emitJson(payload: unknown) {
    this.onmessage?.({
      data: JSON.stringify(payload),
    } as MessageEvent);
  }
}

describe('useChat', () => {
  beforeEach(() => {
    FakeWebSocket.instances = [];
    apiGetMock.mockReset();
    apiGetMock.mockImplementation(async (path: string) => {
      if (path.startsWith('/chat/history/')) return [];
      if (path.startsWith('/chat/traces/session/')) return { items: [], total: 0 };
      if (path.startsWith('/chat/traces/')) {
        return {
          trace_id: 'trace-1',
          session_id: 'sid-1',
          status: 'ok',
          started_at: new Date().toISOString(),
          ended_at: new Date().toISOString(),
          usage: { wave_count: 1, tool_steps_used: 1 },
          steps: [],
          step_count: 0,
        };
      }
      throw new Error(`unexpected api path: ${path}`);
    });
    (globalThis as unknown as { WebSocket: unknown }).WebSocket = FakeWebSocket;
  });

  it('keeps latest step when out-of-order duplicate events arrive', async () => {
    const { result } = renderHook(() => useChat('sid-1', 'student-1'));

    await waitFor(() => {
      expect(FakeWebSocket.instances.length).toBe(1);
    });
    const ws = FakeWebSocket.instances[0];
    act(() => {
      ws.open();
    });

    const ts = new Date().toISOString();
    act(() => {
      ws.emitJson({
        type: 'turn.event',
        trace_id: 'trace-1',
        event: 'capability_started',
        timestamp: ts,
        data: {
          trace_id: 'trace-1',
          step_id: 'step-cap-1',
          event_seq: 2,
          step_kind: 'capability',
          step_status: 'running',
          phase: 'execution',
          capability_id: 'strategy',
        },
      });
    });
    act(() => {
      ws.emitJson({
        type: 'turn.event',
        trace_id: 'trace-1',
        event: 'capability_started',
        timestamp: ts,
        data: {
          trace_id: 'trace-1',
          step_id: 'step-cap-1',
          event_seq: 1,
          step_kind: 'capability',
          step_status: 'queued',
          phase: 'execution',
          capability_id: 'strategy',
        },
      });
    });
    act(() => {
      ws.emitJson({
        type: 'turn.event',
        trace_id: 'trace-1',
        event: 'capability_finished',
        timestamp: ts,
        data: {
          trace_id: 'trace-1',
          step_id: 'step-cap-1',
          event_seq: 3,
          step_kind: 'capability',
          step_status: 'completed',
          phase: 'execution',
          capability_id: 'strategy',
          duration_ms: 120,
        },
      });
    });
    act(() => {
      ws.emitJson({
        type: 'turn.event',
        trace_id: 'trace-1',
        event: 'capability_started',
        timestamp: ts,
        data: {
          trace_id: 'trace-1',
          step_id: 'step-cap-1',
          event_seq: 4,
          step_kind: 'capability',
          step_status: 'running',
          phase: 'execution',
          capability_id: 'strategy',
        },
      });
    });

    await waitFor(() => {
      const trace = result.current.traceById['trace-1'];
      expect(trace).toBeDefined();
      const step = trace.steps.find((item) => item.step_id === 'step-cap-1');
      expect(step?.step_status).toBe('completed');
      expect(trace.steps.length).toBe(1);
      expect(result.current.turnState).toBe('running');
      expect(result.current.metrics.duplicateEventDropped).toBeGreaterThanOrEqual(2);
    });
  });

  it('moves to reconnecting when socket closes during a running turn', async () => {
    const { result } = renderHook(() => useChat('sid-2', 'student-2'));

    await waitFor(() => {
      expect(FakeWebSocket.instances.length).toBe(1);
    });
    const ws = FakeWebSocket.instances[0];
    act(() => {
      ws.open();
      result.current.sendMessage('test reconnect');
    });
    expect(result.current.turnState).toBe('running');

    act(() => {
      ws.close();
    });

    await waitFor(() => {
      expect(result.current.turnState).toBe('reconnecting');
    });
  });

  it('deduplicates rapid repeated sends while reconnecting', async () => {
    const { result } = renderHook(() => useChat('sid-3', 'student-3'));

    await waitFor(() => {
      expect(FakeWebSocket.instances.length).toBe(1);
    });
    const ws1 = FakeWebSocket.instances[0];
    act(() => {
      ws1.open();
      result.current.sendMessage('hello once');
    });
    expect(ws1.sent.length).toBe(1);

    act(() => {
      ws1.close();
    });
    await waitFor(() => {
      expect(result.current.turnState).toBe('reconnecting');
    });

    act(() => {
      result.current.sendMessage('retry payload');
      result.current.sendMessage('retry payload');
    });

    await waitFor(() => {
      expect(FakeWebSocket.instances.length).toBe(2);
      expect(result.current.uiState.pendingSendCount).toBe(1);
    });

    const ws2 = FakeWebSocket.instances[1];
    act(() => {
      ws2.open();
    });

    await waitFor(() => {
      expect(ws2.sent.length).toBe(1);
      expect(result.current.uiState.pendingSendCount).toBe(0);
    });
  });

  it('loads full trace view on demand', async () => {
    const { result } = renderHook(() => useChat('sid-4', 'student-4'));

    await waitFor(() => {
      expect(FakeWebSocket.instances.length).toBe(1);
    });

    act(() => {
      result.current.openTrace('trace-1', 'full');
    });

    await waitFor(() => {
      expect(apiGetMock).toHaveBeenCalledWith('/chat/traces/trace-1', { view: 'full' });
    });
  });

  it('maps answer_synthesis block from turn.result', async () => {
    const { result } = renderHook(() => useChat('sid-5', 'student-5'));

    await waitFor(() => {
      expect(FakeWebSocket.instances.length).toBe(1);
    });
    const ws = FakeWebSocket.instances[0];
    act(() => {
      ws.open();
      ws.emitJson({
        type: 'turn.result',
        trace_id: 'trace-5',
        status: 'ok',
        content: '结论：先做匹配校清单。',
        blocks: [
          {
            id: 'b1',
            kind: 'answer_synthesis',
            capability_id: 'answer_synthesis',
            order: 0,
            payload: {
              summary: '结论：先做匹配校清单。',
              conclusion: '先做匹配校清单。',
              perspectives: [],
              actions: [],
              risks_missing: [],
              degraded: {
                has_degraded: false,
                caps: [],
                reason_codes: [],
                retry_hint: '',
              },
            },
            meta: {},
          },
        ],
        actions: [],
        usage: {},
      });
    });

    await waitFor(() => {
      expect(result.current.messages.length).toBe(1);
      expect(result.current.messages[0].blocks[0].type).toBe('answerSynthesis');
      expect(result.current.messages[0].content).toContain('匹配校');
    });
  });
});

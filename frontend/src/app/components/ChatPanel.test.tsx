import React from 'react';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ChatPanel } from './ChatPanel';
import type { TurnTraceView } from '../../hooks/useChat';

const setSessionIdMock = vi.fn();
const sendMessageMock = vi.fn();
const openTraceMock = vi.fn();

const functionTranslationKeys = new Set([
  'chat_welcome_returning_name',
  'chat_trace_wave_count',
  'chat_trace_tool_count',
  'chat_trace_duration',
  'chat_trace_degraded_count',
  'chat_trace_wave_label',
  'chat_trace_summary_added',
  'chat_trace_summary_dropped',
  'chat_trace_summary_reprioritized',
  'chat_trace_compact_hint',
  'chat_virtualized_hint',
]);

const t = new Proxy(
  {},
  {
    get: (_target, prop) => {
      const key = String(prop);
      if (functionTranslationKeys.has(key)) {
        return (value: unknown) => `${key}:${String(value ?? '')}`;
      }
      return key;
    },
  },
) as Record<string, string | ((name: string) => string)>;

function makeActiveTrace(status: 'running' | 'ok' | 'error'): TurnTraceView {
  return {
    trace_id: 'trace-1',
    session_id: 'sid-1',
    student_id: 'student-1',
    status,
    started_at: new Date().toISOString(),
    ended_at: status === 'running' ? null : new Date().toISOString(),
    usage: {
      wave_count: 1,
      tool_steps_used: 1,
      rejected_by_lock: false,
    },
    step_count: 2,
    steps: [
      {
        trace_id: 'trace-1',
        event: 'capability_started',
        timestamp: new Date().toISOString(),
        step_id: 'step-1',
        event_seq: 1,
        step_kind: 'capability',
        step_status: 'running',
        phase: 'execution',
        wave_index: 1,
        capability_id: 'strategy',
      },
      {
        trace_id: 'trace-1',
        event: 'capability_finished',
        timestamp: new Date().toISOString(),
        step_id: 'step-1',
        event_seq: 2,
        step_kind: 'capability',
        step_status: status === 'running' ? 'running' : 'completed',
        phase: 'execution',
        wave_index: 1,
        capability_id: 'strategy',
        duration_ms: 120,
      },
    ],
  };
}

function makeDefaultChatHookState() {
  return {
    messages: [],
    sendMessage: sendMessageMock,
    isConnected: true,
    isTyping: false,
    turnState: 'running' as const,
    progressEvents: [],
    traceById: {} as Record<string, TurnTraceView>,
    activeTrace: makeActiveTrace('running') as TurnTraceView | null,
    activeTraceView: 'compact' as const,
    activeTraceId: 'trace-1',
    openTrace: openTraceMock,
    isTraceLoading: false,
    uiState: {
      tracePanelMode: 'auto_expand_on_running' as const,
      userScrollLocked: false,
      pendingSendCount: 0,
    },
    metrics: {
      duplicateEventDropped: 0,
      traceMergeCount: 0,
      render_cost_ms: 0,
      scroll_corrections: 0,
      trace_expand_latency_ms: 0,
    },
    reportRenderCost: vi.fn(),
    reportScrollCorrection: vi.fn(),
    setUserScrollLocked: vi.fn(),
    setTracePanelMode: vi.fn(),
  };
}

let chatHookState = makeDefaultChatHookState();

vi.mock('../../hooks/useChat', () => ({
  useChat: () => chatHookState,
}));

vi.mock('../../hooks/useStudent', () => ({
  useStudent: () => ({
    student: null,
    fetchStudent: vi.fn(),
  }),
}));

vi.mock('../../context/AppContext', () => ({
  useApp: () => ({
    t,
    setSessionId: setSessionIdMock,
  }),
}));

describe('ChatPanel trace behavior', () => {
  beforeEach(() => {
    chatHookState = makeDefaultChatHookState();
  });

  it('auto-expands while running and auto-collapses after completion', async () => {
    const { rerender } = render(<ChatPanel sessionId="sid-1" studentId="student-1" />);

    await waitFor(() => {
      expect(screen.getByText('expand_less')).toBeInTheDocument();
      expect(screen.getByText('chat_trace_section_capability')).toBeInTheDocument();
    });

    chatHookState = {
      ...chatHookState,
      activeTrace: makeActiveTrace('ok'),
      activeTraceView: 'compact',
      traceById: { 'trace-1': makeActiveTrace('ok') },
      turnState: 'success',
      uiState: {
        ...chatHookState.uiState,
        tracePanelMode: 'auto_collapse_on_finish',
      },
    };
    rerender(<ChatPanel sessionId="sid-1" studentId="student-1" />);

    await waitFor(() => {
      expect(screen.getByText('expand_more')).toBeInTheDocument();
      expect(screen.queryByText('chat_trace_section_capability')).not.toBeInTheDocument();
    });
  });

  it('keeps user pinned collapse when trace finishes', async () => {
    const { rerender } = render(<ChatPanel sessionId="sid-1" studentId="student-1" />);

    await waitFor(() => {
      expect(screen.getByText('expand_less')).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText('expand_less'));

    await waitFor(() => {
      expect(screen.getByText('expand_more')).toBeInTheDocument();
    });

    chatHookState = {
      ...chatHookState,
      activeTrace: makeActiveTrace('ok'),
      turnState: 'success',
      uiState: {
        ...chatHookState.uiState,
        tracePanelMode: 'user_pinned',
      },
    };
    rerender(<ChatPanel sessionId="sid-1" studentId="student-1" />);

    await waitFor(() => {
      expect(screen.getByText('expand_more')).toBeInTheDocument();
      expect(screen.queryByText('chat_trace_section_capability')).not.toBeInTheDocument();
    });
  });
});

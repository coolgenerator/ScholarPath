import { useState, useEffect, useRef, useCallback, useMemo } from 'react';
import { api, wsUrl } from '../lib/api';
import { parseOfferCompareFromText, parseWhatIfFromText } from '../lib/chatRichContent';
import type {
  ChatBlockWire,
  ChatHistoryEntry,
  ChatMessage,
  ChatSocketMessage,
  GuidedQuestion,
  OfferCompareViewModel,
  QuestionOption,
  RecommendationData,
  RichMessageBlock,
  SessionTraceListResponse,
  TurnTraceResponse,
  TurnTraceStep,
  TurnTraceSummary,
  TurnEventMessage,
  TurnResultMessage,
  WhatIfViewModel,
} from '../lib/types';

export type { QuestionOption, GuidedQuestion };

export interface ChatProgressEvent {
  trace_id: string;
  event: TurnEventMessage['event'];
  data?: Record<string, unknown> | null;
  timestamp: string;
}

export type TurnLifecycleState =
  | 'idle'
  | 'running'
  | 'success'
  | 'error'
  | 'reconnecting';

export type TracePanelMode =
  | 'auto_expand_on_running'
  | 'auto_collapse_on_finish'
  | 'user_pinned';

export interface ChatUiState {
  tracePanelMode: TracePanelMode;
  userScrollLocked: boolean;
  pendingSendCount: number;
}

export interface ChatMetrics {
  duplicateEventDropped: number;
  traceMergeCount: number;
  render_cost_ms: number;
  scroll_corrections: number;
  trace_expand_latency_ms: number;
}

export interface ChatEntry {
  role: 'user' | 'assistant';
  content: string;
  timestamp: string;
  trace_id?: string;
  status?: 'ok' | 'error';
  suggested_actions?: string[] | null;
  execution_digest?: Record<string, unknown> | null;
  guided_questions?: GuidedQuestion[] | null;
  recommendation?: RecommendationData | null;
  offer_compare?: OfferCompareViewModel | null;
  what_if?: WhatIfViewModel | null;
  blocks: RichMessageBlock[];
}

interface NormalizedMessageStore {
  byId: Record<string, ChatEntry>;
  order: string[];
}

interface QueuedOutboundMessage {
  sessionId: string;
  message: ChatMessage & { student_id?: string };
}

export interface TurnTraceView {
  trace_id: string;
  session_id: string;
  student_id?: string | null;
  status: 'running' | 'ok' | 'error';
  started_at: string;
  ended_at?: string | null;
  usage: Record<string, unknown>;
  steps: TurnTraceStep[];
  step_count: number;
}

function nowIso(): string {
  return new Date().toISOString();
}

function ensureTraceView(
  existing: TurnTraceView | undefined,
  options: {
    traceId: string;
    sessionId: string;
    studentId?: string | null;
  },
): TurnTraceView {
  if (existing) return existing;
  return {
    trace_id: options.traceId,
    session_id: options.sessionId,
    student_id: options.studentId ?? null,
    status: 'running',
    started_at: nowIso(),
    ended_at: null,
    usage: {},
    steps: [],
    step_count: 0,
  };
}

interface StepUpsertResult {
  steps: TurnTraceStep[];
  dropped: boolean;
  merged: boolean;
}

function sortTraceSteps(steps: TurnTraceStep[]): TurnTraceStep[] {
  return [...steps].sort((a, b) => {
    const seqA = typeof a.event_seq === 'number' ? a.event_seq : 0;
    const seqB = typeof b.event_seq === 'number' ? b.event_seq : 0;
    if (seqA !== seqB) return seqA - seqB;
    return Date.parse(a.timestamp) - Date.parse(b.timestamp);
  });
}

function isTerminalStepStatus(status?: TurnTraceStep['step_status'] | null): boolean {
  return status === 'completed'
    || status === 'failed'
    || status === 'timeout'
    || status === 'blocked'
    || status === 'cancelled';
}

function upsertTraceStep(steps: TurnTraceStep[], step: TurnTraceStep): StepUpsertResult {
  const index = steps.findIndex((item) => item.step_id === step.step_id);
  const incomingSeq = typeof step.event_seq === 'number' ? step.event_seq : 0;
  if (index < 0) {
    return { steps: sortTraceSteps([...steps, step]), dropped: false, merged: true };
  }
  const existing = steps[index];
  const existingSeq = typeof existing.event_seq === 'number' ? existing.event_seq : 0;
  if (incomingSeq < existingSeq) {
    return { steps, dropped: true, merged: false };
  }

  const existingTerminal = isTerminalStepStatus(existing.step_status);
  if (existingTerminal && step.step_status && step.step_status !== existing.step_status) {
    return { steps, dropped: true, merged: false };
  }

  const merged: TurnTraceStep = {
    ...existing,
    ...step,
    step_status: step.step_status ?? existing.step_status ?? null,
  };
  const next = [...steps];
  next[index] = merged;
  return { steps: sortTraceSteps(next), dropped: false, merged: true };
}

function parseStepFromEvent(event: TurnEventMessage): TurnTraceStep | null {
  const payload = event.data;
  if (!payload || typeof payload !== 'object') return null;
  const data = payload as Record<string, unknown>;
  const stepId = typeof data.step_id === 'string' ? data.step_id : null;
  if (!stepId) return null;
  return {
    trace_id: event.trace_id,
    event: event.event,
    timestamp: event.timestamp,
    step_id: stepId,
    parent_step_id: typeof data.parent_step_id === 'string' ? data.parent_step_id : null,
    step_kind: typeof data.step_kind === 'string' ? (data.step_kind as TurnTraceStep['step_kind']) : null,
    step_status: typeof data.step_status === 'string'
      ? (data.step_status as TurnTraceStep['step_status'])
      : null,
    phase: typeof data.phase === 'string' ? data.phase : null,
    wave_index: typeof data.wave_index === 'number' ? data.wave_index : null,
    capability_id: typeof data.capability_id === 'string' ? data.capability_id : null,
    duration_ms: typeof data.duration_ms === 'number' ? data.duration_ms : null,
    checkpoint_summary:
      data.checkpoint_summary && typeof data.checkpoint_summary === 'object'
        ? (data.checkpoint_summary as Record<string, unknown>)
        : null,
    compact_reason_code:
      typeof data.compact_reason_code === 'string' ? data.compact_reason_code : null,
    event_seq: typeof data.event_seq === 'number' ? data.event_seq : null,
    display: data.display && typeof data.display === 'object'
      ? (data.display as TurnTraceStep['display'])
      : null,
    metrics: data.metrics && typeof data.metrics === 'object' ? (data.metrics as Record<string, unknown>) : null,
    data,
  };
}

function mergeTraceSummary(
  existing: TurnTraceView | undefined,
  summary: TurnTraceSummary,
): TurnTraceView {
  const current = existing ?? ensureTraceView(undefined, {
    traceId: summary.trace_id,
    sessionId: summary.session_id,
    studentId: summary.student_id,
  });
  return {
    ...current,
    trace_id: summary.trace_id,
    session_id: summary.session_id,
    student_id: summary.student_id ?? current.student_id ?? null,
    status: summary.status,
    started_at: summary.started_at,
    ended_at: summary.ended_at ?? null,
    usage: summary.usage ?? current.usage,
    step_count: summary.step_count,
  };
}

function mergeTraceDetail(
  existing: TurnTraceView | undefined,
  detail: TurnTraceResponse,
): TurnTraceView {
  const current = existing ?? ensureTraceView(undefined, {
    traceId: detail.trace_id,
    sessionId: detail.session_id,
    studentId: detail.student_id,
  });
  return {
    ...current,
    trace_id: detail.trace_id,
    session_id: detail.session_id,
    student_id: detail.student_id ?? current.student_id ?? null,
    status: detail.status,
    started_at: detail.started_at,
    ended_at: detail.ended_at ?? null,
    usage: detail.usage ?? {},
    steps: detail.steps ?? [],
    step_count: detail.step_count ?? (detail.steps?.length ?? 0),
  };
}

function nextTurnStateFromEvent(
  current: TurnLifecycleState,
  event: TurnEventMessage,
): TurnLifecycleState {
  if (event.event === 'turn_started' || event.event === 'planning_done' || event.event === 'capability_started') {
    return 'running';
  }
  if (event.event === 'rollback') {
    return 'error';
  }
  if (event.event === 'turn_completed') {
    const payload = event.data && typeof event.data === 'object'
      ? (event.data as Record<string, unknown>)
      : {};
    if (payload.status === 'ok') return 'success';
    if (payload.status === 'error') return 'error';
    return current === 'running' ? 'success' : current;
  }
  return current;
}

function mapWireBlockToRich(block: ChatBlockWire): RichMessageBlock | null {
  if (block.kind === 'answer_synthesis') {
    return { type: 'answerSynthesis', data: block.payload as any };
  }
  if (block.kind === 'recommendation') {
    return { type: 'recommendation', data: block.payload as RecommendationData };
  }
  if (block.kind === 'offer_compare') {
    return { type: 'offerCompare', data: block.payload as OfferCompareViewModel };
  }
  if (block.kind === 'what_if') {
    return { type: 'whatIf', data: block.payload as WhatIfViewModel };
  }
  if (block.kind === 'guided_questions') {
    const payload = block.payload as { questions?: GuidedQuestion[] } | GuidedQuestion[];
    const questions = Array.isArray(payload)
      ? payload
      : Array.isArray(payload.questions)
        ? payload.questions
        : [];
    return { type: 'guidedQuestions', data: questions };
  }
  if (block.kind === 'profile_snapshot') {
    return { type: 'profileSnapshot', data: block.payload as any };
  }
  if (block.kind === 'profile_patch_proposal') {
    return { type: 'profilePatchProposal', data: block.payload as any };
  }
  if (block.kind === 'profile_patch_result') {
    return { type: 'profilePatchResult', data: block.payload as any };
  }
  if (block.kind === 'error') {
    const payload = block.payload as { message?: string };
    return {
      type: 'error',
      data: {
        message: typeof payload.message === 'string' && payload.message.trim() ? payload.message : 'Something went wrong.',
      },
    };
  }
  // Text blocks are rendered via top-level message content.
  if (block.kind === 'text') return null;
  return null;
}

function normalizeBlocks(
  wireBlocks: ChatBlockWire[] | null | undefined,
  fallbackContent: string,
): Pick<ChatEntry, 'blocks' | 'recommendation' | 'guided_questions' | 'offer_compare' | 'what_if'> {
  const sorted = [...(wireBlocks ?? [])].sort((a, b) => a.order - b.order);
  const blocks = sorted
    .map((block) => mapWireBlockToRich(block))
    .filter((item): item is RichMessageBlock => item !== null);

  const recommendation = blocks.find((item) => item.type === 'recommendation')?.data as RecommendationData | undefined;
  const guided = blocks.find((item) => item.type === 'guidedQuestions')?.data as GuidedQuestion[] | undefined;
  const offerCompare = blocks.find((item) => item.type === 'offerCompare')?.data as OfferCompareViewModel | undefined;
  const whatIf = blocks.find((item) => item.type === 'whatIf')?.data as WhatIfViewModel | undefined;

  if (blocks.length === 0) {
    // Fallback for legacy history records without structured blocks.
    const legacyOffer = parseOfferCompareFromText(fallbackContent);
    const legacyWhatIf = legacyOffer ? null : parseWhatIfFromText(fallbackContent);
    const fallbackBlocks: RichMessageBlock[] = legacyOffer
      ? [{ type: 'offerCompare', data: legacyOffer }]
      : legacyWhatIf
        ? [{ type: 'whatIf', data: legacyWhatIf }]
        : [];
    return {
      blocks: fallbackBlocks,
      recommendation: null,
      guided_questions: null,
      offer_compare: legacyOffer,
      what_if: legacyWhatIf,
    };
  }

  return {
    blocks,
    recommendation: recommendation ?? null,
    guided_questions: guided ?? null,
    offer_compare: offerCompare ?? null,
    what_if: whatIf ?? null,
  };
}

function parseHistoryEntry(h: ChatHistoryEntry): ChatEntry {
  const normalized = normalizeBlocks(h.blocks, h.content);
  return {
    role: h.role as 'user' | 'assistant',
    content: h.content,
    timestamp: '',
    trace_id: h.trace_id ?? undefined,
    status: h.status ?? undefined,
    suggested_actions: h.actions ?? null,
    execution_digest: h.execution_digest ?? null,
    ...normalized,
  };
}

function parseTurnResult(result: TurnResultMessage): ChatEntry {
  const normalized = normalizeBlocks(result.blocks, result.content);
  return {
    role: 'assistant',
    content: result.content,
    timestamp: new Date().toISOString(),
    trace_id: result.trace_id,
    status: result.status,
    suggested_actions: result.actions ?? null,
    execution_digest: result.execution_digest ?? null,
    ...normalized,
  };
}

export function useChat(
  sessionId: string | null,
  studentId?: string | null,
  onSessionCreated?: (newId: string) => void,
) {
  const [messageStore, setMessageStore] = useState<NormalizedMessageStore>({
    byId: {},
    order: [],
  });
  const [isConnected, setIsConnected] = useState(false);
  const [turnState, setTurnState] = useState<TurnLifecycleState>('idle');
  const [progressEvents, setProgressEvents] = useState<ChatProgressEvent[]>([]);
  const [traceStore, setTraceStore] = useState<Map<string, TurnTraceView>>(new Map());
  const [traceViewStore, setTraceViewStore] = useState<Map<string, 'compact' | 'full'>>(new Map());
  const [activeTraceId, setActiveTraceId] = useState<string | null>(null);
  const [isTraceLoading, setIsTraceLoading] = useState(false);
  const [pendingSendCount, setPendingSendCount] = useState(0);
  const [userScrollLocked, setUserScrollLocked] = useState(false);
  const [tracePanelModeState, setTracePanelModeState] = useState<{
    mode: TracePanelMode;
    pinnedTraceId: string | null;
  }>({
    mode: 'auto_collapse_on_finish',
    pinnedTraceId: null,
  });
  const [metrics, setMetrics] = useState<ChatMetrics>({
    duplicateEventDropped: 0,
    traceMergeCount: 0,
    render_cost_ms: 0,
    scroll_corrections: 0,
    trace_expand_latency_ms: 0,
  });
  const wsRef = useRef<WebSocket | null>(null);
  const wsSessionRef = useRef<string | null>(null);
  const historyLoadedRef = useRef<string | null>(null);
  const activeSessionRef = useRef<string | null>(sessionId || null);
  const pendingOutboundRef = useRef<QueuedOutboundMessage[]>([]);
  const traceStoreRef = useRef<Map<string, TurnTraceView>>(new Map());
  const traceViewStoreRef = useRef<Map<string, 'compact' | 'full'>>(new Map());
  const lastSendRef = useRef<{ sessionId: string | null; content: string; at: number } | null>(null);
  const messageSeqRef = useRef(0);

  const buildMessageId = useCallback((entry: ChatEntry) => {
    messageSeqRef.current += 1;
    return `${entry.role}-${entry.timestamp || 'no-ts'}-${messageSeqRef.current}`;
  }, []);

  const replaceMessages = useCallback((entries: ChatEntry[]) => {
    const nextById: Record<string, ChatEntry> = {};
    const nextOrder: string[] = [];
    entries.forEach((entry) => {
      const id = buildMessageId(entry);
      nextById[id] = entry;
      nextOrder.push(id);
    });
    setMessageStore({ byId: nextById, order: nextOrder });
  }, [buildMessageId]);

  const appendMessage = useCallback((entry: ChatEntry) => {
    const id = buildMessageId(entry);
    setMessageStore((prev) => ({
      byId: {
        ...prev.byId,
        [id]: entry,
      },
      order: [...prev.order, id],
    }));
  }, [buildMessageId]);

  const refreshPendingSendCount = useCallback((sid: string | null = activeSessionRef.current) => {
    if (!sid) {
      setPendingSendCount(0);
      return;
    }
    const count = pendingOutboundRef.current.filter((item) => item.sessionId === sid).length;
    setPendingSendCount(count);
  }, []);

  const clearPendingForSession = useCallback((sid: string) => {
    pendingOutboundRef.current = pendingOutboundRef.current.filter((item) => item.sessionId !== sid);
    refreshPendingSendCount();
  }, [refreshPendingSendCount]);

  const enqueueOutbound = useCallback((sid: string, message: ChatMessage & { student_id?: string }) => {
    pendingOutboundRef.current = [...pendingOutboundRef.current, { sessionId: sid, message }];
    refreshPendingSendCount(sid);
  }, [refreshPendingSendCount]);

  useEffect(() => {
    traceStoreRef.current = traceStore;
  }, [traceStore]);

  useEffect(() => {
    traceViewStoreRef.current = traceViewStore;
  }, [traceViewStore]);

  const flushPendingMessages = useCallback((sid: string) => {
    if (!wsRef.current || wsSessionRef.current !== sid || wsRef.current.readyState !== WebSocket.OPEN) {
      return;
    }
    const queued = pendingOutboundRef.current.filter((item) => item.sessionId === sid);
    if (queued.length === 0) return;

    pendingOutboundRef.current = pendingOutboundRef.current.filter((item) => item.sessionId !== sid);
    queued.forEach(({ message }) => wsRef.current?.send(JSON.stringify(message)));
    refreshPendingSendCount(sid);
  }, [refreshPendingSendCount]);

  useEffect(() => {
    activeSessionRef.current = sessionId || null;
    if (!sessionId) {
      replaceMessages([]);
      setProgressEvents([]);
      setTraceStore(new Map());
      setTraceViewStore(new Map());
      setActiveTraceId(null);
      setIsTraceLoading(false);
      setPendingSendCount(0);
      setUserScrollLocked(false);
      setTracePanelModeState({
        mode: 'auto_collapse_on_finish',
        pinnedTraceId: null,
      });
      setMetrics({
        duplicateEventDropped: 0,
        traceMergeCount: 0,
        render_cost_ms: 0,
        scroll_corrections: 0,
        trace_expand_latency_ms: 0,
      });
      lastSendRef.current = null;
      messageSeqRef.current = 0;
      historyLoadedRef.current = null;
      setTurnState('idle');
      setIsConnected(false);
      pendingOutboundRef.current = [];
      if (wsRef.current) {
        wsRef.current.close();
        wsRef.current = null;
      }
      wsSessionRef.current = null;
      return;
    }

    if (wsSessionRef.current && wsSessionRef.current !== sessionId) {
      clearPendingForSession(wsSessionRef.current);
      setTraceStore(new Map());
      setTraceViewStore(new Map());
      setActiveTraceId(null);
      setIsTraceLoading(false);
    }

    refreshPendingSendCount(sessionId);
  }, [sessionId, clearPendingForSession, refreshPendingSendCount, replaceMessages]);

  useEffect(() => {
    if (!sessionId || historyLoadedRef.current === sessionId) return;

    setIsTraceLoading(true);
    api.get<ChatHistoryEntry[]>(`/chat/history/${sessionId}`)
      .then((history) => {
        if (history.length > 0) {
          const restored = history.map((entry) => parseHistoryEntry(entry));
          replaceMessages(restored);
          const latestTrace = [...history]
            .reverse()
            .find((entry) => entry.role === 'assistant' && typeof entry.trace_id === 'string' && entry.trace_id.trim());
          if (latestTrace?.trace_id) {
            setActiveTraceId(latestTrace.trace_id);
          }
        }
        historyLoadedRef.current = sessionId;
      })
      .catch(() => {
        historyLoadedRef.current = sessionId;
      })
      .finally(() => {
        api.get<SessionTraceListResponse>(`/chat/traces/session/${sessionId}`, {
          limit: 50,
          view: 'compact',
        })
          .then((response) => {
            setTraceStore((prev) => {
              const next = new Map(prev);
              response.items.forEach((summary) => {
                next.set(summary.trace_id, mergeTraceSummary(next.get(summary.trace_id), summary));
              });
              return next;
            });
            setTraceViewStore((prev) => {
              const next = new Map(prev);
              response.items.forEach((summary) => {
                if (!next.has(summary.trace_id)) {
                  next.set(summary.trace_id, 'compact');
                }
              });
              return next;
            });
          })
          .finally(() => {
            setIsTraceLoading(false);
          });
      });
  }, [sessionId, replaceMessages]);

  const connectWs = useCallback((sid: string) => {
    if (
      wsRef.current &&
      wsSessionRef.current === sid &&
      (wsRef.current.readyState === WebSocket.OPEN || wsRef.current.readyState === WebSocket.CONNECTING)
    ) {
      return wsRef.current;
    }

    if (wsRef.current && wsSessionRef.current !== sid) {
      wsRef.current.close();
      wsRef.current = null;
    }

    const ws = new WebSocket(wsUrl(`/chat/chat/${sid}`));
    wsRef.current = ws;
    wsSessionRef.current = sid;

    ws.onopen = () => {
      if (wsRef.current !== ws || wsSessionRef.current !== sid) return;
      setIsConnected(true);
      setTurnState((prev) => (prev === 'running' ? prev : 'idle'));
      flushPendingMessages(sid);
    };

    ws.onclose = () => {
      if (wsRef.current === ws) {
        wsRef.current = null;
        wsSessionRef.current = null;
      }
      setIsConnected(false);
      setTurnState((prev) => (prev === 'running' ? 'reconnecting' : 'idle'));
      setProgressEvents([]);
      refreshPendingSendCount(sid);
    };

    ws.onerror = () => {
      setIsConnected(false);
      setTurnState((prev) => (prev === 'running' ? 'reconnecting' : 'error'));
      setProgressEvents([]);
      refreshPendingSendCount(sid);
    };

    ws.onmessage = (event) => {
      if (wsSessionRef.current !== sid) return;
      try {
        const data: ChatSocketMessage = JSON.parse(event.data);
        if (data.type === 'turn.event') {
          setProgressEvents((prev) => [
            ...prev,
            {
              trace_id: data.trace_id,
              event: data.event,
              data: data.data ?? null,
              timestamp: data.timestamp,
            },
          ].slice(-12));
          setTraceViewStore((prev) => {
            const next = new Map(prev);
            if (!next.has(data.trace_id)) {
              next.set(data.trace_id, 'compact');
            }
            return next;
          });
          setTraceStore((prev) => {
            const next = new Map(prev);
            const current = ensureTraceView(next.get(data.trace_id), {
              traceId: data.trace_id,
              sessionId: sid,
              studentId,
            });
            const parsedStep = parseStepFromEvent(data);
            const upserted = parsedStep
              ? upsertTraceStep(current.steps, parsedStep)
              : { steps: current.steps, dropped: false, merged: false };
            if (upserted.dropped || upserted.merged) {
              setMetrics((prevMetrics) => ({
                duplicateEventDropped: prevMetrics.duplicateEventDropped + (upserted.dropped ? 1 : 0),
                traceMergeCount: prevMetrics.traceMergeCount + (upserted.merged ? 1 : 0),
              }));
            }
            const nextSteps = upserted.steps;
            const payload = data.data && typeof data.data === 'object'
              ? (data.data as Record<string, unknown>)
              : {};
            const completedStatus = typeof payload.status === 'string'
              ? (payload.status === 'ok' ? 'ok' : 'error')
              : current.status;
            const updated: TurnTraceView = {
              ...current,
              status:
                data.event === 'rollback'
                  ? 'error'
                  : data.event === 'turn_completed'
                    ? completedStatus
                    : current.status,
              started_at: current.started_at || data.timestamp,
              ended_at:
                data.event === 'turn_completed'
                  ? data.timestamp
                  : current.ended_at ?? null,
              steps: nextSteps,
              step_count: nextSteps.length,
            };
            next.set(data.trace_id, updated);
            return next;
          });
          setActiveTraceId((prev) => prev ?? data.trace_id);
          setTurnState((prev) => nextTurnStateFromEvent(prev, data));
          setTracePanelModeState((prev) => {
            const pinnedToCurrentTrace =
              prev.mode === 'user_pinned' &&
              prev.pinnedTraceId === data.trace_id;
            if (pinnedToCurrentTrace) return prev;
            if (
              data.event === 'turn_started'
              || data.event === 'planning_done'
              || data.event === 'capability_started'
            ) {
              return { mode: 'auto_expand_on_running', pinnedTraceId: null };
            }
            if (data.event === 'rollback' || data.event === 'turn_completed') {
              return { mode: 'auto_collapse_on_finish', pinnedTraceId: null };
            }
            return prev;
          });
          return;
        }

        const entry = parseTurnResult(data);
        appendMessage(entry);
        setTraceViewStore((prev) => {
          const next = new Map(prev);
          if (!next.has(data.trace_id)) {
            next.set(data.trace_id, 'compact');
          }
          return next;
        });
        setTraceStore((prev) => {
          const next = new Map(prev);
          const current = ensureTraceView(next.get(data.trace_id), {
            traceId: data.trace_id,
            sessionId: sid,
            studentId,
          });
          next.set(data.trace_id, {
            ...current,
            status: data.status,
            ended_at: nowIso(),
            usage: data.usage ?? {},
            step_count: current.steps.length,
          });
          return next;
        });
        setActiveTraceId(data.trace_id);
        setTurnState(data.status === 'ok' ? 'success' : 'error');
        setTracePanelModeState((prev) => {
          if (prev.mode === 'user_pinned' && prev.pinnedTraceId === data.trace_id) {
            return prev;
          }
          return { mode: 'auto_collapse_on_finish', pinnedTraceId: null };
        });
        setProgressEvents([]);
      } catch {
        // ignore malformed messages
      }
    };

    return ws;
  }, [appendMessage, flushPendingMessages, refreshPendingSendCount, studentId]);

  useEffect(() => {
    if (!sessionId) return;
    connectWs(sessionId);
    return () => {
      clearPendingForSession(sessionId);
      if (wsRef.current && wsSessionRef.current === sessionId) {
        wsRef.current.close();
        wsRef.current = null;
        wsSessionRef.current = null;
      }
      setIsConnected(false);
      setTurnState('idle');
      setTracePanelModeState({
        mode: 'auto_collapse_on_finish',
        pinnedTraceId: null,
      });
      refreshPendingSendCount();
    };
  }, [sessionId, clearPendingForSession, connectWs, refreshPendingSendCount]);

  useEffect(() => {
    setTracePanelModeState((prev) => {
      if (
        prev.mode === 'user_pinned'
        && prev.pinnedTraceId
        && prev.pinnedTraceId !== activeTraceId
      ) {
        return { mode: 'auto_collapse_on_finish', pinnedTraceId: null };
      }
      return prev;
    });
  }, [activeTraceId]);

  const reportRenderCost = useCallback((ms: number) => {
    if (!Number.isFinite(ms) || ms < 0) return;
    const rounded = Math.round(ms * 100) / 100;
    setMetrics((prev) => {
      if (Math.abs(prev.render_cost_ms - rounded) < 0.1) return prev;
      return {
        ...prev,
        render_cost_ms: rounded,
      };
    });
  }, []);

  const reportScrollCorrection = useCallback(() => {
    setMetrics((prev) => ({
      ...prev,
      scroll_corrections: prev.scroll_corrections + 1,
    }));
  }, []);

  const openTrace = useCallback((traceId: string, view: 'compact' | 'full' = 'compact') => {
    if (!traceId) return;
    const startedAt = typeof performance !== 'undefined' && typeof performance.now === 'function'
      ? performance.now()
      : Date.now();
    setActiveTraceId(traceId);
    const current = traceStoreRef.current.get(traceId);
    const currentView = traceViewStoreRef.current.get(traceId);
    const alreadyCovered = view === 'compact'
      ? currentView === 'compact' || currentView === 'full'
      : currentView === 'full';
    if (current && current.status !== 'running' && current.steps.length > 0 && alreadyCovered) {
      return;
    }
    setIsTraceLoading(true);
    api.get<TurnTraceResponse>(`/chat/traces/${traceId}`, { view })
      .then((detail) => {
        setTraceStore((prev) => {
          const next = new Map(prev);
          next.set(traceId, mergeTraceDetail(next.get(traceId), detail));
          return next;
        });
        setTraceViewStore((prev) => {
          const next = new Map(prev);
          const existing = next.get(traceId);
          if (existing !== 'full') {
            next.set(traceId, view);
          }
          return next;
        });
      })
      .catch(() => {
        // keep UI resilient when trace detail is unavailable
      })
      .finally(() => {
        setIsTraceLoading(false);
        if (view === 'full') {
          const endedAt = typeof performance !== 'undefined' && typeof performance.now === 'function'
            ? performance.now()
            : Date.now();
          const latency = Math.max(0, endedAt - startedAt);
          setMetrics((prev) => ({
            ...prev,
            trace_expand_latency_ms: Math.round(latency * 100) / 100,
          }));
        }
      });
  }, []);

  const sendMessage = useCallback(
    (content: string) => {
      const normalizedContent = content.trim();
      if (!normalizedContent) return;
      let sid = activeSessionRef.current;
      const now = Date.now();
      if (
        lastSendRef.current
        && lastSendRef.current.sessionId === sid
        && lastSendRef.current.content === normalizedContent
        && now - lastSendRef.current.at <= 350
      ) {
        return;
      }
      lastSendRef.current = {
        sessionId: sid,
        content: normalizedContent,
        at: now,
      };
      const timestamp = new Date().toISOString();
      const outbound: ChatMessage & { student_id?: string } = {
        role: 'user',
        content: normalizedContent,
        timestamp,
        ...(studentId ? { student_id: studentId } : {}),
      };
      const entry: ChatEntry = {
        role: 'user',
        content: normalizedContent,
        timestamp,
        execution_digest: null,
        blocks: [],
      };

      if (!sid) {
        sid = crypto.randomUUID?.() ?? `session-${Date.now()}-${Math.random().toString(36).slice(2)}`;
        activeSessionRef.current = sid;
        lastSendRef.current = {
          sessionId: sid,
          content: normalizedContent,
          at: now,
        };
        enqueueOutbound(sid, outbound);
        appendMessage(entry);
        setProgressEvents([]);
        setTurnState('running');
        setTracePanelModeState({
          mode: 'auto_expand_on_running',
          pinnedTraceId: null,
        });
        onSessionCreated?.(sid);
        return;
      }

      enqueueOutbound(sid, outbound);
      appendMessage(entry);
      setProgressEvents([]);
      setTurnState('running');
      setTracePanelModeState((prev) => {
        if (prev.mode === 'user_pinned' && prev.pinnedTraceId === activeTraceId) {
          return prev;
        }
        return {
          mode: 'auto_expand_on_running',
          pinnedTraceId: null,
        };
      });

      if (wsRef.current && wsSessionRef.current === sid && wsRef.current.readyState === WebSocket.OPEN) {
        flushPendingMessages(sid);
        return;
      }
      connectWs(sid);
    },
    [studentId, connectWs, enqueueOutbound, flushPendingMessages, onSessionCreated, activeTraceId, appendMessage],
  );

  const messages = useMemo(
    () => messageStore.order.map((id) => messageStore.byId[id]).filter(Boolean),
    [messageStore],
  );
  const activeTrace = activeTraceId ? traceStore.get(activeTraceId) ?? null : null;
  const activeTraceView = activeTraceId
    ? traceViewStore.get(activeTraceId) ?? 'compact'
    : 'compact';
  const traceById: Record<string, TurnTraceView> = {};
  traceStore.forEach((value, key) => {
    traceById[key] = value;
  });
  const setTracePanelMode = useCallback((mode: TracePanelMode, traceId?: string | null) => {
    setTracePanelModeState({
      mode,
      pinnedTraceId: mode === 'user_pinned' ? (traceId ?? activeTraceId ?? null) : null,
    });
  }, [activeTraceId]);
  const isTyping = turnState === 'running';
  const uiState: ChatUiState = {
    tracePanelMode: tracePanelModeState.mode,
    userScrollLocked,
    pendingSendCount,
  };

  return {
    messages,
    sendMessage,
    isConnected,
    isTyping,
    turnState,
    progressEvents,
    traceById,
    activeTrace,
    activeTraceView,
    activeTraceId,
    openTrace,
    isTraceLoading,
    uiState,
    metrics,
    reportRenderCost,
    reportScrollCorrection,
    setUserScrollLocked,
    setTracePanelMode,
  };
}

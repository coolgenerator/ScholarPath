import { useState, useEffect, useRef, useCallback } from 'react';
import { api, wsUrl } from '../lib/api';
import type {
  AdvisorAction,
  AdvisorArtifact,
  AdvisorCapability,
  AdvisorDomain,
  AdvisorEditPayload,
  AdvisorError,
  AdvisorHistoryEntry,
  AdvisorResponse,
  AdvisorRouteMeta,
  AdvisorUiMessage,
  DoneStep,
  GuidedQuestion,
  PendingStep,
  QuestionOption,
} from '../lib/types';

export type { QuestionOption, GuidedQuestion };

export interface ChatEntry {
  role: 'user' | 'assistant';
  content: string;
  timestamp: string;
  turn_id?: string | null;
  message_id?: string | null;
  editable?: boolean;
  edited?: boolean;
  advisor?: AdvisorUiMessage;
  domain?: AdvisorDomain;
  capability?: AdvisorCapability;
  artifacts?: AdvisorArtifact[];
  actions?: AdvisorAction[];
  done?: DoneStep[];
  pending?: PendingStep[];
  next_actions?: AdvisorAction[];
  route_meta?: AdvisorRouteMeta;
  error?: AdvisorError | null;
}

export interface SendMessageInput {
  message: string;
  domain_hint?: AdvisorDomain;
  capability_hint?: string;
  client_context?: Record<string, unknown>;
  edit?: AdvisorEditPayload;
  suppress_user_echo?: boolean;
  user_echo_text?: string;
}

const DEFAULT_ROUTE_META: AdvisorRouteMeta = {
  domain_confidence: 0,
  capability_confidence: 0,
  router_model: 'unknown',
  latency_ms: 0,
  fallback_used: false,
  context_tokens: 0,
  memory_hits: 0,
  rag_hits: 0,
  rag_latency_ms: 0,
  memory_degraded: false,
  guard_result: 'pass',
  guard_reason: 'none',
  primary_capability: null,
  executed_count: 0,
  pending_count: 0,
};

function parseHistoryEntry(h: AdvisorHistoryEntry): ChatEntry {
  const role = h.role === 'assistant' ? 'assistant' : 'user';
  return {
    role,
    content: h.content,
    timestamp: h.created_at || '',
    turn_id: h.turn_id ?? null,
    message_id: h.message_id ?? null,
    editable: Boolean(h.editable),
    edited: Boolean(h.edited),
  };
}

function normalizeSendInput(input: string | SendMessageInput): SendMessageInput {
  if (typeof input === 'string') {
    return { message: input };
  }
  return input;
}

function toAdvisorUiMessage(payload: Partial<AdvisorResponse> & Record<string, unknown>): AdvisorUiMessage {
  const routeMetaRaw =
    typeof payload.route_meta === 'object' && payload.route_meta !== null
      ? (payload.route_meta as Partial<AdvisorRouteMeta>)
      : {};

  return {
    assistant_text:
      typeof payload.assistant_text === 'string'
        ? payload.assistant_text
        : typeof payload.content === 'string'
          ? payload.content
          : '',
    domain: (payload.domain as AdvisorDomain) ?? 'common',
    capability: (payload.capability as AdvisorCapability) ?? 'common.general',
    artifacts: Array.isArray(payload.artifacts) ? (payload.artifacts as AdvisorArtifact[]) : [],
    actions: Array.isArray(payload.actions) ? (payload.actions as AdvisorAction[]) : [],
    done: Array.isArray(payload.done) ? (payload.done as DoneStep[]) : [],
    pending: Array.isArray(payload.pending) ? (payload.pending as PendingStep[]) : [],
    next_actions: Array.isArray(payload.next_actions) ? (payload.next_actions as AdvisorAction[]) : [],
    route_meta: {
      ...DEFAULT_ROUTE_META,
      ...routeMetaRaw,
    },
    error:
      typeof payload.error === 'object' && payload.error !== null
        ? (payload.error as AdvisorError)
        : null,
  };
}

function toAssistantEntry(advisor: AdvisorUiMessage, turnId?: string): ChatEntry {
  return {
    role: 'assistant',
    content: advisor.assistant_text,
    timestamp: new Date().toISOString(),
    turn_id: turnId ?? null,
    editable: false,
    edited: false,
    advisor,
    domain: advisor.domain,
    capability: advisor.capability,
    artifacts: advisor.artifacts,
    actions: advisor.actions,
    done: advisor.done,
    pending: advisor.pending,
    next_actions: advisor.next_actions,
    route_meta: advisor.route_meta,
    error: advisor.error,
  };
}

function buildTransportErrorEntry(message: string): ChatEntry {
  const advisor: AdvisorUiMessage = {
    assistant_text: message,
    domain: 'common',
    capability: 'common.general',
    artifacts: [],
    actions: [],
    done: [],
    pending: [],
    next_actions: [],
    route_meta: {
      ...DEFAULT_ROUTE_META,
      fallback_used: true,
      guard_result: 'invalid_input',
      guard_reason: 'trigger_invalid',
    },
    error: {
      code: 'DEPENDENCY_UNAVAILABLE',
      message,
      retriable: true,
    },
  };
  return toAssistantEntry(advisor);
}

export function useChat(
  sessionId: string | null,
  studentId?: string | null,
  onSessionCreated?: (newId: string) => void,
) {
  const [messages, setMessages] = useState<ChatEntry[]>([]);
  const [isConnected, setIsConnected] = useState(false);
  const [isTyping, setIsTyping] = useState(false);

  const wsRef = useRef<WebSocket | null>(null);
  const historyLoadedRef = useRef<string | null>(null);
  const activeSessionRef = useRef<string | null>(sessionId || null);
  const closingRef = useRef(false);
  const pendingEditRef = useRef<{ snapshot: ChatEntry[] } | null>(null);
  const messagesRef = useRef<ChatEntry[]>([]);

  useEffect(() => {
    messagesRef.current = messages;
  }, [messages]);

  const appendAssistantError = useCallback((message: string) => {
    const pending = pendingEditRef.current;
    if (pending) {
      pendingEditRef.current = null;
      setMessages((prev) => [...pending.snapshot, buildTransportErrorEntry(message)]);
      return;
    }
    setMessages((prev) => [...prev, buildTransportErrorEntry(message)]);
  }, []);

  useEffect(() => {
    activeSessionRef.current = sessionId || null;
    pendingEditRef.current = null;
    if (!sessionId) {
      setMessages([]);
      historyLoadedRef.current = null;
      if (wsRef.current) {
        closingRef.current = true;
        wsRef.current.close();
        wsRef.current = null;
        setIsConnected(false);
      }
    }
  }, [sessionId]);

  useEffect(() => {
    if (!sessionId || historyLoadedRef.current === sessionId) {
      return;
    }

    api
      .get<AdvisorHistoryEntry[]>(`/advisor/v1/sessions/${sessionId}/history`)
      .then((history) => {
        if (history.length > 0) {
          const restored: ChatEntry[] = history.map((entry) => parseHistoryEntry(entry));
          setMessages(restored);
        }
        historyLoadedRef.current = sessionId;
      })
      .catch(() => {
        historyLoadedRef.current = sessionId;
      });
  }, [sessionId]);

  const connectWs = useCallback(
    (sid: string) => {
      if (wsRef.current) {
        closingRef.current = true;
        wsRef.current.close();
      }

      const ws = new WebSocket(wsUrl(`/advisor/v1/sessions/${sid}/stream`));
      wsRef.current = ws;
      closingRef.current = false;

      ws.onopen = () => {
        setIsConnected(true);
      };

      ws.onclose = (event) => {
        setIsConnected(false);
        wsRef.current = null;
        if (closingRef.current || event.code === 1000) {
          closingRef.current = false;
          return;
        }
        appendAssistantError('Advisor connection closed. Please retry your request.');
      };

      ws.onerror = () => {
        setIsConnected(false);
      };

      ws.onmessage = (event) => {
        setIsTyping(false);
        try {
          const payload = JSON.parse(event.data) as Partial<AdvisorResponse> & Record<string, unknown>;
          const advisor = toAdvisorUiMessage(payload);
          const assistantEntry = toAssistantEntry(
            advisor,
            typeof payload.turn_id === 'string' ? payload.turn_id : undefined,
          );
          const pending = pendingEditRef.current;
          if (pending) {
            pendingEditRef.current = null;
            if (advisor.error) {
              setMessages((prev) => [...pending.snapshot, assistantEntry]);
              return;
            }
          }
          setMessages((prev) => [...prev, assistantEntry]);
        } catch {
          appendAssistantError('Received malformed advisor response. Please try again.');
        }
      };

      return ws;
    },
    [appendAssistantError],
  );

  useEffect(() => {
    if (!sessionId) {
      return;
    }

    connectWs(sessionId);
    return () => {
      if (wsRef.current) {
        closingRef.current = true;
        wsRef.current.close();
        wsRef.current = null;
        setIsConnected(false);
      }
    };
  }, [sessionId, connectWs]);

  const sendMessage = useCallback(
    (input: string | SendMessageInput) => {
      const normalized = normalizeSendInput(input);
      const trimmedMessage = normalized.message.trim();
      if (!trimmedMessage) {
        return;
      }

      const turnId = crypto.randomUUID?.() ?? `${Date.now()}`;
      const userEchoText = normalized.user_echo_text ?? trimmedMessage;
      const editPayload = normalized.edit;
      if (editPayload) {
        const targetTurnId = editPayload.target_turn_id;
        const snapshot = messagesRef.current;
        const idx = snapshot.findIndex(
          (item) => item.role === 'user' && item.turn_id === targetTurnId,
        );
        if (idx >= 0) {
          pendingEditRef.current = { snapshot };
          setMessages(() => {
            const next = snapshot.slice(0, idx + 1);
            const target = next[idx];
            next[idx] = {
              ...target,
              content: userEchoText,
              edited: true,
              timestamp: new Date().toISOString(),
            };
            return next;
          });
        }
      } else if (!normalized.suppress_user_echo) {
        setMessages((prev) => [
          ...prev,
          {
            role: 'user',
            content: userEchoText,
            timestamp: new Date().toISOString(),
            turn_id: turnId,
            editable: true,
            edited: false,
          },
        ]);
      }
      setIsTyping(true);

      let sid = activeSessionRef.current;
      if (!sid) {
        sid =
          crypto.randomUUID?.() ??
          `session-${Date.now()}-${Math.random().toString(36).slice(2)}`;
        activeSessionRef.current = sid;
        onSessionCreated?.(sid);
      }

      let socket = wsRef.current;
      if (!socket || socket.readyState === WebSocket.CLOSED) {
        socket = connectWs(sid);
      }

      const requestPayload: Record<string, unknown> = {
        turn_id: turnId,
        session_id: sid,
        message: trimmedMessage,
        ...(studentId ? { student_id: studentId } : {}),
        ...(normalized.domain_hint ? { domain_hint: normalized.domain_hint } : {}),
        ...(normalized.capability_hint ? { capability_hint: normalized.capability_hint } : {}),
        ...(normalized.client_context ? { client_context: normalized.client_context } : {}),
        ...(editPayload ? { edit: editPayload } : {}),
      };

      const sendNow = () => {
        if (!socket || socket.readyState !== WebSocket.OPEN) {
          setIsTyping(false);
          appendAssistantError('Advisor connection is unavailable. Please retry.');
          return;
        }
        socket.send(JSON.stringify(requestPayload));
      };

      if (socket.readyState === WebSocket.OPEN) {
        sendNow();
      } else {
        socket.addEventListener('open', sendNow, { once: true });
      }
    },
    [appendAssistantError, connectWs, onSessionCreated, studentId],
  );

  return { messages, sendMessage, isConnected, isTyping };
}

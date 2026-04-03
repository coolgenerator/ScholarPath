import { useState, useEffect, useRef, useCallback } from 'react';
import { api, wsUrl } from '../lib/api';
import { parseOfferCompareFromText, parseWhatIfFromText } from '../lib/chatRichContent';
import type {
  ChatMessage,
  ChatResponse,
  GuidedQuestion,
  OfferCompareViewModel,
  QuestionOption,
  RecommendationData,
  RichMessageBlock,
  WhatIfViewModel,
} from '../lib/types';

export type { QuestionOption, GuidedQuestion };

export interface ChatEntry {
  role: 'user' | 'assistant';
  content: string;
  timestamp: string;
  intent?: string;
  suggested_actions?: string[] | null;
  guided_questions?: GuidedQuestion[] | null;
  recommendation?: RecommendationData | null;
  offer_compare?: OfferCompareViewModel | null;
  what_if?: WhatIfViewModel | null;
  blocks: RichMessageBlock[];
}

interface HistoryEntry {
  role: string;
  content: string;
}

interface QueuedOutboundMessage {
  sessionId: string;
  message: ChatMessage & { student_id?: string };
}

/**
 * Parse structured markers ([RECOMMENDATION], [GUIDED_OPTIONS]) from
 * a plain-text history entry so that cards render correctly after reload.
 */
function parseHistoryEntry(h: HistoryEntry): ChatEntry {
  let content = h.content;
  let recommendation: RecommendationData | null = null;
  let guided_questions: GuidedQuestion[] | null = null;

  // Parse [RECOMMENDATION]{json}
  if (content.includes('[RECOMMENDATION]')) {
    const [textPart, jsonPart] = content.split('[RECOMMENDATION]', 2);
    content = textPart.trim();
    try {
      recommendation = JSON.parse(jsonPart.trim());
    } catch { /* ignore */ }
  }

  // Parse [GUIDED_OPTIONS]{json}
  if (content.includes('[GUIDED_OPTIONS]')) {
    const [textPart, jsonPart] = content.split('[GUIDED_OPTIONS]', 2);
    content = textPart.trim();
    try {
      const parsed = JSON.parse(jsonPart.trim());
      guided_questions = parsed.questions ?? parsed;
    } catch { /* ignore */ }
  }

  // Strip [INTAKE_COMPLETE] marker
  content = content.replace('[INTAKE_COMPLETE]', '').trim();

  const blocks: RichMessageBlock[] = [];
  const offer_compare = recommendation ? null : parseOfferCompareFromText(content);
  const what_if = recommendation || offer_compare ? null : parseWhatIfFromText(content);

  if (recommendation) blocks.push({ type: 'recommendation', data: recommendation });
  if (offer_compare) blocks.push({ type: 'offerCompare', data: offer_compare });
  if (what_if) blocks.push({ type: 'whatIf', data: what_if });
  if (guided_questions && guided_questions.length > 0) {
    blocks.push({ type: 'guidedQuestions', data: guided_questions });
  }

  return {
    role: h.role as 'user' | 'assistant',
    content,
    timestamp: '',
    recommendation,
    guided_questions,
    offer_compare,
    what_if,
    blocks,
  };
}

function normalizeAssistantEntry(
  data: Pick<ChatResponse, 'content' | 'intent' | 'suggested_actions' | 'guided_questions' | 'recommendation'>,
): Pick<ChatEntry, 'content' | 'intent' | 'suggested_actions' | 'guided_questions' | 'recommendation' | 'offer_compare' | 'what_if' | 'blocks'> {
  const recommendation = data.recommendation ?? null;
  const guided_questions = data.guided_questions ?? null;
  const offer_compare = recommendation ? null : parseOfferCompareFromText(data.content);
  const what_if = recommendation || offer_compare ? null : parseWhatIfFromText(data.content);

  const blocks: RichMessageBlock[] = [];
  if (recommendation) blocks.push({ type: 'recommendation', data: recommendation });
  if (offer_compare) blocks.push({ type: 'offerCompare', data: offer_compare });
  if (what_if) blocks.push({ type: 'whatIf', data: what_if });
  if (guided_questions && guided_questions.length > 0) {
    blocks.push({ type: 'guidedQuestions', data: guided_questions });
  }

  return {
    content: data.content,
    intent: data.intent,
    suggested_actions: data.suggested_actions,
    guided_questions,
    recommendation,
    offer_compare,
    what_if,
    blocks,
  };
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
  const wsSessionRef = useRef<string | null>(null);
  const historyLoadedRef = useRef<string | null>(null);
  const activeSessionRef = useRef<string | null>(sessionId || null);
  const pendingOutboundRef = useRef<QueuedOutboundMessage[]>([]);

  const clearPendingForSession = useCallback((sid: string) => {
    pendingOutboundRef.current = pendingOutboundRef.current.filter((item) => item.sessionId !== sid);
  }, []);

  const enqueueOutbound = useCallback((sid: string, message: ChatMessage & { student_id?: string }) => {
    pendingOutboundRef.current = [...pendingOutboundRef.current, { sessionId: sid, message }];
  }, []);

  const flushPendingMessages = useCallback((sid: string) => {
    if (!wsRef.current || wsSessionRef.current !== sid || wsRef.current.readyState !== WebSocket.OPEN) {
      return;
    }

    const queued = pendingOutboundRef.current.filter((item) => item.sessionId === sid);
    if (queued.length === 0) return;

    pendingOutboundRef.current = pendingOutboundRef.current.filter((item) => item.sessionId !== sid);
    queued.forEach(({ message }) => {
      wsRef.current?.send(JSON.stringify(message));
    });
  }, []);

  useEffect(() => {
    activeSessionRef.current = sessionId || null;
    if (!sessionId) {
      setMessages([]);
      historyLoadedRef.current = null;
      setIsTyping(false);
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
    }
  }, [sessionId, clearPendingForSession]);

  // Load history from backend on mount / sessionId change
  useEffect(() => {
    if (!sessionId || historyLoadedRef.current === sessionId) return;

    api.get<HistoryEntry[]>(`/chat/history/${sessionId}`)
      .then((history) => {
        if (history.length > 0) {
          const restored: ChatEntry[] = history.map((h) => parseHistoryEntry(h));
          setMessages(restored);
        }
        historyLoadedRef.current = sessionId;
      })
      .catch(() => {
        historyLoadedRef.current = sessionId;
      });
  }, [sessionId]);

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
      flushPendingMessages(sid);
    };

    ws.onclose = () => {
      if (wsRef.current === ws) {
        wsRef.current = null;
        wsSessionRef.current = null;
      }
      setIsConnected(false);
      setIsTyping(false);
    };

    ws.onerror = () => {
      setIsConnected(false);
      setIsTyping(false);
    };

    ws.onmessage = (event) => {
      if (wsSessionRef.current !== sid) return;
      setIsTyping(false);
      try {
        const data: ChatResponse = JSON.parse(event.data);
        const entry: ChatEntry = {
          role: 'assistant',
          timestamp: new Date().toISOString(),
          ...normalizeAssistantEntry(data),
        };
        setMessages((prev) => [...prev, entry]);
      } catch {
        // ignore malformed messages
      }
    };

    return ws;
  }, [flushPendingMessages]);

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
    };
  }, [sessionId, clearPendingForSession, connectWs]);

  const sendMessage = useCallback(
    (content: string) => {
      let sid = activeSessionRef.current;
      const timestamp = new Date().toISOString();
      const outbound: ChatMessage & { student_id?: string } = {
        role: 'user',
        content,
        timestamp,
        ...(studentId ? { student_id: studentId } : {}),
      };
      const entry: ChatEntry = {
        role: 'user',
        content,
        timestamp,
        blocks: [],
      };

      if (!sid) {
        sid = crypto.randomUUID?.() ?? `session-${Date.now()}-${Math.random().toString(36).slice(2)}`;
        activeSessionRef.current = sid;
        enqueueOutbound(sid, outbound);
        setMessages((prev) => [...prev, entry]);
        setIsTyping(true);
        onSessionCreated?.(sid);
        return;
      }

      enqueueOutbound(sid, outbound);
      setMessages((prev) => [...prev, entry]);
      setIsTyping(true);

      if (
        wsRef.current &&
        wsSessionRef.current === sid &&
        wsRef.current.readyState === WebSocket.OPEN
      ) {
        flushPendingMessages(sid);
        return;
      }

      connectWs(sid);
    },
    [studentId, connectWs, enqueueOutbound, flushPendingMessages, onSessionCreated],
  );

  return { messages, sendMessage, isConnected, isTyping };
}

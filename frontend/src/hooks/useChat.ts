import { useState, useEffect, useRef, useCallback } from 'react';
import { api, wsUrl } from '../lib/api';
import type { ChatMessage, ChatResponse, QuestionOption, GuidedQuestion, RecommendationData } from '../lib/types';

export type { QuestionOption, GuidedQuestion };

export interface ChatEntry {
  role: 'user' | 'assistant';
  content: string;
  timestamp: string;
  intent?: string;
  suggested_actions?: string[] | null;
  guided_questions?: GuidedQuestion[] | null;
  recommendation?: RecommendationData | null;
}

interface HistoryEntry {
  role: string;
  content: string;
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

  return {
    role: h.role as 'user' | 'assistant',
    content,
    timestamp: '',
    recommendation,
    guided_questions,
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
  const historyLoadedRef = useRef<string | null>(null);
  // Track the active sessionId internally so we can lazily create it
  const activeSessionRef = useRef<string | null>(sessionId || null);

  // Sync ref when prop changes
  useEffect(() => {
    activeSessionRef.current = sessionId || null;
    // If sessionId was cleared (new session), reset messages
    if (!sessionId) {
      setMessages([]);
      historyLoadedRef.current = null;
      if (wsRef.current) {
        wsRef.current.close();
        wsRef.current = null;
        setIsConnected(false);
      }
    }
  }, [sessionId]);

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

  // Connect WebSocket when sessionId is set
  const connectWs = useCallback((sid: string) => {
    if (wsRef.current) {
      wsRef.current.close();
    }

    const ws = new WebSocket(wsUrl(`/chat/chat/${sid}`));
    wsRef.current = ws;

    ws.onopen = () => setIsConnected(true);
    ws.onclose = () => setIsConnected(false);
    ws.onerror = () => setIsConnected(false);

    ws.onmessage = (event) => {
      setIsTyping(false);
      try {
        const data: ChatResponse = JSON.parse(event.data);
        const entry: ChatEntry = {
          role: 'assistant',
          content: data.content,
          timestamp: new Date().toISOString(),
          intent: data.intent,
          suggested_actions: data.suggested_actions,
          guided_questions: data.guided_questions,
          recommendation: data.recommendation,
        };
        setMessages((prev) => [...prev, entry]);
      } catch {
        // ignore malformed messages
      }
    };

    return ws;
  }, []);

  // Auto-connect when sessionId exists
  useEffect(() => {
    if (!sessionId) return;
    connectWs(sessionId);
    return () => {
      if (wsRef.current) {
        wsRef.current.close();
        wsRef.current = null;
        setIsConnected(false);
      }
    };
  }, [sessionId, connectWs]);

  const sendMessage = useCallback(
    (content: string) => {
      // Lazy session creation: if no session yet, generate one now
      let sid = activeSessionRef.current;
      if (!sid) {
        sid = crypto.randomUUID?.() ?? `session-${Date.now()}-${Math.random().toString(36).slice(2)}`;
        activeSessionRef.current = sid;
        onSessionCreated?.(sid);
        // Connect WebSocket for the new session
        const ws = connectWs(sid);
        // Queue the message to send once connected
        const entry: ChatEntry = {
          role: 'user',
          content,
          timestamp: new Date().toISOString(),
        };
        setMessages((prev) => [...prev, entry]);
        setIsTyping(true);
        ws.addEventListener('open', () => {
          const msg: ChatMessage & { student_id?: string } = {
            role: 'user',
            content,
            timestamp: new Date().toISOString(),
            ...(studentId ? { student_id: studentId } : {}),
          };
          ws.send(JSON.stringify(msg));
        }, { once: true });
        return;
      }

      if (!wsRef.current || wsRef.current.readyState !== WebSocket.OPEN) return;

      const msg: ChatMessage & { student_id?: string } = {
        role: 'user',
        content,
        timestamp: new Date().toISOString(),
        ...(studentId ? { student_id: studentId } : {}),
      };

      const entry: ChatEntry = {
        role: 'user',
        content,
        timestamp: msg.timestamp,
      };

      setMessages((prev) => [...prev, entry]);
      setIsTyping(true);
      wsRef.current.send(JSON.stringify(msg));
    },
    [studentId, connectWs, onSessionCreated],
  );

  return { messages, sendMessage, isConnected, isTyping };
}

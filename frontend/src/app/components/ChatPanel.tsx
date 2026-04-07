import React, { useState, useRef, useEffect, useCallback, useLayoutEffect } from 'react';
import ReactMarkdown from 'react-markdown';
import { useChat, ChatEntry } from '../../hooks/useChat';
import { useStudent } from '../../hooks/useStudent';
import { useApp } from '../../context/AppContext';
import { GuidedQuestionCard } from './GuidedQuestionCard';
import { RecommendationCard } from './RecommendationCard';
import {
  AnswerSynthesisCard,
  ErrorStateCard,
  OfferCompareCard,
  ProfilePatchProposalCard,
  ProfilePatchResultCard,
  ProfileSnapshotCard,
  WhatIfDeltaCard,
} from './StructuredMessageCards';
import { ExecutionTracePanel } from './chat/ExecutionTracePanel';
import { TraceProgressTimeline } from './chat/TraceProgressTimeline';
import { ChatComposer } from './chat/ChatComposer';
import { MessageListBoundary } from './chat/MessageListBoundary';
import { ExecutionDigestCard } from './chat/ExecutionDigestCard';

function formatTime(timestamp: string): string {
  try {
    return new Date(timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  } catch {
    return '';
  }
}

function getMessageKey(message: ChatEntry, index: number): string {
  const stamp = message.timestamp || `history-${index}`;
  return `${message.role}-${stamp}-${index}-${message.content.slice(0, 24)}`;
}

function getBlockKey(messageKey: string, kind: string, index: number): string {
  return `${messageKey}-${kind}-${index}`;
}

interface ChatPanelProps {
  sessionId: string | null;
  studentId?: string | null;
  fullWidth?: boolean;
}

type ConversationPhase = 'welcome' | 'firstTurnPending' | 'liveConversation';
const MESSAGE_WINDOW_SIZE = 120;
const MESSAGE_WINDOW_STEP = 60;

export function ChatPanel({ sessionId, studentId, fullWidth }: ChatPanelProps) {
  const { t, setSessionId } = useApp();

  const handleSessionCreated = useCallback((newId: string) => {
    setSessionId(newId);
  }, [setSessionId]);

  const {
    messages,
    sendMessage,
    isConnected,
    isTyping,
    turnState,
    progressEvents,
    activeTrace,
    activeTraceView,
    activeTraceId,
    openTrace,
    isTraceLoading,
    uiState,
    reportRenderCost,
    reportScrollCorrection,
    setUserScrollLocked,
    setTracePanelMode,
  } = useChat(sessionId || null, studentId, handleSessionCreated);
  const { student, fetchStudent } = useStudent();
  const [input, setInput] = useState('');
  const [dismissedCards, setDismissedCards] = useState<Set<string>>(new Set());
  const [animatedMessageKeys, setAnimatedMessageKeys] = useState<string[]>([]);
  const [animatedBlockKeys, setAnimatedBlockKeys] = useState<string[]>([]);
  const [pendingScrollAfterReveal, setPendingScrollAfterReveal] = useState(false);
  const scrollRef = useRef<HTMLDivElement>(null);
  const previousMessageKeysRef = useRef<string[]>([]);
  const animationTimeoutsRef = useRef<number[]>([]);
  const scrollTimeoutRef = useRef<number | null>(null);
  const lastAutoScrollAtRef = useRef(0);
  const [visibleStartIndex, setVisibleStartIndex] = useState(0);
  const pendingWindowExpandRef = useRef<{ previousHeight: number } | null>(null);
  const renderMark = typeof performance !== 'undefined' && typeof performance.now === 'function'
    ? performance.now()
    : Date.now();

  useEffect(() => {
    if (studentId) fetchStudent(studentId);
  }, [studentId, fetchStudent]);

  const scrollToBottom = useCallback((behavior: ScrollBehavior = 'smooth') => {
    if (!scrollRef.current) return;
    scrollRef.current.scrollTo({
      top: scrollRef.current.scrollHeight,
      behavior,
    });
    lastAutoScrollAtRef.current = Date.now();
  }, []);

  const isNearBottom = useCallback((threshold = 120) => {
    if (!scrollRef.current) return true;
    const { scrollTop, scrollHeight, clientHeight } = scrollRef.current;
    return scrollHeight - (scrollTop + clientHeight) <= threshold;
  }, []);

  const maybeAutoScroll = useCallback((behavior: ScrollBehavior = 'smooth', force = false) => {
    const allow = force || !uiState.userScrollLocked || isNearBottom();
    if (!allow) return;
    scrollToBottom(behavior);
    reportScrollCorrection();
    if (uiState.userScrollLocked) {
      setUserScrollLocked(false);
    }
  }, [isNearBottom, reportScrollCorrection, scrollToBottom, setUserScrollLocked, uiState.userScrollLocked]);

  useEffect(() => {
    const minStart = Math.max(0, messages.length - MESSAGE_WINDOW_SIZE);
    setVisibleStartIndex((prev) => {
      if (messages.length <= MESSAGE_WINDOW_SIZE) return 0;
      if (prev === 0 && minStart > 0) return minStart;
      if (prev > minStart) return minStart;
      return prev;
    });
  }, [messages.length]);

  useLayoutEffect(() => {
    if (!pendingWindowExpandRef.current || !scrollRef.current) return;
    const { previousHeight } = pendingWindowExpandRef.current;
    const el = scrollRef.current;
    const delta = el.scrollHeight - previousHeight;
    if (delta > 0) {
      el.scrollTop += delta;
      reportScrollCorrection();
    }
    pendingWindowExpandRef.current = null;
  }, [visibleStartIndex, reportScrollCorrection]);

  const handleScroll = useCallback(() => {
    const container = scrollRef.current;
    if (!container) return;
    if (Date.now() - lastAutoScrollAtRef.current < 150) {
      return;
    }
    const shouldLock = !isNearBottom();
    if (shouldLock !== uiState.userScrollLocked) {
      setUserScrollLocked(shouldLock);
    }
    if (
      container.scrollTop <= 80
      && visibleStartIndex > 0
      && !pendingWindowExpandRef.current
    ) {
      pendingWindowExpandRef.current = { previousHeight: container.scrollHeight };
      setVisibleStartIndex((prev) => Math.max(0, prev - MESSAGE_WINDOW_STEP));
    }
  }, [isNearBottom, setUserScrollLocked, uiState.userScrollLocked, visibleStartIndex]);

  useEffect(() => {
    previousMessageKeysRef.current = [];
    animationTimeoutsRef.current.forEach((timeoutId) => window.clearTimeout(timeoutId));
    animationTimeoutsRef.current = [];
    if (scrollTimeoutRef.current) {
      window.clearTimeout(scrollTimeoutRef.current);
      scrollTimeoutRef.current = null;
    }
    setAnimatedMessageKeys([]);
    setAnimatedBlockKeys([]);
    setDismissedCards(new Set());
    setPendingScrollAfterReveal(false);
    setVisibleStartIndex(0);
    pendingWindowExpandRef.current = null;
    setUserScrollLocked(false);
    setTracePanelMode('auto_collapse_on_finish', null);
  }, [sessionId, setUserScrollLocked, setTracePanelMode]);

  useEffect(() => (
    () => {
      animationTimeoutsRef.current.forEach((timeoutId) => window.clearTimeout(timeoutId));
      if (scrollTimeoutRef.current) {
        window.clearTimeout(scrollTimeoutRef.current);
      }
    }
  ), []);

  useEffect(() => {
    const nextMessageKeys = messages.map((message, index) => getMessageKey(message, index));
    const previousMessageKeys = new Set(previousMessageKeysRef.current);

    if (
      previousMessageKeysRef.current.length === 0 &&
      messages.some((message) => message.role === 'assistant' && Boolean(message.timestamp))
    ) {
      previousMessageKeysRef.current = nextMessageKeys;
      return;
    }

    previousMessageKeysRef.current = nextMessageKeys;

    const nextAnimatedMessages: string[] = [];
    const nextAnimatedBlocks: string[] = [];

    messages.forEach((message, index) => {
      const messageKey = nextMessageKeys[index];
      if (!message.timestamp || previousMessageKeys.has(messageKey)) return;

      nextAnimatedMessages.push(messageKey);

      message.blocks.forEach((_, blockIndex) => {
        nextAnimatedBlocks.push(getBlockKey(messageKey, 'block', blockIndex));
      });

      (message.suggested_actions ?? []).forEach((_, actionIndex) => {
        nextAnimatedBlocks.push(getBlockKey(messageKey, 'action', actionIndex));
      });

    });

    if (nextAnimatedMessages.length === 0 && nextAnimatedBlocks.length === 0) return;

    setAnimatedMessageKeys((prev) => Array.from(new Set([...prev, ...nextAnimatedMessages])));
    setAnimatedBlockKeys((prev) => Array.from(new Set([...prev, ...nextAnimatedBlocks])));

    const timeoutId = window.setTimeout(() => {
      setAnimatedMessageKeys((prev) => prev.filter((key) => !nextAnimatedMessages.includes(key)));
      setAnimatedBlockKeys((prev) => prev.filter((key) => !nextAnimatedBlocks.includes(key)));
      animationTimeoutsRef.current = animationTimeoutsRef.current.filter((id) => id !== timeoutId);
    }, 1200);

    animationTimeoutsRef.current.push(timeoutId);
  }, [messages]);
  
  const hasUserMessage = messages.some((message) => message.role === 'user');
  const assistantMessageCount = messages.filter((message) => message.role === 'assistant').length;
  const hasAssistantResponse = assistantMessageCount > 0;
  const conversationPhase: ConversationPhase = !hasUserMessage && !isTyping
    ? 'welcome'
    : hasAssistantResponse
      ? 'liveConversation'
      : 'firstTurnPending';
  const isInitialWelcomeState = conversationPhase === 'welcome';
  const showDetailedBriefing = Boolean(student) && conversationPhase === 'welcome';
  const showPendingBriefing = Boolean(student) && conversationPhase === 'firstTurnPending';
  const showCompactContextBar = Boolean(student) && conversationPhase === 'liveConversation';
  const showIntroShell = isInitialWelcomeState || showDetailedBriefing || showPendingBriefing;
  const shouldInlinePendingTyping = isTyping && conversationPhase === 'firstTurnPending' && messages[messages.length - 1]?.role === 'user';
  const quickActions = [
    t.chat_quick_recommend,
    t.chat_quick_evaluate,
    t.chat_quick_strategy,
  ];
  const briefingMetrics = student ? [
    { label: t.prof_gpa, value: `${student.gpa}/${student.gpa_scale}` },
    { label: t.prof_sat_total, value: student.sat_total ?? '—' },
    { label: t.chat_budget_label, value: student.budget_usd ? `$${(student.budget_usd / 1000).toFixed(0)}K` : '—' },
    { label: t.chat_cycle_label, value: student.target_year },
  ] : [];
  const statusLabel = turnState === 'reconnecting'
    ? `${t.chat_status_reconnecting}${uiState.pendingSendCount > 0 ? ` · ${uiState.pendingSendCount}` : ''}`
    : isTyping
      ? t.chat_status_thinking
      : isConnected
        ? t.chat_status_live
        : t.chat_status_offline;
  const headerSubtitle = t.chat_header_subtitle;
  const railStyle = { maxWidth: '960px' } as React.CSSProperties;
  const assistantBubbleStyle = { maxWidth: 'min(44rem, 96%)' } as React.CSSProperties;
  const userBubbleStyle = { maxWidth: 'min(34rem, 92%)' } as React.CSSProperties;
  const structuredStyle = { maxWidth: 'min(48rem, 100%)' } as React.CSSProperties;

  const progressLabel = useCallback((event: string): string => {
    if (event === 'turn_started') return t.chat_progress_turn_started;
    if (event === 'planning_done') return t.chat_progress_planning_done;
    if (event === 'capability_started') return t.chat_progress_capability_started;
    if (event === 'capability_finished') return t.chat_progress_capability_finished;
    if (event === 'rollback') return t.chat_progress_rollback;
    if (event === 'turn_completed') return t.chat_progress_turn_completed;
    return event;
  }, [
    t.chat_progress_capability_finished,
    t.chat_progress_capability_started,
    t.chat_progress_planning_done,
    t.chat_progress_rollback,
    t.chat_progress_turn_completed,
    t.chat_progress_turn_started,
  ]);

  useEffect(() => {
    const end = typeof performance !== 'undefined' && typeof performance.now === 'function'
      ? performance.now()
      : Date.now();
    reportRenderCost(end - renderMark);
  }, [
    activeTrace?.step_count,
    isTyping,
    messages.length,
    progressEvents.length,
    renderMark,
    reportRenderCost,
    visibleStartIndex,
  ]);

  useEffect(() => {
    if (!isTyping) return;
    maybeAutoScroll('smooth');
  }, [isTyping, maybeAutoScroll]);

  useEffect(() => {
    const latestMessage = messages[messages.length - 1];
    if (!latestMessage) return;

    if (scrollTimeoutRef.current) {
      window.clearTimeout(scrollTimeoutRef.current);
      scrollTimeoutRef.current = null;
    }

    const hasRevealContent =
      latestMessage.blocks.length > 0 ||
      Boolean(latestMessage.suggested_actions?.length);

    if (latestMessage.role === 'assistant' && hasRevealContent) {
      const delay = assistantMessageCount <= 1 ? 520 : 360;
      const shouldForceScroll = assistantMessageCount <= 1;
      const canAutoScroll = shouldForceScroll || !uiState.userScrollLocked || isNearBottom();
      if (!canAutoScroll) {
        setPendingScrollAfterReveal(false);
        return;
      }
      setPendingScrollAfterReveal(true);
      scrollTimeoutRef.current = window.setTimeout(() => {
        maybeAutoScroll('smooth', shouldForceScroll);
        setPendingScrollAfterReveal(false);
        scrollTimeoutRef.current = null;
      }, delay);
      return;
    }

    setPendingScrollAfterReveal(false);
    maybeAutoScroll(latestMessage.timestamp ? 'smooth' : 'auto');
  }, [assistantMessageCount, isNearBottom, maybeAutoScroll, messages, uiState.userScrollLocked]);

  const handleSend = () => {
    const trimmed = input.trim();
    if (!trimmed) return;
    sendMessage(trimmed);
    setInput('');
  };

  const handleKeyDown = (event: React.KeyboardEvent) => {
    if (event.key === 'Enter' && !event.shiftKey) {
      event.preventDefault();
      handleSend();
    }
  };

  const handleSuggestedAction = (action: string) => {
    sendMessage(action);
  };

  const handleGuidedSubmit = useCallback(
    (cardId: string, answers: Record<string, string | string[]>) => {
      const parts: string[] = [];
      for (const [, value] of Object.entries(answers)) {
        if (Array.isArray(value)) {
          if (value.length > 0) parts.push(value.join(', '));
        } else if (value) {
          parts.push(value);
        }
      }
      const message = parts.join('; ');
      if (message) {
        sendMessage(message);
      }
      setDismissedCards((prev) => new Set(prev).add(cardId));
    },
    [sendMessage],
  );

  const renderStructuredBlock = (
    msgIndex: number,
    messageKey: string,
    block: ChatEntry['blocks'][number],
    blockIndex: number,
    firstAssistant: boolean,
  ) => {
    const blockKey = getBlockKey(messageKey, 'block', blockIndex);
    const animated = animatedBlockKeys.includes(blockKey);
    const animationDelay = firstAssistant ? 150 + blockIndex * 70 : 210 + blockIndex * 80;
    const animationStyle = animated ? { animationDelay: `${animationDelay}ms` } : undefined;
    const wrapperProps = animated
      ? { className: firstAssistant ? 'chat-animate-card-soft' : 'chat-animate-card', style: animationStyle }
      : {};

    if (block.type === 'recommendation') {
      return (
        <div key={blockKey} {...wrapperProps}>
          <RecommendationCard data={block.data} />
        </div>
      );
    }

    if (block.type === 'answerSynthesis') {
      return (
        <div key={blockKey} {...wrapperProps}>
          <AnswerSynthesisCard data={block.data} />
        </div>
      );
    }

    if (block.type === 'offerCompare') {
      return (
        <div key={blockKey} {...wrapperProps}>
          <OfferCompareCard data={block.data} />
        </div>
      );
    }

    if (block.type === 'whatIf') {
      return (
        <div key={blockKey} {...wrapperProps}>
          <WhatIfDeltaCard data={block.data} />
        </div>
      );
    }

    if (block.type === 'guidedQuestions') {
      const cardId = `${msgIndex}:${blockIndex}`;
      if (dismissedCards.has(cardId)) return null;
      return (
        <div key={blockKey} {...wrapperProps}>
          <GuidedQuestionCard
            questions={block.data}
            onSubmit={(answers) => handleGuidedSubmit(cardId, answers)}
          />
        </div>
      );
    }

    if (block.type === 'error') {
      return (
        <div key={blockKey} {...wrapperProps}>
          <ErrorStateCard message={block.data.message} />
        </div>
      );
    }

    if (block.type === 'profileSnapshot') {
      return (
        <div key={blockKey} {...wrapperProps}>
          <ProfileSnapshotCard data={block.data} />
        </div>
      );
    }

    if (block.type === 'profilePatchProposal') {
      return (
        <div key={blockKey} {...wrapperProps}>
          <ProfilePatchProposalCard
            data={block.data}
            onConfirm={(command) => sendMessage(command)}
            onReedit={(command) => sendMessage(command)}
          />
        </div>
      );
    }

    if (block.type === 'profilePatchResult') {
      return (
        <div key={blockKey} {...wrapperProps}>
          <ProfilePatchResultCard data={block.data} />
        </div>
      );
    }

    return null;
  };

  const renderedMessages = messages.slice(visibleStartIndex);
  const hiddenMessageCount = visibleStartIndex;

  return (
    <section className={`${fullWidth ? 'w-full' : 'w-[40%] border-r border-outline-variant/10'} relative z-10 flex h-full flex-col bg-[radial-gradient(circle_at_top,rgba(0,64,161,0.08),transparent_34%)] bg-white`}>
      <header className="sticky top-0 z-20 border-b border-outline-variant/5 bg-white/78 px-4 backdrop-blur-xl sm:px-6 lg:px-8">
        <div className="mx-auto flex h-16 w-full items-center justify-between gap-4" style={railStyle}>
          <div className="flex min-w-0 items-center gap-3">
            <div className="hidden h-10 w-10 items-center justify-center rounded-2xl border border-primary/10 bg-primary/5 text-primary shadow-sm sm:flex">
              <span className="material-symbols-outlined text-[18px]" style={{ fontVariationSettings: "'FILL' 1" }}>
                forum
              </span>
            </div>
            <div className="min-w-0">
              <div className="flex items-center gap-2">
                <h2 className="truncate font-headline text-base font-extrabold tracking-tight text-on-surface">{t.chat_title}</h2>
                <span className={`h-2 w-2 rounded-full ${isConnected ? 'bg-tertiary shadow-[0_0_10px_rgba(0,83,18,0.35)]' : 'bg-on-surface-variant/30'} ${isConnected ? 'animate-pulse' : ''}`}></span>
              </div>
              <div className="mt-0.5 flex items-center gap-2 text-[11px] font-semibold text-on-surface-variant/55">
                <span className="truncate">{headerSubtitle}</span>
                <span className="h-1 w-1 rounded-full bg-on-surface-variant/30"></span>
                <span>{statusLabel}</span>
              </div>
            </div>
          </div>

          {student?.profile_completed && (
            <div className="hidden shrink-0 items-center gap-2 rounded-full border border-outline-variant/10 bg-white/80 px-3 py-1.5 text-[11px] font-semibold text-on-surface-variant/65 shadow-sm md:flex">
              <span className="material-symbols-outlined text-sm text-primary">person</span>
              <span>{student.name}</span>
              <span className="h-1 w-1 rounded-full bg-on-surface-variant/30"></span>
              <span>{student.curriculum_type}</span>
            </div>
          )}
        </div>
      </header>

      <div
        ref={scrollRef}
        onScroll={handleScroll}
        className="flex-1 overflow-y-auto px-4 pb-32 pt-4 font-body sm:px-6 sm:pb-36 sm:pt-6 lg:px-8"
      >
        <div className="mx-auto w-full" style={railStyle}>
          <div className={`chat-welcome-shell ${showIntroShell ? 'is-visible mb-5 sm:mb-7' : 'is-hidden mb-0'} ${conversationPhase === 'firstTurnPending' ? 'is-condensed' : ''}`}>
            {isInitialWelcomeState && (
              <div className="chat-animate-welcome rounded-[2rem] border border-outline-variant/12 bg-white/92 px-5 py-5 shadow-[0_24px_64px_rgba(15,23,42,0.08)] backdrop-blur sm:px-6 sm:py-6">
                <div className="text-[10px] font-bold uppercase tracking-[0.18em] text-on-surface-variant/55">
                  {t.chat_welcome_kicker}
                </div>
                <h3 className="mt-2 max-w-2xl font-headline text-2xl font-black tracking-tight text-on-surface sm:text-[2rem]">
                  {t.chat_welcome_title}
                </h3>
                <p className="mt-3 max-w-2xl text-sm leading-7 text-on-surface-variant/72 sm:text-[15px]">
                  {student && student.profile_completed
                    ? t.chat_welcome_returning_name(student.name)
                    : t.chat_welcome}
                </p>
              </div>
            )}

            {showDetailedBriefing && student && (
              <div className={`rounded-[1.75rem] border border-outline-variant/10 bg-surface-container-lowest/92 px-4 py-4 shadow-[0_18px_40px_rgba(15,23,42,0.06)] backdrop-blur sm:px-5 ${isInitialWelcomeState ? 'mt-4' : ''}`}>
                <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
                  <div className="min-w-0">
                    <div className="text-[10px] font-bold uppercase tracking-[0.16em] text-on-surface-variant/55">
                      {t.chat_briefing_label}
                    </div>
                    <div className="mt-1 flex flex-wrap items-center gap-2">
                      <div className="font-headline text-sm font-black text-on-surface">{student.name}</div>
                      <span className="inline-flex items-center gap-1 rounded-full border border-outline-variant/10 bg-white px-2.5 py-1 text-[11px] font-semibold text-on-surface-variant/75">
                        {student.curriculum_type}
                      </span>
                      <span className="inline-flex items-center gap-1 rounded-full border border-primary/12 bg-primary/5 px-2.5 py-1 text-[11px] font-semibold text-primary">
                        {t.chat_applying_prefix} {student.target_year}
                      </span>
                    </div>
                  </div>

                  {student.intended_majors && student.intended_majors.length > 0 && (
                    <div className="flex flex-wrap gap-2 sm:justify-end">
                      {student.intended_majors.slice(0, 3).map((major, index) => (
                        <span key={`${major}-${index}`} className="inline-flex items-center rounded-full border border-outline-variant/10 bg-white px-3 py-1.5 text-[11px] font-semibold text-on-surface-variant/75">
                          {major}
                        </span>
                      ))}
                    </div>
                  )}
                </div>

                <div className="mt-4 grid grid-cols-2 gap-2 sm:grid-cols-4">
                  {briefingMetrics.map((metric) => (
                    <div key={metric.label} className="rounded-2xl border border-outline-variant/8 bg-white px-3 py-2.5 shadow-sm">
                      <div className="text-[9px] font-bold uppercase tracking-[0.14em] text-on-surface-variant/50">
                        {metric.label}
                      </div>
                      <div className="mt-1 text-sm font-black text-on-surface">{metric.value}</div>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {showPendingBriefing && student && (
              <div className="rounded-[1.45rem] border border-outline-variant/10 bg-white/94 px-4 py-3 shadow-[0_12px_28px_rgba(15,23,42,0.06)] backdrop-blur">
                <div className="flex flex-wrap items-center gap-2">
                  <span className="text-[10px] font-bold uppercase tracking-[0.16em] text-on-surface-variant/50">
                    {t.chat_briefing_label}
                  </span>
                  <span className="font-headline text-sm font-black text-on-surface">{student.name}</span>
                  <span className="inline-flex items-center rounded-full border border-outline-variant/10 bg-surface-container-low/45 px-2.5 py-1 text-[11px] font-semibold text-on-surface-variant/72">
                    {student.curriculum_type}
                  </span>
                  <span className="inline-flex items-center rounded-full border border-primary/12 bg-primary/5 px-2.5 py-1 text-[11px] font-semibold text-primary">
                    {t.chat_applying_prefix} {student.target_year}
                  </span>
                </div>
                <div className="mt-3 flex flex-wrap gap-2">
                  {briefingMetrics.map((metric) => (
                    <span
                      key={metric.label}
                      className="inline-flex items-center gap-1 rounded-full border border-outline-variant/10 bg-surface-container-lowest px-2.5 py-1.5 text-[11px] font-semibold text-on-surface-variant/72"
                    >
                      <span className="text-[10px] font-bold uppercase tracking-[0.12em] text-on-surface-variant/48">
                        {metric.label}
                      </span>
                      <span className="text-on-surface">{metric.value}</span>
                    </span>
                  ))}
                </div>
              </div>
            )}

            {isInitialWelcomeState && (
              <div className="mt-4">
                <div className="mb-3 text-[10px] font-bold uppercase tracking-[0.16em] text-on-surface-variant/55">
                  {t.chat_quick_actions_label}
                </div>
                <div className="grid grid-cols-1 gap-2 sm:grid-cols-3">
                  {quickActions.map((action, index) => (
                    <button
                      key={action}
                      onClick={() => sendMessage(action)}
                      className="chat-animate-chip rounded-[1.35rem] border border-primary/12 bg-white px-4 py-3 text-left shadow-sm transition-all duration-300 hover:-translate-y-0.5 hover:border-primary/25 hover:bg-primary/5 hover:shadow-[0_16px_30px_rgba(0,64,161,0.08)]"
                      style={{ animationDelay: `${140 + index * 70}ms` }}
                    >
                      <div className="flex items-center justify-between gap-3">
                        <div className="font-headline text-sm font-bold text-on-surface">{action}</div>
                        <span className="material-symbols-outlined text-[18px] text-primary/60">north_east</span>
                      </div>
                    </button>
                  ))}
                </div>
              </div>
            )}
          </div>

          {showCompactContextBar && student && (
            <div className="mb-4 rounded-[1.35rem] border border-outline-variant/8 bg-white/88 px-4 py-3 shadow-[0_10px_24px_rgba(15,23,42,0.05)] backdrop-blur">
              <div className="flex flex-wrap items-center gap-2 text-[11px] font-semibold text-on-surface-variant/72">
                <span className="font-headline text-sm font-black text-on-surface">{student.name}</span>
                <span className="inline-flex items-center rounded-full border border-outline-variant/10 bg-surface-container-low/45 px-2.5 py-1">{student.curriculum_type}</span>
                <span className="inline-flex items-center rounded-full border border-primary/12 bg-primary/5 px-2.5 py-1 text-primary">
                  {t.chat_applying_prefix} {student.target_year}
                </span>
                {student.intended_majors?.[0] && (
                  <span className="inline-flex items-center rounded-full border border-outline-variant/10 bg-surface-container-lowest px-2.5 py-1">
                    {student.intended_majors[0]}
                  </span>
                )}
              </div>
            </div>
          )}

          <MessageListBoundary
            title={t.chat_render_error_title}
            description={t.chat_render_error_desc}
            retryLabel={t.chat_render_error_retry}
          >
            <div
              className={`flex flex-col ${conversationPhase === 'firstTurnPending' ? 'gap-4 sm:gap-5' : 'gap-6 sm:gap-7'}`}
              data-scroll-pending={pendingScrollAfterReveal ? 'true' : 'false'}
            >
            {hiddenMessageCount > 0 && (
              <div className="mx-auto rounded-full border border-outline-variant/20 bg-white/80 px-3 py-1 text-[11px] font-semibold text-on-surface-variant/70">
                {t.chat_virtualized_hint(hiddenMessageCount)}
              </div>
            )}
            {renderedMessages.map((msg: ChatEntry, localIndex: number) => {
              const i = visibleStartIndex + localIndex;
              const messageKey = getMessageKey(msg, i);
              const animateMessage = animatedMessageKeys.includes(messageKey);
              const assistantIndex = msg.role === 'assistant'
                ? messages.slice(0, i + 1).filter((entry) => entry.role === 'assistant').length - 1
                : -1;
              const isFirstAssistantMessage = assistantIndex === 0;
              const showInlineTyping = shouldInlinePendingTyping && i === messages.length - 1 && msg.role === 'user';
              const synthesisBlock = msg.blocks.find((block) => block.type === 'answerSynthesis');
              const assistantSummary = synthesisBlock
                ? (msg.content?.trim() || synthesisBlock.data.summary || synthesisBlock.data.conclusion || '')
                : msg.content;

              return (
                <div key={messageKey} className={`flex w-full ${msg.role === 'user' ? 'justify-end' : 'justify-start'}`}>
                  {msg.role === 'user' ? (
                    <div className="flex max-w-full flex-col items-end gap-2">
                      <div
                        className={`rounded-[1.7rem] rounded-br-md bg-primary px-4 py-3.5 text-sm leading-relaxed text-on-primary shadow-[0_22px_44px_rgba(3,2,19,0.16)] sm:px-5 ${animateMessage ? 'chat-animate-bubble-right' : ''}`}
                        style={userBubbleStyle}
                      >
                        {msg.content}
                      </div>
                      <span className="text-[10px] font-bold uppercase tracking-[0.16em] text-on-surface-variant/45">
                        {t.chat_tag_user} {msg.timestamp ? `\u2022 ${formatTime(msg.timestamp)}` : ''}
                      </span>
                      {showInlineTyping && (
                        <div className="flex w-full justify-end pt-1">
                          <div className="flex flex-col items-end gap-1">
                            <div className="rounded-[1.35rem] rounded-tr-md border border-outline-variant/10 bg-white/96 px-4 py-3 shadow-[0_12px_28px_rgba(15,23,42,0.08)] backdrop-blur">
                              <div className="flex items-center gap-1.5">
                                <span className="chat-typing-dot" style={{ animationDelay: '0ms' }}></span>
                                <span className="chat-typing-dot" style={{ animationDelay: '140ms' }}></span>
                                <span className="chat-typing-dot" style={{ animationDelay: '280ms' }}></span>
                              </div>
                            </div>
                  <span className="text-[10px] font-bold uppercase tracking-[0.16em] text-on-surface-variant/45">
                              {t.chat_tag_ai} • {statusLabel}
                          </span>
                          </div>
                        </div>
                      )}
                    </div>
                  ) : (
                    <div className="flex max-w-full flex-col items-start gap-3">
                      <div
                        className={`chat-markdown-shell prose prose-sm rounded-[1.8rem] rounded-bl-md border border-outline-variant/10 bg-white/96 px-4 py-4 text-sm leading-relaxed text-on-surface shadow-[0_18px_45px_rgba(15,23,42,0.08)] backdrop-blur prose-headings:text-on-surface prose-strong:text-on-surface prose-p:my-1.5 prose-ul:my-1.5 prose-ol:my-1.5 prose-li:my-0.5 sm:px-5 sm:py-4 ${animateMessage ? (isFirstAssistantMessage ? 'chat-animate-bubble-left-soft' : 'chat-animate-bubble-left') : ''}`}
                        style={assistantBubbleStyle}
                      >
                        <ReactMarkdown>{assistantSummary}</ReactMarkdown>
                      </div>
                      <span className="text-[10px] font-bold uppercase tracking-[0.16em] text-on-surface-variant/45">
                        {t.chat_tag_ai} {msg.timestamp ? `\u2022 ${formatTime(msg.timestamp)}` : ''}
                      </span>
                      {msg.trace_id && (
                        <button
                          onClick={() => openTrace(msg.trace_id!)}
                          className={`rounded-full border px-2.5 py-1 text-[10px] font-bold tracking-[0.12em] transition ${
                            activeTraceId === msg.trace_id
                              ? 'border-primary/35 bg-primary/10 text-primary'
                              : 'border-outline-variant/30 bg-white/70 text-on-surface-variant hover:border-primary/25 hover:text-primary'
                          }`}
                        >
                          {t.chat_trace_button}
                        </button>
                      )}

                      {msg.execution_digest && typeof msg.execution_digest === 'object' && (
                        <div className="w-full" style={structuredStyle}>
                          <ExecutionDigestCard digest={msg.execution_digest} />
                        </div>
                      )}

                      {msg.blocks.length > 0 && (
                        <div className="flex w-full flex-col gap-3" style={structuredStyle}>
                          {msg.blocks.map((block, blockIndex) => renderStructuredBlock(i, messageKey, block, blockIndex, isFirstAssistantMessage))}
                        </div>
                      )}

                      {msg.suggested_actions && msg.suggested_actions.length > 0 && (
                        <div className="mt-1 flex flex-wrap gap-2">
                          {msg.suggested_actions.map((action, j) => {
                            const actionKey = getBlockKey(messageKey, 'action', j);
                            const animateAction = animatedBlockKeys.includes(actionKey);
                            return (
                              <button
                                key={actionKey}
                                onClick={() => handleSuggestedAction(action)}
                                className={`rounded-full border border-primary/15 bg-primary/5 px-3 py-1.5 text-xs font-bold text-primary transition-all duration-300 hover:-translate-y-0.5 hover:bg-primary/10 ${animateAction ? 'chat-animate-chip' : ''}`}
                                style={animateAction ? { animationDelay: `${260 + j * 70}ms` } : undefined}
                              >
                                {action}
                              </button>
                            );
                          })}
                        </div>
                      )}

                    </div>
                  )}
                </div>
              );
            })}

            {isTyping && !shouldInlinePendingTyping && (
              <div className="flex w-full justify-start">
                <div className="flex max-w-full flex-col items-start gap-2">
                  <div
                    className={`${assistantMessageCount <= 1 ? 'chat-animate-bubble-left-soft' : 'chat-animate-bubble-left'} rounded-[1.8rem] rounded-bl-md border border-outline-variant/10 bg-white/96 px-5 py-4 shadow-[0_16px_36px_rgba(15,23,42,0.08)] backdrop-blur`}
                    style={{ maxWidth: 'min(16rem, 88%)' }}
                  >
                    <div className="flex items-center gap-1.5">
                      <span className="chat-typing-dot" style={{ animationDelay: '0ms' }}></span>
                      <span className="chat-typing-dot" style={{ animationDelay: '140ms' }}></span>
                      <span className="chat-typing-dot" style={{ animationDelay: '280ms' }}></span>
                    </div>
                  </div>
                  <span className="text-[10px] font-bold uppercase tracking-[0.16em] text-on-surface-variant/45">
                    {t.chat_tag_ai} • {statusLabel}
                  </span>
                  <TraceProgressTimeline events={progressEvents} labelForEvent={progressLabel} />
                </div>
              </div>
            )}

              <ExecutionTracePanel
                activeTrace={activeTrace}
                activeTraceView={activeTraceView}
                isTraceLoading={isTraceLoading}
                tracePanelMode={uiState.tracePanelMode}
                onTracePanelModeChange={setTracePanelMode}
                onLoadFullTrace={(traceId) => openTrace(traceId, 'full')}
              />
            </div>
          </MessageListBoundary>
        </div>
      </div>

      <ChatComposer
        value={input}
        placeholder={t.chat_placeholder}
        onChange={setInput}
        onSend={handleSend}
        onKeyDown={handleKeyDown}
      />
    </section>
  );
}

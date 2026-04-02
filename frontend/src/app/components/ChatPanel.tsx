import React, { useState, useRef, useEffect, useCallback } from 'react';
import ReactMarkdown from 'react-markdown';
import { useChat, ChatEntry } from '../../hooks/useChat';
import { useStudent } from '../../hooks/useStudent';
import { useApp } from '../../context/AppContext';
import { InfoCard, OfferComparisonCard, StrategyPlanCard, WhatIfResultCard } from './chat/ArtifactCards';
import { GuidedQuestionCard } from './GuidedQuestionCard';
import { RecommendationCard } from './RecommendationCard';
import type {
  AdvisorAction,
  AdvisorArtifact,
  AdvisorDomain,
  AdvisorCapability,
  GuidedIntakeArtifact,
  SchoolRecommendationArtifact,
  OfferComparisonArtifact,
  StrategyPlanArtifact,
  WhatIfResultArtifact,
  InfoCardArtifact,
  DoneStep,
  PendingStep,
} from '../../lib/types';

function formatTime(timestamp: string): string {
  try {
    return new Date(timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  } catch {
    return '';
  }
}

function formatCapability(capability: AdvisorCapability | string): string {
  return capability
    .replace(/\./g, ' · ')
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function formatDomain(domain: AdvisorDomain | string): string {
  return domain.replace(/\b\w/g, (char) => char.toUpperCase());
}


function actionDefaultMessage(actionId: string): string {
  if (actionId === 'queue.run_pending') {
    return '请执行这个待处理步骤。';
  }
  if (actionId === 'step.retry') {
    return '请重试刚才失败的步骤。';
  }
  if (actionId === 'route.clarify') {
    return '我来补充澄清一下我的需求。';
  }
  return '请继续处理下一步。';
}

function sanitizeForTestId(value: string): string {
  return value.replace(/[^a-zA-Z0-9_-]+/g, '-');
}

function statusBadgeClass(status: DoneStep['status']): string {
  if (status === 'succeeded') {
    return 'bg-tertiary/10 text-tertiary border-tertiary/20';
  }
  if (status === 'degraded') {
    return 'bg-secondary-fixed/40 text-on-secondary-fixed-variant border-secondary-fixed/30';
  }
  return 'bg-error/10 text-error border-error/20';
}

function pendingReasonLabel(reason: PendingStep['reason']): string {
  const mapping: Record<PendingStep['reason'], string> = {
    over_limit: 'Over limit',
    conflict: 'Conflict',
    low_confidence: 'Low confidence',
    requires_user_trigger: 'Requires trigger',
    dependency_wait: 'Dependency wait',
  };
  return mapping[reason];
}

interface ChatPanelProps {
  sessionId: string;
  studentId?: string | null;
  fullWidth?: boolean;
}

export function ChatPanel({ sessionId, studentId, fullWidth }: ChatPanelProps) {
  const { t, setSessionId } = useApp();

  const handleSessionCreated = useCallback((newId: string) => {
    setSessionId(newId);
  }, [setSessionId]);

  const { messages, sendMessage, isConnected, isTyping } = useChat(sessionId || null, studentId, handleSessionCreated);
  const { student, fetchStudent } = useStudent();
  const [input, setInput] = useState('');
  const [editingTurnId, setEditingTurnId] = useState<string | null>(null);
  const [editingValue, setEditingValue] = useState('');
  const [dismissedCards, setDismissedCards] = useState<Set<string>>(new Set());
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (studentId) fetchStudent(studentId);
  }, [studentId, fetchStudent]);

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [messages, isTyping]);

  const handleSend = () => {
    const trimmed = input.trim();
    if (!trimmed) return;
    sendMessage(trimmed);
    setInput('');
  };

  const startEdit = useCallback((entry: ChatEntry) => {
    if (!entry.turn_id) {
      return;
    }
    setEditingTurnId(entry.turn_id);
    setEditingValue(entry.content);
  }, []);

  const cancelEdit = useCallback(() => {
    setEditingTurnId(null);
    setEditingValue('');
  }, []);

  const submitEdit = useCallback(() => {
    const targetTurnId = editingTurnId;
    const nextText = editingValue.trim();
    if (!targetTurnId || !nextText) {
      return;
    }
    sendMessage({
      message: nextText,
      suppress_user_echo: true,
      user_echo_text: nextText,
      edit: {
        target_turn_id: targetTurnId,
        mode: 'overwrite',
      },
    });
    setEditingTurnId(null);
    setEditingValue('');
  }, [editingTurnId, editingValue, sendMessage]);

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  const handleGuidedSubmit = useCallback(
    (artifactKey: string, answers: Record<string, string | string[]>) => {
      const parts: string[] = [];
      for (const value of Object.values(answers)) {
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
      setDismissedCards((prev) => new Set(prev).add(artifactKey));
    },
    [sendMessage],
  );

  const handleNextAction = useCallback((action: AdvisorAction) => {
    const payload = action.payload || {};

    const domainHint =
      typeof payload.domain_hint === 'string'
        ? payload.domain_hint
        : typeof payload.domain === 'string'
          ? payload.domain
          : undefined;

    const capabilityHint =
      typeof payload.capability_hint === 'string'
        ? payload.capability_hint
        : typeof payload.capability === 'string'
          ? payload.capability
          : undefined;

    const trigger =
      typeof payload.trigger === 'string'
        ? payload.trigger
        : action.action_id;

    const nestedClientContext =
      typeof payload.client_context === 'object' && payload.client_context !== null
        ? (payload.client_context as Record<string, unknown>)
        : {};

    const message =
      typeof payload.message === 'string'
        ? payload.message
        : actionDefaultMessage(action.action_id);

    sendMessage({
      message,
      domain_hint: domainHint as AdvisorDomain | undefined,
      capability_hint: capabilityHint,
      client_context: {
        ...nestedClientContext,
        trigger,
      },
      suppress_user_echo: true,
    });
  }, [sendMessage]);

  const renderArtifact = useCallback(
    (artifact: AdvisorArtifact, artifactKey: string) => {
      switch (artifact.type) {
        case 'guided_intake': {
          const typedArtifact = artifact as GuidedIntakeArtifact;
          if (dismissedCards.has(artifactKey) || typedArtifact.questions.length === 0) {
            return null;
          }
          return (
            <GuidedQuestionCard
              key={artifactKey}
              questions={typedArtifact.questions}
              onSubmit={(answers) => handleGuidedSubmit(artifactKey, answers)}
            />
          );
        }
        case 'school_recommendation': {
          const typedArtifact = artifact as SchoolRecommendationArtifact;
          return <RecommendationCard key={artifactKey} data={typedArtifact.data} />;
        }
        case 'offer_comparison':
          return <OfferComparisonCard key={artifactKey} artifact={artifact as OfferComparisonArtifact} />;
        case 'strategy_plan':
          return <StrategyPlanCard key={artifactKey} artifact={artifact as StrategyPlanArtifact} />;
        case 'what_if_result':
          return <WhatIfResultCard key={artifactKey} artifact={artifact as WhatIfResultArtifact} />;
        case 'info_card':
          return <InfoCard key={artifactKey} artifact={artifact as InfoCardArtifact} />;
        default:
          return null;
      }
    },
    [dismissedCards, handleGuidedSubmit],
  );

  return (
    <section
      className={`${fullWidth ? 'w-full' : 'w-[40%]'} bg-white flex flex-col h-full border-r border-outline-variant/10 relative z-10`}
      data-testid="advisor-panel"
    >
      <header className="h-16 px-8 flex items-center bg-white/80 backdrop-blur-md sticky top-0 z-20 border-b border-outline-variant/5">
        <div className="flex items-center gap-3">
          <h2 className="font-headline text-base font-extrabold text-on-surface tracking-tight">{t.chat_title}</h2>
          <span className={`w-2 h-2 rounded-full ${isConnected ? 'bg-tertiary shadow-[0_0_8px_rgba(0,83,18,0.4)]' : 'bg-on-surface-variant/30'} animate-pulse`}></span>
        </div>
      </header>

      <div
        ref={scrollRef}
        className="flex-1 overflow-y-auto px-8 py-8 space-y-8 font-body"
        data-testid="advisor-message-list"
      >
        {messages.length === 0 && !isTyping && (
          <div className="space-y-6">
            <div className="flex flex-col items-start max-w-[90%]">
              <div className="bg-surface-container-high/40 text-on-surface p-5 rounded-2xl rounded-bl-none text-sm leading-relaxed border border-outline-variant/10">
                {student && student.profile_completed
                  ? (t.chat_welcome_returning ?? `Welcome back, ${student.name}! I have your profile loaded. You can ask me to recommend schools, evaluate a specific school, or adjust your preferences.`)
                  : t.chat_welcome}
              </div>
              <span className="mt-2 text-[10px] font-bold text-on-surface-variant/50 ml-1 uppercase tracking-widest">{t.chat_tag_ai}</span>
            </div>

            {student && (
              <div className="max-w-[90%]">
                <div className="bg-white border border-outline-variant/15 rounded-2xl p-5 shadow-sm space-y-4">
                  <div className="flex items-center gap-3">
                    <div className="w-10 h-10 bg-primary rounded-xl flex items-center justify-center">
                      <span className="material-symbols-outlined text-on-primary text-xl">person</span>
                    </div>
                    <div>
                      <div className="font-headline text-sm font-bold text-on-surface">{student.name}</div>
                      <div className="text-[10px] text-on-surface-variant/60">
                        {student.curriculum_type} &bull; Target {student.target_year}
                        {student.profile_completed && (
                          <span className="ml-1.5 text-tertiary font-bold">&#10003; Complete</span>
                        )}
                      </div>
                    </div>
                  </div>

                  <div className="grid grid-cols-4 gap-2">
                    <div className="text-center p-2 bg-surface-container-low/40 rounded-lg">
                      <div className="text-[8px] text-on-surface-variant font-bold uppercase tracking-widest">GPA</div>
                      <div className="text-sm font-black text-on-surface">{student.gpa}/{student.gpa_scale}</div>
                    </div>
                    <div className="text-center p-2 bg-surface-container-low/40 rounded-lg">
                      <div className="text-[8px] text-on-surface-variant font-bold uppercase tracking-widest">SAT</div>
                      <div className="text-sm font-black text-on-surface">{student.sat_total ?? '—'}</div>
                    </div>
                    <div className="text-center p-2 bg-surface-container-low/40 rounded-lg">
                      <div className="text-[8px] text-on-surface-variant font-bold uppercase tracking-widest">TOEFL</div>
                      <div className="text-sm font-black text-on-surface">{student.toefl_total ?? '—'}</div>
                    </div>
                    <div className="text-center p-2 bg-surface-container-low/40 rounded-lg">
                      <div className="text-[8px] text-on-surface-variant font-bold uppercase tracking-widest">Budget</div>
                      <div className="text-sm font-black text-on-surface">{student.budget_usd ? `$${(student.budget_usd / 1000).toFixed(0)}K` : '—'}</div>
                    </div>
                  </div>

                  {student.intended_majors && student.intended_majors.length > 0 && (
                    <div className="flex flex-wrap gap-1.5">
                      {student.intended_majors.map((major, idx) => (
                        <span key={idx} className="px-2.5 py-1 bg-primary/5 text-primary text-[10px] font-bold rounded-md border border-primary/10">{major}</span>
                      ))}
                    </div>
                  )}

                  <div className="flex flex-wrap gap-2 pt-1">
                    {[
                      t.chat_quick_recommend ?? 'Recommend schools for me',
                      t.chat_quick_evaluate ?? 'Evaluate Stanford',
                      t.chat_quick_strategy ?? 'Help with my application strategy',
                    ].map((action, idx) => (
                      <button
                        key={idx}
                        onClick={() => sendMessage(action)}
                        className="px-3 py-1.5 text-xs font-bold text-primary bg-primary/5 border border-primary/15 rounded-xl hover:bg-primary/10 transition-colors"
                      >
                        {action}
                      </button>
                    ))}
                  </div>
                </div>
              </div>
            )}
          </div>
        )}

        {messages.map((msg: ChatEntry, i: number) => {
          const artifacts = msg.artifacts ?? [];
          const done = msg.done ?? [];
          const pending = msg.pending ?? [];
          const nextActions = msg.next_actions ?? [];
          const compatibilityActions = msg.actions ?? [];

          return (
            <div key={i}>
              {msg.role === 'user' ? (
                <div className="flex flex-col items-end ml-auto max-w-[90%]">
                  {msg.turn_id && editingTurnId === msg.turn_id ? (
                    <div className="bg-primary/10 border border-primary/20 p-4 rounded-2xl rounded-br-none text-sm leading-relaxed shadow-lg shadow-primary/10 w-full max-w-[680px]">
                      <textarea
                        value={editingValue}
                        onChange={(e) => setEditingValue(e.target.value)}
                        className="w-full bg-transparent text-on-surface border border-primary/20 rounded-xl p-3 min-h-[88px] resize-y focus:outline-none focus:ring-2 focus:ring-primary/20"
                        data-testid="advisor-edit-input"
                      />
                      <div className="mt-3 flex items-center justify-end gap-2">
                        <button
                          onClick={cancelEdit}
                          className="px-3 py-1.5 text-xs font-bold text-on-surface-variant bg-white border border-outline-variant/20 rounded-xl hover:bg-surface-container-high/30 transition-colors"
                          data-testid="advisor-edit-cancel"
                        >
                          Cancel
                        </button>
                        <button
                          onClick={submitEdit}
                          disabled={!editingValue.trim()}
                          className="px-3 py-1.5 text-xs font-bold text-on-primary bg-primary rounded-xl hover:scale-[1.02] transition-transform disabled:opacity-50 disabled:hover:scale-100"
                          data-testid="advisor-edit-save"
                        >
                          Save & Regenerate
                        </button>
                      </div>
                    </div>
                  ) : (
                    <div className="bg-primary text-on-primary p-5 rounded-2xl rounded-br-none text-sm leading-relaxed shadow-lg shadow-primary/10">
                      {msg.content}
                    </div>
                  )}
                  <span className="mt-2 text-[10px] font-bold text-on-surface-variant/50 mr-1 uppercase tracking-widest">
                    {t.chat_tag_user} {msg.timestamp ? `• ${formatTime(msg.timestamp)}` : ''}
                  </span>
                  <div className="mt-1 flex items-center gap-2">
                    {msg.edited && (
                      <span className="px-2 py-0.5 text-[9px] font-bold rounded-md bg-surface-container-high text-on-surface-variant uppercase tracking-widest">
                        Edited
                      </span>
                    )}
                    {msg.editable && msg.turn_id && editingTurnId !== msg.turn_id && (
                      <button
                        onClick={() => startEdit(msg)}
                        className="px-2 py-0.5 text-[9px] font-bold text-primary bg-primary/5 border border-primary/20 rounded-md uppercase tracking-widest hover:bg-primary/10 transition-colors"
                        data-testid="advisor-user-edit"
                      >
                        Edit
                      </button>
                    )}
                  </div>
                </div>
              ) : (
                <div className="flex flex-col items-start max-w-[95%]" data-testid="advisor-assistant-message">
                  <div className="bg-surface-container-high/40 text-on-surface p-5 rounded-2xl rounded-bl-none text-sm leading-relaxed border border-outline-variant/10 prose prose-sm max-w-none prose-headings:text-on-surface prose-strong:text-on-surface prose-p:my-1 prose-ul:my-1 prose-ol:my-1 prose-li:my-0.5">
                    <ReactMarkdown>{msg.content}</ReactMarkdown>

                    {(msg.domain || msg.capability) && (
                      <div className="mt-4 p-3 bg-white rounded-xl border border-outline-variant/20 shadow-sm">
                        <div className="flex flex-wrap items-center gap-2">
                          {msg.domain && (
                            <span className="px-2 py-1 text-[10px] font-bold rounded-md bg-primary/10 text-primary uppercase tracking-widest">
                              {formatDomain(msg.domain)}
                            </span>
                          )}
                          {msg.capability && (
                            <span className="px-2 py-1 text-[10px] font-bold rounded-md bg-surface-container-high/60 text-on-surface-variant uppercase tracking-widest">
                              {formatCapability(msg.capability)}
                            </span>
                          )}
                        </div>
                      </div>
                    )}

                    {msg.error && (
                      <div
                        className="mt-4 p-3 bg-error/5 border border-error/20 rounded-xl text-xs text-error/80 space-y-1"
                        data-testid="advisor-error"
                      >
                        <div className="font-black uppercase tracking-widest text-[10px]">{msg.error.code}</div>
                        <div>{msg.error.message}</div>
                      </div>
                    )}

                    {(done.length > 0 || pending.length > 0) && (
                      <div className="mt-4 grid grid-cols-2 gap-3">
                        <div className="bg-white rounded-xl border border-outline-variant/20 p-3" data-testid="advisor-done">
                          <div className="text-[10px] font-black text-on-surface-variant uppercase tracking-widest mb-2">Done</div>
                          {done.length === 0 ? (
                            <div className="text-xs text-on-surface-variant/50">None</div>
                          ) : (
                            <div className="space-y-2">
                              {done.map((step, idx) => (
                                <div
                                  key={idx}
                                  className="text-xs border border-outline-variant/10 rounded-lg p-2"
                                  data-testid="advisor-done-step"
                                >
                                  <div className="flex items-center justify-between gap-2">
                                    <span className="font-bold text-on-surface truncate">{formatCapability(step.capability)}</span>
                                    <span className={`px-1.5 py-0.5 border rounded text-[9px] font-black uppercase tracking-widest ${statusBadgeClass(step.status)}`}>
                                      {step.status}
                                    </span>
                                  </div>
                                  {step.message && <div className="text-on-surface-variant/60 mt-1">{step.message}</div>}
                                </div>
                              ))}
                            </div>
                          )}
                        </div>

                        <div className="bg-white rounded-xl border border-outline-variant/20 p-3" data-testid="advisor-pending">
                          <div className="text-[10px] font-black text-on-surface-variant uppercase tracking-widest mb-2">Pending</div>
                          {pending.length === 0 ? (
                            <div className="text-xs text-on-surface-variant/50">None</div>
                          ) : (
                            <div className="space-y-2">
                              {pending.map((step, idx) => (
                                <div
                                  key={idx}
                                  className="text-xs border border-outline-variant/10 rounded-lg p-2"
                                  data-testid="advisor-pending-step"
                                >
                                  <div className="font-bold text-on-surface truncate">{formatCapability(step.capability)}</div>
                                  <div className="mt-1 text-on-surface-variant/60">{pendingReasonLabel(step.reason)}</div>
                                  {step.message && <div className="mt-1 text-on-surface-variant/60">{step.message}</div>}
                                </div>
                              ))}
                            </div>
                          )}
                        </div>
                      </div>
                    )}

                    {msg.route_meta && (
                      <div className="mt-3 text-[10px] text-on-surface-variant/50 uppercase tracking-widest">
                        guard: {msg.route_meta.guard_result} · executed: {msg.route_meta.executed_count} · pending: {msg.route_meta.pending_count}
                      </div>
                    )}
                  </div>

                  <span className="mt-2 text-[10px] font-bold text-on-surface-variant/50 ml-1 uppercase tracking-widest">
                    {t.chat_tag_ai} {msg.timestamp ? `• ${formatTime(msg.timestamp)}` : ''}
                  </span>

                  {artifacts.map((artifact, artifactIdx) => {
                    const artifactKey = `${i}-${artifactIdx}`;
                    const node = renderArtifact(artifact, artifactKey);
                    if (!node) {
                      return null;
                    }
                    return (
                      <div
                        key={artifactKey}
                        data-testid="advisor-artifact"
                        data-artifact-type={artifact.type}
                      >
                        {node}
                      </div>
                    );
                  })}

                  {nextActions.length > 0 && (
                    <div className="flex flex-wrap gap-2 mt-3" data-testid="advisor-next-actions">
                      {nextActions.map((action, idx) => (
                        <button
                          key={idx}
                          onClick={() => handleNextAction(action)}
                          data-testid={`advisor-next-action-${sanitizeForTestId(action.action_id)}-${idx}`}
                          data-action-id={action.action_id}
                          className="px-3 py-1.5 text-xs font-bold text-primary bg-primary/5 border border-primary/15 rounded-xl hover:bg-primary/10 transition-colors"
                        >
                          {action.label}
                        </button>
                      ))}
                    </div>
                  )}

                  {compatibilityActions.length > 0 && (
                    <div className="flex flex-wrap gap-2 mt-2" data-testid="advisor-actions-compat">
                      {compatibilityActions.map((action, idx) => (
                        <span
                          key={idx}
                          className="px-3 py-1 text-[10px] font-bold text-on-surface-variant bg-surface-container-high/50 border border-outline-variant/15 rounded-xl uppercase tracking-widest"
                        >
                          {action.label}
                        </span>
                      ))}
                    </div>
                  )}
                </div>
              )}
            </div>
          );
        })}

        {isTyping && (
          <div className="flex flex-col items-start max-w-[90%]">
            <div className="bg-surface-container-high/40 text-on-surface p-5 rounded-2xl rounded-bl-none text-sm leading-relaxed border border-outline-variant/10">
              <div className="flex items-center gap-1.5">
                <span className="w-2 h-2 rounded-full bg-on-surface-variant/40 animate-bounce" style={{ animationDelay: '0ms' }}></span>
                <span className="w-2 h-2 rounded-full bg-on-surface-variant/40 animate-bounce" style={{ animationDelay: '150ms' }}></span>
                <span className="w-2 h-2 rounded-full bg-on-surface-variant/40 animate-bounce" style={{ animationDelay: '300ms' }}></span>
              </div>
            </div>
          </div>
        )}
      </div>

      <div className="p-8 bg-gradient-to-t from-white via-white to-transparent">
        <div className="relative flex items-center bg-surface-container-highest rounded-2xl px-6 py-4 shadow-sm border border-outline-variant/20 focus-within:border-primary-fixed-dim focus-within:ring-4 focus-within:ring-primary/5 transition-all">
          <input
            className="flex-1 bg-transparent border-none focus:ring-0 text-sm placeholder:text-on-surface-variant/60 py-1 outline-none"
            placeholder={t.chat_placeholder}
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            data-testid="advisor-input"
          />
          <button
            onClick={handleSend}
            disabled={!input.trim()}
            data-testid="advisor-send"
            className="ml-4 p-2.5 bg-primary text-on-primary rounded-xl hover:scale-105 transition-transform flex items-center justify-center shadow-lg shadow-primary/20 disabled:opacity-50 disabled:hover:scale-100"
          >
            <span className="material-symbols-outlined text-sm font-bold">arrow_upward</span>
          </button>
        </div>
      </div>
    </section>
  );
}

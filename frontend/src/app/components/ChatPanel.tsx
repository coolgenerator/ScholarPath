import React, { useState, useRef, useEffect, useCallback } from 'react';
import ReactMarkdown from 'react-markdown';
import { useChat, ChatEntry } from '../../hooks/useChat';
import { useStudent } from '../../hooks/useStudent';
import { useApp } from '../../context/AppContext';
import { GuidedQuestionCard } from './GuidedQuestionCard';
import { RecommendationCard } from './RecommendationCard';

function formatTime(timestamp: string): string {
  try {
    return new Date(timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  } catch {
    return '';
  }
}

interface ChatPanelProps {
  sessionId: string;
  studentId?: string | null;
  fullWidth?: boolean;
}

export function ChatPanel({ sessionId, studentId, fullWidth }: ChatPanelProps) {
  const { t, setSessionId } = useApp();

  // When user sends the first message in a new session, useChat generates a sessionId
  // and calls this callback so we persist it in app state + URL
  const handleSessionCreated = useCallback((newId: string) => {
    setSessionId(newId);
  }, [setSessionId]);

  const { messages, sendMessage, isConnected, isTyping } = useChat(sessionId || null, studentId, handleSessionCreated);
  const { student, fetchStudent } = useStudent();
  const [input, setInput] = useState('');
  const scrollRef = useRef<HTMLDivElement>(null);

  // Load existing profile for context display
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

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  const [dismissedCards, setDismissedCards] = useState<Set<number>>(new Set());

  const handleSuggestedAction = (action: string) => {
    sendMessage(action);
  };

  const handleGuidedSubmit = useCallback(
    (msgIndex: number, answers: Record<string, string | string[]>) => {
      // Format answers into a readable message
      const parts: string[] = [];
      for (const [key, val] of Object.entries(answers)) {
        if (Array.isArray(val)) {
          if (val.length > 0) parts.push(val.join(', '));
        } else if (val) {
          parts.push(val);
        }
      }
      const message = parts.join('; ');
      if (message) {
        sendMessage(message);
      }
      setDismissedCards((prev) => new Set(prev).add(msgIndex));
    },
    [sendMessage],
  );

  return (
    <section className={`${fullWidth ? 'w-full' : 'w-[40%]'} bg-white flex flex-col h-full border-r border-outline-variant/10 relative z-10`}>
      <header className="h-16 px-8 flex items-center bg-white/80 backdrop-blur-md sticky top-0 z-20 border-b border-outline-variant/5">
        <div className="flex items-center gap-3">
          <h2 className="font-headline text-base font-extrabold text-on-surface tracking-tight">{t.chat_title}</h2>
          <span className={`w-2 h-2 rounded-full ${isConnected ? 'bg-tertiary shadow-[0_0_8px_rgba(0,83,18,0.4)]' : 'bg-on-surface-variant/30'} animate-pulse`}></span>
        </div>
      </header>

      <div ref={scrollRef} className="flex-1 overflow-y-auto px-8 py-8 space-y-8 font-body">
        {messages.length === 0 && !isTyping && (
          <div className="space-y-6">
            {/* Welcome message */}
            <div className="flex flex-col items-start max-w-[90%]">
              <div className="bg-surface-container-high/40 text-on-surface p-5 rounded-2xl rounded-bl-none text-sm leading-relaxed border border-outline-variant/10">
                {student && student.profile_completed
                  ? (t.chat_welcome_returning ?? `Welcome back, ${student.name}! I have your profile loaded. You can ask me to recommend schools, evaluate a specific school, or adjust your preferences.`)
                  : t.chat_welcome}
              </div>
              <span className="mt-2 text-[10px] font-bold text-on-surface-variant/50 ml-1 uppercase tracking-widest">{t.chat_tag_ai}</span>
            </div>

            {/* Profile summary card if student exists */}
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
                      <div className="text-sm font-black text-on-surface">{student.sat_total ?? '\u2014'}</div>
                    </div>
                    <div className="text-center p-2 bg-surface-container-low/40 rounded-lg">
                      <div className="text-[8px] text-on-surface-variant font-bold uppercase tracking-widest">TOEFL</div>
                      <div className="text-sm font-black text-on-surface">{student.toefl_total ?? '\u2014'}</div>
                    </div>
                    <div className="text-center p-2 bg-surface-container-low/40 rounded-lg">
                      <div className="text-[8px] text-on-surface-variant font-bold uppercase tracking-widest">Budget</div>
                      <div className="text-sm font-black text-on-surface">{student.budget_usd ? `$${(student.budget_usd / 1000).toFixed(0)}K` : '\u2014'}</div>
                    </div>
                  </div>

                  {student.intended_majors && student.intended_majors.length > 0 && (
                    <div className="flex flex-wrap gap-1.5">
                      {student.intended_majors.map((m, i) => (
                        <span key={i} className="px-2.5 py-1 bg-primary/5 text-primary text-[10px] font-bold rounded-md border border-primary/10">{m}</span>
                      ))}
                    </div>
                  )}

                  {/* Quick action suggestions */}
                  <div className="flex flex-wrap gap-2 pt-1">
                    {[
                      t.chat_quick_recommend ?? 'Recommend schools for me',
                      t.chat_quick_evaluate ?? 'Evaluate Stanford',
                      t.chat_quick_strategy ?? 'Help with my application strategy',
                    ].map((action, i) => (
                      <button
                        key={i}
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

        {messages.map((msg: ChatEntry, i: number) => (
          <div key={i}>
            {msg.role === 'user' ? (
              <div className="flex flex-col items-end ml-auto max-w-[90%]">
                <div className="bg-primary text-on-primary p-5 rounded-2xl rounded-br-none text-sm leading-relaxed shadow-lg shadow-primary/10">
                  {msg.content}
                </div>
                <span className="mt-2 text-[10px] font-bold text-on-surface-variant/50 mr-1 uppercase tracking-widest">
                  {t.chat_tag_user} {msg.timestamp ? `\u2022 ${formatTime(msg.timestamp)}` : ''}
                </span>
              </div>
            ) : (
              <div className="flex flex-col items-start max-w-[95%]">
                <div className="bg-surface-container-high/40 text-on-surface p-5 rounded-2xl rounded-bl-none text-sm leading-relaxed border border-outline-variant/10 prose prose-sm max-w-none prose-headings:text-on-surface prose-strong:text-on-surface prose-p:my-1 prose-ul:my-1 prose-ol:my-1 prose-li:my-0.5">
                  <ReactMarkdown>{msg.content}</ReactMarkdown>
                  {msg.intent && msg.intent !== 'general' && msg.intent !== 'error' && msg.intent !== 'system' && (
                    <div className="mt-4 p-5 bg-white rounded-xl border border-outline-variant/20 shadow-sm">
                      <div className="flex items-center gap-3 mb-3">
                        <span className="material-symbols-outlined text-tertiary text-[20px]" style={{ fontVariationSettings: "'FILL' 1" }}>analytics</span>
                        <span className="font-bold text-xs">{msg.intent.replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase())}</span>
                      </div>
                    </div>
                  )}
                </div>
                <span className="mt-2 text-[10px] font-bold text-on-surface-variant/50 ml-1 uppercase tracking-widest">
                  {t.chat_tag_ai} {msg.timestamp ? `\u2022 ${formatTime(msg.timestamp)}` : ''}
                </span>

                {msg.suggested_actions && msg.suggested_actions.length > 0 && (
                  <div className="flex flex-wrap gap-2 mt-3">
                    {msg.suggested_actions.map((action, j) => (
                      <button
                        key={j}
                        onClick={() => handleSuggestedAction(action)}
                        className="px-3 py-1.5 text-xs font-bold text-primary bg-primary/5 border border-primary/15 rounded-xl hover:bg-primary/10 transition-colors"
                      >
                        {action}
                      </button>
                    ))}
                  </div>
                )}

                {msg.recommendation && (
                  <RecommendationCard data={msg.recommendation} />
                )}

                {msg.guided_questions && msg.guided_questions.length > 0 && !dismissedCards.has(i) && (
                  <GuidedQuestionCard
                    questions={msg.guided_questions}
                    onSubmit={(answers) => handleGuidedSubmit(i, answers)}
                  />
                )}
              </div>
            )}
          </div>
        ))}

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
          />
          <button
            onClick={handleSend}
            disabled={!input.trim()}
            className="ml-4 p-2.5 bg-primary text-on-primary rounded-xl hover:scale-105 transition-transform flex items-center justify-center shadow-lg shadow-primary/20 disabled:opacity-50 disabled:hover:scale-100"
          >
            <span className="material-symbols-outlined text-sm font-bold">arrow_upward</span>
          </button>
        </div>
      </div>
    </section>
  );
}

import React, { useState, useEffect } from 'react';
import { useApp } from '../../context/AppContext';
import { sessionsApi, ChatSessionResponse } from '../../lib/api/sessions';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from './ui/dropdown-menu';
import { DashboardInput } from './ui/dashboard-input';

interface SessionActionState {
  renamingSessionId: string | null;
  draftTitle: string;
  busySessionId: string | null;
  error: string | null;
}

function shouldShowPreview(title: string, preview?: string | null): boolean {
  if (!preview) return false;
  const normalizedTitle = title.trim().toLowerCase();
  const normalizedPreview = preview.trim().toLowerCase();
  if (!normalizedPreview) return false;
  if (normalizedPreview === normalizedTitle) return false;
  if (normalizedPreview.includes(normalizedTitle) || normalizedTitle.includes(normalizedPreview)) return false;
  return true;
}

export function HistoryPanel() {
  const { studentId, sessionId, setSessionId, clearSession, setActiveNav, t, locale } = useApp();
  const [sessions, setSessions] = useState<ChatSessionResponse[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [actionState, setActionState] = useState<SessionActionState>({
    renamingSessionId: null,
    draftTitle: '',
    busySessionId: null,
    error: null,
  });

  useEffect(() => {
    setIsLoading(true);
    const fetcher = studentId
      ? sessionsApi.list(studentId)
      : sessionsApi.listRecent();
    fetcher
      .then((data) => setSessions(data))
      .catch(() => {})
      .finally(() => setIsLoading(false));
  }, [studentId]);

  const handleSelectSession = (sid: string) => {
    setSessionId(sid);
    setActiveNav('advisor');
  };

  const startRename = (session: ChatSessionResponse) => {
    setActionState((prev) => ({
      ...prev,
      renamingSessionId: session.session_id,
      draftTitle: session.title,
      error: null,
    }));
  };

  const cancelRename = () => {
    setActionState((prev) => ({
      ...prev,
      renamingSessionId: null,
      draftTitle: '',
      error: null,
    }));
  };

  const submitRename = async (target: ChatSessionResponse) => {
    const nextTitle = actionState.draftTitle.trim();
    if (!nextTitle || nextTitle === target.title) {
      cancelRename();
      return;
    }

    setActionState((prev) => ({ ...prev, busySessionId: target.session_id, error: null }));
    try {
      const updated = await sessionsApi.update(target.session_id, { title: nextTitle });
      setSessions((prev) => prev.map((session) => (
        session.session_id === target.session_id ? updated : session
      )));
      cancelRename();
    } catch (error) {
      setActionState((prev) => ({
        ...prev,
        busySessionId: null,
        error: error instanceof Error ? error.message : String(error),
      }));
      return;
    }

    setActionState((prev) => ({ ...prev, busySessionId: null }));
  };

  const handleDelete = async (target: ChatSessionResponse) => {
    const confirmed = window.confirm(t.hist_delete_confirm);
    if (!confirmed) return;

    setActionState((prev) => ({ ...prev, busySessionId: target.session_id, error: null }));
    try {
      await sessionsApi.remove(target.session_id);
      const remaining = sessions.filter((session) => session.session_id !== target.session_id);
      setSessions(remaining);

      if (target.session_id === sessionId) {
        const fallback = remaining[0];
        if (fallback) {
          setSessionId(fallback.session_id);
        } else {
          clearSession();
        }
        setActiveNav('advisor');
      }

      if (actionState.renamingSessionId === target.session_id) {
        cancelRename();
      }
      setActionState((prev) => ({ ...prev, busySessionId: null }));
    } catch (error) {
      setActionState((prev) => ({
        ...prev,
        busySessionId: null,
        error: error instanceof Error ? error.message : String(error),
      }));
    }
  };

  const formatDate = (iso: string) => {
    try {
      return new Date(iso).toLocaleDateString(locale === 'zh' ? 'zh-CN' : 'en-US', {
        month: 'short',
        day: 'numeric',
        year: 'numeric',
      });
    } catch {
      return '';
    }
  };

  const formatTime = (iso: string) => {
    try {
      return new Date(iso).toLocaleTimeString(locale === 'zh' ? 'zh-CN' : undefined, {
        hour: '2-digit',
        minute: '2-digit',
      });
    } catch {
      return '';
    }
  };

  return (
    <section className="w-full bg-background flex flex-col h-full overflow-hidden font-body">
      <header className="sticky top-0 z-20 flex h-16 items-center justify-between border-b border-outline-variant/10 bg-background/90 px-4 backdrop-blur-md sm:px-6 lg:px-8">
        <div>
          <h1 className="font-headline text-lg font-black text-on-surface tracking-tight">{t.hist_title}</h1>
          <p className="text-[9px] text-on-surface-variant font-bold tracking-[0.1em] uppercase">{t.hist_subtitle}</p>
        </div>
      </header>

      <div className="flex-1 overflow-y-auto px-4 py-5 space-y-8 sm:px-6 sm:py-6 lg:px-8 lg:py-8">
        {/* Sessions list */}
        <div className="space-y-3">
          <div className="flex items-center gap-3 mb-6">
            <div className="w-10 h-10 rounded-xl bg-primary/10 flex items-center justify-center">
              <span className="material-symbols-outlined text-primary text-xl">schedule</span>
            </div>
            <div>
              <h3 className="font-headline text-base font-black text-on-surface">{t.hist_recent}</h3>
              <p className="text-[10px] text-on-surface-variant/70">
                {isLoading ? '...' : `${sessions.length} ${t.hist_sessions_label}`}
              </p>
            </div>
          </div>

          {actionState.error && (
            <div className="mb-4 rounded-2xl border border-error/15 bg-error/5 px-4 py-3 text-sm text-error">
              {actionState.error}
            </div>
          )}

          {isLoading && (
            <div className="space-y-3">
              {[1, 2, 3].map((i) => (
                <div key={i} className="h-24 rounded-2xl bg-surface-container-highest/30 animate-pulse" />
              ))}
            </div>
          )}

          {!isLoading && sessions.length === 0 && (
            <div className="text-center py-12 text-on-surface-variant/50">
              <span className="material-symbols-outlined text-4xl mb-3 block">chat_bubble_outline</span>
              <p className="text-sm font-bold">{t.hist_empty}</p>
              <p className="text-xs mt-1">{t.hist_empty_desc}</p>
            </div>
          )}

          {sessions.map((s) => {
            const isCurrent = s.session_id === sessionId;
            const showPreview = shouldShowPreview(s.title, s.preview);

            return (
              <div
                key={s.id}
                className={`relative flex items-start gap-4 rounded-3xl p-5 transition-all cursor-pointer ${
                  isCurrent
                    ? 'bg-primary/6 ring-1 ring-primary/20 shadow-sm'
                    : 'bg-surface-container-lowest shadow-sm ring-1 ring-outline-variant/8 hover:shadow-md'
                }`}
                onClick={() => handleSelectSession(s.session_id)}
              >
                <div className={`w-3 h-3 mt-1.5 rounded-full shrink-0 ${isCurrent ? 'bg-primary shadow-lg shadow-primary/30' : 'bg-on-surface-variant/20'}`} />
                <div className="flex-1 min-w-0">
                  <div className="mb-1 flex items-center gap-2">
                    {actionState.renamingSessionId === s.session_id ? (
                      <div
                        className="flex flex-1 flex-wrap items-center gap-2"
                        onClick={(event) => event.stopPropagation()}
                      >
                        <DashboardInput
                          variant="compact"
                          className="min-w-0 flex-1 text-sm font-semibold"
                          value={actionState.draftTitle}
                          onChange={(event) => setActionState((prev) => ({ ...prev, draftTitle: event.target.value }))}
                          onKeyDown={(event) => {
                            if (event.key === 'Enter') {
                              event.preventDefault();
                              void submitRename(s);
                            }
                            if (event.key === 'Escape') {
                              event.preventDefault();
                              cancelRename();
                            }
                          }}
                          autoFocus
                        />
                        <button
                          onClick={(event) => {
                            event.stopPropagation();
                            void submitRename(s);
                          }}
                          disabled={actionState.busySessionId === s.session_id}
                          className="rounded-lg bg-primary px-3 py-1.5 text-[10px] font-bold uppercase tracking-[0.12em] text-on-primary disabled:opacity-50"
                        >
                          {t.common_save}
                        </button>
                        <button
                          onClick={(event) => {
                            event.stopPropagation();
                            cancelRename();
                          }}
                          className="rounded-lg border border-outline-variant/15 px-3 py-1.5 text-[10px] font-bold uppercase tracking-[0.12em] text-on-surface-variant"
                        >
                          {t.prof_cancel}
                        </button>
                      </div>
                    ) : (
                      <span className="font-headline text-sm font-bold text-on-surface truncate">{s.title}</span>
                    )}
                    {isCurrent && (
                      <span className="px-2 py-0.5 bg-primary text-on-primary text-[8px] font-bold uppercase tracking-widest rounded-md shrink-0">
                        {t.hist_active}
                      </span>
                    )}
                  </div>
                  {showPreview && (
                    <p className="mb-2 line-clamp-1 text-xs text-on-surface-variant/65">{s.preview}</p>
                  )}
                  <div className="flex flex-wrap items-center gap-x-4 gap-y-2 text-[10px] font-bold uppercase tracking-widest text-on-surface-variant/50">
                    <span className="flex items-center gap-1">
                      <span className="material-symbols-outlined text-xs">calendar_today</span>
                      {formatDate(s.created_at)} {formatTime(s.last_active_at)}
                    </span>
                    <span className="flex items-center gap-1">
                      <span className="material-symbols-outlined text-xs">chat</span>
                      {s.message_count} {t.hist_turns}
                    </span>
                    {s.school_count > 0 && (
                      <span className="flex items-center gap-1">
                        <span className="material-symbols-outlined text-xs">school</span>
                        {s.school_count} {t.hist_school_refs}
                      </span>
                    )}
                  </div>
                </div>
                <div className="mt-1 flex items-center gap-1 self-start">
                  <DropdownMenu>
                    <DropdownMenuTrigger asChild onClick={(event) => event.stopPropagation()}>
                      <button
                        className="flex h-8 w-8 items-center justify-center rounded-full text-on-surface-variant/50 transition-colors hover:bg-surface-container-high/60 hover:text-on-surface"
                        aria-label={t.hist_session_actions}
                      >
                        <span className="material-symbols-outlined text-lg">more_horiz</span>
                      </button>
                    </DropdownMenuTrigger>
                    <DropdownMenuContent align="end" className="w-44">
                      <DropdownMenuItem
                        onClick={(event) => {
                          event.stopPropagation();
                          startRename(s);
                        }}
                      >
                        <span className="material-symbols-outlined text-base">drive_file_rename_outline</span>
                        {t.hist_rename}
                      </DropdownMenuItem>
                      <DropdownMenuItem
                        variant="destructive"
                        onClick={(event) => {
                          event.stopPropagation();
                          void handleDelete(s);
                        }}
                      >
                        <span className="material-symbols-outlined text-base">delete</span>
                        {t.hist_delete}
                      </DropdownMenuItem>
                    </DropdownMenuContent>
                  </DropdownMenu>
                  <span className="material-symbols-outlined text-on-surface-variant/30 text-xl">chevron_right</span>
                </div>
              </div>
            );
          })}
        </div>

        {/* Activity Overview */}
        <div className="rounded-3xl bg-surface-container-lowest p-6 shadow-sm ring-1 ring-outline-variant/8 sm:p-8">
          <h3 className="font-headline text-base font-black text-on-surface mb-6">{t.hist_activity}</h3>
          <div className="grid grid-cols-1 gap-4 sm:grid-cols-3 sm:gap-6">
            <div className="text-center">
              <div className="w-14 h-14 mx-auto rounded-2xl bg-primary/10 flex items-center justify-center mb-3">
                <span className="material-symbols-outlined text-primary text-2xl">chat_bubble</span>
              </div>
              <div className="text-2xl font-black text-on-surface">{sessions.length}</div>
              <div className="text-[9px] font-bold text-on-surface-variant uppercase tracking-widest mt-1">{t.hist_sessions}</div>
            </div>
            <div className="text-center">
              <div className="w-14 h-14 mx-auto rounded-2xl bg-tertiary/10 flex items-center justify-center mb-3">
                <span className="material-symbols-outlined text-tertiary text-2xl">chat</span>
              </div>
              <div className="text-2xl font-black text-on-surface">
                {sessions.reduce((sum, s) => sum + s.message_count, 0)}
              </div>
              <div className="text-[9px] font-bold text-on-surface-variant uppercase tracking-widest mt-1">{t.hist_turns}</div>
            </div>
            <div className="text-center">
              <div className="w-14 h-14 mx-auto rounded-2xl bg-secondary-fixed/30 flex items-center justify-center mb-3">
                <span className="material-symbols-outlined text-on-secondary-fixed-variant text-2xl">school</span>
              </div>
              <div className="text-2xl font-black text-on-surface">
                {sessions.reduce((sum, s) => sum + s.school_count, 0) || '\u2014'}
              </div>
              <div className="text-[9px] font-bold text-on-surface-variant uppercase tracking-widest mt-1">{t.hist_schools_evaluated}</div>
            </div>
          </div>
        </div>

        <div className="h-12" />
      </div>
    </section>
  );
}

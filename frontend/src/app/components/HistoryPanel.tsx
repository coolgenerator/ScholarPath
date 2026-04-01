import React, { useState, useEffect } from 'react';
import { useApp } from '../../context/AppContext';
import { sessionsApi, ChatSessionResponse } from '../../lib/api/sessions';

export function HistoryPanel() {
  const { studentId, sessionId, setSessionId, setActiveNav, t } = useApp();
  const [sessions, setSessions] = useState<ChatSessionResponse[]>([]);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    if (!studentId) {
      setIsLoading(false);
      return;
    }
    setIsLoading(true);
    sessionsApi
      .list(studentId)
      .then((data) => setSessions(data))
      .catch(() => {})
      .finally(() => setIsLoading(false));
  }, [studentId]);

  const handleSelectSession = (sid: string) => {
    setSessionId(sid);
    setActiveNav('advisor');
  };

  const formatDate = (iso: string) => {
    try {
      return new Date(iso).toLocaleDateString('en-US', {
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
      return new Date(iso).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    } catch {
      return '';
    }
  };

  return (
    <section className="w-full bg-background flex flex-col h-full overflow-hidden font-body">
      <header className="h-16 px-10 flex items-center justify-between sticky top-0 bg-background/90 backdrop-blur-md z-20 border-b border-outline-variant/10">
        <div>
          <h1 className="font-headline text-lg font-black text-on-surface tracking-tight">{t.hist_title}</h1>
          <p className="text-[9px] text-on-surface-variant font-bold tracking-[0.1em] uppercase">{t.hist_subtitle}</p>
        </div>
      </header>

      <div className="flex-1 overflow-y-auto px-10 py-10 space-y-8">
        {/* Sessions list */}
        <div className="space-y-1">
          <div className="flex items-center gap-3 mb-6">
            <div className="w-10 h-10 rounded-xl bg-primary/10 flex items-center justify-center">
              <span className="material-symbols-outlined text-primary text-xl">schedule</span>
            </div>
            <div>
              <h3 className="font-headline text-base font-black text-on-surface">{t.hist_recent}</h3>
              <p className="text-[10px] text-on-surface-variant/70">
                {isLoading ? '...' : `${sessions.length} session${sessions.length !== 1 ? 's' : ''}`}
              </p>
            </div>
          </div>

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
              <p className="text-sm font-bold">{t.hist_empty || 'No sessions yet'}</p>
              <p className="text-xs mt-1">{t.hist_empty_desc || 'Start a conversation with the Advisor to begin.'}</p>
            </div>
          )}

          {sessions.map((s) => {
            const isCurrent = s.session_id === sessionId;
            return (
              <div
                key={s.id}
                className={`relative flex items-start gap-5 p-5 rounded-2xl border transition-all cursor-pointer ${
                  isCurrent
                    ? 'bg-primary/5 border-primary/20 shadow-sm'
                    : 'bg-surface-container-lowest border-outline-variant/10 hover:shadow-sm hover:border-outline-variant/20'
                }`}
                onClick={() => handleSelectSession(s.session_id)}
              >
                <div className={`w-3 h-3 mt-1.5 rounded-full shrink-0 ${isCurrent ? 'bg-primary shadow-lg shadow-primary/30' : 'bg-on-surface-variant/20'}`} />
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2 mb-1">
                    <span className="font-headline text-sm font-bold text-on-surface truncate">{s.title}</span>
                    {isCurrent && (
                      <span className="px-2 py-0.5 bg-primary text-on-primary text-[8px] font-bold uppercase tracking-widest rounded-md shrink-0">
                        {t.hist_active}
                      </span>
                    )}
                  </div>
                  {s.preview && (
                    <p className="text-xs text-on-surface-variant/70 mb-2 truncate">{s.preview}</p>
                  )}
                  <div className="flex items-center gap-4 text-[10px] text-on-surface-variant/50 font-bold uppercase tracking-widest">
                    <span className="flex items-center gap-1">
                      <span className="material-symbols-outlined text-xs">calendar_today</span>
                      {formatDate(s.created_at)}
                    </span>
                    <span className="flex items-center gap-1">
                      <span className="material-symbols-outlined text-xs">chat</span>
                      {s.message_count} msgs
                    </span>
                    {s.school_count > 0 && (
                      <span className="flex items-center gap-1">
                        <span className="material-symbols-outlined text-xs">school</span>
                        {s.school_count} schools
                      </span>
                    )}
                  </div>
                </div>
                <span className="material-symbols-outlined text-on-surface-variant/30 text-xl mt-1">chevron_right</span>
              </div>
            );
          })}
        </div>

        {/* Activity Overview */}
        <div className="bg-surface-container-lowest rounded-3xl p-8 border border-outline-variant/10">
          <h3 className="font-headline text-base font-black text-on-surface mb-6">{t.hist_activity}</h3>
          <div className="grid grid-cols-3 gap-6">
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
              <div className="text-[9px] font-bold text-on-surface-variant uppercase tracking-widest mt-1">Messages</div>
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

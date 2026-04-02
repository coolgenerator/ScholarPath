import React, { useState, useRef, useEffect } from 'react';
import { Sidebar } from './components/Sidebar';
import { ChatPanel } from './components/ChatPanel';
import { SchoolListPanel } from './components/SchoolListPanel';
import { ResourcesPanel } from './components/ResourcesPanel';
import { OffersPanel } from './components/OffersPanel';
import { DecisionsPanel } from './components/DecisionsPanel';
import { HistoryPanel } from './components/HistoryPanel';
import { ProfilePanel } from './components/ProfilePanel';
import { useApp } from '../context/AppContext';

function MainContent({ activeNav, studentId, sessionId }: { activeNav: string; studentId: string | null; sessionId: string }) {
  switch (activeNav) {
    case 'advisor':
      return <ChatPanel sessionId={sessionId} studentId={studentId} fullWidth />;
    case 'school-list':
      return <SchoolListPanel studentId={studentId} />;
    case 'discover':
      return <ResourcesPanel studentId={studentId} />;
    case 'offers':
      return <OffersPanel studentId={studentId} />;
    case 'decisions':
      return <DecisionsPanel studentId={studentId} />;
    case 'history':
      return <HistoryPanel />;
    case 'profile':
      return <ProfilePanel studentId={studentId} />;
    default:
      return <SchoolListPanel studentId={studentId} />;
  }
}

function TopBar() {
  const { studentName, locale, setLocale, setActiveNav, setStudentId, setStudentName, setSessionId, t } = useApp();
  const [avatarOpen, setAvatarOpen] = useState(false);
  const dropdownRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target as Node)) {
        setAvatarOpen(false);
      }
    }
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, []);

  return (
    <div className="h-12 bg-background/80 backdrop-blur-md border-b border-outline-variant/10 flex items-center justify-end px-6 gap-3 shrink-0 z-30">
      {/* Language toggle */}
      <div className="flex items-center gap-0.5 bg-surface-container-high/30 rounded-lg px-1 py-0.5">
        <button
          onClick={() => setLocale('en')}
          className={`px-2 py-1 rounded-md text-[11px] font-bold transition-colors ${locale === 'en' ? 'bg-white text-primary shadow-sm' : 'text-on-surface-variant/50 hover:text-on-surface-variant'}`}
        >
          EN
        </button>
        <button
          onClick={() => setLocale('zh')}
          className={`px-2 py-1 rounded-md text-[11px] font-bold transition-colors ${locale === 'zh' ? 'bg-white text-primary shadow-sm' : 'text-on-surface-variant/50 hover:text-on-surface-variant'}`}
        >
          中文
        </button>
      </div>

      {/* Avatar dropdown */}
      <div ref={dropdownRef} className="relative">
        <button
          onClick={() => setAvatarOpen(!avatarOpen)}
          data-testid="topbar-avatar-toggle"
          className="flex items-center gap-2 pl-2 pr-1 py-1 rounded-xl hover:bg-surface-container-high/40 transition-colors"
        >
          <span className="text-xs font-bold text-on-surface hidden sm:inline">{studentName ?? 'Student'}</span>
          <div className="w-8 h-8 rounded-full bg-primary flex items-center justify-center text-on-primary text-sm font-black">
            {(studentName ?? 'S').charAt(0).toUpperCase()}
          </div>
        </button>

        {avatarOpen && (
          <div className="absolute right-0 top-11 w-52 bg-white rounded-2xl shadow-xl border border-outline-variant/15 py-1.5 z-50">
            {/* User info */}
            <div className="px-4 py-3 border-b border-outline-variant/10">
              <div className="font-headline text-sm font-bold text-on-surface">{studentName ?? 'Student'}</div>
              <div className="text-[10px] text-on-surface-variant/50 mt-0.5">ScholarPath User</div>
            </div>

            {/* Profile */}
            <button
              onClick={() => { setActiveNav('profile'); setAvatarOpen(false); }}
              data-testid="topbar-profile"
              className="w-full text-left px-4 py-2.5 text-sm text-on-surface hover:bg-surface-container-high/40 transition-colors flex items-center gap-3"
            >
              <span className="material-symbols-outlined text-lg text-on-surface-variant">person</span>
              {t.prof_title ?? 'My Profile'}
            </button>

            {/* Support */}
            <button
              className="w-full text-left px-4 py-2.5 text-sm text-on-surface hover:bg-surface-container-high/40 transition-colors flex items-center gap-3"
            >
              <span className="material-symbols-outlined text-lg text-on-surface-variant">help</span>
              {t.nav_support}
            </button>

            {/* Divider + Logout */}
            <div className="border-t border-outline-variant/10 mt-1 pt-1">
              <button
                onClick={() => {
                  setStudentId(null);
                  setStudentName(null);
                  setSessionId(crypto.randomUUID());
                  setActiveNav('advisor');
                  setAvatarOpen(false);
                  localStorage.removeItem('sp_student_id');
                  localStorage.removeItem('sp_student_name');
                  localStorage.removeItem('sp_session_id');
                }}
                className="w-full text-left px-4 py-2.5 text-sm text-error hover:bg-error/5 transition-colors flex items-center gap-3"
              >
                <span className="material-symbols-outlined text-lg">logout</span>
                {t.nav_logout}
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

export default function App() {
  const { studentId, sessionId, activeNav, sidebarCollapsed } = useApp();

  return (
    <div className="bg-background text-on-surface flex min-h-screen overflow-hidden font-body selection:bg-primary/20">
      <Sidebar />
      <div className={`${sidebarCollapsed ? 'ml-[72px]' : 'ml-64'} flex-1 flex flex-col h-screen overflow-hidden transition-all duration-300`}>
        <TopBar />
        <main className="flex-1 flex overflow-hidden">
          <MainContent activeNav={activeNav} studentId={studentId} sessionId={sessionId} />
        </main>
      </div>
    </div>
  );
}

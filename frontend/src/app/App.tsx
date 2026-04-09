import React, { Suspense, lazy, useState, useRef, useEffect } from 'react';
import { AnimatePresence, motion } from 'motion/react';
import { useNavigate } from 'react-router';
import { Sidebar } from './components/Sidebar';
import { AnimatedWorkspacePage } from './components/WorkspaceMotion';
import { PageFallback } from './components/PageFallback';
import { useIsMobile } from './components/ui/use-mobile';
import { useApp } from '../context/AppContext';

const LazyChatPanel = lazy(() => import('./components/ChatPanel').then((module) => ({ default: module.ChatPanel })));
const LazySchoolListPanel = lazy(() => import('./components/SchoolListPanel').then((module) => ({ default: module.SchoolListPanel })));
const LazyResourcesPanel = lazy(() => import('./components/ResourcesPanel').then((module) => ({ default: module.ResourcesPanel })));
const LazyOffersPanel = lazy(() => import('./components/OffersPanel').then((module) => ({ default: module.OffersPanel })));
const LazyDecisionsPanel = lazy(() => import('./components/DecisionsPanel').then((module) => ({ default: module.DecisionsPanel })));
const LazyHistoryPanel = lazy(() => import('./components/HistoryPanel').then((module) => ({ default: module.HistoryPanel })));

function MainContent({ activeNav, studentId, sessionId }: { activeNav: string; studentId: string | null; sessionId: string | null }) {
  switch (activeNav) {
    case 'advisor':
      return <LazyChatPanel sessionId={sessionId} studentId={studentId} fullWidth />;
    case 'school-list':
      return <LazySchoolListPanel studentId={studentId} />;
    case 'discover':
      return <LazyResourcesPanel studentId={studentId} />;
    case 'offers':
      return <LazyOffersPanel studentId={studentId} />;
    case 'decisions':
      return <LazyDecisionsPanel studentId={studentId} />;
    case 'history':
      return <LazyHistoryPanel />;
    default:
      return <LazySchoolListPanel studentId={studentId} />;
  }
}

function TopBar({
  isMobile,
  onOpenMobileNav,
}: {
  isMobile: boolean;
  onOpenMobileNav: () => void;
}) {
  const navigate = useNavigate();
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
    <div className="z-30 flex h-12 items-center justify-between border-b border-outline-variant/10 bg-background/80 px-4 backdrop-blur-md sm:px-6">
      <motion.div
        className="flex items-center gap-2"
        initial={{ opacity: 0, x: -10 }}
        animate={{ opacity: 1, x: 0 }}
        transition={{ duration: 0.36, ease: [0.22, 1, 0.36, 1] }}
      >
        {isMobile ? (
          <button
            onClick={onOpenMobileNav}
            aria-label={t.nav_open_menu}
            className="dashboard-hover-lift flex h-9 w-9 items-center justify-center rounded-xl border border-outline-variant/10 bg-white text-on-surface shadow-sm transition-colors hover:bg-surface-container-high/50"
          >
            <span className="material-symbols-outlined text-[20px]">menu</span>
          </button>
        ) : (
          <div className="w-9" aria-hidden="true" />
        )}
      </motion.div>

      <motion.div
        className="flex items-center gap-2 sm:gap-3"
        initial={{ opacity: 0, x: 10 }}
        animate={{ opacity: 1, x: 0 }}
        transition={{ duration: 0.36, ease: [0.22, 1, 0.36, 1], delay: 0.05 }}
      >
        <div className="flex items-center gap-0.5 rounded-lg bg-surface-container-high/30 px-1 py-0.5">
          <button
            onClick={() => setLocale('en')}
            className={`rounded-md px-2 py-1 text-[11px] font-bold transition-colors ${locale === 'en' ? 'bg-white text-primary shadow-sm' : 'text-on-surface-variant/50 hover:text-on-surface-variant'}`}
          >
            EN
          </button>
          <button
            onClick={() => setLocale('zh')}
            className={`rounded-md px-2 py-1 text-[11px] font-bold transition-colors ${locale === 'zh' ? 'bg-white text-primary shadow-sm' : 'text-on-surface-variant/50 hover:text-on-surface-variant'}`}
          >
            中文
          </button>
        </div>

        <div ref={dropdownRef} className="relative">
          <button
            onClick={() => setAvatarOpen(!avatarOpen)}
            className="dashboard-hover-lift flex items-center gap-2 rounded-xl py-1 pl-2 pr-1 transition-colors hover:bg-surface-container-high/40"
          >
            <span className="hidden text-xs font-bold text-on-surface sm:inline">{studentName ?? t.common_student}</span>
            <div className="flex h-8 w-8 items-center justify-center rounded-full bg-primary text-sm font-black text-on-primary">
              {(studentName ?? t.common_student).charAt(0).toUpperCase()}
            </div>
          </button>

          {avatarOpen && (
            <div className="absolute right-0 top-11 z-50 w-52 rounded-2xl border border-outline-variant/15 bg-white py-1.5 shadow-xl">
              <div className="border-b border-outline-variant/10 px-4 py-3">
                <div className="font-headline text-sm font-bold text-on-surface">{studentName ?? t.common_student}</div>
                <div className="mt-0.5 text-[10px] text-on-surface-variant/50">{t.nav_user_label}</div>
              </div>

              <button
                onClick={() => { navigate('/profile'); setAvatarOpen(false); }}
                className="flex w-full items-center gap-3 px-4 py-2.5 text-left text-sm text-on-surface transition-colors hover:bg-surface-container-high/40"
              >
                <span className="material-symbols-outlined text-lg text-on-surface-variant">person</span>
                {t.prof_title}
              </button>

              <button
                className="flex w-full items-center gap-3 px-4 py-2.5 text-left text-sm text-on-surface transition-colors hover:bg-surface-container-high/40"
              >
                <span className="material-symbols-outlined text-lg text-on-surface-variant">help</span>
                {t.nav_support}
              </button>

              <div className="mt-1 border-t border-outline-variant/10 pt-1">
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
                  className="flex w-full items-center gap-3 px-4 py-2.5 text-left text-sm text-error transition-colors hover:bg-error/5"
                >
                  <span className="material-symbols-outlined text-lg">logout</span>
                  {t.nav_logout}
                </button>
              </div>
            </div>
          )}
        </div>
      </motion.div>
    </div>
  );
}

export default function App() {
  const { studentId, sessionId, activeNav, sidebarCollapsed, locale, t } = useApp();
  const isMobile = useIsMobile();
  const [mobileNavOpen, setMobileNavOpen] = useState(false);
  const sidebarOffset = isMobile ? 'ml-0' : sidebarCollapsed ? 'ml-[72px]' : 'ml-64';
  const suspenseFallback = <PageFallback variant={activeNav === 'advisor' ? 'advisor' : 'dashboard'} activeNav={activeNav} />;

  useEffect(() => {
    const pageTitles: Record<string, string> = {
      advisor: t.nav_advisor,
      'school-list': t.sl_title,
      discover: t.disc_title,
      offers: t.off_title,
      decisions: t.dec_title,
      history: t.hist_title,
    };
    const sectionTitle = pageTitles[activeNav] ?? t.nav_advisor;

    document.title = `${sectionTitle} | ScholarPath`;
    document.documentElement.lang = locale === 'zh' ? 'zh-CN' : 'en';
  }, [activeNav, locale, t]);

  return (
    <div className="bg-background text-on-surface flex min-h-screen overflow-hidden font-body selection:bg-primary/20">
      <Sidebar mobileOpen={mobileNavOpen} onMobileOpenChange={setMobileNavOpen} />
      <div className={`${sidebarOffset} flex-1 flex flex-col h-screen overflow-hidden transition-all duration-300`}>
        <TopBar isMobile={isMobile} onOpenMobileNav={() => setMobileNavOpen(true)} />
        <main className="flex-1 flex overflow-hidden">
          <Suspense fallback={suspenseFallback}>
            {activeNav === 'advisor' ? (
              <MainContent activeNav={activeNav} studentId={studentId} sessionId={sessionId} />
            ) : (
              <AnimatePresence mode="wait" initial={false}>
                <AnimatedWorkspacePage key={activeNav} className="w-full">
                  <MainContent activeNav={activeNav} studentId={studentId} sessionId={sessionId} />
                </AnimatedWorkspacePage>
              </AnimatePresence>
            )}
          </Suspense>
        </main>
      </div>
    </div>
  );
}

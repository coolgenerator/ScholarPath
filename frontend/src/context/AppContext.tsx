import React, { createContext, useContext, useState, useMemo, useEffect, useCallback } from 'react';
import { useNavigate, useLocation } from 'react-router';
import { locales, Locale } from '../i18n';
import { api } from '../lib/api';

// localStorage helpers for Set<string> persistence
function loadSet(key: string): Set<string> {
  try {
    const raw = localStorage.getItem(key);
    return raw ? new Set(JSON.parse(raw)) : new Set();
  } catch {
    return new Set();
  }
}

function saveSet(key: string, set: Set<string>) {
  localStorage.setItem(key, JSON.stringify([...set]));
}

interface AppContextValue {
  studentId: string | null;
  setStudentId: (id: string | null) => void;
  sessionId: string;
  setSessionId: (id: string) => void;
  studentName: string | null;
  setStudentName: (name: string | null) => void;
  activeNav: string;
  setActiveNav: (nav: string) => void;
  // School management
  favoriteSchoolIds: Set<string>;
  toggleFavorite: (schoolId: string) => void;
  blacklistedSchoolIds: Set<string>;
  toggleBlacklist: (schoolId: string) => void;
  // Sidebar
  sidebarCollapsed: boolean;
  toggleSidebar: () => void;
  // i18n
  locale: Locale;
  setLocale: (locale: Locale) => void;
  t: typeof locales.en;
}

const AppContext = createContext<AppContextValue | null>(null);

function generateSessionId(): string {
  return crypto.randomUUID?.() ?? `session-${Date.now()}-${Math.random().toString(36).slice(2)}`;
}

export function AppProvider({ children }: { children: React.ReactNode }) {
  const navigate = useNavigate();
  const location = useLocation();

  // Extract sessionId and nav from URL: /s/:sessionId/:nav
  function parseUrl() {
    const parts = location.pathname.split('/').filter(Boolean);
    // /s/:sessionId/:nav?
    if (parts[0] === 's' && parts[1]) {
      return { urlSessionId: parts[1], urlNav: parts[2] || null };
    }
    return { urlSessionId: null, urlNav: null };
  }

  const { urlSessionId, urlNav } = parseUrl();

  // Persist studentId and sessionId in localStorage so they survive page reloads
  const [studentId, setStudentIdState] = useState<string | null>(() => {
    return localStorage.getItem('sp_student_id') || null;
  });
  const [sessionId, setSessionIdState] = useState<string>(() => {
    // Priority: URL > localStorage > new
    return urlSessionId || localStorage.getItem('sp_session_id') || generateSessionId();
  });
  const [studentName, setStudentNameState] = useState<string | null>(() => {
    return localStorage.getItem('sp_student_name') || null;
  });
  const [activeNav, setActiveNavState] = useState<string>(() => {
    return urlNav || 'advisor';
  });

  const setStudentId = useCallback((id: string | null) => {
    setStudentIdState(id);
    if (id) localStorage.setItem('sp_student_id', id);
    else localStorage.removeItem('sp_student_id');
  }, []);

  const setSessionId = useCallback((id: string) => {
    setSessionIdState(id);
    localStorage.setItem('sp_session_id', id);
  }, []);

  const setStudentName = useCallback((name: string | null) => {
    setStudentNameState(name);
    if (name) localStorage.setItem('sp_student_name', name);
    else localStorage.removeItem('sp_student_name');
  }, []);

  // Sync URL when sessionId or activeNav changes
  const setActiveNav = useCallback((nav: string) => {
    setActiveNavState(nav);
  }, []);

  // Push URL on state changes
  useEffect(() => {
    const target = `/s/${sessionId}/${activeNav}`;
    if (location.pathname !== target) {
      navigate(target, { replace: true });
    }
  }, [sessionId, activeNav]);

  // On initial mount: persist sessionId and redirect to URL
  useEffect(() => {
    localStorage.setItem('sp_session_id', sessionId);
  }, []);

  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const toggleSidebar = useCallback(() => setSidebarCollapsed((p) => !p), []);

  const [favoriteSchoolIds, setFavoriteSchoolIds] = useState<Set<string>>(new Set());
  const [blacklistedSchoolIds, setBlacklistedSchoolIds] = useState<Set<string>>(new Set());

  const [locale, setLocaleState] = useState<Locale>(() => {
    return (localStorage.getItem('sp_locale') as Locale) || 'en';
  });
  const t = locales[locale];
  const setLocale = useCallback((l: Locale) => {
    setLocaleState(l);
    localStorage.setItem('sp_locale', l);
  }, []);

  // Auto-seed schools + demo student on startup
  useEffect(() => {
    // Always seed schools (idempotent — skips existing)
    api.post('/seed/schools').catch(() => {});

    if (studentId) return;
    api.post<{ student_id?: string }>('/seed/demo-student')
      .then((data) => {
        if (data.student_id) {
          setStudentId(data.student_id);
          setStudentName('Demo Student');
          // Also seed demo evaluations + offers (idempotent)
          api.post('/seed/demo-evaluations').catch(() => {});
          api.post('/seed/demo-offers').catch(() => {});
        }
      })
      .catch(() => {});
  }, []);

  // Load favorites/blacklist from localStorage when studentId changes
  useEffect(() => {
    if (!studentId) return;
    setFavoriteSchoolIds(loadSet(`sp_favorites_${studentId}`));
    setBlacklistedSchoolIds(loadSet(`sp_blacklist_${studentId}`));
  }, [studentId]);

  const toggleFavorite = useCallback((schoolId: string) => {
    setFavoriteSchoolIds((prev) => {
      const next = new Set(prev);
      if (next.has(schoolId)) next.delete(schoolId);
      else next.add(schoolId);
      if (studentId) saveSet(`sp_favorites_${studentId}`, next);
      return next;
    });
  }, [studentId]);

  const toggleBlacklist = useCallback((schoolId: string) => {
    setBlacklistedSchoolIds((prev) => {
      const next = new Set(prev);
      if (next.has(schoolId)) next.delete(schoolId);
      else next.add(schoolId);
      if (studentId) saveSet(`sp_blacklist_${studentId}`, next);
      return next;
    });
  }, [studentId]);

  const value = useMemo(
    () => ({
      studentId,
      setStudentId,
      sessionId,
      setSessionId,
      studentName,
      setStudentName,
      activeNav,
      setActiveNav,
      favoriteSchoolIds,
      toggleFavorite,
      blacklistedSchoolIds,
      toggleBlacklist,
      sidebarCollapsed,
      toggleSidebar,
      locale,
      setLocale,
      t,
    }),
    [studentId, sessionId, studentName, activeNav, favoriteSchoolIds, blacklistedSchoolIds, toggleFavorite, toggleBlacklist, sidebarCollapsed, toggleSidebar, locale, setLocale, t],
  );

  return <AppContext.Provider value={value}>{children}</AppContext.Provider>;
}

export function useApp(): AppContextValue {
  const ctx = useContext(AppContext);
  if (!ctx) throw new Error('useApp must be used within AppProvider');
  return ctx;
}

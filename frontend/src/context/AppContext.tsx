import React, { createContext, useContext, useState, useMemo, useEffect, useCallback } from 'react';
import { useNavigate, useLocation } from 'react-router';
import { locales, Locale } from '../i18n';
import { studentsApi } from '../lib/api/students';
import { createSessionId } from '../lib/workspaceSession';

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
  // Auth
  authToken: string | null;
  userId: string | null;
  isAuthenticated: boolean;
  login: (token: string, userId: string, studentId: string | null) => void;
  logout: () => void;
  // Student / session
  studentId: string | null;
  setStudentId: (id: string | null) => void;
  sessionId: string;
  setSessionId: (id: string) => void;
  clearSession: () => void;
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

export function AppProvider({ children }: { children: React.ReactNode }) {
  const navigate = useNavigate();
  const location = useLocation();

  // ---- Auth state ----
  const [authToken, setAuthTokenState] = useState<string | null>(() => {
    return localStorage.getItem('sp_auth_token') || null;
  });
  const [userId, setUserIdState] = useState<string | null>(() => {
    return localStorage.getItem('sp_user_id') || null;
  });

  const isAuthenticated = authToken !== null;

  // Extract sessionId and nav from URL: /s/:sessionId/:nav
  function parseUrl() {
    const parts = location.pathname.split('/').filter(Boolean);
    // /s/:sessionId/:nav?
    if (parts[0] === 's' && parts[1]) {
      const isBlankSession = parts[1] === 'new';
      return {
        urlSessionId: isBlankSession ? null : parts[1],
        isBlankSession,
        urlNav: parts[2] || null,
      };
    }
    return { urlSessionId: null, isBlankSession: false, urlNav: null };
  }

  const { urlSessionId, isBlankSession, urlNav } = parseUrl();

  // Persist studentId and sessionId in localStorage so they survive page reloads
  const [studentId, setStudentIdState] = useState<string | null>(() => {
    return localStorage.getItem('sp_student_id') || null;
  });
  const [sessionId, setSessionIdState] = useState<string>(() => {
    // Priority: URL > localStorage > new
    if (isBlankSession) return '';
    return urlSessionId || localStorage.getItem('sp_session_id') || createSessionId();
  });
  const [studentName, setStudentNameState] = useState<string | null>(() => {
    return localStorage.getItem('sp_student_name') || null;
  });
  const [studentBootstrapReady, setStudentBootstrapReady] = useState(false);
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
    if (id) {
      localStorage.setItem('sp_session_id', id);
    } else {
      localStorage.removeItem('sp_session_id');
    }
  }, []);

  const clearSession = useCallback(() => {
    setSessionIdState('');
    localStorage.removeItem('sp_session_id');
  }, []);

  const resetStudentIdentity = useCallback(() => {
    setStudentIdState(null);
    setStudentNameState(null);
    setSessionIdState('');
    setFavoriteSchoolIds(new Set());
    setBlacklistedSchoolIds(new Set());
    localStorage.removeItem('sp_student_id');
    localStorage.removeItem('sp_student_name');
    localStorage.removeItem('sp_session_id');
  }, []);

  const setStudentName = useCallback((name: string | null) => {
    setStudentNameState(name);
    if (name) localStorage.setItem('sp_student_name', name);
    else localStorage.removeItem('sp_student_name');
  }, []);

  const login = useCallback((token: string, uid: string, sid: string | null) => {
    setAuthTokenState(token);
    setUserIdState(uid);
    localStorage.setItem('sp_auth_token', token);
    localStorage.setItem('sp_user_id', uid);
    if (sid) {
      setStudentIdState(sid);
      localStorage.setItem('sp_student_id', sid);
    }
  }, []);

  const logout = useCallback(() => {
    setAuthTokenState(null);
    setUserIdState(null);
    resetStudentIdentity();
    localStorage.removeItem('sp_auth_token');
    localStorage.removeItem('sp_user_id');
    navigate('/login');
  }, [navigate, resetStudentIdentity]);

  // Sync URL when sessionId or activeNav changes
  const setActiveNav = useCallback((nav: string) => {
    setActiveNavState(nav);
  }, []);

  // Push URL on state changes — only when inside /s/ workspace routes
  useEffect(() => {
    if (!location.pathname.startsWith('/s/') && !location.pathname.startsWith('/s')) return;
    const target = sessionId ? `/s/${sessionId}/${activeNav}` : `/s/new/${activeNav}`;
    if (location.pathname !== target) {
      navigate(target, { replace: true });
    }
  }, [sessionId, activeNav, location.pathname, navigate]);

  // On initial mount: persist sessionId and redirect to URL
  useEffect(() => {
    if (sessionId) {
      localStorage.setItem('sp_session_id', sessionId);
    }
  }, [sessionId]);

  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const toggleSidebar = useCallback(() => setSidebarCollapsed((p) => !p), []);

  const [favoriteSchoolIds, setFavoriteSchoolIds] = useState<Set<string>>(new Set());
  const [blacklistedSchoolIds, setBlacklistedSchoolIds] = useState<Set<string>>(new Set());

  const [locale, setLocaleState] = useState<Locale>(() => {
    const storedLocale = localStorage.getItem('sp_locale');
    return storedLocale === 'en' || storedLocale === 'zh' ? storedLocale : 'zh';
  });
  const t = locales[locale];
  const setLocale = useCallback((l: Locale) => {
    setLocaleState(l);
    localStorage.setItem('sp_locale', l);
  }, []);

  useEffect(() => {
    let cancelled = false;

    async function bootstrapStudent() {
      if (!studentId) {
        setStudentBootstrapReady(true);
        return;
      }

      try {
        const student = await studentsApi.get(studentId);
        if (cancelled) return;

        if (student.name && student.name !== studentName) {
          setStudentNameState(student.name);
          localStorage.setItem('sp_student_name', student.name);
        }
      } catch {
        if (cancelled) return;
        resetStudentIdentity();
      } finally {
        if (!cancelled) {
          setStudentBootstrapReady(true);
        }
      }
    }

    setStudentBootstrapReady(false);
    void bootstrapStudent();

    return () => {
      cancelled = true;
    };
  }, [studentId, studentName, resetStudentIdentity]);

  // Auto-seed demo data only in local development.
  useEffect(() => {
    const isDev = import.meta.env.DEV === true || import.meta.env.MODE === 'development';
    if (!isDev || !studentBootstrapReady) return;

    // Seed schools (idempotent — skips existing)
    fetch('/api/seed/schools', { method: 'POST' }).catch(() => {});

    if (studentId) return;
    fetch('/api/seed/demo-student', { method: 'POST' })
      .then((r) => r.json())
      .then((data) => {
        if (data.student_id) {
          setStudentId(data.student_id);
          setStudentName('Demo Student');
          // Also seed demo evaluations + offers (idempotent)
          fetch('/api/seed/demo-evaluations', { method: 'POST' }).catch(() => {});
          fetch('/api/seed/demo-offers', { method: 'POST' }).catch(() => {});
        }
      })
      .catch(() => {});
  }, [studentBootstrapReady, studentId, setStudentId, setStudentName]);

  // Load favorites/blacklist from localStorage when studentId changes
  useEffect(() => {
    if (!studentBootstrapReady) return;
    if (!studentId) {
      setFavoriteSchoolIds(new Set());
      setBlacklistedSchoolIds(new Set());
      return;
    }
    setFavoriteSchoolIds(loadSet(`sp_favorites_${studentId}`));
    setBlacklistedSchoolIds(loadSet(`sp_blacklist_${studentId}`));
  }, [studentBootstrapReady, studentId]);

  const resolvedStudentId = studentBootstrapReady ? studentId : null;
  const resolvedStudentName = studentBootstrapReady ? studentName : null;

  const toggleFavorite = useCallback((schoolId: string) => {
    setFavoriteSchoolIds((prev) => {
      const next = new Set(prev);
      if (next.has(schoolId)) next.delete(schoolId);
      else next.add(schoolId);
      if (resolvedStudentId) saveSet(`sp_favorites_${resolvedStudentId}`, next);
      return next;
    });
  }, [resolvedStudentId]);

  const toggleBlacklist = useCallback((schoolId: string) => {
    setBlacklistedSchoolIds((prev) => {
      const next = new Set(prev);
      if (next.has(schoolId)) next.delete(schoolId);
      else next.add(schoolId);
      if (resolvedStudentId) saveSet(`sp_blacklist_${resolvedStudentId}`, next);
      return next;
    });
  }, [resolvedStudentId]);

  const value = useMemo(
    () => ({
      authToken,
      userId,
      isAuthenticated,
      login,
      logout,
      studentId: resolvedStudentId,
      setStudentId,
      sessionId,
      setSessionId,
      clearSession,
      studentName: resolvedStudentName,
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
    [authToken, userId, isAuthenticated, login, logout, resolvedStudentId, sessionId, setStudentId, setSessionId, clearSession, resolvedStudentName, activeNav, favoriteSchoolIds, blacklistedSchoolIds, toggleFavorite, toggleBlacklist, sidebarCollapsed, toggleSidebar, locale, setLocale, t],
  );

  return <AppContext.Provider value={value}>{children}</AppContext.Provider>;
}

export function useApp(): AppContextValue {
  const ctx = useContext(AppContext);
  if (!ctx) throw new Error('useApp must be used within AppProvider');
  return ctx;
}

export function useOptionalApp(): AppContextValue | null {
  return useContext(AppContext);
}

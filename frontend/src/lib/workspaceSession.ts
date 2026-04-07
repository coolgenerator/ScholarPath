export const WORKSPACE_STORAGE_KEYS = {
  studentId: 'sp_student_id',
  studentName: 'sp_student_name',
  sessionId: 'sp_session_id',
  locale: 'sp_locale',
} as const;

export function createSessionId(): string {
  return crypto.randomUUID?.() ?? `session-${Date.now()}-${Math.random().toString(36).slice(2)}`;
}

export function buildWorkspacePath(sessionId: string | null | undefined, nav = 'advisor'): string {
  return `/s/${sessionId || 'new'}/${nav}`;
}

export function readWorkspaceSnapshot() {
  if (typeof window === 'undefined') {
    return { studentId: null, studentName: null, sessionId: null };
  }

  return {
    studentId: localStorage.getItem(WORKSPACE_STORAGE_KEYS.studentId),
    studentName: localStorage.getItem(WORKSPACE_STORAGE_KEYS.studentName),
    sessionId: localStorage.getItem(WORKSPACE_STORAGE_KEYS.sessionId),
  };
}

export function persistWorkspaceIdentity({
  studentId,
  studentName,
  sessionId,
  locale,
}: {
  studentId: string;
  studentName: string;
  sessionId: string;
  locale?: 'en' | 'zh';
}) {
  localStorage.setItem(WORKSPACE_STORAGE_KEYS.studentId, studentId);
  localStorage.setItem(WORKSPACE_STORAGE_KEYS.studentName, studentName);
  localStorage.setItem(WORKSPACE_STORAGE_KEYS.sessionId, sessionId);

  if (locale) {
    localStorage.setItem(WORKSPACE_STORAGE_KEYS.locale, locale);
  }
}

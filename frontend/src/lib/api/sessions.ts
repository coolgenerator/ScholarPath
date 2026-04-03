import { api } from './index';
import type { ChatSessionResponse } from '../types';

export const sessionsApi = {
  list(studentId: string) {
    return api.get<ChatSessionResponse[]>(`/sessions/student/${studentId}`);
  },
  create(payload: { student_id: string; session_id: string; title?: string }) {
    return api.post<ChatSessionResponse>('/sessions/', payload);
  },
  update(sessionId: string, payload: Partial<Pick<ChatSessionResponse, 'title' | 'preview' | 'message_count' | 'school_count'>>) {
    return api.put<ChatSessionResponse>(`/sessions/${sessionId}`, payload);
  },
  remove(sessionId: string) {
    return api.delete<void>(`/sessions/${sessionId}`);
  },
};

export type { ChatSessionResponse } from '../types';

import { api } from './index';
import type { ChatSessionResponse } from '../types';

export type { ChatSessionResponse } from '../types';

export const sessionsApi = {
  list(studentId: string) {
    return api.get<ChatSessionResponse[]>(`/sessions/student/${studentId}`);
  },
};

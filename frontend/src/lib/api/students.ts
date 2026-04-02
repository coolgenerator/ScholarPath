import { api } from './index';
import type { StudentCreate, StudentResponse, StudentUpdate } from '../types';

export const studentsApi = {
  create(payload: StudentCreate) {
    return api.post<StudentResponse>('/students/', payload);
  },
  get(studentId: string) {
    return api.get<StudentResponse>(`/students/${studentId}`);
  },
  update(studentId: string, payload: StudentUpdate) {
    return api.put<StudentResponse>(`/students/${studentId}`, payload);
  },
  delete(studentId: string) {
    return api.delete<void>(`/students/${studentId}`);
  },
};

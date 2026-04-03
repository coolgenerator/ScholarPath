import { api } from './index';
import type { StudentCreate, StudentResponse } from '../types';

export const studentsApi = {
  create(payload: StudentCreate) {
    return api.post<StudentResponse>('/students/', payload);
  },
  get(studentId: string) {
    return api.get<StudentResponse>(`/students/${studentId}`);
  },
  remove(studentId: string) {
    return api.delete<void>(`/students/${studentId}`);
  },
};

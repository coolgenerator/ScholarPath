import { api } from './index';
import type { SchoolListResponse, SchoolResponse } from '../types';

export interface SchoolListHints {
  interests?: string[];
  preferences?: string[];
  [key: string]: unknown;
}

export interface GenerateSchoolListResult {
  task_id?: string;
  status: string;
  count?: number;
  schools?: unknown[];
}

export const schoolsApi = {
  list(params?: Record<string, string | number | boolean | null | undefined>) {
    return api.get<SchoolListResponse>('/schools/', params);
  },
  get(schoolId: string) {
    return api.get<SchoolResponse>(`/schools/${schoolId}`);
  },
  generateList(studentId: string, hints?: SchoolListHints) {
    return api.post<GenerateSchoolListResult>(`/schools/students/${studentId}/school-list`, hints ?? {});
  },
  getGeneratedList(studentId: string) {
    return api.get(`/schools/students/${studentId}/school-list`);
  },
  lookup(name: string) {
    return api.post<SchoolResponse>('/schools/lookup', { name });
  },
};

import { api } from './index';
import type { GenerateSchoolListResponse, SchoolListResponse, SchoolResponse } from '../types';

export const schoolsApi = {
  list(params?: Record<string, string>) {
    return api.get<SchoolListResponse>('/schools/', params);
  },
  get(schoolId: string) {
    return api.get<SchoolResponse>(`/schools/${schoolId}`);
  },
  lookup(name: string) {
    return api.post<SchoolResponse>('/schools/lookup', { name });
  },
  generateList(studentId: string, hints?: { interests?: string[]; preferences?: string[] }) {
    return api.post<GenerateSchoolListResponse>(`/schools/students/${studentId}/school-list`, hints ?? {});
  },
};

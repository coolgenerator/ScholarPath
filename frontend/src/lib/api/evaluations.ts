import { api } from './index';
import type { EvaluationResponse, TieredSchoolList } from '../types';

export const evaluationsApi = {
  evaluate(studentId: string, schoolId: string) {
    return api.post<EvaluationResponse>(`/evaluations/students/${studentId}/evaluate/${schoolId}`);
  },
  list(studentId: string) {
    return api.get<EvaluationResponse[]>(`/evaluations/students/${studentId}/evaluations`);
  },
  tiers(studentId: string) {
    return api.get<TieredSchoolList>(`/evaluations/students/${studentId}/tiers`);
  },
};

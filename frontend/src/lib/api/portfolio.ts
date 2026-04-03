import { api } from './index';
import type { StudentPortfolioPatch, StudentPortfolioResponse } from '../types';

export const portfolioApi = {
  get(studentId: string) {
    return api.get<StudentPortfolioResponse>(`/students/${studentId}/portfolio`);
  },
  patch(studentId: string, payload: StudentPortfolioPatch) {
    return api.patch<StudentPortfolioResponse>(`/students/${studentId}/portfolio`, payload);
  },
};

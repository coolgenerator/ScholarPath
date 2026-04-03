import { api } from './index';
import type { CausalDagResponse, CyEdge, CyNode } from '../types';

export const causalApi = {
  getDag(studentId: string, schoolId: string) {
    return api.get<CausalDagResponse>(`/causal/students/${studentId}/schools/${schoolId}/dag`);
  },
};

export type { CausalDagResponse, CyEdge, CyNode } from '../types';

import { api } from './index';
import type { WhatIfResponse } from '../types';

export type { WhatIfResponse } from '../types';

export const simulationsApi = {
  whatIf(studentId: string, schoolId: string, interventions: Record<string, unknown>) {
    return api.post<WhatIfResponse>(`/simulations/students/${studentId}/schools/${schoolId}/what-if`, {
      interventions,
    });
  },
};

import { api } from './index';
import type { ScenarioCompareResponse, WhatIfResponse } from '../types';

export const simulationsApi = {
  whatIf(studentId: string, schoolId: string, interventions: Record<string, unknown>) {
    return api.post<WhatIfResponse>(`/simulations/students/${studentId}/schools/${schoolId}/what-if`, {
      interventions,
    });
  },
  compareScenarios(studentId: string, scenarios: Array<{ interventions: Record<string, unknown> }>) {
    return api.post<ScenarioCompareResponse>(`/simulations/students/${studentId}/compare-scenarios`, {
      scenarios,
    });
  },
};

export type { WhatIfResponse } from '../types';

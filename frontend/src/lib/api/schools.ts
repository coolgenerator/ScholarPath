import { api } from './index';
import type {
  GenerateSchoolListResponse,
  RecommendationScenarioPack,
  RecommendationPrefilterMeta,
  SchoolListResponse,
  SchoolResponse,
} from '../types';

interface SchoolListHints {
  interests?: string[];
  preferences?: string[];
  budget_cap_usd?: number;
}

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
  generateList(studentId: string, hints?: SchoolListHints) {
    return api.post<GenerateSchoolListResponse>(`/schools/students/${studentId}/school-list`, hints ?? {});
  },
  getCommunityReviews(schoolId: string) {
    return api.get<{ school_id: string; count: number; reviews: Array<{ source: string; title: string; body: string; score: number; url: string; comments: Array<{ author: string; body: string; score: number }> }> }>(`/schools/${schoolId}/community-reviews`);
  },
  getCommunityReport(schoolId: string) {
    return api.get<import('../types').CommunityReportResponse>(`/schools/${schoolId}/community-report`);
  },
  getCommunityClaimsGraph(schoolId: string) {
    return api.get<import('../types').ClaimsGraphResponse>(`/schools/${schoolId}/claims-graph`);
  },
  generateScenarioPack(studentId: string, hints?: SchoolListHints) {
    return api.post<{
      status: 'completed';
      count: number;
      scenario_pack?: RecommendationScenarioPack | null;
      prefilter_meta?: RecommendationPrefilterMeta | null;
    }>(`/schools/students/${studentId}/scenario-pack`, hints ?? {});
  },
};

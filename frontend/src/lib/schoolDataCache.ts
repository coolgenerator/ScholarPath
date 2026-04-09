/**
 * Global in-memory cache for school community data.
 * Survives component unmounts so data isn't lost when switching tabs.
 */

import { schoolsApi } from './api/schools';
import type { CommunityReportResponse, ClaimsGraphResponse } from './types';

interface CacheEntry<T> {
  data: T;
  fetchedAt: number;
}

type FetchStatus = 'idle' | 'loading' | 'done' | 'error';

interface RawReview {
  source: string;
  title: string;
  body: string;
  score: number;
  url: string;
  comments: Array<{ author: string; body: string; score: number }>;
}

interface SchoolFetchState {
  communityReport: { status: FetchStatus; progress: string; data?: CommunityReportResponse; rawReviews?: RawReview[] };
  claimsGraph: { status: FetchStatus; progress: string; data?: ClaimsGraphResponse };
}

const cache = new Map<string, SchoolFetchState>();
const listeners = new Map<string, Set<() => void>>();

function getState(schoolId: string): SchoolFetchState {
  if (!cache.has(schoolId)) {
    cache.set(schoolId, {
      communityReport: { status: 'idle', progress: '' },
      claimsGraph: { status: 'idle', progress: '' },
    });
  }
  return cache.get(schoolId)!;
}

function notify(schoolId: string) {
  listeners.get(schoolId)?.forEach((fn) => fn());
}

export function subscribe(schoolId: string, fn: () => void): () => void {
  if (!listeners.has(schoolId)) listeners.set(schoolId, new Set());
  listeners.get(schoolId)!.add(fn);
  return () => { listeners.get(schoolId)?.delete(fn); };
}

export function getSchoolDataState(schoolId: string): SchoolFetchState {
  return getState(schoolId);
}

export async function fetchCommunityReport(schoolId: string): Promise<CommunityReportResponse | null> {
  const state = getState(schoolId);

  // Already loaded
  if (state.communityReport.status === 'done' && state.communityReport.data) {
    return state.communityReport.data;
  }

  // Already in progress (another component triggered it)
  if (state.communityReport.status === 'loading') {
    return new Promise((resolve) => {
      const unsub = subscribe(schoolId, () => {
        const s = getState(schoolId);
        if (s.communityReport.status !== 'loading') {
          unsub();
          resolve(s.communityReport.data ?? null);
        }
      });
    });
  }

  state.communityReport = { status: 'loading', progress: '正在从 Reddit、小红书、知乎等采集...' };
  notify(schoolId);

  // Poll for raw reviews while the full report is being generated
  let pollInterval: ReturnType<typeof setInterval> | null = null;
  let lastReviewCount = 0;

  try {
    // Start polling for raw reviews every 5 seconds
    pollInterval = setInterval(async () => {
      try {
        const raw = await schoolsApi.getCommunityReviews(schoolId);
        if (raw.reviews.length > lastReviewCount) {
          lastReviewCount = raw.reviews.length;
          const currentState = getState(schoolId);
          currentState.communityReport = {
            ...currentState.communityReport,
            progress: `已采集 ${raw.reviews.length} 条评论，持续收集中...`,
            rawReviews: raw.reviews,
          };
          notify(schoolId);
        }
      } catch { /* ignore polling errors */ }
    }, 4000);

    // Meanwhile, request the full report (triggers collection + summarization)
    const data = await schoolsApi.getCommunityReport(schoolId);
    state.communityReport = { status: 'done', progress: '完成', data };
    notify(schoolId);
    return data;
  } catch {
    const currentState = getState(schoolId);
    if (currentState.communityReport.rawReviews?.length) {
      currentState.communityReport = { ...currentState.communityReport, status: 'done', progress: '评论已采集（AI 总结暂不可用）' };
      notify(schoolId);
      return null;
    }
    state.communityReport = { status: 'error', progress: '未找到社区评价' };
    notify(schoolId);
    return null;
  } finally {
    if (pollInterval) clearInterval(pollInterval);
  }
}

export async function fetchClaimsGraph(schoolId: string): Promise<ClaimsGraphResponse | null> {
  const state = getState(schoolId);

  if (state.claimsGraph.status === 'done' && state.claimsGraph.data) {
    return state.claimsGraph.data;
  }

  if (state.claimsGraph.status === 'loading') {
    return new Promise((resolve) => {
      const unsub = subscribe(schoolId, () => {
        const s = getState(schoolId);
        if (s.claimsGraph.status !== 'loading') {
          unsub();
          resolve(s.claimsGraph.data ?? null);
        }
      });
    });
  }

  // Wait for community report to finish first (it triggers data collection)
  const reportState = getState(schoolId).communityReport;
  if (reportState.status === 'loading') {
    state.claimsGraph = { status: 'loading', progress: '等待社区评论采集完成...' };
    notify(schoolId);
    await new Promise<void>((resolve) => {
      const unsub = subscribe(schoolId, () => {
        const s = getState(schoolId);
        if (s.communityReport.status !== 'loading') { unsub(); resolve(); }
      });
    });
  }

  state.claimsGraph = { status: 'loading', progress: '正在提取核心观点和争议...' };
  notify(schoolId);

  try {
    const data = await schoolsApi.getCommunityClaimsGraph(schoolId);
    state.claimsGraph = { status: 'done', progress: '完成', data };
    notify(schoolId);
    return data;
  } catch {
    state.claimsGraph = { status: 'error', progress: '未能生成观点图谱' };
    notify(schoolId);
    return null;
  }
}

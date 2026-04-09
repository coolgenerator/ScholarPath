import { api } from './index';
import type {
  CompareReportRequest,
  CompareReportResponse,
  EvaluationResponse,
  TieredSchoolList,
} from '../types';

export interface CompareStreamEvent {
  event: 'orientation' | 'recommendation' | 'error';
  data: Record<string, unknown>;
}

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
  compareReport(studentId: string, body: CompareReportRequest) {
    return api.post<CompareReportResponse>(
      `/evaluations/students/${studentId}/compare-report`,
      body,
    );
  },
  /**
   * Streaming comparison: reads NDJSON lines and calls `onEvent` for each.
   * Returns when the stream is fully consumed.
   */
  async compareReportStream(
    studentId: string,
    body: CompareReportRequest,
    onEvent: (event: CompareStreamEvent) => void,
  ): Promise<void> {
    const token = localStorage.getItem('sp_auth_token');
    const res = await fetch(`/api/evaluations/students/${studentId}/compare-report/stream`, {
      method: 'POST',
      credentials: 'same-origin',
      headers: {
        'Content-Type': 'application/json',
        ...(token ? { Authorization: `Bearer ${token}` } : {}),
      },
      body: JSON.stringify(body),
    });
    if (!res.ok) throw new Error(`compare-report/stream failed: ${res.status}`);
    const reader = res.body?.getReader();
    if (!reader) throw new Error('No response body');
    const decoder = new TextDecoder();
    let buffer = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop() ?? '';
      for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed) continue;
        try {
          onEvent(JSON.parse(trimmed) as CompareStreamEvent);
        } catch { /* skip malformed lines */ }
      }
    }
    // Process any remaining buffer
    if (buffer.trim()) {
      try {
        onEvent(JSON.parse(buffer.trim()) as CompareStreamEvent);
      } catch { /* skip */ }
    }
  },
};

import { api } from './index';
import type { GoNoGoReport, TaskStatus } from '../types';

export { type GoNoGoReport } from '../types';

export const reportsApi = {
  generateGoNoGo(studentId: string, offerId: string) {
    return api.post<GoNoGoReport>(`/reports/students/${studentId}/offers/${offerId}/go-no-go`);
  },
  get(reportId: string) {
    return api.get<GoNoGoReport>(`/reports/${reportId}`);
  },
  getTask(taskId: string) {
    return api.get<TaskStatus>(`/tasks/${taskId}`);
  },
};

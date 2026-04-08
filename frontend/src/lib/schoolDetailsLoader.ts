import { schoolsApi } from './api/schools';
import type { SchoolResponse } from './types';

const schoolCache = new Map<string, SchoolResponse>();
const failedSchoolIds = new Set<string>();
const inflightRequests = new Map<string, Promise<SchoolResponse | null>>();
const queuedTasks: Array<() => void> = [];

const MAX_CONCURRENT_REQUESTS = 6;
let activeRequests = 0;

function scheduleRequest<T>(task: () => Promise<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    const run = () => {
      activeRequests += 1;
      task()
        .then(resolve)
        .catch(reject)
        .finally(() => {
          activeRequests -= 1;
          const nextTask = queuedTasks.shift();
          if (nextTask) nextTask();
        });
    };

    if (activeRequests < MAX_CONCURRENT_REQUESTS) {
      run();
      return;
    }

    queuedTasks.push(run);
  });
}

export function getCachedSchoolDetails(ids: string[]): Record<string, SchoolResponse> {
  return ids.reduce<Record<string, SchoolResponse>>((acc, id) => {
    const school = schoolCache.get(id);
    if (school) acc[id] = school;
    return acc;
  }, {});
}

export function getMissingSchoolIds(ids: string[]): string[] {
  return [...new Set(ids)].filter(
    (id) => id && !schoolCache.has(id) && !failedSchoolIds.has(id),
  );
}

export async function loadSchoolDetail(id: string): Promise<SchoolResponse | null> {
  if (!id) return null;

  const cached = schoolCache.get(id);
  if (cached) return cached;

  if (failedSchoolIds.has(id)) return null;

  const inflight = inflightRequests.get(id);
  if (inflight) return inflight;

  const request = scheduleRequest(async () => {
    try {
      const school = await schoolsApi.get(id);
      schoolCache.set(id, school);
      return school;
    } catch {
      failedSchoolIds.add(id);
      return null;
    } finally {
      inflightRequests.delete(id);
    }
  });

  inflightRequests.set(id, request);
  return request;
}

export async function loadSchoolDetails(ids: string[]): Promise<Record<string, SchoolResponse>> {
  const uniqueIds = [...new Set(ids)].filter(Boolean);
  await Promise.all(uniqueIds.map((id) => loadSchoolDetail(id)));
  return getCachedSchoolDetails(uniqueIds);
}

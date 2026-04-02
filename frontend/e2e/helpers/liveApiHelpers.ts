import { expect, type APIRequestContext } from '@playwright/test';

type JsonObject = Record<string, unknown>;

export interface SeedContext {
  studentId: string;
}

export interface LegacyProbeCase {
  name: string;
  method: 'GET' | 'POST' | 'PUT';
  path: string;
  body?: JsonObject;
}

function asRecord(value: unknown, label: string): JsonObject {
  expect(value, `${label} should be an object`).not.toBeNull();
  expect(typeof value, `${label} should be an object`).toBe('object');
  return value as JsonObject;
}

function asArray(value: unknown, label: string): unknown[] {
  expect(Array.isArray(value), `${label} should be an array`).toBe(true);
  return value as unknown[];
}

function assertDateString(value: unknown, label: string): void {
  expect(typeof value, `${label} should be a string`).toBe('string');
  expect(Number.isNaN(Date.parse(String(value))), `${label} should be parseable date`).toBe(false);
}

async function postOk(request: APIRequestContext, path: string, data?: JsonObject): Promise<JsonObject> {
  const response = await request.post(path, data ? { data } : undefined);
  expect(response.ok(), `${path} should return 2xx`).toBeTruthy();
  return asRecord(await response.json(), path);
}

export async function seedDemoData(request: APIRequestContext): Promise<SeedContext> {
  await postOk(request, '/api/seed/schools');
  const studentPayload = await postOk(request, '/api/seed/demo-student');
  await postOk(request, '/api/seed/demo-evaluations');
  await postOk(request, '/api/seed/demo-offers');

  const studentId = studentPayload.student_id;
  expect(typeof studentId, 'seed demo-student must return student_id').toBe('string');

  return { studentId: String(studentId) };
}

export function assertSchoolsContract(payload: unknown): string {
  const root = asRecord(payload, 'schools response');
  const items = asArray(root.items, 'schools.items');

  expect(typeof root.total).toBe('number');
  expect(typeof root.page).toBe('number');
  expect(typeof root.per_page).toBe('number');
  expect(items.length, 'schools.items should not be empty').toBeGreaterThan(0);

  const first = asRecord(items[0], 'schools.items[0]');
  for (const field of ['id', 'name', 'city', 'state', 'school_type', 'size_category']) {
    expect(typeof first[field], `schools.items[0].${field} should be string`).toBe('string');
  }

  return String(first.id);
}

export function assertEvaluationsContract(payload: unknown): void {
  const root = asRecord(payload, 'evaluations tiers response');
  const tierKeys = ['reach', 'target', 'safety', 'likely'];

  for (const tier of tierKeys) {
    asArray(root[tier], `evaluations.${tier}`);
  }

  const first = tierKeys
    .map((tier) => asArray(root[tier], `evaluations.${tier}`))
    .find((tierList) => tierList.length > 0)?.[0];

  expect(first, 'at least one evaluation tier item is expected').toBeTruthy();
  const evalItem = asRecord(first, 'evaluations item');

  for (const field of ['id', 'student_id', 'school_id', 'tier']) {
    expect(typeof evalItem[field], `evaluations item ${field} should be string`).toBe('string');
  }

  expect(typeof evalItem.overall_score, 'evaluations item overall_score should be number').toBe('number');
  const score = Number(evalItem.overall_score);
  expect(score >= 0 && score <= 1, 'overall_score should be in [0, 1]').toBe(true);
}

export function assertOffersContract(payload: unknown): string {
  const offers = asArray(payload, 'offers response');
  expect(offers.length, 'offers should not be empty').toBeGreaterThan(0);

  const first = asRecord(offers[0], 'offers[0]');
  for (const field of ['id', 'student_id', 'school_id', 'status']) {
    expect(typeof first[field], `offers[0].${field} should be string`).toBe('string');
  }
  expect(typeof first.total_aid, 'offers[0].total_aid should be number').toBe('number');

  return String(first.id);
}

export function assertHistoryContract(payload: unknown, expectedSessionId: string): void {
  const sessions = asArray(payload, 'sessions response');
  expect(sessions.length, 'sessions should not be empty').toBeGreaterThan(0);

  const session = sessions
    .map((item) => asRecord(item, 'session item'))
    .find((item) => item.session_id === expectedSessionId);

  expect(session, `session ${expectedSessionId} should exist`).toBeTruthy();
  const target = asRecord(session, 'target session');

  for (const field of ['session_id', 'title']) {
    expect(typeof target[field], `session.${field} should be string`).toBe('string');
  }
  for (const field of ['message_count', 'school_count']) {
    expect(typeof target[field], `session.${field} should be number`).toBe('number');
  }

  assertDateString(target.created_at, 'session.created_at');
  assertDateString(target.last_active_at, 'session.last_active_at');
}

export function buildLegacyProbeCases(args: {
  studentId: string;
  schoolId: string;
  offerId: string;
}): LegacyProbeCase[] {
  return [
    {
      name: 'seed duplicated prefix',
      method: 'POST',
      path: '/api/api/seed/schools',
    },
    {
      name: 'offers duplicated prefix',
      method: 'PUT',
      path: `/api/offers/offers/${args.offerId}`,
      body: { status: 'admitted' },
    },
    {
      name: 'reports duplicated prefix',
      method: 'GET',
      path: '/api/reports/reports/not-a-uuid',
    },
    {
      name: 'tasks duplicated prefix',
      method: 'GET',
      path: '/api/tasks/tasks/legacy-probe-task',
    },
    {
      name: 'tasks result duplicated prefix',
      method: 'GET',
      path: '/api/tasks/tasks/legacy-probe-task/result',
    },
    {
      name: 'schools duplicated prefix',
      method: 'GET',
      path: `/api/schools/schools/${args.schoolId}`,
    },
    {
      name: 'evaluations duplicated prefix',
      method: 'GET',
      path: `/api/evaluations/evaluations/students/${args.studentId}/tiers`,
    },
    {
      name: 'sessions duplicated prefix',
      method: 'GET',
      path: `/api/sessions/sessions/student/${args.studentId}`,
    },
    {
      name: 'legacy chat history removed',
      method: 'GET',
      path: '/api/chat/history/legacy-probe-session',
    },
    {
      name: 'legacy chat websocket route removed',
      method: 'GET',
      path: '/api/chat/chat/legacy-probe-session',
    },
  ];
}

export async function requestCase(
  request: APIRequestContext,
  probeCase: LegacyProbeCase,
): Promise<number> {
  if (probeCase.method === 'GET') {
    return (await request.get(probeCase.path)).status();
  }
  if (probeCase.method === 'POST') {
    return (await request.post(probeCase.path, probeCase.body ? { data: probeCase.body } : undefined)).status();
  }
  return (await request.put(probeCase.path, probeCase.body ? { data: probeCase.body } : undefined)).status();
}

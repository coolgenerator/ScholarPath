import { expect, test } from '@playwright/test';

import {
  assertEvaluationsContract,
  assertHistoryContract,
  assertOffersContract,
  assertSchoolsContract,
  seedDemoData,
} from './helpers/liveApiHelpers';

test('@live-contract-audit API field-level contract audit for schools/evaluations/offers/history', async ({ request }) => {
  const { studentId } = await seedDemoData(request);

  const schoolsResponse = await request.get('/api/schools/?per_page=1');
  expect(schoolsResponse.ok(), 'schools endpoint should return 2xx').toBeTruthy();
  const schoolId = assertSchoolsContract(await schoolsResponse.json());
  expect(schoolId.length).toBeGreaterThan(0);

  const tiersResponse = await request.get(`/api/evaluations/students/${studentId}/tiers`);
  expect(tiersResponse.ok(), 'evaluations tiers endpoint should return 2xx').toBeTruthy();
  assertEvaluationsContract(await tiersResponse.json());

  const offersResponse = await request.get(`/api/offers/students/${studentId}/offers`);
  expect(offersResponse.ok(), 'offers endpoint should return 2xx').toBeTruthy();
  const offerId = assertOffersContract(await offersResponse.json());
  expect(offerId.length).toBeGreaterThan(0);

  const sessionId = `live-contract-audit-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  const createSessionResponse = await request.post('/api/sessions/', {
    data: {
      student_id: studentId,
      session_id: sessionId,
      title: 'Live Contract Audit Session',
    },
  });
  expect(createSessionResponse.status(), 'session create should return 201').toBe(201);

  const historyResponse = await request.get(`/api/sessions/student/${studentId}`);
  expect(historyResponse.ok(), 'session history endpoint should return 2xx').toBeTruthy();
  assertHistoryContract(await historyResponse.json(), sessionId);
});

import { expect, test } from '@playwright/test';

import {
  assertOffersContract,
  assertSchoolsContract,
  buildLegacyProbeCases,
  requestCase,
  seedDemoData,
} from './helpers/liveApiHelpers';

test('@legacy-probe duplicated legacy paths must return 404', async ({ request }) => {
  const { studentId } = await seedDemoData(request);

  const schoolsResponse = await request.get('/api/schools/?per_page=1');
  expect(schoolsResponse.ok(), 'schools endpoint should return 2xx').toBeTruthy();
  const schoolId = assertSchoolsContract(await schoolsResponse.json());

  const offersResponse = await request.get(`/api/offers/students/${studentId}/offers`);
  expect(offersResponse.ok(), 'offers endpoint should return 2xx').toBeTruthy();
  const offerId = assertOffersContract(await offersResponse.json());

  const probeCases = buildLegacyProbeCases({
    studentId,
    schoolId,
    offerId,
  });

  for (const probeCase of probeCases) {
    const statusCode = await requestCase(request, probeCase);
    expect(statusCode, `${probeCase.name} should be hard-cut 404`).toBe(404);
  }
});

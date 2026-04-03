from __future__ import annotations

from fastapi import APIRouter

from scholarpath.api.routes.chat import router as chat_router
from scholarpath.api.routes.evaluations import router as evaluations_router
from scholarpath.api.routes.offers import router as offers_router
from scholarpath.api.routes.reports import router as reports_router
from scholarpath.api.routes.schools import router as schools_router
from scholarpath.api.routes.simulations import router as simulations_router
from scholarpath.api.routes.students import router as students_router
from scholarpath.api.routes.seed import router as seed_router
from scholarpath.api.routes.tasks import router as tasks_router
from scholarpath.api.routes.vectors import router as vectors_router
from scholarpath.api.routes.usage import router as usage_router
from scholarpath.api.routes.sessions import router as sessions_router
from scholarpath.api.routes.causal import router as causal_router
from scholarpath.api.routes.causal_data import router as causal_data_router
from scholarpath.api.routes.enrich import router as enrich_router

router = APIRouter()

router.include_router(students_router)
router.include_router(schools_router)
router.include_router(evaluations_router)
router.include_router(offers_router)
router.include_router(simulations_router)
router.include_router(reports_router)
router.include_router(chat_router)
router.include_router(tasks_router)
router.include_router(seed_router)
router.include_router(vectors_router)
router.include_router(usage_router)
router.include_router(sessions_router)
router.include_router(causal_router)
router.include_router(causal_data_router)
router.include_router(enrich_router)

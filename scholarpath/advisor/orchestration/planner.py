"""Planner component for advisor orchestrator routing."""

from __future__ import annotations

import logging
from typing import Any

from scholarpath.advisor.contracts import AdvisorCapability, AdvisorDomain, AdvisorRequest
from scholarpath.advisor.router_policy import DOMAIN_DESCRIPTIONS
from scholarpath.advisor.router_prompt import build_single_shot_route_prompt
from scholarpath.llm.client import LLMClient

from .constants import CLASSIFIABLE_DOMAINS, ROUTABLE_DOMAINS, TRIGGER_ACTIONS
from .registry import CapabilityRegistry
from .types import IntentCandidate, RouteDecision
from .utils import (
    bound_confidence,
    compute_intent_clarity,
    conflict_group,
    contains_ambiguous_expression,
    contains_portfolio_signal,
    contains_school_or_offer_signal,
    contains_smalltalk_signal,
    fallback_common_capability,
    has_unresolved_conflict,
    is_emotional_message,
    select_primary,
    signal_domain_from_message,
    sort_and_dedupe_candidates,
)

logger = logging.getLogger(__name__)


class Planner:
    """Planner: classify domain + multi-intent and produce route decision."""

    def __init__(
        self,
        *,
        llm: LLMClient,
        registry: CapabilityRegistry,
    ) -> None:
        self._llm = llm
        self._registry = registry

    async def plan(
        self,
        *,
        request: AdvisorRequest,
        context: dict[str, Any],
        trigger: str,
    ) -> RouteDecision:
        explicit_run = trigger in TRIGGER_ACTIONS and bool(request.capability_hint)
        if explicit_run:
            requested = self._registry.get(str(request.capability_hint))
            if requested is None:
                return RouteDecision(
                    domain="common",
                    candidates=[],
                    primary=None,
                    domain_confidence=0.0,
                    capability_confidence=0.0,
                    intent_clarity=0.0,
                    unresolved_conflict=False,
                    ambiguous_expression=contains_ambiguous_expression(request.message),
                    explicit_run=True,
                    explicit_definition=None,
                    llm_calls=0,
                )
            candidate = IntentCandidate(
                capability=requested.capability_id,
                confidence=1.0,
                conflict_group=conflict_group(requested.capability_id),
                source="trigger",
            )
            return RouteDecision(
                domain=requested.domain,
                candidates=[candidate],
                primary=candidate,
                domain_confidence=1.0,
                capability_confidence=1.0,
                intent_clarity=1.0,
                unresolved_conflict=False,
                ambiguous_expression=False,
                explicit_run=True,
                explicit_definition=requested,
                llm_calls=0,
            )

        hinted_definition = self._registry.get(str(request.capability_hint or ""))
        if hinted_definition is not None and hinted_definition.domain in CLASSIFIABLE_DOMAINS:
            hinted_candidate = IntentCandidate(
                capability=hinted_definition.capability_id,
                confidence=1.0,
                conflict_group=conflict_group(hinted_definition.capability_id),
                source="hint",
            )
            return RouteDecision(
                domain=hinted_definition.domain,
                candidates=[hinted_candidate],
                primary=hinted_candidate,
                domain_confidence=1.0,
                capability_confidence=1.0,
                intent_clarity=1.0,
                unresolved_conflict=False,
                ambiguous_expression=False,
                explicit_run=False,
                explicit_definition=hinted_definition,
                llm_calls=0,
            )

        llm_calls = 0
        domain, domain_confidence, candidates, model_intent_clarity, planner_calls = await self._single_shot_route(
            message=request.message,
            context=context,
            domain_hint=request.domain_hint,
        )
        llm_calls += planner_calls

        signal_domain = signal_domain_from_message(request.message)
        if signal_domain is not None and domain == "common":
            domain = signal_domain
            domain_confidence = max(domain_confidence, 0.90)

        if domain == "common":
            candidates, model_intent_clarity = self._apply_common_heuristics(
                candidates=candidates,
                message=request.message,
                intent_clarity=model_intent_clarity,
            )
        else:
            candidates = self._inject_cross_domain_candidates(
                domain=domain,
                message=request.message,
                capability_hint=request.capability_hint,
                domain_hint=request.domain_hint,
                candidates=candidates,
            )
            if not candidates:
                fallback_cap, fallback_conf = await self._classify_single_capability(
                    domain=domain,
                    message=request.message,
                    context=context,
                )
                llm_calls += 1
                if fallback_cap is not None:
                    candidates.append(
                        IntentCandidate(
                            capability=fallback_cap,
                            confidence=fallback_conf,
                            conflict_group=conflict_group(fallback_cap),
                            source="fallback",
                        )
                    )
            candidates = sort_and_dedupe_candidates(candidates)
        primary = select_primary(candidates)
        capability_confidence = primary.confidence if primary is not None else 0.0
        unresolved_conflict = has_unresolved_conflict(candidates, primary=primary)
        ambiguous_expression = contains_ambiguous_expression(request.message)
        intent_clarity = compute_intent_clarity(
            candidates=candidates,
            ambiguous_expression=ambiguous_expression,
            has_explicit_target=bool(request.capability_hint or request.domain_hint),
            model_intent_clarity=model_intent_clarity,
        )

        return RouteDecision(
            domain=domain,
            candidates=candidates,
            primary=primary,
            domain_confidence=domain_confidence,
            capability_confidence=capability_confidence,
            intent_clarity=intent_clarity,
            unresolved_conflict=unresolved_conflict,
            ambiguous_expression=ambiguous_expression,
            explicit_run=False,
            explicit_definition=None,
            llm_calls=llm_calls,
        )

    async def _single_shot_route(
        self,
        *,
        message: str,
        context: dict[str, Any],
        domain_hint: AdvisorDomain | None,
    ) -> tuple[AdvisorDomain, float, list[IntentCandidate], float | None, int]:
        if domain_hint in CLASSIFIABLE_DOMAINS:
            hinted_domain: AdvisorDomain = domain_hint
            return hinted_domain, 1.0, [], None, 0

        domain_rows = "\n".join(f"- {k}: {DOMAIN_DESCRIPTIONS[k]}" for k in CLASSIFIABLE_DOMAINS)
        capability_rows: list[str] = []
        for domain in CLASSIFIABLE_DOMAINS:
            for definition in self._registry.list_by_domain(domain):
                if definition.domain not in {"undergrad", "offer", "common"}:
                    continue
                capability_rows.append(
                    f"- {definition.capability_id}: domain={definition.domain}; "
                    f"conflict_group={conflict_group(definition.capability_id)}; {definition.description}"
                )
        prompt = build_single_shot_route_prompt(
            domain_rows=domain_rows,
            capability_rows=capability_rows,
        )
        messages = [
            {"role": "system", "content": prompt},
            {
                "role": "user",
                "content": (
                    f"Recent context:\n{context.get('route_prompt_context', context.get('recent_messages', ''))}\n\n"
                    f"User message:\n{message}"
                ),
            },
        ]
        try:
            result = await self._llm.complete_json(
                messages,
                temperature=0.1,
                max_tokens=640,
                caller="advisor.router.plan",
            )
        except Exception:
            logger.warning("Single-shot route classification failed; fallback to split routing", exc_info=True)
            return await self._legacy_route_split(message=message, context=context)

        raw_domain = str(result.get("domain", "common")).strip().lower()
        domain_confidence = bound_confidence(result.get("domain_confidence", result.get("confidence", 0.0)))
        if raw_domain not in CLASSIFIABLE_DOMAINS:
            raw_domain = "common"
            domain_confidence = 0.0
        domain: AdvisorDomain = raw_domain  # type: ignore[assignment]

        raw_candidates = result.get("candidates", [])
        if isinstance(raw_candidates, dict):
            raw_candidates = [raw_candidates]
        if not isinstance(raw_candidates, list):
            raw_candidates = []

        candidates: list[IntentCandidate] = []
        for row in raw_candidates:
            if not isinstance(row, dict):
                continue
            raw_cap = str(row.get("capability", "")).strip()
            definition = self._registry.get(raw_cap)
            if definition is None:
                continue
            if domain == "common" and definition.domain != "common":
                continue
            if domain in ROUTABLE_DOMAINS and definition.domain not in {domain, "common"}:
                continue
            confidence = bound_confidence(row.get("confidence", 0.0))
            cap_conflict_group = str(row.get("conflict_group", "")).strip() or conflict_group(definition.capability_id)
            candidates.append(
                IntentCandidate(
                    capability=definition.capability_id,
                    confidence=confidence,
                    conflict_group=cap_conflict_group,
                    source="llm",
                )
            )

        raw_clarity = result.get("intent_clarity")
        intent_clarity = bound_confidence(raw_clarity) if raw_clarity is not None else None

        return domain, domain_confidence, sort_and_dedupe_candidates(candidates), intent_clarity, 1

    async def _legacy_route_split(
        self,
        *,
        message: str,
        context: dict[str, Any],
    ) -> tuple[AdvisorDomain, float, list[IntentCandidate], float | None, int]:
        domain, domain_confidence = await self._classify_domain(
            message=message,
            context=context,
            domain_hint=None,
        )
        llm_calls = 1
        if domain == "common":
            candidates, clarity = await self._classify_common_capability(
                message=message,
                context=context,
                capability_hint=None,
            )
            return domain, domain_confidence, candidates, clarity, llm_calls + 1

        candidates, clarity = await self._multi_intent_classify(
            domain=domain,
            message=message,
            context=context,
            capability_hint=None,
        )
        candidates = self._inject_cross_domain_candidates(
            domain=domain,
            message=message,
            capability_hint=None,
            domain_hint=None,
            candidates=candidates,
        )
        return domain, domain_confidence, candidates, clarity, llm_calls + 1

    def _apply_common_heuristics(
        self,
        *,
        candidates: list[IntentCandidate],
        message: str,
        intent_clarity: float | None,
    ) -> tuple[list[IntentCandidate], float | None]:
        out = list(candidates)

        def _upsert(capability: AdvisorCapability, confidence: float, source: str) -> None:
            for idx, row in enumerate(out):
                if row.capability != capability:
                    continue
                out[idx] = IntentCandidate(
                    capability=capability,
                    confidence=max(row.confidence, confidence),
                    conflict_group=conflict_group(capability),
                    source=row.source,
                )
                return
            out.append(
                IntentCandidate(
                    capability=capability,
                    confidence=confidence,
                    conflict_group=conflict_group(capability),
                    source=source,
                )
            )

        if is_emotional_message(message):
            _upsert("common.emotional_support", 0.90, "heuristic")
        if contains_portfolio_signal(message) or contains_smalltalk_signal(message):
            _upsert("common.general", 0.86, "heuristic")
        if contains_ambiguous_expression(message):
            _upsert("common.clarify", 0.93, "heuristic")
            intent_clarity = min(intent_clarity or 0.50, 0.50)

        if not out:
            fallback_capability, fallback_confidence = fallback_common_capability(message)
            _upsert(fallback_capability, fallback_confidence, "fallback")
            if intent_clarity is None:
                intent_clarity = 0.75 if fallback_capability != "common.clarify" else 0.50

        sorted_candidates = sort_and_dedupe_candidates(out)
        if intent_clarity is None:
            top = sorted_candidates[0].capability
            intent_clarity = 0.75 if top != "common.clarify" else 0.50
        return sorted_candidates, intent_clarity

    async def _classify_domain(
        self,
        *,
        message: str,
        context: dict[str, Any],
        domain_hint: AdvisorDomain | None,
    ) -> tuple[AdvisorDomain, float]:
        if domain_hint in CLASSIFIABLE_DOMAINS:
            return domain_hint, 1.0

        prompt = (
            "Classify the user turn into one domain.\\n\\n"
            "Domains:\\n"
            + "\\n".join(f"- {k}: {DOMAIN_DESCRIPTIONS[k]}" for k in CLASSIFIABLE_DOMAINS)
            + "\\n\\nReturn JSON: {\"domain\": \"...\", \"confidence\": 0.0-1.0}"
        )
        messages = [
            {"role": "system", "content": prompt},
            {
                "role": "user",
                "content": (
                    f"Recent context:\\n{context.get('route_prompt_context', context.get('recent_messages', ''))}\\n\\n"
                    f"User message:\\n{message}"
                ),
            },
        ]
        try:
            result = await self._llm.complete_json(
                messages,
                temperature=0.1,
                max_tokens=256,
                caller="advisor.router.domain",
            )
        except Exception:
            logger.warning("Domain classification failed; fallback to common", exc_info=True)
            return "common", 0.0

        raw_domain = str(result.get("domain", "common")).strip().lower()
        confidence = bound_confidence(result.get("confidence", 0.0))
        if raw_domain not in CLASSIFIABLE_DOMAINS:
            return "common", 0.0
        return raw_domain, confidence  # type: ignore[return-value]

    async def _multi_intent_classify(
        self,
        *,
        domain: AdvisorDomain,
        message: str,
        context: dict[str, Any],
        capability_hint: str | None,
    ) -> tuple[list[IntentCandidate], float | None]:
        if domain not in ROUTABLE_DOMAINS:
            return [], None

        candidates: list[IntentCandidate] = []
        if capability_hint:
            hinted = self._registry.get(capability_hint)
            if hinted and hinted.domain == domain:
                candidates.append(
                    IntentCandidate(
                        capability=hinted.capability_id,
                        confidence=1.0,
                        conflict_group=conflict_group(hinted.capability_id),
                        source="hint",
                    )
                )

        domain_caps = self._registry.list_by_domain(domain)
        if not domain_caps:
            return candidates, None

        rows = "\\n".join(
            f"- {d.capability_id}: {d.description}; conflict_group={conflict_group(d.capability_id)}"
            for d in domain_caps
        )
        messages = [
            {
                "role": "system",
                "content": (
                    "Extract multi-intent candidates from one message.\\n"
                    f"Domain: {domain}\\n"
                    "Return top capabilities sorted by importance/confidence.\\n"
                    f"Candidates:\\n{rows}\\n\\n"
                    "Return JSON: {\"candidates\": [{\"capability\": \"...\", \"confidence\": 0.0-1.0, "
                    "\"conflict_group\": \"...\"}], \"intent_clarity\": 0.0-1.0}"
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Recent context:\\n{context.get('route_prompt_context', context.get('recent_messages', ''))}\\n\\n"
                    f"User message:\\n{message}"
                ),
            },
        ]

        model_intent_clarity: float | None = None
        try:
            result = await self._llm.complete_json(
                messages,
                temperature=0.1,
                max_tokens=512,
                caller="advisor.router.multi_intent",
            )
            raw_candidates = result.get("candidates", [])
            raw_clarity = result.get("intent_clarity")
            if raw_clarity is not None:
                model_intent_clarity = bound_confidence(raw_clarity)
        except Exception:
            logger.warning("Multi-intent classification failed; fallback single", exc_info=True)
            raw_candidates = []
            result = {}

        if isinstance(raw_candidates, dict):
            raw_candidates = [raw_candidates]
        if not isinstance(raw_candidates, list):
            raw_candidates = []

        if not raw_candidates and isinstance(result, dict):
            single_capability = str(result.get("capability", "")).strip()
            if single_capability:
                raw_candidates = [
                    {
                        "capability": single_capability,
                        "confidence": result.get("confidence", 0.0),
                        "conflict_group": result.get("conflict_group", ""),
                    }
                ]

        for row in raw_candidates:
            if not isinstance(row, dict):
                continue
            raw_cap = str(row.get("capability", "")).strip()
            definition = self._registry.get(raw_cap)
            if definition is None or definition.domain != domain:
                continue
            confidence = bound_confidence(row.get("confidence", 0.0))
            cap_conflict_group = str(row.get("conflict_group", "")).strip() or conflict_group(definition.capability_id)
            candidates.append(
                IntentCandidate(
                    capability=definition.capability_id,
                    confidence=confidence,
                    conflict_group=cap_conflict_group,
                    source="llm",
                )
            )

        if not candidates:
            fallback_cap, fallback_conf = await self._classify_single_capability(
                domain=domain,
                message=message,
                context=context,
            )
            if fallback_cap:
                candidates.append(
                    IntentCandidate(
                        capability=fallback_cap,
                        confidence=fallback_conf,
                        conflict_group=conflict_group(fallback_cap),
                        source="fallback",
                    )
                )

        return sort_and_dedupe_candidates(candidates), model_intent_clarity

    async def _classify_single_capability(
        self,
        *,
        domain: AdvisorDomain,
        message: str,
        context: dict[str, Any],
    ) -> tuple[AdvisorCapability | None, float]:
        candidates = self._registry.list_by_domain(domain)
        if not candidates:
            return None, 0.0

        candidate_rows = "\\n".join(f"- {c.capability_id}: {c.description}" for c in candidates)
        messages = [
            {
                "role": "system",
                "content": (
                    "Pick exactly one capability ID for the current turn.\\n"
                    f"Domain: {domain}\\n"
                    f"Candidates:\\n{candidate_rows}\\n\\n"
                    "Return JSON: {\"capability\": \"<candidate_id>\", \"confidence\": 0.0-1.0}"
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Recent context:\\n{context.get('route_prompt_context', context.get('recent_messages', ''))}\\n\\n"
                    f"User message:\\n{message}"
                ),
            },
        ]
        try:
            result = await self._llm.complete_json(
                messages,
                temperature=0.1,
                max_tokens=256,
                caller="advisor.router.capability",
            )
        except Exception:
            return None, 0.0
        raw = str(result.get("capability", "")).strip()
        confidence = bound_confidence(result.get("confidence", 0.0))
        definition = self._registry.get(raw)
        if definition is None or definition.domain != domain:
            return None, 0.0
        return definition.capability_id, confidence

    async def _classify_common_capability(
        self,
        *,
        message: str,
        context: dict[str, Any],
        capability_hint: str | None,
    ) -> tuple[list[IntentCandidate], float | None]:
        if capability_hint:
            hinted = self._registry.get(capability_hint)
            if hinted is not None and hinted.domain == "common":
                return (
                    [
                        IntentCandidate(
                            capability=hinted.capability_id,
                            confidence=1.0,
                            conflict_group=conflict_group(hinted.capability_id),
                            source="hint",
                        )
                    ],
                    1.0,
                )

        def _upsert_candidate(
            rows: list[IntentCandidate],
            *,
            capability: AdvisorCapability,
            confidence: float,
            source: str,
        ) -> None:
            for idx, row in enumerate(rows):
                if row.capability != capability:
                    continue
                rows[idx] = IntentCandidate(
                    capability=capability,
                    confidence=max(row.confidence, confidence),
                    conflict_group=conflict_group(capability),
                    source=row.source,
                )
                return
            rows.append(
                IntentCandidate(
                    capability=capability,
                    confidence=confidence,
                    conflict_group=conflict_group(capability),
                    source=source,
                )
            )

        messages = [
            {
                "role": "system",
                "content": (
                    "Extract common-capability intents from the user turn.\\n"
                    "Return ordered candidates by confidence and importance.\\n"
                    "Candidates:\\n"
                    "- common.general: casual chat, portfolio filling guidance, non-school Q&A\\n"
                    "- common.emotional_support: stress, anxiety, emotional support\\n"
                    "- common.clarify: user intent is highly unclear and needs routing clarification\\n\\n"
                    "Return JSON: {\"candidates\": [{\"capability\": \"...\", \"confidence\": 0.0-1.0, "
                    "\"conflict_group\": \"...\"}], \"intent_clarity\": 0.0-1.0}. "
                    "If only one intent exists, still return one-row candidates."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Recent context:\\n{context.get('route_prompt_context', context.get('recent_messages', ''))}\\n\\n"
                    f"User message:\\n{message}"
                ),
            },
        ]
        candidates: list[IntentCandidate] = []
        intent_clarity: float | None = None
        try:
            result = await self._llm.complete_json(
                messages,
                temperature=0.1,
                max_tokens=256,
                caller="advisor.router.common_capability",
            )
            raw_candidates = result.get("candidates", [])
            if isinstance(raw_candidates, dict):
                raw_candidates = [raw_candidates]
            if not isinstance(raw_candidates, list):
                raw_candidates = []
            if not raw_candidates:
                single_capability = str(result.get("capability", "")).strip()
                if single_capability:
                    raw_candidates = [
                        {
                            "capability": single_capability,
                            "confidence": result.get("confidence", 0.0),
                            "conflict_group": result.get("conflict_group", ""),
                        }
                    ]
            for row in raw_candidates:
                if not isinstance(row, dict):
                    continue
                raw_cap = str(row.get("capability", "")).strip()
                definition = self._registry.get(raw_cap)
                if definition is None or definition.domain != "common":
                    continue
                confidence = bound_confidence(row.get("confidence", 0.0))
                cap_conflict_group = str(row.get("conflict_group", "")).strip() or conflict_group(definition.capability_id)
                candidates.append(
                    IntentCandidate(
                        capability=definition.capability_id,
                        confidence=confidence,
                        conflict_group=cap_conflict_group,
                        source="common",
                    )
                )
            raw_clarity = result.get("intent_clarity")
            if raw_clarity is not None:
                intent_clarity = bound_confidence(raw_clarity)
        except Exception:
            logger.warning("Common capability classification failed; fallback heuristic", exc_info=True)

        emotional_signal = is_emotional_message(message)
        portfolio_signal = contains_portfolio_signal(message)
        ambiguous_signal = contains_ambiguous_expression(message)

        if emotional_signal:
            _upsert_candidate(
                candidates,
                capability="common.emotional_support",
                confidence=0.90,
                source="heuristic",
            )
        if portfolio_signal:
            _upsert_candidate(
                candidates,
                capability="common.general",
                confidence=0.86,
                source="heuristic",
            )
        if ambiguous_signal:
            _upsert_candidate(
                candidates,
                capability="common.clarify",
                confidence=0.93,
                source="heuristic",
            )
            intent_clarity = min(intent_clarity or 0.50, 0.50)

        if not candidates:
            fallback_capability, fallback_confidence = fallback_common_capability(message)
            _upsert_candidate(
                candidates,
                capability=fallback_capability,
                confidence=fallback_confidence,
                source="fallback",
            )
            if intent_clarity is None:
                intent_clarity = 0.75 if fallback_capability != "common.clarify" else 0.50

        sorted_candidates = sort_and_dedupe_candidates(candidates)
        if intent_clarity is None:
            top = sorted_candidates[0].capability
            intent_clarity = 0.75 if top != "common.clarify" else 0.50

        return sorted_candidates, intent_clarity

    def _inject_cross_domain_candidates(
        self,
        *,
        domain: AdvisorDomain,
        message: str,
        capability_hint: str | None,
        domain_hint: AdvisorDomain | None,
        candidates: list[IntentCandidate],
    ) -> list[IntentCandidate]:
        """Add non-primary common intents for mixed requests in undergrad/offer turns."""
        if domain not in ROUTABLE_DOMAINS:
            return sort_and_dedupe_candidates(candidates)
        if capability_hint:
            return sort_and_dedupe_candidates(candidates)
        if domain_hint == "common":
            return sort_and_dedupe_candidates(candidates)

        out = list(candidates)
        has_emotional = is_emotional_message(message)
        has_portfolio = contains_portfolio_signal(message)
        has_chat = contains_smalltalk_signal(message)
        has_strong_school_offer_signal = contains_school_or_offer_signal(message)

        if has_emotional:
            out.append(
                IntentCandidate(
                    capability="common.emotional_support",
                    confidence=0.78,
                    conflict_group=conflict_group("common.emotional_support"),
                    source="cross_domain",
                )
            )
        if has_portfolio:
            out.append(
                IntentCandidate(
                    capability="common.general",
                    confidence=0.78,
                    conflict_group=conflict_group("common.general"),
                    source="cross_domain",
                )
            )
        elif has_chat and has_strong_school_offer_signal:
            out.append(
                IntentCandidate(
                    capability="common.general",
                    confidence=0.70,
                    conflict_group=conflict_group("common.general"),
                    source="cross_domain",
                )
            )
        return sort_and_dedupe_candidates(out)

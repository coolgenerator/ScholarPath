from __future__ import annotations

from scholarpath.scripts.admission_truth_public_shared import (
    MetricsSchoolContext,
    load_ranked_school_allowlist,
    normalise_school_key,
    resolve_school_alias,
    resolve_school_name_for_ingest,
)


def test_resolve_school_alias_high_frequency_keys():
    school_name, changed = resolve_school_alias("umich")
    assert school_name == "University of Michigan, Ann Arbor"
    assert changed is True

    school_name2, changed2 = resolve_school_alias("Georgia Institute of Technology")
    assert school_name2 == "Georgia Institute of Technology"
    assert changed2 is False


def test_resolve_school_name_for_ingest_prefers_context_name():
    ctx = MetricsSchoolContext(
        covered_school_ids={"school-1"},
        covered_name_index={normalise_school_key("University of North Carolina at Chapel Hill"): "University of North Carolina at Chapel Hill"},
        all_name_index={normalise_school_key("University of North Carolina at Chapel Hill"): "University of North Carolina at Chapel Hill"},
    )
    school_name, changed = resolve_school_name_for_ingest("UNC", context=ctx)
    assert school_name == "University of North Carolina at Chapel Hill"
    assert changed is True


def test_load_ranked_school_allowlist_contains_expected_aliases():
    ctx = load_ranked_school_allowlist()
    assert ctx.version
    assert len(ctx.allowed_keys) >= 140
    assert ctx.alias_map.get(normalise_school_key("mit")) == "Massachusetts Institute of Technology"
    assert normalise_school_key("Williams College") in ctx.allowed_keys

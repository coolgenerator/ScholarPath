from __future__ import annotations

from scholarpath.scripts.admission_truth_public_reddit import (
    _build_parser,
    _detect_stage_heading,
    _is_noisy_autocreated_school_name,
    _looks_like_school_name,
    _split_school_candidates,
    parse_decisions,
    parse_profile,
)


def test_detect_stage_heading_with_inline_payload():
    stage, payload = _detect_stage_heading("Acceptances: MIT, Stanford, UC Berkeley")
    assert stage == "admit"
    assert "MIT" in payload


def test_split_school_candidates_removes_notes():
    schools = _split_school_candidates("MIT (EA) - likely commit; Stanford, Harvard")
    assert "MIT" in schools
    assert "Stanford" in schools
    assert "Harvard" in schools


def test_parse_decisions_from_sectioned_markdown():
    text = """
    **Acceptances**
    - MIT
    - Stanford University (REA deferred then accepted)

    **Waitlists**
    - Carnegie Mellon University

    **Rejections**
    - Harvard
    - Princeton

    **Academics**
    GPA: 4.0
    """
    rows = parse_decisions(text)
    by_stage = {}
    for row in rows:
        by_stage.setdefault(row.stage, set()).add(row.school_name)
    assert "MIT" in by_stage.get("admit", set())
    assert "Stanford University" in by_stage.get("admit", set())
    assert "Carnegie Mellon University" in by_stage.get("waitlist", set())
    assert "Harvard" in by_stage.get("reject", set())
    assert "Princeton" in by_stage.get("reject", set())


def test_parse_profile_extracts_scores_and_budget_flags():
    text = """
    Demographics:
    Income Bracket: low-income

    Academics:
    GPA (UW/W): 3.92/4.61
    SAT: 1540
    ACT: 35

    Intended Major(s): Computer Science, Math
    """
    profile = parse_profile(text)
    assert profile.gpa > 0
    assert profile.sat_total == 1540
    assert profile.act_composite == 35
    assert profile.need_financial_aid is True
    assert profile.budget_usd <= 30000
    assert any("Computer" in item for item in profile.intended_majors)


def test_parse_decisions_ignores_narrative_under_stage():
    text = """
    Acceptances:
    - Duke University
    Super excited about Duke! I thought it was over after Ivy day.
    """
    rows = parse_decisions(text)
    assert len(rows) == 1
    assert rows[0].school_name == "Duke University"


def test_parse_decisions_from_freeform_sentences():
    text = """
    I got accepted to University of Florida.
    Got rejected from Duke University.
    Waitlisted at Carnegie Mellon University.
    """
    rows = parse_decisions(text)
    by_stage = {}
    for row in rows:
        by_stage.setdefault(row.stage, set()).add(row.school_name)
    assert "University of Florida" in by_stage.get("admit", set())
    assert "Duke University" in by_stage.get("reject", set())
    assert "Carnegie Mellon University" in by_stage.get("waitlist", set())


def test_parse_decisions_freeform_ignores_question_sentences():
    text = "Do you think I can get accepted to Stanford University?"
    rows = parse_decisions(text)
    assert rows == []


def test_looks_like_school_name_rejects_noise_fragments():
    assert _looks_like_school_name("EA Penn State University Park + $60k") is False
    assert _looks_like_school_name("I got cooked because of my junior grades") is False
    assert _looks_like_school_name("Rice reject") is False
    assert _looks_like_school_name("MIT") is True


def test_is_noisy_autocreated_school_name_rejects_legacy_artifacts():
    assert _is_noisy_autocreated_school_name("\\- Pitt Honors College") is True
    assert _is_noisy_autocreated_school_name("UVA reject") is True


def test_parser_defaults_ranked_allowlist_enabled():
    parser = _build_parser()
    args = parser.parse_args([])
    assert args.use_ranked_allowlist is True
    assert args.only_metrics_schools is True

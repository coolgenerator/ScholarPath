from __future__ import annotations

from scholarpath.scripts.admission_truth_public_collegeconfidential import (
    _build_parser,
    _build_topic_url,
    _clean_cc_html,
    _extract_school_from_topic_title,
    _parse_decisions_cc,
    _parse_iso_datetime,
)


def test_clean_cc_html_normalizes_lists_and_text() -> None:
    raw = "<p><strong>Acceptances</strong></p><ul><li>MIT</li><li>Stanford University</li></ul>"
    text = _clean_cc_html(raw)
    assert "Acceptances" in text
    assert "- MIT" in text
    assert "- Stanford University" in text


def test_parse_iso_datetime_handles_z_suffix() -> None:
    dt = _parse_iso_datetime("2026-04-05T02:03:04.000Z")
    assert dt.year == 2026
    assert dt.month == 4
    assert dt.day == 5
    assert dt.tzinfo is not None


def test_build_topic_url_prefers_slug() -> None:
    url = _build_topic_url("https://talk.collegeconfidential.com", 12345, "sample-thread")
    assert url == "https://talk.collegeconfidential.com/t/sample-thread/12345"


def test_parse_decisions_cc_supports_freeform_sentences() -> None:
    text = "I got accepted to University of Michigan and was rejected from Harvard."
    rows = _parse_decisions_cc(text)
    keys = {(row.stage, row.school_name) for row in rows}
    assert ("admit", "University of Michigan") in keys
    assert ("reject", "Harvard") in keys


def test_extract_school_from_topic_title_handles_official_thread() -> None:
    school = _extract_school_from_topic_title("Texas A&M Class of 2030 Official Thread")
    assert school == "Texas A&M"


def test_parse_decisions_cc_uses_topic_title_fallback() -> None:
    rows = _parse_decisions_cc(
        "My daughter was accepted yesterday!",
        topic_title="Case Western Early Action Class of 2030 Official Thread",
    )
    keys = {(row.stage, row.school_name) for row in rows}
    assert ("admit", "Case Western") in keys


def test_parser_defaults_ranked_allowlist_enabled() -> None:
    parser = _build_parser()
    args = parser.parse_args([])
    assert args.use_ranked_allowlist is True
    assert args.only_metrics_schools is True

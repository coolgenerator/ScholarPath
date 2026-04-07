from scholarpath.scripts.admission_truth_public_cleanup_global import (
    _build_parser,
    _match_enhanced_noisy_reason,
    _match_noisy_reason,
)


def test_enhanced_noisy_reason_hits_generic_placeholders() -> None:
    assert _match_enhanced_noisy_reason("every T20") == "generic_exact_phrase"
    assert _match_enhanced_noisy_reason("top schools") == "generic_exact_phrase"
    assert _match_enhanced_noisy_reason("Waiting on") == "generic_exact_phrase"
    assert (
        _match_enhanced_noisy_reason("Parents of the HS")
        == "generic_tokens_without_institution_keyword"
    )


def test_enhanced_noisy_reason_skips_real_school_names() -> None:
    assert _match_enhanced_noisy_reason("University of California, Berkeley") is None
    assert _match_enhanced_noisy_reason("Georgia Institute of Technology") is None
    assert _match_enhanced_noisy_reason("The New School") is None


def test_match_noisy_reason_respects_enhanced_toggle() -> None:
    # Base noisy matcher should not treat generic phrase as noisy today.
    assert _match_noisy_reason("every T20", enhanced_rules=False) is None
    # Enhanced mode should catch it.
    assert _match_noisy_reason("every T20", enhanced_rules=True) == "generic_exact_phrase"

    # Base parser noisy pattern still wins regardless of toggle.
    assert _match_noisy_reason("https://example.com", enhanced_rules=False) == "base_noisy_rule"
    assert _match_noisy_reason("https://example.com", enhanced_rules=True) == "base_noisy_rule"


def test_parser_accepts_new_flags() -> None:
    parser = _build_parser()
    args = parser.parse_args(["--dry-run", "--enhanced-rules"])
    assert args.dry_run is True
    assert args.enhanced_rules is True

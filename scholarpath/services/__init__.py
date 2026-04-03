"""ScholarPath service layer."""

from scholarpath.services.student_service import (
    check_profile_completeness,
    create_student,
    delete_student,
    get_student,
    update_student,
)
from scholarpath.services.portfolio_service import (
    apply_portfolio_patch,
    canonicalize_preferences,
    get_portfolio,
    get_student_canonical_preferences,
    get_student_sat_equivalent,
)
from scholarpath.services.school_service import (
    generate_school_list,
    get_school_detail,
    search_schools,
)
from scholarpath.services.evaluation_service import (
    evaluate_school_fit,
    generate_strategy,
    get_tiered_list,
)
from scholarpath.services.offer_service import (
    compare_offers,
    create_offer,
    list_offers,
    update_offer,
)
from scholarpath.services.simulation_service import (
    compare_scenarios,
    run_what_if,
)
from scholarpath.services.report_service import generate_go_no_go

__all__ = [
    # Student
    "create_student",
    "get_student",
    "update_student",
    "delete_student",
    "check_profile_completeness",
    "get_portfolio",
    "apply_portfolio_patch",
    "canonicalize_preferences",
    "get_student_canonical_preferences",
    "get_student_sat_equivalent",
    # School
    "search_schools",
    "get_school_detail",
    "generate_school_list",
    # Evaluation
    "evaluate_school_fit",
    "get_tiered_list",
    "generate_strategy",
    # Offer
    "create_offer",
    "list_offers",
    "update_offer",
    "compare_offers",
    # Simulation
    "run_what_if",
    "compare_scenarios",
    # Report
    "generate_go_no_go",
]

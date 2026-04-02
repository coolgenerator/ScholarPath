export interface StudentCreate {
  name: string;
  gpa: number;
  gpa_scale: string;
  sat_total?: number | null;
  act_composite?: number | null;
  toefl_total?: number | null;
  curriculum_type: string;
  ap_courses?: string[] | null;
  extracurriculars?: unknown[] | Record<string, unknown> | null;
  awards?: unknown[] | Record<string, unknown> | null;
  intended_majors: string[];
  budget_usd?: number | null;
  need_financial_aid?: boolean | null;
  preferences?: Record<string, unknown> | null;
  ed_preference?: string | null;
  target_year: number;
}

export interface StudentResponse {
  id: string;
  created_at: string;
  name: string;
  gpa: number;
  gpa_scale: string;
  sat_total?: number | null;
  act_composite?: number | null;
  toefl_total?: number | null;
  curriculum_type: string;
  ap_courses?: string[] | null;
  extracurriculars?: unknown[] | Record<string, unknown> | null;
  awards?: unknown[] | Record<string, unknown> | null;
  intended_majors?: string[] | null;
  budget_usd: number;
  need_financial_aid: boolean;
  preferences?: Record<string, unknown> | null;
  ed_preference?: string | null;
  target_year: number;
  profile_completed: boolean;
}

export type StudentUpdate = Partial<StudentCreate>;

export interface ProgramResponse {
  id: string;
  school_id: string;
  name: string;
  department: string;
  us_news_rank?: number | null;
  avg_class_size?: number | null;
  has_research_opps: boolean;
  has_coop: boolean;
  description?: string | null;
}

export interface SchoolResponse {
  id: string;
  created_at: string;
  name: string;
  name_cn?: string | null;
  city: string;
  state: string;
  school_type: string;
  size_category: string;
  us_news_rank?: number | null;
  acceptance_rate?: number | null;
  sat_25?: number | null;
  sat_75?: number | null;
  act_25?: number | null;
  act_75?: number | null;
  tuition_oos?: number | null;
  avg_net_price?: number | null;
  intl_student_pct?: number | null;
  student_faculty_ratio?: number | null;
  graduation_rate_4yr?: number | null;
  endowment_per_student?: number | null;
  campus_setting?: string | null;
  website_url?: string | null;
  programs?: ProgramResponse[];
}

export interface SchoolListResponse {
  items: SchoolResponse[];
  total: number;
  page: number;
  per_page: number;
}

export interface EvaluationResponse {
  id: string;
  created_at: string;
  student_id: string;
  school_id: string;
  tier: string;
  academic_fit: number;
  financial_fit: number;
  career_fit: number;
  life_fit: number;
  overall_score: number;
  admission_probability?: number | null;
  ed_ea_recommendation?: string | null;
  reasoning: string;
  fit_details?: Record<string, unknown> | null;
  causal_engine_version?: string | null;
  causal_model_version?: string | null;
  estimate_confidence?: number | null;
  label_type?: string | null;
  fallback_used?: boolean | null;
}

export interface TieredSchoolList {
  reach: EvaluationResponse[];
  target: EvaluationResponse[];
  safety: EvaluationResponse[];
  likely: EvaluationResponse[];
}

export interface EvaluationWithSchool extends EvaluationResponse {
  school?: SchoolResponse;
}

export interface OfferCreate {
  school_id: string;
  status: string;
  tuition?: number | null;
  room_and_board?: number | null;
  books_supplies?: number | null;
  personal_expenses?: number | null;
  transportation?: number | null;
  merit_scholarship?: number;
  need_based_grant?: number;
  loan_offered?: number;
  work_study?: number;
  honors_program?: boolean;
  conditions?: string | null;
  decision_deadline?: string | null;
  notes?: string | null;
}

export type OfferUpdate = Partial<OfferCreate>;

export interface OfferResponse {
  id: string;
  created_at: string;
  student_id: string;
  school_id: string;
  school_name?: string | null;
  status: string;
  tuition?: number | null;
  room_and_board?: number | null;
  books_supplies?: number | null;
  personal_expenses?: number | null;
  transportation?: number | null;
  total_cost?: number | null;
  merit_scholarship: number;
  need_based_grant: number;
  loan_offered: number;
  work_study: number;
  total_aid: number;
  net_cost?: number | null;
  honors_program: boolean;
  conditions?: string | null;
  decision_deadline?: string | null;
  notes?: string | null;
}

export interface OfferComparisonResponse {
  offers: OfferResponse[];
  comparison_scores: Array<Record<string, unknown>>;
  causal_comparison_matrix: Record<string, Record<string, unknown>>;
  recommendation?: string | null;
  causal_engine_version?: string | null;
  causal_model_version?: string | null;
  estimate_confidence?: number | null;
  fallback_used?: boolean | null;
}

export interface WhatIfResponse {
  original_scores: Record<string, number>;
  modified_scores: Record<string, number>;
  deltas: Record<string, number>;
  explanation: string;
  causal_engine_version?: string | null;
  causal_model_version?: string | null;
  estimate_confidence?: number | null;
  label_type?: string | null;
  fallback_used?: boolean | null;
  fallback_reason?: string | null;
}

export interface GoNoGoReport {
  id: string;
  created_at: string;
  student_id: string;
  offer_id: string;
  overall_score: number;
  ci_lower?: number;
  ci_upper?: number;
  confidence_lower?: number;
  confidence_upper?: number;
  sub_scores: Record<string, number>;
  recommendation: string;
  top_factors: unknown[];
  risks: unknown[];
  narrative: string;
  what_if_results?: Record<string, unknown> | null;
  causal_engine_version?: string | null;
  causal_model_version?: string | null;
  estimate_confidence?: number | null;
  label_type?: string | null;
  fallback_used?: boolean | null;
}

export interface TaskStatus {
  task_id: string;
  status: string;
  error?: string;
  result_ready?: boolean;
}

export interface ChatSessionResponse {
  id: string;
  student_id: string;
  session_id: string;
  title: string;
  preview?: string | null;
  message_count: number;
  school_count: number;
  created_at: string;
  last_active_at: string;
}

export interface QuestionOption {
  label: string;
  value: string;
  icon?: string | null;
}

export interface GuidedQuestion {
  id: string;
  title: string;
  description?: string | null;
  options: QuestionOption[];
  allow_custom: boolean;
  custom_placeholder: string;
  multi_select: boolean;
}

export interface RecommendedSchool {
  school_name: string;
  school_name_cn?: string | null;
  tier: string;
  rank?: number | null;
  overall_score: number;
  admission_probability: number;
  acceptance_rate?: number | null;
  net_price?: number | null;
  key_reasons: string[];
  sub_scores: Record<string, number>;
}

export interface RecommendationData {
  narrative: string;
  schools: RecommendedSchool[];
  ed_recommendation?: string | null;
  ea_recommendations: string[];
  strategy_summary?: string | null;
}

export type AdvisorDomain = 'undergrad' | 'offer' | 'graduate' | 'summer' | 'common';

export type AdvisorCapability =
  | 'undergrad.profile.intake'
  | 'undergrad.school.recommend'
  | 'undergrad.school.query'
  | 'undergrad.strategy.plan'
  | 'offer.compare'
  | 'offer.decision'
  | 'offer.what_if'
  | 'graduate.program.recommend'
  | 'summer.program.recommend'
  | 'common.general'
  | 'common.emotional_support'
  | 'common.clarify';

export type AdvisorErrorCode =
  | 'ROUTE_LOW_CONFIDENCE'
  | 'INVALID_INPUT'
  | 'CAPABILITY_FAILED'
  | 'DEPENDENCY_UNAVAILABLE';

export type AdvisorStepStatus = 'succeeded' | 'degraded' | 'failed';
export type PendingReason =
  | 'over_limit'
  | 'conflict'
  | 'low_confidence'
  | 'requires_user_trigger'
  | 'dependency_wait';

export type AdvisorGuardResult = 'pass' | 'clarify' | 'invalid_input';
export type AdvisorGuardReason =
  | 'low_confidence'
  | 'conflict'
  | 'invalid_input'
  | 'trigger_invalid'
  | 'none';

export interface GuidedIntakeArtifact {
  type: 'guided_intake';
  questions: GuidedQuestion[];
}

export interface SchoolRecommendationArtifact {
  type: 'school_recommendation';
  data: RecommendationData;
}

export interface OfferComparisonArtifact {
  type: 'offer_comparison';
  offers: Array<Record<string, unknown>>;
  comparison_matrix: Record<string, Record<string, unknown>>;
  recommendation?: string | null;
}

export interface StrategyPlanArtifact {
  type: 'strategy_plan';
  strategy: Record<string, unknown>;
}

export interface WhatIfResultArtifact {
  type: 'what_if_result';
  interventions: Record<string, number>;
  deltas: Record<string, number>;
  explanation?: string | null;
}

export interface InfoCardArtifact {
  type: 'info_card';
  title: string;
  summary: string;
  data: Record<string, unknown>;
}

export type AdvisorArtifact =
  | GuidedIntakeArtifact
  | SchoolRecommendationArtifact
  | OfferComparisonArtifact
  | StrategyPlanArtifact
  | WhatIfResultArtifact
  | InfoCardArtifact;

export interface AdvisorAction {
  action_id: string;
  label: string;
  payload: Record<string, unknown>;
}

export interface DoneStep {
  capability: AdvisorCapability;
  status: AdvisorStepStatus;
  message?: string | null;
  retry_count: number;
}

export interface PendingStep {
  capability: AdvisorCapability;
  reason: PendingReason;
  message?: string | null;
}

export interface AdvisorRouteMeta {
  domain_confidence: number;
  capability_confidence: number;
  router_model: string;
  latency_ms: number;
  fallback_used: boolean;
  context_tokens: number;
  memory_hits: number;
  rag_hits: number;
  rag_latency_ms: number;
  memory_degraded: boolean;
  guard_result: AdvisorGuardResult;
  guard_reason: AdvisorGuardReason;
  primary_capability?: AdvisorCapability | null;
  executed_count: number;
  pending_count: number;
}

export interface AdvisorError {
  code: AdvisorErrorCode;
  message: string;
  retriable: boolean;
  detail?: Record<string, unknown> | null;
}

export interface AdvisorResponse {
  turn_id: string;
  domain: AdvisorDomain;
  capability: AdvisorCapability;
  assistant_text: string;
  artifacts: AdvisorArtifact[];
  actions: AdvisorAction[];
  done: DoneStep[];
  pending: PendingStep[];
  next_actions: AdvisorAction[];
  route_meta: AdvisorRouteMeta;
  error?: AdvisorError | null;
}

export interface AdvisorEditPayload {
  target_turn_id: string;
  mode?: 'overwrite';
}

export interface AdvisorUiMessage {
  assistant_text: string;
  domain: AdvisorDomain;
  capability: AdvisorCapability;
  artifacts: AdvisorArtifact[];
  actions: AdvisorAction[];
  done: DoneStep[];
  pending: PendingStep[];
  next_actions: AdvisorAction[];
  route_meta: AdvisorRouteMeta;
  error?: AdvisorError | null;
}

export interface AdvisorHistoryEntry {
  role: string;
  content: string;
  message_id?: string | null;
  turn_id?: string | null;
  created_at?: string | null;
  editable?: boolean;
  edited?: boolean;
}

export type UUID = string;

export interface ProgramResponse {
  id: UUID;
  school_id: UUID;
  name: string;
  department: string;
  us_news_rank?: number | null;
  avg_class_size?: number | null;
  has_research_opps: boolean;
  has_coop: boolean;
  description?: string | null;
}

export interface SchoolResponse {
  id: UUID;
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
  programs: ProgramResponse[];
}

export interface SchoolListResponse {
  items: SchoolResponse[];
  total: number;
  page: number;
  per_page: number;
}

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

export interface StudentUpdate {
  name?: string | null;
  gpa?: number | null;
  gpa_scale?: string | null;
  sat_total?: number | null;
  act_composite?: number | null;
  toefl_total?: number | null;
  curriculum_type?: string | null;
  ap_courses?: string[] | null;
  extracurriculars?: unknown[] | Record<string, unknown> | null;
  awards?: unknown[] | Record<string, unknown> | null;
  intended_majors?: string[] | null;
  budget_usd?: number | null;
  need_financial_aid?: boolean | null;
  preferences?: Record<string, unknown> | null;
  ed_preference?: string | null;
  target_year?: number | null;
}

export interface StudentResponse {
  id: UUID;
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

export interface PortfolioIdentity {
  name: string;
  target_year: number;
}

export interface PortfolioAcademics {
  gpa: number;
  gpa_scale: string;
  sat_total?: number | null;
  act_composite?: number | null;
  toefl_total?: number | null;
  curriculum_type: string;
  ap_courses?: string[] | null;
  intended_majors?: string[] | null;
}

export interface PortfolioActivities {
  extracurriculars?: unknown[] | Record<string, unknown> | null;
  awards?: unknown[] | Record<string, unknown> | null;
}

export interface PortfolioFinance {
  budget_usd: number;
  need_financial_aid: boolean;
}

export interface PortfolioStrategy {
  ed_preference?: string | null;
}

export interface PortfolioPreferences {
  interests?: string[] | null;
  risk_preference?: string | null;
  cost_priority?: string | null;
  location?: string[] | null;
  size?: string[] | null;
  culture?: string[] | null;
  career_goal?: string | null;
  research_vs_teaching?: string | null;
  target_schools?: string[] | null;
  financial_aid_type?: string | null;
  ui_preference_tags?: string[] | null;
}

export interface PortfolioCompletion {
  profile_completed: boolean;
  completion_pct: number;
  missing_fields: string[];
}

export interface StudentPortfolioResponse {
  student_id: UUID;
  identity: PortfolioIdentity;
  academics: PortfolioAcademics;
  activities: PortfolioActivities;
  finance: PortfolioFinance;
  strategy: PortfolioStrategy;
  preferences: PortfolioPreferences;
  completion: PortfolioCompletion;
}

export interface StudentPortfolioPatch {
  identity?: Partial<PortfolioIdentity>;
  academics?: Partial<PortfolioAcademics>;
  activities?: Partial<PortfolioActivities>;
  finance?: Partial<PortfolioFinance>;
  strategy?: Partial<PortfolioStrategy>;
  preferences?: Partial<PortfolioPreferences>;
}

export interface EvaluationResponse {
  id: UUID;
  created_at: string;
  student_id: UUID;
  school_id: UUID;
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
  school_id: UUID;
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

export interface OfferUpdate {
  status?: string | null;
  tuition?: number | null;
  room_and_board?: number | null;
  books_supplies?: number | null;
  personal_expenses?: number | null;
  transportation?: number | null;
  merit_scholarship?: number | null;
  need_based_grant?: number | null;
  loan_offered?: number | null;
  work_study?: number | null;
  honors_program?: boolean | null;
  conditions?: string | null;
  decision_deadline?: string | null;
  notes?: string | null;
}

export interface OfferResponse {
  id: UUID;
  created_at: string;
  student_id: UUID;
  school_id: UUID;
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

export interface WhatIfResponse {
  original_scores: Record<string, number>;
  modified_scores: Record<string, number>;
  deltas: Record<string, number>;
  explanation: string;
}

export interface ScenarioCompareResponse {
  results: WhatIfResponse[];
  summary: string;
}

export interface ScenarioCompareRequestItem {
  school_id: UUID;
  interventions: Record<string, number>;
  label?: string;
}

export interface GoNoGoReport {
  id: UUID;
  created_at: string;
  student_id: UUID;
  offer_id: UUID;
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
  prefilter_tag?: 'eligible' | 'stretch' | 'no_budget' | string | null;
  is_stretch?: boolean;
  rank_delta?: number | null;
  scenario_reason?: string | null;
  scenario_score?: number | null;
  baseline_rank?: number | null;
  outcome_breakdown?: Record<string, number> | null;
}

export interface RecommendationPrefilterMeta {
  budget_cap_used?: number | null;
  eligible_count?: number;
  stretch_count?: number;
  excluded_count?: number;
  excluded_reasons_summary?: Record<string, number>;
  prefilter_enabled?: boolean;
}

export interface RecommendationScenario {
  id: string;
  label: string;
  schools: RecommendedSchool[];
}

export interface RecommendationScenarioPack {
  baseline: RecommendedSchool[];
  scenarios: RecommendationScenario[];
  meta?: Record<string, unknown>;
}

export interface RecommendationData {
  narrative: string;
  schools: RecommendedSchool[];
  ed_recommendation?: string | null;
  ea_recommendations: string[];
  strategy_summary?: string | null;
  prefilter_meta?: RecommendationPrefilterMeta | null;
  scenario_pack?: RecommendationScenarioPack | null;
}

export interface ChatMessage {
  role: string;
  content: string;
  timestamp: string;
}

export interface ChatBlockWire {
  id: string;
  kind:
    | 'answer_synthesis'
    | 'recommendation'
    | 'offer_compare'
    | 'what_if'
    | 'guided_questions'
    | 'profile_snapshot'
    | 'profile_patch_proposal'
    | 'profile_patch_result'
    | 'text'
    | 'error';
  capability_id: string;
  order: number;
  payload: Record<string, unknown>;
  meta?: Record<string, unknown> | null;
}

export interface ProfileSnapshotPayload {
  portfolio: StudentPortfolioResponse;
  completion?: PortfolioCompletion;
}

export interface ProfilePatchProposalPayload {
  proposal_id: string;
  patch: StudentPortfolioPatch;
  summary: string;
  confirm_command: string;
  reedit_command: string;
  expires_after_user_turns: number;
  missing_fields?: string[];
}

export interface ProfilePatchResultPayload {
  proposal_id: string;
  applied: boolean;
  changed_fields: string[];
  portfolio: StudentPortfolioResponse;
}

export interface AnswerSynthesisPerspective {
  angle: string;
  claim: string;
  evidence: string;
  source_caps: string[];
  confidence: number;
}

export interface AnswerSynthesisAction {
  step: string;
  rationale: string;
  priority: 'high' | 'medium' | 'low' | string;
}

export interface AnswerSynthesisPayload {
  summary: string;
  conclusion: string;
  perspectives: AnswerSynthesisPerspective[];
  actions: AnswerSynthesisAction[];
  risks_missing: string[];
  degraded: {
    has_degraded: boolean;
    caps: string[];
    reason_codes: string[];
    retry_hint: string;
  };
}

export interface TurnEventMessage {
  type: 'turn.event';
  trace_id: string;
  event:
    | 'turn_started'
    | 'planning_done'
    | 'capability_started'
    | 'capability_finished'
    | 'rollback'
    | 'turn_completed';
  data?: Record<string, unknown> | null;
  timestamp: string;
}

export interface TurnResultMessage {
  type: 'turn.result';
  trace_id: string;
  status: 'ok' | 'error';
  content: string;
  blocks: ChatBlockWire[];
  actions: string[];
  execution_digest?: Record<string, unknown> | null;
  usage?: Record<string, unknown>;
}

export type ChatSocketMessage = TurnEventMessage | TurnResultMessage;

export interface TurnTraceStep {
  trace_id: string;
  event:
    | 'turn_started'
    | 'planning_done'
    | 'capability_started'
    | 'capability_finished'
    | 'rollback'
    | 'turn_completed';
  timestamp: string;
  step_id: string;
  parent_step_id?: string | null;
  step_kind?: 'turn' | 'wave' | 'checkpoint' | 'capability' | 'rollback' | null;
  step_status?:
    | 'queued'
    | 'running'
    | 'completed'
    | 'failed'
    | 'blocked'
    | 'cancelled'
    | 'timeout'
    | 'retrying'
    | null;
  phase?: string | null;
  wave_index?: number | null;
  capability_id?: string | null;
  duration_ms?: number | null;
  checkpoint_summary?: Record<string, unknown> | null;
  compact_reason_code?: string | null;
  event_seq?: number | null;
  display?: {
    title?: string | null;
    badge?: string | null;
    severity?: 'info' | 'success' | 'warning' | 'error' | null;
  } | null;
  metrics?: Record<string, unknown> | null;
  data?: Record<string, unknown> | null;
}

export interface TurnTraceSummary {
  trace_id: string;
  session_id: string;
  student_id?: string | null;
  status: 'running' | 'ok' | 'error';
  started_at: string;
  ended_at?: string | null;
  usage?: Record<string, unknown>;
  step_count: number;
}

export interface TurnTraceResponse {
  trace_id: string;
  session_id: string;
  student_id?: string | null;
  status: 'running' | 'ok' | 'error';
  started_at: string;
  ended_at?: string | null;
  usage?: Record<string, unknown>;
  steps: TurnTraceStep[];
  step_count: number;
}

export interface SessionTraceListResponse {
  items: TurnTraceSummary[];
  total: number;
}

export interface ChatHistoryEntry {
  role: string;
  content: string;
  status?: 'ok' | 'error' | null;
  trace_id?: string | null;
  blocks?: ChatBlockWire[] | null;
  actions?: string[] | null;
  execution_digest?: Record<string, unknown> | null;
}

export interface GenerateSchoolListResponse {
  status: 'completed';
  count: number;
  schools: unknown[];
  prefilter_meta?: RecommendationPrefilterMeta | null;
  scenario_pack?: RecommendationScenarioPack | null;
}

export interface ChatSessionResponse {
  id: UUID;
  student_id: UUID;
  session_id: string;
  title: string;
  preview?: string | null;
  message_count: number;
  school_count: number;
  created_at: string;
  last_active_at: string;
}

export interface CyNode {
  data: {
    id: string;
    label: string;
    node_type: string;
    prior_belief: number;
    propagated_belief?: number | null;
    confidence: number;
    color?: string;
  };
}

export interface CyEdge {
  data: {
    id: string;
    source: string;
    target: string;
    strength: number;
    mechanism: string;
    causal_type: string;
    evidence_score?: number;
    width?: number;
    line_style: string;
  };
}

export interface CausalDagResponse {
  elements: {
    nodes: CyNode[];
    edges: CyEdge[];
  };
  metadata?: {
    num_nodes: number;
    num_edges: number;
  };
}

export type ComparisonMetricKey =
  | 'net_cost'
  | 'total_aid'
  | 'tuition'
  | 'total_cost'
  | 'merit_scholarship'
  | 'career_outlook'
  | 'academic_fit'
  | 'life_satisfaction'
  | 'honors_program';

export interface OfferCompareSchool {
  id: string;
  schoolName: string;
  status?: string | null;
  badges?: string[];
  metrics: Partial<Record<ComparisonMetricKey, number | boolean | null>>;
}

export interface OfferCompareViewModel {
  source: 'chat' | 'offers';
  title?: string;
  description?: string;
  summary?: string;
  schools: OfferCompareSchool[];
  metricOrder: ComparisonMetricKey[];
}

export interface WhatIfDeltaItem {
  key: string;
  value: number;
}

export interface WhatIfViewModel {
  title?: string;
  deltas: WhatIfDeltaItem[];
  explanation?: string;
  suggestions: string[];
}

export type RichMessageBlock =
  | { type: 'answerSynthesis'; data: AnswerSynthesisPayload }
  | { type: 'recommendation'; data: RecommendationData }
  | { type: 'offerCompare'; data: OfferCompareViewModel }
  | { type: 'whatIf'; data: WhatIfViewModel }
  | { type: 'guidedQuestions'; data: GuidedQuestion[] }
  | { type: 'profileSnapshot'; data: ProfileSnapshotPayload }
  | { type: 'profilePatchProposal'; data: ProfilePatchProposalPayload }
  | { type: 'profilePatchResult'; data: ProfilePatchResultPayload }
  | { type: 'text'; data: { text: string } }
  | { type: 'error'; data: { message: string } };

import { useMemo } from 'react';
import type { StudentResponse, PortfolioPreferences } from '../lib/types';
import type { Locale } from '../i18n/locales';

export interface PromptSuggestion {
  /** Short chip label shown in the UI */
  label: string;
  /** Full detailed prompt sent to the advisor */
  prompt: string;
}

/**
 * Build 3 contextual quick-start prompts from both the student profile
 * and the triage context (degree_level, application_stage, interests).
 *
 * Memoised per student-id + locale; cached in localStorage keyed by
 * a hash of all fields that affect generation.
 */
export function useSuggestedPrompts(
  student: StudentResponse | null | undefined,
  locale: Locale,
  hasOffers?: boolean,
): PromptSuggestion[] {
  return useMemo(() => {
    if (!student) return fallback(locale);

    const cacheKey = `sp_prompts_${student.id}_${locale}`;
    const profileHash = hashProfile(student, hasOffers);
    const cached = readCache(cacheKey, profileHash);
    if (cached) return cached;

    const suggestions = generate(student, locale, hasOffers);
    writeCache(cacheKey, profileHash, suggestions);
    return suggestions;
  }, [student, locale, hasOffers]);
}

// ---------------------------------------------------------------------------
// Context extraction
// ---------------------------------------------------------------------------

interface ProfileContext {
  zh: boolean;
  major: string;
  level: string;           // 'undergrad' | 'graduate' | ''
  stage: string;           // 'researching' | 'applying' | 'admitted' | ''
  interests: string[];     // from preferences
  sat: number;
  budget: number;
  needAid: boolean;
  edPref: string | null;
  hasOffers: boolean;
  profileComplete: boolean;
}

function extractContext(s: StudentResponse, locale: Locale, hasOffers?: boolean): ProfileContext {
  const prefs = (s.preferences ?? {}) as Partial<PortfolioPreferences>;
  const zh = locale === 'zh';
  const level = prefs.application_level
    ?? (s.degree_level === 'undergraduate' ? '' : s.degree_level === 'masters' || s.degree_level === 'phd' ? 'graduate' : '');
  return {
    zh,
    major: s.intended_majors?.[0] ?? '',
    level,
    stage: prefs.application_stage ?? '',
    interests: prefs.interests ?? [],
    sat: s.sat_total ?? 0,
    budget: s.budget_usd ?? 0,
    needAid: s.need_financial_aid ?? false,
    edPref: s.ed_preference ?? null,
    hasOffers: hasOffers ?? false,
    profileComplete: s.profile_completed,
  };
}

// ---------------------------------------------------------------------------
// Generation — stage-aware
// ---------------------------------------------------------------------------

function generate(s: StudentResponse, locale: Locale, hasOffers?: boolean): PromptSuggestion[] {
  const ctx = extractContext(s, locale, hasOffers);

  // Dispatch by stage
  if (ctx.stage === 'admitted' || ctx.hasOffers) return admittedPrompts(ctx);
  if (ctx.stage === 'applying') return applyingPrompts(ctx);
  if (ctx.stage === 'researching') return researchingPrompts(ctx);

  // No stage set yet — generic but profile-aware
  return defaultPrompts(ctx);
}

/** Stage: researching — help explore and narrow down */
function researchingPrompts(ctx: ProfileContext): PromptSuggestion[] {
  const { zh, major, level } = ctx;
  const majorLabel = major || (zh ? '我感兴趣的' : 'my interests');
  const levelTag = level === 'graduate' ? (zh ? '研究生' : 'graduate') : (zh ? '本科' : 'undergraduate');

  return [
    {
      label: zh ? '智能选校推荐' : 'School recommendations',
      prompt: zh
        ? `我正在研究${levelTag}选校，方向是${majorLabel}。请根据我的GPA、标化成绩和预算，推荐 8-10 所分层学校（冲刺/匹配/保底），并说明推荐理由。`
        : `I'm researching ${levelTag} schools in ${majorLabel}. Based on my GPA, test scores, and budget, recommend 8-10 tiered schools (reach/match/safety) with reasoning.`,
    },
    {
      label: zh ? `评估${pickSchool(ctx)}` : `Evaluate ${pickSchool(ctx)}`,
      prompt: zh
        ? `帮我深度评估${pickSchool(ctx)}：学术匹配度、录取概率、费用与奖学金、就业前景，以及是否适合我的${majorLabel}方向。`
        : `Give me a deep evaluation of ${pickSchool(ctx)}: academic fit, admission chances, cost & aid, career outcomes, and fit for ${majorLabel}.`,
    },
    {
      label: zh ? '选校方向分析' : 'Explore directions',
      prompt: zh
        ? `根据我的背景和兴趣，帮我分析不同${levelTag}方向的就业前景和申请难度，给出选校策略建议。`
        : `Based on my profile and interests, analyze career prospects and application difficulty across ${levelTag} directions, and suggest a strategy.`,
    },
  ];
}

/** Stage: applying — help with strategy and execution */
function applyingPrompts(ctx: ProfileContext): PromptSuggestion[] {
  const { zh, major, edPref, needAid, interests } = ctx;
  const majorLabel = major || interests[0] || (zh ? '我的方向' : 'my field');

  const out: PromptSuggestion[] = [
    {
      label: zh ? '申请策略优化' : 'Application strategy',
      prompt: zh
        ? `我正在申请阶段，方向是${majorLabel}。请帮我优化选校分层、申请时间线和 ED/EA 策略，结合我的成绩和预算给出具体建议。`
        : `I'm in the application phase for ${majorLabel}. Optimize my school tiering, timeline, and ED/EA strategy based on my scores and budget.`,
    },
  ];

  if (edPref) {
    out.push({
      label: zh ? 'ED/EA 策略分析' : 'ED/EA analysis',
      prompt: zh
        ? `我倾向${edPref}，帮我分析这个选择是否最优，以及备选方案。哪些学校的提前批次对我的录取概率提升最大？`
        : `I'm leaning toward ${edPref}. Analyze if this is optimal and suggest alternatives. Which schools' early rounds give me the biggest admission boost?`,
    });
  } else {
    out.push({
      label: zh ? `评估${pickSchool(ctx)}` : `Evaluate ${pickSchool(ctx)}`,
      prompt: zh
        ? `帮我评估${pickSchool(ctx)}的录取概率、费用和${majorLabel}项目实力，以及是否值得放在我的申请名单中。`
        : `Evaluate ${pickSchool(ctx)}: admission chances, cost, ${majorLabel} program strength, and whether it belongs on my list.`,
    });
  }

  if (needAid) {
    out.push({
      label: zh ? '奖学金机会' : 'Scholarship opportunities',
      prompt: zh
        ? `在我的申请名单中，哪些学校最可能提供奖学金或助学金？帮我制定一个最大化财务支持的策略。`
        : `Among my target schools, which are most likely to offer scholarships or grants? Help me maximize financial aid.`,
    });
  } else {
    out.push({
      label: zh ? '文书与定位' : 'Essay positioning',
      prompt: zh
        ? `根据我的背景和${majorLabel}方向，帮我分析如何在文书中定位自己的独特优势。`
        : `Based on my profile and ${majorLabel} focus, help me position my unique strengths in essays.`,
    });
  }

  return out;
}

/** Stage: admitted — help with decisions and offers */
function admittedPrompts(ctx: ProfileContext): PromptSuggestion[] {
  const { zh, major } = ctx;
  const majorLabel = major || (zh ? '我的方向' : 'my field');

  return [
    {
      label: zh ? '对比 Offer' : 'Compare offers',
      prompt: zh
        ? `帮我对比已收到的所有 Offer：综合学术实力、费用、奖学金、就业前景和校园生活，给出推荐排序和分析。`
        : `Compare all my offers: weigh academics, cost, scholarships, career outcomes, and campus life. Give me a ranked recommendation with analysis.`,
    },
    {
      label: zh ? '性价比分析' : 'Value analysis',
      prompt: zh
        ? `对比各个 Offer 的总费用、净费用和投资回报率，哪个选择在${majorLabel}方向的性价比最高？`
        : `Compare total cost, net cost, and ROI across my offers. Which is the best value for ${majorLabel}?`,
    },
    {
      label: zh ? '最终决策建议' : 'Decision advice',
      prompt: zh
        ? `综合考虑学术、费用、职业发展和个人偏好，帮我做最终择校决策。列出每个选择的关键优劣势。`
        : `Considering academics, cost, career prospects, and personal fit, help me make a final school decision. List key pros and cons for each.`,
    },
  ];
}

/** No stage — generic but profile-aware */
function defaultPrompts(ctx: ProfileContext): PromptSuggestion[] {
  const { zh, major, profileComplete, needAid } = ctx;

  if (!profileComplete) {
    return [
      {
        label: zh ? '完善档案' : 'Complete profile',
        prompt: zh ? '帮我补全档案中缺失的信息，引导我一步步填写。' : 'Help me complete my profile step by step.',
      },
      {
        label: zh ? '选校推荐' : 'Recommend schools',
        prompt: zh ? '根据我目前的信息，先给我一些初步的选校建议。' : 'Based on what you know so far, give me some initial school suggestions.',
      },
      {
        label: zh ? '申请策略' : 'Application strategy',
        prompt: zh ? '给我一个大致的申请规划和时间线建议。' : 'Give me a general application plan and timeline.',
      },
    ];
  }

  const majorLabel = major || (zh ? '我的方向' : 'my field');
  return [
    {
      label: zh ? '智能选校' : 'School recommendations',
      prompt: zh
        ? `根据我的完整档案，推荐 8-10 所${majorLabel}方向的分层学校，并说明每所学校的推荐理由。`
        : `Based on my full profile, recommend 8-10 tiered schools for ${majorLabel} with reasoning for each.`,
    },
    {
      label: zh ? `评估${pickSchool(ctx)}` : `Evaluate ${pickSchool(ctx)}`,
      prompt: zh
        ? `帮我深度评估${pickSchool(ctx)}的匹配度、录取概率和费用情况。`
        : `Deep-evaluate ${pickSchool(ctx)}: fit, admission chances, and cost.`,
    },
    needAid
      ? {
          label: zh ? '奖学金策略' : 'Scholarship strategy',
          prompt: zh
            ? '在我的预算范围内，有哪些奖学金机会和省钱策略？帮我制定最大化援助的计划。'
            : 'What scholarship opportunities and cost-saving strategies fit my budget? Help me maximize aid.',
        }
      : {
          label: zh ? '申请策略' : 'Application strategy',
          prompt: zh
            ? '根据我的背景，给出选校分层、ED/EA策略和申请时间线建议。'
            : 'Based on my profile, suggest school tiering, ED/EA strategy, and application timeline.',
        },
  ];
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function pickSchool(ctx: ProfileContext): string {
  const { sat, zh } = ctx;
  if (sat >= 1500) return zh ? '斯坦福大学' : 'Stanford';
  if (sat >= 1400) return zh ? '密歇根大学' : 'U of Michigan';
  if (sat >= 1300) return zh ? '波士顿大学' : 'Boston University';
  if (sat >= 1200) return zh ? '宾州州立大学' : 'Penn State';
  return zh ? '加州大学尔湾分校' : 'UC Irvine';
}

function fallback(locale: Locale): PromptSuggestion[] {
  const zh = locale === 'zh';
  return [
    {
      label: zh ? '推荐学校' : 'Recommend schools',
      prompt: zh ? '帮我推荐适合我的学校' : 'Recommend schools that fit me',
    },
    {
      label: zh ? '评估学校' : 'Evaluate a school',
      prompt: zh ? '帮我评估一所学校的匹配度' : 'Evaluate how well a school fits me',
    },
    {
      label: zh ? '申请策略' : 'Application strategy',
      prompt: zh ? '给我一些申请策略建议' : 'Give me application strategy advice',
    },
  ];
}

function hashProfile(s: StudentResponse, hasOffers?: boolean): string {
  const prefs = (s.preferences ?? {}) as Record<string, unknown>;
  const parts = [
    s.profile_completed,
    s.degree_level,
    s.sat_total,
    s.intended_majors?.join(','),
    s.need_financial_aid,
    s.budget_usd,
    s.ed_preference,
    prefs.application_stage,
    prefs.application_level,
    (prefs.interests as string[] | undefined)?.join(','),
    hasOffers,
  ];
  return parts.map(String).join('|');
}

function readCache(key: string, hash: string): PromptSuggestion[] | null {
  try {
    const raw = localStorage.getItem(key);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (parsed.hash !== hash) return null;
    return parsed.suggestions as PromptSuggestion[];
  } catch {
    return null;
  }
}

function writeCache(key: string, hash: string, suggestions: PromptSuggestion[]): void {
  try {
    localStorage.setItem(key, JSON.stringify({ hash, suggestions }));
  } catch { /* quota exceeded — ignore */ }
}

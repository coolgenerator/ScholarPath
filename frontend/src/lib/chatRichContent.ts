import type {
  ComparisonMetricKey,
  OfferCompareSchool,
  OfferCompareViewModel,
  WhatIfDeltaItem,
  WhatIfViewModel,
} from './types';
import type { OfferComparison } from './api/offers';

const DEFAULT_COMPARE_ORDER: ComparisonMetricKey[] = [
  'net_cost',
  'total_aid',
  'career_outlook',
  'academic_fit',
  'life_satisfaction',
];

const OFFERS_COMPARE_ORDER: ComparisonMetricKey[] = [
  'net_cost',
  'total_aid',
  'total_cost',
  'tuition',
  'merit_scholarship',
  'honors_program',
];

function parseMoney(value: string): number | null {
  if (!value || /unknown|n\/a|—|暂无/i.test(value)) return null;
  const numeric = value.replace(/[^0-9.-]/g, '');
  if (!numeric) return null;
  const parsed = Number(numeric);
  return Number.isFinite(parsed) ? parsed : null;
}

function parsePercent(value: string): number | null {
  const match = value.match(/(-?\d+(?:\.\d+)?)\s*%/);
  if (!match) return null;
  const parsed = Number(match[1]);
  if (!Number.isFinite(parsed)) return null;
  return parsed / 100;
}

function collectMetrics(schools: OfferCompareSchool[]): ComparisonMetricKey[] {
  const keys = new Set<ComparisonMetricKey>();
  for (const key of DEFAULT_COMPARE_ORDER) {
    if (schools.some((school) => school.metrics[key] != null)) {
      keys.add(key);
    }
  }
  return [...keys];
}

function normalizeOutcomeKey(label: string): string {
  const normalized = label.toLowerCase().replace(/[_\s-]+/g, ' ').trim();
  if (/admission/.test(normalized) || /录取/.test(normalized)) return 'admission_probability';
  if (/academic/.test(normalized) || /学术/.test(normalized)) return 'academic_outcome';
  if (/career/.test(normalized) || /就业|职业/.test(normalized)) return 'career_outcome';
  if (/life/.test(normalized) || /生活/.test(normalized)) return 'life_satisfaction';
  if (/phd/.test(normalized) || /博士/.test(normalized)) return 'phd_probability';
  return normalized.replace(/\s+/g, '_');
}

export function parseOfferCompareFromText(content: string): OfferCompareViewModel | null {
  if (!/\*\*.+?\*\*/.test(content) || !/(net cost|total aid|career outlook|academic fit|life satisfaction|offers compare|offer 对比)/i.test(content)) {
    return null;
  }

  const summaryMatch = content.match(/\*\*(?:My recommendation|Recommendation|建议|我的建议)\*\*:?\s*([\s\S]+)$/i);
  const summary = summaryMatch?.[1]?.trim();
  const compareBody = summaryMatch ? content.slice(0, summaryMatch.index).trim() : content.trim();
  const schoolBlocks = [...compareBody.matchAll(/^\*\*(.+?)\*\*\s*([\s\S]*?)(?=^\*\*.+?\*\*\s*$|$)/gm)];

  const schools: OfferCompareSchool[] = schoolBlocks
    .map((match, index) => {
      const schoolName = match[1].trim();
      const block = match[2];
      const metrics: OfferCompareSchool['metrics'] = {};

      const netCostMatch = block.match(/(?:Net cost|净费用?)\s*:\s*([^\n|]+)(?:\|\s*(?:Total aid|总资助)\s*:\s*([^\n]+))?/i);
      const inlineAid = netCostMatch?.[2] ?? null;
      const totalAidMatch = block.match(/(?:Total aid|总资助)\s*:\s*([^\n]+)/i);
      const careerMatch = block.match(/(?:Career outlook|Career outcome|职业前景|职业结果)\s*:\s*([^\n|]+)(?:\|\s*(?:Academic fit|Academic outcome|学术匹配|学术结果)\s*:\s*([^\n|]+))?(?:\|\s*(?:Life satisfaction|Life fit|生活满意度|生活匹配)\s*:\s*([^\n]+))?/i);

      metrics.net_cost = netCostMatch ? parseMoney(netCostMatch[1]) : null;
      metrics.total_aid = totalAidMatch ? parseMoney(totalAidMatch[1]) : inlineAid ? parseMoney(inlineAid) : null;
      metrics.career_outlook = careerMatch ? parsePercent(careerMatch[1]) : null;
      metrics.academic_fit = careerMatch?.[2] ? parsePercent(careerMatch[2]) : null;
      metrics.life_satisfaction = careerMatch?.[3] ? parsePercent(careerMatch[3]) : null;

      return {
        id: `chat-offer-${index}`,
        schoolName,
        metrics,
      };
    })
    .filter((school) => Object.values(school.metrics).some((value) => value != null));

  if (schools.length < 2) return null;

  return {
    source: 'chat',
    title: 'Offer Comparison',
    description: 'Structured side-by-side comparison extracted from the advisor response.',
    summary,
    schools,
    metricOrder: collectMetrics(schools),
  };
}

export function parseWhatIfFromText(content: string): WhatIfViewModel | null {
  if (!/(simulation shows|what-if|scenario|模拟结果|情景模拟)/i.test(content)) return null;

  const deltas: WhatIfDeltaItem[] = [...content.matchAll(/^-+\s*\*\*(.+?)\*\*\s*:\s*(\d+(?:\.\d+)?)%\s*(increase|decrease|提升|下降)/gim)]
    .map((match) => {
      const key = normalizeOutcomeKey(match[1]);
      const value = Number(match[2]) / 100;
      const direction = /decrease|下降/i.test(match[3]) ? -1 : 1;
      return {
        key,
        value: value * direction,
      };
    });

  if (deltas.length < 1) return null;

  const explanation = content
    .replace(/^[\s\S]*?(?=^-+\s*\*\*.+?\*\*\s*:)/m, '')
    .replace(/^-+\s*\*\*.+?\*\*\s*:\s*\d+(?:\.\d+)?%\s*(?:increase|decrease|提升|下降)\s*$/gim, '')
    .trim();

  return {
    title: 'What-If Analysis',
    deltas,
    explanation,
    suggestions: [],
  };
}

export function normalizeOfferComparison(comparison: OfferComparison): OfferCompareViewModel {
  const comparisonLookup = new Map(
    comparison.comparison_scores.map((score) => [String(score.offer_id), score]),
  );

  const schools: OfferCompareSchool[] = comparison.offers.map((offer) => {
    const score = comparisonLookup.get(String(offer.id)) ?? {};
    return {
      id: String(offer.id),
      schoolName: offer.school_name ?? 'School',
      status: offer.status,
      badges: [
        offer.status,
        ...(offer.honors_program ? ['Honors'] : []),
      ],
      metrics: {
        net_cost: offer.net_cost ?? (score.net_cost as number | null | undefined) ?? null,
        total_aid: offer.total_aid ?? (score.total_aid as number | null | undefined) ?? null,
        total_cost: offer.total_cost ?? (score.total_cost as number | null | undefined) ?? null,
        tuition: offer.tuition ?? (score.tuition as number | null | undefined) ?? null,
        merit_scholarship:
          offer.merit_scholarship ?? (score.merit_scholarship as number | null | undefined) ?? null,
        honors_program: offer.honors_program ?? (score.honors_program as boolean | null | undefined) ?? false,
      },
    };
  });

  return {
    source: 'offers',
    title: 'Offer Comparison',
    description: 'Financial and package overview across all admitted offers currently on file.',
    schools,
    metricOrder: OFFERS_COMPARE_ORDER.filter((key) =>
      schools.some((school) => school.metrics[key] != null),
    ),
  };
}

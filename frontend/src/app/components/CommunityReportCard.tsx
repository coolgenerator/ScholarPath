import React, { useEffect, useState, useSyncExternalStore, useCallback } from 'react';
import type { CommunityReportResponse, CommunityDimension } from '../../lib/types';
import { fetchCommunityReport, getSchoolDataState, subscribe } from '../../lib/schoolDataCache';

const DIMENSION_ICONS: Record<string, string> = {
  academic_experience: 'school',
  campus_life: 'apartment',
  career_employment: 'work',
  value_for_money: 'savings',
  overall_vibe: 'mood',
};

const SCORE_COLORS = [
  'bg-error/60',        // 1-2
  'bg-error/40',        // 3-4
  'bg-warning/50',      // 5-6
  'bg-tertiary/50',     // 7-8
  'bg-tertiary',        // 9-10
];

function getScoreColor(score: number): string {
  if (score <= 2) return SCORE_COLORS[0];
  if (score <= 4) return SCORE_COLORS[1];
  if (score <= 6) return SCORE_COLORS[2];
  if (score <= 8) return SCORE_COLORS[3];
  return SCORE_COLORS[4];
}

function DimensionRow({ id, dim }: { id: string; dim: CommunityDimension }) {
  const [expanded, setExpanded] = useState(false);
  const icon = DIMENSION_ICONS[id] || 'info';

  return (
    <div className="border-b border-outline-variant/8 last:border-b-0">
      <button
        className="flex w-full items-center gap-3 px-1 py-3 text-left transition hover:bg-surface-container-high/20"
        onClick={() => setExpanded(!expanded)}
      >
        <span className="material-symbols-outlined text-on-surface-variant/60 text-lg">{icon}</span>
        <div className="flex-1 min-w-0">
          <div className="text-xs font-bold text-on-surface">{dim.label_cn}</div>
        </div>
        <div className="flex items-center gap-2">
          <div className="flex gap-0.5">
            {Array.from({ length: 10 }).map((_, i) => (
              <div
                key={i}
                className={`h-2.5 w-1.5 rounded-full ${i < dim.score ? getScoreColor(dim.score) : 'bg-outline-variant/15'}`}
              />
            ))}
          </div>
          <span className="text-sm font-black text-on-surface w-6 text-right">{dim.score}</span>
          <span className="material-symbols-outlined text-sm text-on-surface-variant/40 transition-transform" style={{ transform: expanded ? 'rotate(180deg)' : '' }}>
            expand_more
          </span>
        </div>
      </button>
      {expanded && (
        <div className="pb-3 pl-9 pr-2 space-y-2">
          <p className="text-xs leading-relaxed text-on-surface-variant/75">{dim.summary}</p>
          {dim.key_quotes.length > 0 && (
            <div className="space-y-1.5">
              {dim.key_quotes.map((quote, i) => (
                <div key={i} className="flex gap-2 text-[11px] text-on-surface-variant/55 italic">
                  <span className="text-primary/40 shrink-0">"</span>
                  <span className="line-clamp-3">{quote}</span>
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

interface CommunityReportCardProps {
  schoolId: string;
}

export function CommunityReportCard({ schoolId }: CommunityReportCardProps) {
  const subFn = useCallback((cb: () => void) => subscribe(schoolId, cb), [schoolId]);
  const state = useSyncExternalStore(subFn, () => getSchoolDataState(schoolId).communityReport);

  // Trigger fetch on mount (cache-aware, won't duplicate)
  useEffect(() => { fetchCommunityReport(schoolId); }, [schoolId]);

  const { status, progress, data: report, rawReviews } = state as any;

  if (status === 'idle') return null;

  if (status === 'loading') {
    const SOURCE_ICONS: Record<string, string> = {
      reddit: '🔵', xiaohongshu: '📕', zhihu: '🟦', '1point3acres': '🌱', niche: '🟢', college_confidential: '🟡',
    };
    return (
      <div className="rounded-2xl border border-outline-variant/10 bg-white p-5 shadow-sm">
        <div className="flex items-center gap-2 mb-3">
          <span className="material-symbols-outlined animate-spin text-primary text-lg">progress_activity</span>
          <span className="text-xs font-bold text-on-surface-variant/60">{progress}</span>
        </div>
        <div className="w-full h-1.5 bg-surface-container-high/40 rounded-full overflow-hidden mb-3">
          <div className="h-full bg-primary/60 rounded-full" style={{ width: rawReviews?.length ? '70%' : '30%', animation: 'indeterminate 1.5s ease-in-out infinite' }} />
        </div>
        <style>{`@keyframes indeterminate { 0% { margin-left: 0; width: 30%; } 50% { margin-left: 30%; width: 50%; } 100% { margin-left: 70%; width: 30%; } }`}</style>
        {/* Show raw reviews as they arrive */}
        {rawReviews && rawReviews.length > 0 && (
          <div className="space-y-2 max-h-60 overflow-y-auto">
            {rawReviews.slice(0, 8).map((r: any, i: number) => (
              <div key={i} className="flex gap-2 rounded-lg bg-surface-container-lowest/50 px-3 py-2 border border-outline-variant/5">
                <span className="shrink-0 text-sm">{SOURCE_ICONS[r.source] || '📝'}</span>
                <div className="min-w-0 flex-1">
                  <div className="flex items-center gap-2">
                    <span className="text-[10px] font-bold text-on-surface-variant/40 uppercase">{r.source}</span>
                    {r.score > 0 && <span className="text-[10px] text-on-surface-variant/40">▲{r.score}</span>}
                  </div>
                  <p className="text-xs text-on-surface truncate font-semibold">{r.title}</p>
                  {r.body && <p className="text-[11px] text-on-surface-variant/60 line-clamp-2 mt-0.5">{r.body}</p>}
                </div>
                {r.url && (
                  <a href={r.url} target="_blank" rel="noopener noreferrer" className="shrink-0 text-on-surface-variant/30 hover:text-primary transition">
                    <span className="material-symbols-outlined text-sm">open_in_new</span>
                  </a>
                )}
              </div>
            ))}
          </div>
        )}
      </div>
    );
  }

  if (status === 'error' || !report) {
    // Still show raw reviews if available even without LLM report
    if (rawReviews?.length) {
      const SOURCE_ICONS: Record<string, string> = {
        reddit: '🔵', xiaohongshu: '📕', zhihu: '🟦', '1point3acres': '🌱', niche: '🟢', college_confidential: '🟡',
      };
      return (
        <div className="rounded-2xl border border-outline-variant/10 bg-white p-5 shadow-sm">
          <div className="flex items-center gap-2 mb-3">
            <span className="material-symbols-outlined text-primary text-lg">forum</span>
            <h4 className="font-headline text-sm font-black text-on-surface">社区评论</h4>
            <span className="text-[10px] font-bold text-on-surface-variant/50">{rawReviews.length} 条</span>
          </div>
          <div className="space-y-2 max-h-80 overflow-y-auto">
            {rawReviews.map((r: any, i: number) => (
              <div key={i} className="flex gap-2 rounded-lg bg-surface-container-lowest/50 px-3 py-2 border border-outline-variant/5">
                <span className="shrink-0 text-sm">{SOURCE_ICONS[r.source] || '📝'}</span>
                <div className="min-w-0 flex-1">
                  <p className="text-xs text-on-surface font-semibold">{r.title}</p>
                  {r.body && <p className="text-[11px] text-on-surface-variant/60 line-clamp-2 mt-0.5">{r.body}</p>}
                </div>
                {r.url && (
                  <a href={r.url} target="_blank" rel="noopener noreferrer" className="shrink-0 text-on-surface-variant/30 hover:text-primary">
                    <span className="material-symbols-outlined text-sm">open_in_new</span>
                  </a>
                )}
              </div>
            ))}
          </div>
        </div>
      );
    }
    return null;
  }

  const dims = report.dimensions;
  const dimEntries = Object.entries(dims);

  return (
    <div className="rounded-2xl border border-outline-variant/10 bg-white p-5 shadow-sm">
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-2">
          <span className="material-symbols-outlined text-primary text-lg">forum</span>
          <h4 className="font-headline text-sm font-black text-on-surface">社区评价</h4>
          <span className="text-[10px] font-bold text-on-surface-variant/50">{report.review_count} 条评论</span>
        </div>
        {report.overall_score != null && (
          <div className="flex items-center gap-1.5">
            <span className="text-lg font-black text-on-surface">{report.overall_score.toFixed(1)}</span>
            <span className="text-[9px] font-bold text-on-surface-variant/40 uppercase">/10</span>
          </div>
        )}
      </div>

      {report.overall_summary && (
        <p className="text-xs leading-relaxed text-on-surface-variant/70 mb-4 border-l-2 border-primary/20 pl-3">
          {report.overall_summary}
        </p>
      )}

      <div>
        {dimEntries.map(([key, dim]) => (
          <DimensionRow key={key} id={key} dim={dim} />
        ))}
      </div>
    </div>
  );
}

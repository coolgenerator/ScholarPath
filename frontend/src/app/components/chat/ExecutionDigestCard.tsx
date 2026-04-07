import React, { useMemo, useState } from 'react';
import { useApp } from '../../../context/AppContext';

interface ExecutionDigestCardProps {
  digest: Record<string, unknown>;
}

function asStringList(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value
    .map((item) => String(item).trim())
    .filter((item) => item.length > 0);
}

export function ExecutionDigestCard({ digest }: ExecutionDigestCardProps) {
  const { t } = useApp();
  const [expanded, setExpanded] = useState(false);
  const summary = typeof digest.summary === 'string' ? digest.summary : '';
  const whatDone = typeof digest.what_done === 'string' ? digest.what_done : '';
  const whyNext = typeof digest.why_next === 'string' ? digest.why_next : '';
  const needsInput = asStringList(digest.needs_input);

  const statText = useMemo(() => {
    const waveCount = Number(digest.wave_count ?? 0);
    const toolSteps = Number(digest.tool_steps_used ?? 0);
    if (!Number.isFinite(waveCount) && !Number.isFinite(toolSteps)) return '';
    return `${t.chat_exec_digest_wave_count(Number.isFinite(waveCount) ? waveCount : 0)} · ${t.chat_exec_digest_tool_count(Number.isFinite(toolSteps) ? toolSteps : 0)}`;
  }, [digest.tool_steps_used, digest.wave_count, t]);

  return (
    <div className="rounded-xl border border-outline-variant/15 bg-surface-container-low/40 px-3 py-2.5 text-xs text-on-surface-variant/82">
      <button
        onClick={() => setExpanded((prev) => !prev)}
        className="flex w-full items-center justify-between gap-3 text-left"
      >
        <div className="min-w-0">
          <div className="text-[10px] font-bold uppercase tracking-[0.12em] text-on-surface-variant/58">
            {t.chat_exec_digest_title}
          </div>
          {summary && (
            <div className="mt-1 truncate text-xs text-on-surface-variant/80">{summary}</div>
          )}
          {statText && (
            <div className="mt-1 text-[11px] text-on-surface-variant/65">{statText}</div>
          )}
        </div>
        <span className="material-symbols-outlined text-base text-on-surface-variant/70">
          {expanded ? 'expand_less' : 'expand_more'}
        </span>
      </button>

      {expanded && (
        <div className="mt-2.5 space-y-2 rounded-lg border border-outline-variant/10 bg-white/75 px-2.5 py-2">
          {whatDone && (
            <div>
              <div className="text-[10px] font-semibold uppercase tracking-[0.12em] text-on-surface-variant/55">
                {t.chat_exec_digest_what_done}
              </div>
              <div className="mt-0.5 text-xs text-on-surface-variant/78">{whatDone}</div>
            </div>
          )}
          {whyNext && (
            <div>
              <div className="text-[10px] font-semibold uppercase tracking-[0.12em] text-on-surface-variant/55">
                {t.chat_exec_digest_why_next}
              </div>
              <div className="mt-0.5 text-xs text-on-surface-variant/78">{whyNext}</div>
            </div>
          )}
          <div>
            <div className="text-[10px] font-semibold uppercase tracking-[0.12em] text-on-surface-variant/55">
              {t.chat_exec_digest_needs_input}
            </div>
            {needsInput.length > 0 ? (
              <ul className="mt-0.5 list-disc space-y-0.5 pl-4 text-xs text-on-surface-variant/78">
                {needsInput.slice(0, 4).map((item, index) => (
                  <li key={`${item}-${index}`}>{item}</li>
                ))}
              </ul>
            ) : (
              <div className="mt-0.5 text-xs text-on-surface-variant/70">{t.chat_exec_digest_no_missing}</div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

import React, { useEffect, useMemo, useRef, useState } from 'react';
import ReactMarkdown from 'react-markdown';
import { useApp } from '../../context/AppContext';
import { useOffers } from '../../hooks/useOffers';
import { useEvaluations } from '../../hooks/useEvaluations';
import { useReports } from '../../hooks/useReports';
import { normalizeOfferComparison } from '../../lib/chatRichContent';
import type { EvaluationWithSchool, GoNoGoReport } from '../../lib/types';
import { schoolsApi } from '../../lib/api/schools';
import type { OfferResponse } from '../../lib/api/offers';
import { OfferCompareCard } from './StructuredMessageCards';
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from './ui/sheet';
import {
  DASHBOARD_SELECT_EMPTY_VALUE,
  DashboardFieldLabel,
  DashboardSelect,
  DashboardSelectContent,
  DashboardSelectItem,
  DashboardSelectTrigger,
  DashboardSelectValue,
} from './ui/dashboard-select';
import { DashboardCheckboxField } from './ui/dashboard-checkbox';
import { DashboardInput, DashboardTextarea } from './ui/dashboard-input';
import { DashboardSegmentedGroup, DashboardSegmentedItem } from './ui/dashboard-segmented';
import { AnimatedWorkspacePage, MotionItem, MotionSection, MotionStagger, MotionSurface } from './WorkspaceMotion';

function formatCurrency(value: number | null | undefined): string {
  if (value == null) return '—';
  return `$${value.toLocaleString()}`;
}

function formatK(value: number | null | undefined): string {
  if (value == null) return '—';
  return `$${(value / 1000).toFixed(0)}K`;
}

const STATUS_STYLES: Record<string, { bg: string; text: string }> = {
  admitted:   { bg: 'bg-tertiary/10', text: 'text-tertiary' },
  waitlisted: { bg: 'bg-secondary-fixed/50', text: 'text-on-secondary-fixed-variant' },
  denied:    { bg: 'bg-error/10', text: 'text-error' },
  deferred:  { bg: 'bg-primary/10', text: 'text-primary' },
  committed: { bg: 'bg-tertiary', text: 'text-on-tertiary' },
  declined:  { bg: 'bg-on-surface-variant/10', text: 'text-on-surface-variant' },
};

const STATUS_ACCENTS: Record<string, 'status-admitted' | 'status-waitlisted' | 'status-denied' | 'status-deferred'> = {
  admitted: 'status-admitted',
  waitlisted: 'status-waitlisted',
  denied: 'status-denied',
  deferred: 'status-deferred',
};

function getOfferStatusLabel(status: string, t: Record<string, any>): string {
  switch (status) {
    case 'admitted':
      return t.off_status_admitted;
    case 'waitlisted':
      return t.off_status_waitlisted;
    case 'denied':
      return t.off_status_denied;
    case 'deferred':
      return t.off_status_deferred;
    case 'committed':
      return t.off_status_committed;
    case 'declined':
      return t.off_status_declined;
    default:
      return status;
  }
}

function getOfferReportCacheKey(studentId: string, offerId: string): string {
  return `sp_offer_report_${studentId}_${offerId}`;
}

function getLooseItemText(item: unknown): string {
  if (typeof item === 'string') return item;
  if (item && typeof item === 'object') {
    const record = item as Record<string, unknown>;
    const candidate = record.title ?? record.label ?? record.factor ?? record.risk ?? record.description;
    if (typeof candidate === 'string' && candidate.trim()) return candidate;
    try {
      return JSON.stringify(record);
    } catch {
      return String(record);
    }
  }
  return String(item);
}

function getConfidenceBounds(report: GoNoGoReport): [number | null, number | null] {
  const lower = report.ci_lower ?? report.confidence_lower ?? null;
  const upper = report.ci_upper ?? report.confidence_upper ?? null;
  return [lower, upper];
}

function ReportList({
  items,
  emptyLabel,
}: {
  items: unknown[];
  emptyLabel: string;
}) {
  if (items.length === 0) {
    return <div className="text-sm text-on-surface-variant/55">{emptyLabel}</div>;
  }

  return (
    <div className="space-y-2">
      {items.map((item, index) => (
        <div key={`${index}-${getLooseItemText(item)}`} className="rounded-2xl border border-outline-variant/10 bg-surface-container-low/35 px-4 py-3 text-sm leading-relaxed text-on-surface">
          {getLooseItemText(item)}
        </div>
      ))}
    </div>
  );
}

// ─── Add Offer Form ───

function AddOfferForm({ evaluations, onSubmit, onCancel, t }: {
  evaluations: EvaluationWithSchool[];
  onSubmit: (data: Record<string, unknown>) => void;
  onCancel: () => void;
  t: Record<string, any>;
}) {
  const [schoolId, setSchoolId] = useState('');
  const [schoolQuery, setSchoolQuery] = useState('');
  const [matchedSchoolName, setMatchedSchoolName] = useState('');
  const [isLookingUp, setIsLookingUp] = useState(false);
  const [lookupError, setLookupError] = useState('');
  const lookupTimerRef = useRef<number | null>(null);
  const [program, setProgram] = useState('');
  const [status, setStatus] = useState('admitted');
  const [meritScholarship, setMeritScholarship] = useState('');
  const [notes, setNotes] = useState('');

  const statusOptions = ['admitted', 'waitlisted', 'denied', 'deferred'] as const;

  const handleSchoolInput = (value: string) => {
    setSchoolQuery(value);
    setSchoolId('');
    setMatchedSchoolName('');
    setLookupError('');

    if (lookupTimerRef.current) window.clearTimeout(lookupTimerRef.current);

    if (value.trim().length < 2) return;

    lookupTimerRef.current = window.setTimeout(async () => {
      setIsLookingUp(true);
      setLookupError('');
      try {
        const result = await schoolsApi.lookup(value.trim());
        setSchoolId(result.id);
        setMatchedSchoolName(result.name + (result.name_cn ? ` (${result.name_cn})` : ''));
      } catch {
        setLookupError(t.off_school_not_found ?? '未找到匹配学校，请尝试更完整的名称');
      } finally {
        setIsLookingUp(false);
      }
    }, 600);
  };

  const handleSubmit = () => {
    if (!schoolId) return;
    onSubmit({
      school_id: schoolId,
      status,
      program: program || undefined,
      merit_scholarship: Number(meritScholarship) || 0,
      notes: notes || undefined,
    });
  };

  return (
    <div className="fixed inset-0 bg-black/30 backdrop-blur-sm z-[100] flex items-center justify-center" onClick={onCancel}>
      <div className="max-h-[85vh] w-[640px] max-w-[calc(100vw-2rem)] overflow-y-auto rounded-3xl bg-white shadow-2xl" onClick={(e) => e.stopPropagation()}>
        <div className="p-6 border-b border-outline-variant/10 flex items-center justify-between">
          <h3 className="font-headline text-lg font-black text-on-surface">{t.off_form_title}</h3>
          <button onClick={onCancel} className="w-8 h-8 rounded-full flex items-center justify-center hover:bg-surface-container-high">
            <span className="material-symbols-outlined text-on-surface-variant">close</span>
          </button>
        </div>
        <div className="p-6 space-y-6">
          {/* School input with auto-lookup */}
          <div>
            <DashboardFieldLabel>{t.off_school}</DashboardFieldLabel>
            <div className="relative">
              <DashboardInput
                type="text"
                className="px-3 py-2"
                placeholder={t.off_school_placeholder ?? '输入学校名称，如 MIT、北大...'}
                value={schoolQuery}
                onChange={(e) => handleSchoolInput(e.target.value)}
              />
              {isLookingUp && (
                <div className="absolute right-3 top-1/2 -translate-y-1/2">
                  <span className="material-symbols-outlined animate-spin text-sm text-on-surface-variant/40">progress_activity</span>
                </div>
              )}
            </div>
            {matchedSchoolName && (
              <div className="mt-1.5 flex items-center gap-1.5 text-xs text-tertiary">
                <span className="material-symbols-outlined text-sm">check_circle</span>
                {matchedSchoolName}
              </div>
            )}
            {lookupError && (
              <div className="mt-1.5 text-xs text-error">{lookupError}</div>
            )}
          </div>

          {/* Program / Major */}
          <div>
            <DashboardFieldLabel>{t.off_program}</DashboardFieldLabel>
            <DashboardInput
              type="text"
              className="px-3 py-2"
              placeholder={t.off_program_placeholder}
              value={program}
              onChange={(e) => setProgram(e.target.value)}
            />
          </div>

          {/* Status */}
          <div>
            <label className="text-[10px] font-bold text-on-surface-variant uppercase tracking-widest mb-1.5 block">{t.off_status}</label>
            <DashboardSegmentedGroup
              type="single"
              value={status}
              onValueChange={(value) => {
                if (value) setStatus(value as typeof status);
              }}
            >
              {statusOptions.map((s) => (
                <DashboardSegmentedItem
                  key={s}
                  value={s}
                  accent={STATUS_ACCENTS[s]}
                  className="capitalize"
                >
                  {getOfferStatusLabel(s, t)}
                </DashboardSegmentedItem>
              ))}
            </DashboardSegmentedGroup>
          </div>

          {/* Scholarship */}
          <div>
            <DashboardFieldLabel>{t.off_merit_scholarship}</DashboardFieldLabel>
            <DashboardInput
              type="number"
              className="px-3 py-2"
              placeholder="$0"
              value={meritScholarship}
              onChange={(e) => setMeritScholarship(e.target.value)}
            />
          </div>

          {/* Notes */}
          <div>
            <DashboardFieldLabel>{t.off_notes}</DashboardFieldLabel>
            <DashboardTextarea
              className="resize-none"
              rows={2}
              placeholder={t.off_program_conditions_deadlines}
              value={notes}
              onChange={(e) => setNotes(e.target.value)}
            />
          </div>

          <button
            onClick={handleSubmit}
            disabled={!schoolId}
            className="w-full py-3 bg-primary text-on-primary rounded-xl font-bold text-sm hover:brightness-110 transition-all shadow-md disabled:opacity-50"
          >
            {t.off_save}
          </button>
        </div>
      </div>
    </div>
  );
}

// ─── Offer Card ───

function OfferCard({
  offer,
  t,
  onOpenReport,
  reportLabel,
  reportLoading,
}: {
  offer: OfferResponse;
  t: Record<string, any>;
  onOpenReport?: () => void;
  reportLabel?: string;
  reportLoading?: boolean;
}) {
  const statusStyle = STATUS_STYLES[offer.status] ?? STATUS_STYLES.admitted;
  const hasDeadline = offer.decision_deadline != null;
  const isActionable = offer.status === 'admitted' || offer.status === 'committed';

  return (
    <div className="dashboard-surface dashboard-hover-lift flex h-full flex-col p-5 sm:p-6">
      {/* Header: school name + net cost */}
      <div className="mb-3 flex items-start justify-between gap-4">
        <div className="min-w-0">
          <h3 className="font-headline text-base font-bold text-on-surface truncate">{offer.school_name ?? t.common_school}</h3>
          {offer.program && (
            <div className="text-xs text-on-surface-variant/70 mt-0.5 truncate">{offer.program}</div>
          )}
          <div className="flex items-center gap-2 mt-1.5">
            <span className={`inline-block px-2 py-0.5 ${statusStyle.bg} ${statusStyle.text} text-[9px] font-black uppercase tracking-widest rounded-md`}>
              {getOfferStatusLabel(offer.status, t)}
            </span>
            {offer.honors_program && (
              <span className="px-2 py-0.5 bg-primary/10 text-primary text-[9px] font-black uppercase tracking-widest rounded-md">{t.off_honors_program}</span>
            )}
          </div>
        </div>
        <div className="text-right shrink-0">
          <div className="text-2xl font-black text-on-surface">{formatCurrency(offer.net_cost)}</div>
          <div className="text-[8px] font-bold text-on-surface-variant/50 uppercase tracking-widest">{t.off_net_cost}</div>
        </div>
      </div>

      {/* Cost breakdown — always show rows for consistency */}
      <div className="grid grid-cols-2 gap-x-4 gap-y-1 mb-3 text-xs">
        <div className="flex justify-between">
          <span className="text-on-surface-variant/60">{t.off_tuition_fees}</span>
          <span className="font-bold text-on-surface">{offer.tuition != null ? formatK(offer.tuition) : '—'}</span>
        </div>
        <div className="flex justify-between">
          <span className="text-on-surface-variant/60">{t.off_room_board}</span>
          <span className="font-bold text-on-surface">{offer.room_and_board != null ? formatK(offer.room_and_board) : '—'}</span>
        </div>
        <div className="flex justify-between col-span-2 border-t border-outline-variant/10 pt-1 mt-1">
          <span className="text-on-surface-variant/80 font-bold">{t.off_total_cost}</span>
          <span className="font-black text-on-surface">{offer.total_cost != null ? formatCurrency(offer.total_cost) : '—'}</span>
        </div>
      </div>

      {/* Aid breakdown */}
      <div className="grid grid-cols-4 gap-1.5">
        <div className="bg-surface-container-low/40 px-2 py-1.5 rounded-lg border border-outline-variant/5">
          <div className="text-[7px] text-on-surface-variant font-bold uppercase tracking-widest">{t.off_scholarships}</div>
          <div className="text-sm font-black text-tertiary">{formatCurrency(offer.merit_scholarship)}</div>
        </div>
        <div className="bg-surface-container-low/40 px-2 py-1.5 rounded-lg border border-outline-variant/5">
          <div className="text-[7px] text-on-surface-variant font-bold uppercase tracking-widest">{t.off_grants}</div>
          <div className="text-sm font-black text-on-surface">{formatCurrency(offer.need_based_grant)}</div>
        </div>
        <div className="bg-surface-container-low/40 px-2 py-1.5 rounded-lg border border-outline-variant/5">
          <div className="text-[7px] text-on-surface-variant font-bold uppercase tracking-widest">{t.off_loans}</div>
          <div className="text-sm font-black text-error/70">{formatCurrency(offer.loan_offered)}</div>
        </div>
        <div className="bg-surface-container-low/40 px-2 py-1.5 rounded-lg border border-outline-variant/5">
          <div className="text-[7px] text-on-surface-variant font-bold uppercase tracking-widest">{t.off_work_study}</div>
          <div className="text-sm font-black text-primary">{formatCurrency(offer.work_study)}</div>
        </div>
      </div>

      {/* Bottom section — pushed down with mt-auto for uniform card height */}
      <div className="mt-auto pt-3">
        {/* Deadline */}
        {hasDeadline && (
          <span className="text-[10px] font-bold text-on-surface-variant/60 flex items-center gap-1 mb-1.5">
            <span className="material-symbols-outlined text-xs">calendar_today</span>
            {t.off_deadline}: {new Date(offer.decision_deadline!).toLocaleDateString()}
          </span>
        )}

        {/* Notes — fixed 2 lines */}
        <p className="text-xs text-on-surface-variant/60 line-clamp-2 min-h-[2.5em]">
          {offer.notes || '\u00A0'}
        </p>

        {isActionable && onOpenReport && reportLabel && (
          <div className="mt-3 border-t border-outline-variant/10 pt-3">
            <button
              onClick={onOpenReport}
              disabled={reportLoading}
              className="inline-flex w-full items-center justify-center gap-2 rounded-xl border border-primary/15 bg-primary/5 px-4 py-2 text-xs font-bold text-primary transition-colors hover:bg-primary/10 disabled:cursor-not-allowed disabled:opacity-50 sm:w-auto"
            >
              <span className="material-symbols-outlined text-sm">
                {reportLabel === t.off_report_view ? 'analytics' : 'description'}
              </span>
              {reportLoading ? t.off_report_loading : reportLabel}
            </button>
          </div>
        )}
      </div>
    </div>
  );
}

function OfferReportSheet({
  open,
  onOpenChange,
  offer,
  report,
  isLoading,
  error,
  onGenerate,
  t,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  offer: OfferResponse | null;
  report: GoNoGoReport | null;
  isLoading: boolean;
  error: Error | null;
  onGenerate: () => void;
  t: Record<string, any>;
}) {
  const [lower, upper] = report ? getConfidenceBounds(report) : [null, null];
  const scoreEntries = report ? Object.entries(report.sub_scores ?? {}) : [];
  const scoreLabelMap: Record<string, string> = {
    academic: t.common_academic,
    financial: t.common_financial,
    career: t.common_career,
    life: t.common_life,
  };

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent side="right" className="w-full overflow-y-auto border-l border-outline-variant/10 bg-background sm:max-w-2xl">
        <SheetHeader className="border-b border-outline-variant/10 px-6 py-5">
          <SheetTitle className="font-headline text-lg font-black text-on-surface">
            {offer ? `${offer.school_name ?? t.common_school} · ${t.off_report_sheet_title}` : t.off_report_sheet_title}
          </SheetTitle>
          <SheetDescription className="text-sm leading-relaxed text-on-surface-variant/70">
            {t.off_report_sheet_desc}
          </SheetDescription>
        </SheetHeader>

        <div className="space-y-6 px-6 py-6">
          {error && (
            <div className="rounded-2xl border border-error/15 bg-error/5 px-4 py-3 text-sm text-error">
              {error.message}
            </div>
          )}

          {!report && !isLoading && (
            <div className="rounded-3xl border border-outline-variant/10 bg-surface-container-lowest p-6 text-center">
              <div className="mx-auto mb-4 flex h-16 w-16 items-center justify-center rounded-2xl bg-primary/10">
                <span className="material-symbols-outlined text-3xl text-primary">analytics</span>
              </div>
              <h3 className="font-headline text-lg font-black text-on-surface">{t.off_report_empty_title}</h3>
              <p className="mt-2 text-sm leading-relaxed text-on-surface-variant/70">
                {t.off_report_empty_desc}
              </p>
              <button
                onClick={onGenerate}
                className="mt-5 inline-flex items-center gap-2 rounded-xl bg-primary px-4 py-2.5 text-sm font-bold text-on-primary shadow-md transition-all hover:brightness-110"
              >
                <span className="material-symbols-outlined text-sm">play_arrow</span>
                {t.off_report_generate}
              </button>
            </div>
          )}

          {isLoading && (
            <div className="space-y-4">
              {[...Array(3)].map((_, index) => (
                <div key={index} className="h-28 animate-pulse rounded-2xl bg-surface-container-high/40" />
              ))}
            </div>
          )}

          {report && !isLoading && (
            <>
              <div className="rounded-3xl border border-outline-variant/10 bg-surface-container-lowest p-6">
                <div className="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
                  <div>
                    <div className="text-[10px] font-bold uppercase tracking-[0.14em] text-on-surface-variant/55">
                      {t.off_report_recommendation}
                    </div>
                    <div className="mt-2 text-2xl font-black text-on-surface">
                      {String(report.recommendation ?? t.common_na)}
                    </div>
                    {lower != null && upper != null && (
                      <div className="mt-2 text-sm text-on-surface-variant/65">
                        {t.common_confidence}: {Math.round(lower * 100)}% - {Math.round(upper * 100)}%
                      </div>
                    )}
                  </div>
                  <div className="rounded-2xl border border-primary/15 bg-primary/5 px-4 py-3 text-right">
                    <div className="text-[10px] font-bold uppercase tracking-[0.14em] text-primary/80">
                      {t.common_overall}
                    </div>
                    <div className="mt-1 text-3xl font-black text-primary">
                      {Math.round((report.overall_score ?? 0) * 100)}%
                    </div>
                  </div>
                </div>
              </div>

              <div className="grid grid-cols-2 gap-3 xl:grid-cols-4">
                {scoreEntries.map(([key, value]) => (
                  <div key={key} className="rounded-2xl border border-outline-variant/10 bg-surface-container-lowest px-4 py-3">
                    <div className="text-[10px] font-bold uppercase tracking-[0.12em] text-on-surface-variant/55">
                      {scoreLabelMap[key] ?? key}
                    </div>
                    <div className="mt-2 text-xl font-black text-on-surface">
                      {Math.round((value ?? 0) * 100)}%
                    </div>
                  </div>
                ))}
              </div>

              <div className="rounded-3xl border border-outline-variant/10 bg-surface-container-lowest p-6">
                <div className="mb-3 text-[10px] font-bold uppercase tracking-[0.14em] text-on-surface-variant/55">
                  {t.off_report_narrative}
                </div>
                <div className="prose prose-sm max-w-none text-sm leading-relaxed text-on-surface prose-headings:text-on-surface prose-strong:text-on-surface prose-p:my-2 prose-ul:my-2 prose-li:my-1">
                  <ReactMarkdown>{report.narrative}</ReactMarkdown>
                </div>
              </div>

              <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
                <div className="rounded-3xl border border-outline-variant/10 bg-surface-container-lowest p-6">
                  <div className="mb-3 text-[10px] font-bold uppercase tracking-[0.14em] text-on-surface-variant/55">
                    {t.dec_top_factors}
                  </div>
                  <ReportList
                    items={report.top_factors ?? []}
                    emptyLabel={t.off_report_empty_factors}
                  />
                </div>

                <div className="rounded-3xl border border-outline-variant/10 bg-surface-container-lowest p-6">
                  <div className="mb-3 text-[10px] font-bold uppercase tracking-[0.14em] text-on-surface-variant/55">
                    {t.dec_risks}
                  </div>
                  <ReportList
                    items={report.risks ?? []}
                    emptyLabel={t.off_report_empty_risks}
                  />
                </div>
              </div>

              <div className="rounded-3xl border border-outline-variant/10 bg-surface-container-lowest p-6">
                <div className="mb-3 text-[10px] font-bold uppercase tracking-[0.14em] text-on-surface-variant/55">
                  {t.off_report_usage_title}
                </div>
                <p className="text-sm leading-relaxed text-on-surface-variant/70">
                  {t.off_report_footer}
                </p>
              </div>
            </>
          )}
        </div>
      </SheetContent>
    </Sheet>
  );
}

// ─── Main Panel ───

interface OffersPanelProps {
  studentId: string | null;
}

export function OffersPanel({ studentId }: OffersPanelProps) {
  const { t } = useApp();
  const { offers, comparison, isLoading, createOffer, compareOffers } = useOffers(studentId);
  const {
    report,
    isLoading: isReportLoading,
    error: reportError,
    generateReport,
    loadReport,
    clearReport,
  } = useReports();
  const { tieredList } = useEvaluations(studentId);
  const [showForm, setShowForm] = useState(false);
  const [comparing, setComparing] = useState(false);
  const [reportSheetOpen, setReportSheetOpen] = useState(false);
  const [selectedReportOffer, setSelectedReportOffer] = useState<OfferResponse | null>(null);
  const [cachedReportIds, setCachedReportIds] = useState<Record<string, string>>({});

  const allEvals: EvaluationWithSchool[] = tieredList
    ? [...tieredList.reach, ...tieredList.target, ...tieredList.safety, ...tieredList.likely]
    : [];

  const admittedOffers = offers.filter((o) => o.status === 'admitted' || o.status === 'committed');

  useEffect(() => {
    if (!studentId) {
      setCachedReportIds({});
      return;
    }

    const nextCache: Record<string, string> = {};
    offers.forEach((offer) => {
      const reportId = window.localStorage.getItem(getOfferReportCacheKey(studentId, offer.id));
      if (reportId) {
        nextCache[offer.id] = reportId;
      }
    });
    setCachedReportIds(nextCache);
  }, [studentId, offers]);

  const updateCachedReport = useMemo(() => ({
    set(offerId: string, reportId: string) {
      if (!studentId) return;
      window.localStorage.setItem(getOfferReportCacheKey(studentId, offerId), reportId);
      setCachedReportIds((prev) => ({ ...prev, [offerId]: reportId }));
    },
    clear(offerId: string) {
      if (!studentId) return;
      window.localStorage.removeItem(getOfferReportCacheKey(studentId, offerId));
      setCachedReportIds((prev) => {
        const next = { ...prev };
        delete next[offerId];
        return next;
      });
    },
  }), [studentId]);

  const handleCompare = async () => {
    setComparing(true);
    await compareOffers();
    setComparing(false);
  };

  const handleCreateOffer = async (data: Record<string, unknown>) => {
    await createOffer(data as Parameters<typeof createOffer>[0]);
    setShowForm(false);
  };

  const openReportSheet = async (offer: OfferResponse) => {
    setSelectedReportOffer(offer);
    setReportSheetOpen(true);
    clearReport();

    const cachedReportId = cachedReportIds[offer.id];
    if (!cachedReportId) return;

    const restored = await loadReport(cachedReportId);
    if (!restored) {
      updateCachedReport.clear(offer.id);
      clearReport();
    }
  };

  const handleGenerateReport = async () => {
    if (!studentId || !selectedReportOffer) return;
    const generated = await generateReport(studentId, selectedReportOffer.id);
    if (generated) {
      updateCachedReport.set(selectedReportOffer.id, generated.id);
    }
  };

  const handleReportSheetOpenChange = (open: boolean) => {
    setReportSheetOpen(open);
    if (!open) {
      setSelectedReportOffer(null);
      clearReport();
    }
  };

  return (
    <AnimatedWorkspacePage className="w-full bg-background font-body">
      <section className="w-full flex flex-col h-full overflow-hidden">
      <header className="sticky top-0 z-20 flex min-h-16 items-center border-b border-outline-variant/10 bg-background/90 px-4 py-3 backdrop-blur-md sm:px-6 lg:px-8">
        <MotionSection role="toolbar">
          <div>
            <h1 className="font-headline text-lg font-black text-on-surface tracking-tight">{t.off_title}</h1>
            <p className="text-[9px] text-on-surface-variant font-bold tracking-[0.1em] uppercase">
              {t.off_subtitle} {offers.length > 0 && `\u2022 ${t.off_offer_count(offers.length)}`}
            </p>
          </div>
        </MotionSection>
      </header>

      <div className="flex-1 overflow-y-auto px-4 py-5 space-y-8 sm:px-6 sm:py-6 lg:px-8 lg:py-8">
        {/* Loading */}
        {isLoading && (
          <div className="space-y-4">
            {[...Array(3)].map((_, i) => (
              <div key={i} className="animate-pulse bg-surface-container-high/60 rounded-2xl h-36" />
            ))}
          </div>
        )}

        {/* Empty State */}
        {!isLoading && offers.length === 0 && (
          <MotionSurface className="flex flex-col items-center justify-center py-24 text-center">
            <div className="w-20 h-20 rounded-3xl bg-surface-container-high/40 flex items-center justify-center mb-6">
              <span className="material-symbols-outlined text-4xl text-on-surface-variant/50">local_offer</span>
            </div>
            <h3 className="font-headline text-xl font-black text-on-surface mb-2">{t.off_empty_title}</h3>
            <p className="text-sm text-on-surface-variant/70 max-w-sm leading-relaxed mb-6">
              {t.off_empty_desc}
            </p>
            <button
              onClick={() => setShowForm(true)}
              className="px-5 py-2.5 bg-primary text-on-primary text-sm font-bold rounded-xl hover:brightness-110 transition-all shadow-md"
            >
              {t.off_first}
            </button>
          </MotionSurface>
        )}

        {/* Summary stats */}
        {!isLoading && offers.length > 0 && (
          <>
            <MotionSection role="toolbar" delay={0.04}>
              <div className="dashboard-toolbar-rail p-4 sm:p-5">
              <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
                <div className="max-w-2xl">
                  <div className="text-[10px] font-bold uppercase tracking-[0.14em] text-on-surface-variant/55">
                    {t.off_actions_kicker}
                  </div>
                  <h2 className="mt-1 font-headline text-base font-black text-on-surface">{t.off_actions_title}</h2>
                  <p className="mt-2 text-sm leading-relaxed text-on-surface-variant/65">
                    {t.off_actions_desc}
                  </p>
                </div>

                <div className="flex flex-col gap-2 sm:flex-row sm:flex-wrap">
                  {admittedOffers.length >= 2 && (
                    <button
                      onClick={handleCompare}
                      disabled={comparing}
                      className="inline-flex w-full items-center justify-center gap-1.5 rounded-xl border border-tertiary/15 bg-tertiary/10 px-4 py-2 text-xs font-bold uppercase tracking-widest text-tertiary transition-colors hover:bg-tertiary/15 disabled:opacity-50 sm:w-auto"
                    >
                      <span className="material-symbols-outlined text-sm">compare_arrows</span>
                      {comparing ? t.off_comparing : t.off_compare}
                    </button>
                  )}
                  <button
                    onClick={() => setShowForm(true)}
                    className="inline-flex w-full items-center justify-center gap-1.5 rounded-xl bg-primary px-4 py-2 text-xs font-bold text-on-primary shadow-md transition-all hover:brightness-110 sm:w-auto"
                  >
                    <span className="material-symbols-outlined text-sm">add</span>
                    {t.off_record}
                  </button>
                </div>
              </div>
              </div>
            </MotionSection>

            <MotionStagger className="grid grid-cols-2 gap-4 xl:grid-cols-4" delay={0.06} stagger={0.05} role="metric">
              <MotionItem role="metric">
                <div className="dashboard-surface-muted p-5 text-center">
                  <div className="text-3xl font-black text-on-surface">{offers.length}</div>
                  <div className="mt-1 text-[9px] font-bold uppercase tracking-widest text-on-surface-variant/50">{t.off_total}</div>
                </div>
              </MotionItem>
              <MotionItem role="metric">
                <div className="dashboard-surface-muted p-5 text-center">
                  <div className="text-3xl font-black text-tertiary">{admittedOffers.length}</div>
                  <div className="mt-1 text-[9px] font-bold uppercase tracking-widest text-on-surface-variant/50">{t.off_admitted}</div>
                </div>
              </MotionItem>
              <MotionItem role="metric">
                <div className="dashboard-surface-muted p-5 text-center">
                  <div className="text-3xl font-black text-on-surface">
                    {admittedOffers.filter((o) => o.net_cost != null).length > 0
                      ? formatCurrency(Math.min(...admittedOffers.filter((o) => o.net_cost != null).map((o) => o.net_cost!)))
                      : '—'}
                  </div>
                  <div className="mt-1 text-[9px] font-bold uppercase tracking-widest text-on-surface-variant/50">{t.off_lowest_cost}</div>
                </div>
              </MotionItem>
              <MotionItem role="metric">
                <div className="dashboard-surface-muted p-5 text-center">
                  <div className="text-3xl font-black text-primary">
                    {admittedOffers.length > 0 ? formatCurrency(Math.max(...admittedOffers.map((o) => o.total_aid))) : '—'}
                  </div>
                  <div className="mt-1 text-[9px] font-bold uppercase tracking-widest text-on-surface-variant/50">{t.off_best_aid}</div>
                </div>
              </MotionItem>
            </MotionStagger>
          </>
        )}

        {/* Offer Cards */}
        {!isLoading && offers.length > 0 && (
          <MotionSection className="space-y-4" delay={0.08}>
            <div className="mb-4">
              <h2 className="font-headline text-base font-black text-on-surface">{t.off_all}</h2>
              <p className="mt-1 text-xs leading-relaxed text-on-surface-variant/60">
                {t.off_all_desc}
              </p>
            </div>
            <MotionStagger className="grid grid-cols-1 gap-4 xl:grid-cols-2" delay={0.04} stagger={0.06}>
              {offers.map((offer) => (
                <MotionItem key={offer.id} role="surface">
                  <OfferCard
                    offer={offer}
                    t={t}
                    onOpenReport={
                      offer.status === 'admitted' || offer.status === 'committed'
                        ? () => { void openReportSheet(offer); }
                        : undefined
                    }
                    reportLabel={cachedReportIds[offer.id] ? t.off_report_view : t.off_report_generate}
                    reportLoading={isReportLoading && selectedReportOffer?.id === offer.id}
                  />
                </MotionItem>
              ))}
            </MotionStagger>
          </MotionSection>
        )}

        {/* Comparison Results */}
        {comparison && (
          <MotionSection delay={0.1}>
            <OfferCompareCard data={normalizeOfferComparison(comparison)} />
          </MotionSection>
        )}

        <div className="h-12" />
      </div>

      {/* Add Offer Modal */}
      {showForm && (
        <AddOfferForm
          evaluations={allEvals}
          onSubmit={handleCreateOffer}
          onCancel={() => setShowForm(false)}
          t={t}
        />
      )}

        <OfferReportSheet
          open={reportSheetOpen}
          onOpenChange={handleReportSheetOpenChange}
          offer={selectedReportOffer}
          report={report}
          isLoading={isReportLoading}
          error={reportError}
          onGenerate={handleGenerateReport}
          t={t}
        />
    </section>
    </AnimatedWorkspacePage>
  );
}

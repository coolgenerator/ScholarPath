import React, { useState } from 'react';
import { useApp } from '../../context/AppContext';
import { useOffers } from '../../hooks/useOffers';
import { useEvaluations } from '../../hooks/useEvaluations';
import type { EvaluationWithSchool } from '../../lib/types';
import type { OfferResponse } from '../../lib/api/offers';

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

// ─── Add Offer Form ───

function AddOfferForm({ evaluations, onSubmit, onCancel, t }: {
  evaluations: EvaluationWithSchool[];
  onSubmit: (data: Record<string, unknown>) => void;
  onCancel: () => void;
  t: Record<string, string>;
}) {
  const [schoolId, setSchoolId] = useState('');
  const [status, setStatus] = useState('admitted');
  // Cost of Attendance
  const [tuition, setTuition] = useState('');
  const [roomAndBoard, setRoomAndBoard] = useState('');
  const [booksSupplies, setBooksSupplies] = useState('');
  const [personalExpenses, setPersonalExpenses] = useState('');
  const [transportation, setTransportation] = useState('');
  // Financial Aid
  const [meritScholarship, setMeritScholarship] = useState('');
  const [needBasedGrant, setNeedBasedGrant] = useState('');
  const [loanOffered, setLoanOffered] = useState('');
  const [workStudy, setWorkStudy] = useState('');
  const [honorsProgram, setHonorsProgram] = useState(false);
  const [notes, setNotes] = useState('');

  const costTotal =
    (Number(tuition) || 0) +
    (Number(roomAndBoard) || 0) +
    (Number(booksSupplies) || 0) +
    (Number(personalExpenses) || 0) +
    (Number(transportation) || 0);
  const aidTotal =
    (Number(meritScholarship) || 0) +
    (Number(needBasedGrant) || 0) +
    (Number(loanOffered) || 0) +
    (Number(workStudy) || 0);
  const netCost = costTotal - aidTotal;

  const handleSubmit = () => {
    if (!schoolId) return;
    onSubmit({
      school_id: schoolId,
      status,
      tuition: Number(tuition) || undefined,
      room_and_board: Number(roomAndBoard) || undefined,
      books_supplies: Number(booksSupplies) || undefined,
      personal_expenses: Number(personalExpenses) || undefined,
      transportation: Number(transportation) || undefined,
      merit_scholarship: Number(meritScholarship) || 0,
      need_based_grant: Number(needBasedGrant) || 0,
      loan_offered: Number(loanOffered) || 0,
      work_study: Number(workStudy) || 0,
      honors_program: honorsProgram,
      notes: notes || undefined,
    });
  };

  return (
    <div className="fixed inset-0 bg-black/30 backdrop-blur-sm z-[100] flex items-center justify-center" onClick={onCancel}>
      <div className="bg-white rounded-3xl shadow-2xl w-[640px] max-h-[85vh] overflow-y-auto" onClick={(e) => e.stopPropagation()}>
        <div className="p-6 border-b border-outline-variant/10 flex items-center justify-between">
          <h3 className="font-headline text-lg font-black text-on-surface">{t.off_form_title}</h3>
          <button onClick={onCancel} className="w-8 h-8 rounded-full flex items-center justify-center hover:bg-surface-container-high">
            <span className="material-symbols-outlined text-on-surface-variant">close</span>
          </button>
        </div>
        <div className="p-6 space-y-6">
          {/* School select */}
          <div>
            <label className="text-[10px] font-bold text-on-surface-variant uppercase tracking-widest mb-1.5 block">{t.off_school}</label>
            <select
              className="w-full bg-surface-container-high/40 border border-outline-variant/20 rounded-xl px-4 py-3 text-sm text-on-surface outline-none focus:border-primary"
              value={schoolId}
              onChange={(e) => setSchoolId(e.target.value)}
            >
              <option value="">{t.off_select_school}</option>
              {evaluations.map((ev) => (
                <option key={ev.school_id} value={ev.school_id}>{ev.school?.name ?? ev.school_id}</option>
              ))}
            </select>
          </div>

          {/* Status */}
          <div>
            <label className="text-[10px] font-bold text-on-surface-variant uppercase tracking-widest mb-1.5 block">{t.off_status}</label>
            <div className="flex gap-2 flex-wrap">
              {['admitted', 'waitlisted', 'denied', 'deferred'].map((s) => (
                <button
                  key={s}
                  onClick={() => setStatus(s)}
                  className={`px-3 py-1.5 rounded-lg text-xs font-bold capitalize transition-colors ${
                    status === s
                      ? `${STATUS_STYLES[s]?.bg} ${STATUS_STYLES[s]?.text}`
                      : 'bg-surface-container-high/30 text-on-surface-variant hover:bg-surface-container-high/50'
                  }`}
                >
                  {s}
                </button>
              ))}
            </div>
          </div>

          {/* Cost of Attendance */}
          <div>
            <div className="flex items-center gap-2 mb-3">
              <span className="material-symbols-outlined text-error/70 text-base">payments</span>
              <span className="text-[10px] font-bold text-on-surface-variant uppercase tracking-widest">Cost of Attendance</span>
            </div>
            <div className="grid grid-cols-3 gap-3">
              {[
                { label: 'Tuition & Fees', value: tuition, set: setTuition, placeholder: 'e.g. 36000' },
                { label: 'Room & Board', value: roomAndBoard, set: setRoomAndBoard, placeholder: 'e.g. 12500' },
                { label: 'Books & Supplies', value: booksSupplies, set: setBooksSupplies, placeholder: 'e.g. 1200' },
                { label: 'Personal Expenses', value: personalExpenses, set: setPersonalExpenses, placeholder: 'e.g. 2400' },
                { label: 'Transportation', value: transportation, set: setTransportation, placeholder: 'e.g. 1500' },
              ].map(({ label, value, set, placeholder }) => (
                <div key={label}>
                  <label className="text-[9px] font-bold text-on-surface-variant/70 uppercase tracking-widest mb-1 block">{label}</label>
                  <input
                    type="number"
                    className="w-full bg-surface-container-high/40 border border-outline-variant/20 rounded-xl px-3 py-2 text-sm text-on-surface outline-none focus:border-primary"
                    placeholder={placeholder}
                    value={value}
                    onChange={(e) => set(e.target.value)}
                  />
                </div>
              ))}
            </div>
          </div>

          {/* Financial Aid */}
          <div>
            <div className="flex items-center gap-2 mb-3">
              <span className="material-symbols-outlined text-tertiary text-base">savings</span>
              <span className="text-[10px] font-bold text-on-surface-variant uppercase tracking-widest">Financial Aid Package</span>
            </div>
            <div className="grid grid-cols-2 gap-3">
              {[
                { label: 'Merit Scholarship', value: meritScholarship, set: setMeritScholarship },
                { label: 'Need-Based Grant', value: needBasedGrant, set: setNeedBasedGrant },
                { label: 'Loan Offered', value: loanOffered, set: setLoanOffered },
                { label: 'Work-Study', value: workStudy, set: setWorkStudy },
              ].map(({ label, value, set }) => (
                <div key={label}>
                  <label className="text-[9px] font-bold text-on-surface-variant/70 uppercase tracking-widest mb-1 block">{label}</label>
                  <input
                    type="number"
                    className="w-full bg-surface-container-high/40 border border-outline-variant/20 rounded-xl px-3 py-2 text-sm text-on-surface outline-none focus:border-primary"
                    placeholder="$0"
                    value={value}
                    onChange={(e) => set(e.target.value)}
                  />
                </div>
              ))}
            </div>
            <div className="flex items-center gap-3 mt-3">
              <input
                type="checkbox"
                id="honors"
                checked={honorsProgram}
                onChange={(e) => setHonorsProgram(e.target.checked)}
                className="w-4 h-4 rounded border-outline-variant/30 text-primary focus:ring-primary/20"
              />
              <label htmlFor="honors" className="text-xs font-bold text-on-surface-variant uppercase tracking-widest">Honors Program</label>
            </div>
          </div>

          {/* Live net cost preview */}
          {costTotal > 0 && (
            <div className="bg-surface-container-lowest rounded-2xl p-4 border border-outline-variant/10">
              <div className="flex justify-between items-center text-sm">
                <span className="text-on-surface-variant">Total Cost</span>
                <span className="font-black text-on-surface">{formatCurrency(costTotal)}</span>
              </div>
              <div className="flex justify-between items-center text-sm mt-1">
                <span className="text-on-surface-variant">Total Aid</span>
                <span className="font-black text-tertiary">−{formatCurrency(aidTotal)}</span>
              </div>
              <div className="border-t border-outline-variant/10 mt-2 pt-2 flex justify-between items-center">
                <span className="text-xs font-bold text-on-surface-variant uppercase tracking-widest">Est. Net Cost / Year</span>
                <span className={`text-lg font-black ${netCost > 0 ? 'text-on-surface' : 'text-tertiary'}`}>{formatCurrency(netCost)}</span>
              </div>
            </div>
          )}

          {/* Notes */}
          <div>
            <label className="text-[10px] font-bold text-on-surface-variant uppercase tracking-widest mb-1 block">{t.off_notes}</label>
            <textarea
              className="w-full bg-surface-container-high/40 border border-outline-variant/20 rounded-xl px-4 py-2.5 text-sm text-on-surface outline-none focus:border-primary resize-none"
              rows={2}
              placeholder="Program, conditions, deadlines..."
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

function OfferCard({ offer, onSelect, isSelected, t }: { offer: OfferResponse; onSelect: () => void; isSelected: boolean; t: Record<string, string> }) {
  const statusStyle = STATUS_STYLES[offer.status] ?? STATUS_STYLES.admitted;
  const hasDeadline = offer.decision_deadline != null;

  return (
    <div
      className={`bg-surface-container-lowest rounded-2xl p-6 border transition-all cursor-pointer ${
        isSelected ? 'border-primary/30 shadow-md ring-2 ring-primary/10' : 'border-outline-variant/10 hover:shadow-sm'
      }`}
      onClick={onSelect}
    >
      <div className="flex items-start justify-between mb-4">
        <div>
          <h3 className="font-headline text-base font-bold text-on-surface">{offer.school_name ?? 'School'}</h3>
          <div className="flex items-center gap-2 mt-1">
            <span className={`inline-block px-2 py-0.5 ${statusStyle.bg} ${statusStyle.text} text-[9px] font-black uppercase tracking-widest rounded-md`}>
              {offer.status}
            </span>
            {offer.honors_program && (
              <span className="px-2 py-0.5 bg-primary/10 text-primary text-[9px] font-black uppercase tracking-widest rounded-md">Honors</span>
            )}
          </div>
        </div>
        <div className="text-right">
          <div className="text-2xl font-black text-on-surface">{formatCurrency(offer.net_cost)}</div>
          <div className="text-[8px] font-bold text-on-surface-variant/50 uppercase tracking-widest">{t.off_net_cost}</div>
        </div>
      </div>

      {/* Cost breakdown */}
      <div className="grid grid-cols-2 gap-x-4 gap-y-1 mb-3">
        {offer.tuition != null && (
          <div className="flex justify-between text-xs">
            <span className="text-on-surface-variant/60">Tuition</span>
            <span className="font-bold text-on-surface">{formatK(offer.tuition)}</span>
          </div>
        )}
        {offer.room_and_board != null && (
          <div className="flex justify-between text-xs">
            <span className="text-on-surface-variant/60">Room & Board</span>
            <span className="font-bold text-on-surface">{formatK(offer.room_and_board)}</span>
          </div>
        )}
        {offer.total_cost != null && (
          <div className="flex justify-between text-xs col-span-2 border-t border-outline-variant/10 pt-1 mt-1">
            <span className="text-on-surface-variant/80 font-bold">Total COA</span>
            <span className="font-black text-on-surface">{formatCurrency(offer.total_cost)}</span>
          </div>
        )}
      </div>

      {/* Aid breakdown */}
      <div className="grid grid-cols-4 gap-2">
        <div className="bg-surface-container-low/40 px-2.5 py-2 rounded-xl border border-outline-variant/5">
          <div className="text-[7px] text-on-surface-variant font-bold uppercase tracking-widest">{t.off_scholarships}</div>
          <div className="text-sm font-black text-tertiary">{formatCurrency(offer.merit_scholarship)}</div>
        </div>
        <div className="bg-surface-container-low/40 px-2.5 py-2 rounded-xl border border-outline-variant/5">
          <div className="text-[7px] text-on-surface-variant font-bold uppercase tracking-widest">{t.off_grants}</div>
          <div className="text-sm font-black text-on-surface">{formatCurrency(offer.need_based_grant)}</div>
        </div>
        <div className="bg-surface-container-low/40 px-2.5 py-2 rounded-xl border border-outline-variant/5">
          <div className="text-[7px] text-on-surface-variant font-bold uppercase tracking-widest">{t.off_loans}</div>
          <div className="text-sm font-black text-error/70">{formatCurrency(offer.loan_offered)}</div>
        </div>
        <div className="bg-surface-container-low/40 px-2.5 py-2 rounded-xl border border-outline-variant/5">
          <div className="text-[7px] text-on-surface-variant font-bold uppercase tracking-widest">Work-Study</div>
          <div className="text-sm font-black text-primary">{formatCurrency(offer.work_study)}</div>
        </div>
      </div>

      {/* Deadline + notes */}
      <div className="mt-3 flex items-center gap-3">
        {hasDeadline && (
          <span className="text-[10px] font-bold text-on-surface-variant/60 flex items-center gap-1">
            <span className="material-symbols-outlined text-xs">calendar_today</span>
            Deadline: {new Date(offer.decision_deadline!).toLocaleDateString()}
          </span>
        )}
      </div>
      {offer.notes && (
        <p className="text-xs text-on-surface-variant/60 mt-2 line-clamp-2">{offer.notes}</p>
      )}
    </div>
  );
}

// ─── Main Panel ───

interface OffersPanelProps {
  studentId: string | null;
}

export function OffersPanel({ studentId }: OffersPanelProps) {
  const { t } = useApp();
  const { offers, comparison, isLoading, createOffer, compareOffers } = useOffers(studentId);
  const { tieredList } = useEvaluations(studentId);
  const [showForm, setShowForm] = useState(false);
  const [selectedOfferIds, setSelectedOfferIds] = useState<Set<string>>(new Set());
  const [comparing, setComparing] = useState(false);

  const allEvals: EvaluationWithSchool[] = tieredList
    ? [...tieredList.reach, ...tieredList.target, ...tieredList.safety, ...tieredList.likely]
    : [];

  const admittedOffers = offers.filter((o) => o.status === 'admitted' || o.status === 'committed');

  const toggleSelect = (id: string) => {
    setSelectedOfferIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const handleCompare = async () => {
    setComparing(true);
    await compareOffers();
    setComparing(false);
  };

  const handleCreateOffer = async (data: Record<string, unknown>) => {
    await createOffer(data as Parameters<typeof createOffer>[0]);
    setShowForm(false);
  };

  return (
    <section className="w-full bg-background flex flex-col h-full overflow-hidden font-body" data-testid="offers-panel">
      <header className="h-16 px-10 flex items-center justify-between sticky top-0 bg-background/90 backdrop-blur-md z-20 border-b border-outline-variant/10">
        <div>
          <h1 className="font-headline text-lg font-black text-on-surface tracking-tight">{t.off_title}</h1>
          <p className="text-[9px] text-on-surface-variant font-bold tracking-[0.1em] uppercase">
            {t.off_subtitle} {offers.length > 0 && `\u2022 ${offers.length} Offers`}
          </p>
        </div>
        <div className="flex items-center gap-2">
          {admittedOffers.length >= 2 && (
            <button
              onClick={handleCompare}
              disabled={comparing}
              className="px-4 py-2 bg-tertiary/10 text-tertiary text-xs font-bold uppercase tracking-widest rounded-xl border border-tertiary/15 hover:bg-tertiary/15 transition-colors flex items-center gap-1.5 disabled:opacity-50"
            >
              <span className="material-symbols-outlined text-sm">compare_arrows</span>
              {comparing ? t.off_comparing : t.off_compare}
            </button>
          )}
          <button
            onClick={() => setShowForm(true)}
            className="px-4 py-2 bg-primary text-on-primary text-xs font-bold rounded-xl hover:brightness-110 transition-all shadow-md flex items-center gap-1.5"
          >
            <span className="material-symbols-outlined text-sm">add</span>
            {t.off_record}
          </button>
        </div>
      </header>

      <div className="flex-1 overflow-y-auto px-10 py-8 space-y-8">
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
          <div className="flex flex-col items-center justify-center py-24 text-center">
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
          </div>
        )}

        {/* Summary stats */}
        {!isLoading && offers.length > 0 && (
          <div className="grid grid-cols-4 gap-4">
            <div className="bg-surface-container-lowest rounded-2xl p-5 border border-outline-variant/10 text-center">
              <div className="text-3xl font-black text-on-surface">{offers.length}</div>
              <div className="text-[9px] font-bold text-on-surface-variant/50 uppercase tracking-widest mt-1">{t.off_total}</div>
            </div>
            <div className="bg-surface-container-lowest rounded-2xl p-5 border border-outline-variant/10 text-center">
              <div className="text-3xl font-black text-tertiary">{admittedOffers.length}</div>
              <div className="text-[9px] font-bold text-on-surface-variant/50 uppercase tracking-widest mt-1">{t.off_admitted}</div>
            </div>
            <div className="bg-surface-container-lowest rounded-2xl p-5 border border-outline-variant/10 text-center">
              <div className="text-3xl font-black text-on-surface">
                {admittedOffers.filter((o) => o.net_cost != null).length > 0
                  ? formatCurrency(Math.min(...admittedOffers.filter((o) => o.net_cost != null).map((o) => o.net_cost!)))
                  : '—'}
              </div>
              <div className="text-[9px] font-bold text-on-surface-variant/50 uppercase tracking-widest mt-1">{t.off_lowest_cost}</div>
            </div>
            <div className="bg-surface-container-lowest rounded-2xl p-5 border border-outline-variant/10 text-center">
              <div className="text-3xl font-black text-primary">
                {admittedOffers.length > 0 ? formatCurrency(Math.max(...admittedOffers.map((o) => o.total_aid))) : '—'}
              </div>
              <div className="text-[9px] font-bold text-on-surface-variant/50 uppercase tracking-widest mt-1">{t.off_best_aid}</div>
            </div>
          </div>
        )}

        {/* Offer Cards */}
        {!isLoading && offers.length > 0 && (
          <div>
            <h2 className="font-headline text-base font-black text-on-surface mb-4">{t.off_all}</h2>
            <div className="grid grid-cols-2 gap-4">
              {offers.map((offer) => (
                <OfferCard
                  key={offer.id}
                  offer={offer}
                  isSelected={selectedOfferIds.has(offer.id)}
                  onSelect={() => toggleSelect(offer.id)}
                  t={t}
                />
              ))}
            </div>
          </div>
        )}

        {/* Comparison Results */}
        {comparison && (
          <div className="bg-surface-container-lowest rounded-3xl p-8 border border-outline-variant/10">
            <div className="flex items-center gap-3 mb-6">
              <div className="w-10 h-10 rounded-xl bg-tertiary/10 flex items-center justify-center">
                <span className="material-symbols-outlined text-tertiary text-xl">compare_arrows</span>
              </div>
              <h3 className="font-headline text-lg font-black text-on-surface">{t.off_comparison}</h3>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-outline-variant/10">
                    <th className="text-left py-3 text-[10px] font-bold text-on-surface-variant uppercase tracking-widest">{t.off_school}</th>
                    <th className="text-right py-3 text-[10px] font-bold text-on-surface-variant uppercase tracking-widest">Tuition</th>
                    <th className="text-right py-3 text-[10px] font-bold text-on-surface-variant uppercase tracking-widest">Total COA</th>
                    <th className="text-right py-3 text-[10px] font-bold text-on-surface-variant uppercase tracking-widest">Total Aid</th>
                    <th className="text-right py-3 text-[10px] font-bold text-on-surface-variant uppercase tracking-widest">{t.off_net_cost}</th>
                    <th className="text-center py-3 text-[10px] font-bold text-on-surface-variant uppercase tracking-widest">Honors</th>
                  </tr>
                </thead>
                <tbody>
                  {comparison.comparison_scores
                    .sort((a, b) => (a.net_cost ?? 0) - (b.net_cost ?? 0))
                    .map((score) => (
                      <tr key={score.offer_id} className="border-b border-outline-variant/5">
                        <td className="py-3 font-bold">{score.school_name ?? score.offer_id}</td>
                        <td className="py-3 text-right">{formatCurrency(score.tuition)}</td>
                        <td className="py-3 text-right">{formatCurrency(score.total_cost)}</td>
                        <td className="py-3 text-right font-bold text-tertiary">{formatCurrency(score.total_aid)}</td>
                        <td className="py-3 text-right font-black">{formatCurrency(score.net_cost)}</td>
                        <td className="py-3 text-center">
                          {score.honors_program ? (
                            <span className="px-2 py-0.5 bg-tertiary/10 text-tertiary text-[9px] font-black rounded-md">YES</span>
                          ) : (
                            <span className="text-on-surface-variant/30">—</span>
                          )}
                        </td>
                      </tr>
                    ))}
                </tbody>
              </table>
            </div>
          </div>
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
    </section>
  );
}

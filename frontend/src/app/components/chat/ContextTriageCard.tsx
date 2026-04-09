import React, { useState } from 'react';
import type { StudentResponse, PortfolioPreferences, StudentPortfolioPatch } from '../../../lib/types';

interface ContextTriageCardProps {
  student: StudentResponse;
  t: Record<string, any>;
  hasOffers: boolean;
  onPatchPortfolio: (patch: StudentPortfolioPatch) => void;
  onNavigate: (nav: string) => void;
}

type TriageStep = 'level' | 'stage' | 'direction' | 'offer_shortcut' | 'done';

/**
 * Progressive chip-selector for missing key context.
 * Skips any step whose data is already present on the student profile.
 */
export function ContextTriageCard({ student, t, hasOffers, onPatchPortfolio, onNavigate }: ContextTriageCardProps) {
  const prefs = (student.preferences ?? {}) as Partial<PortfolioPreferences>;

  const hasLevel = Boolean(student.degree_level && student.degree_level !== 'undergraduate')
    || prefs.application_level != null;
  const hasStage = prefs.application_stage != null;
  const hasInterests = (prefs.interests && prefs.interests.length > 0)
    || (student.intended_majors && student.intended_majors.length > 0);

  const resolveStep = (
    level: boolean,
    stage: boolean,
    stageValue: string | null,
  ): TriageStep => {
    if (!level) return 'level';
    if (!stage) return 'stage';
    if (stageValue === 'applying' && !hasInterests) return 'direction';
    if (stageValue === 'admitted' && !hasOffers) return 'offer_shortcut';
    return 'done';
  };

  const [step, setStep] = useState<TriageStep>(
    () => resolveStep(hasLevel, hasStage, prefs.application_stage ?? null),
  );
  const [selectedDirs, setSelectedDirs] = useState<Set<string>>(new Set(prefs.interests ?? []));

  if (step === 'done') return null;

  const handleLevel = (value: string) => {
    // Map UI value → degree_level DB enum
    const degreeLevel = value === 'graduate' ? 'masters' : 'undergraduate';
    onPatchPortfolio({ identity: { degree_level: degreeLevel } });
    // Also store in preferences for the triage card's own tracking
    onPatchPortfolio({ preferences: { application_level: value } });
    setStep(resolveStep(true, hasStage, prefs.application_stage ?? null));
  };

  const handleStage = (stage: string) => {
    onPatchPortfolio({ preferences: { application_stage: stage } });
    if (stage === 'applying' && !hasInterests) {
      setStep('direction');
    } else if (stage === 'admitted' && !hasOffers) {
      setStep('offer_shortcut');
    } else {
      setStep('done');
    }
  };

  const toggleDir = (dir: string) => {
    setSelectedDirs((prev) => {
      const next = new Set(prev);
      if (next.has(dir)) next.delete(dir);
      else next.add(dir);
      return next;
    });
  };

  const confirmDirs = () => {
    if (selectedDirs.size > 0) {
      onPatchPortfolio({ preferences: { interests: Array.from(selectedDirs) } });
    }
    setStep('done');
  };

  const levelOptions = [
    { value: 'undergrad', label: t.chat_triage_level_undergrad, icon: 'school' },
    { value: 'graduate', label: t.chat_triage_level_graduate, icon: 'history_edu' },
  ];

  const stageOptions = [
    { value: 'researching', label: t.chat_triage_stage_researching, icon: 'search' },
    { value: 'applying', label: t.chat_triage_stage_applying, icon: 'send' },
    ...(hasOffers ? [] : [{ value: 'admitted', label: t.chat_triage_stage_admitted, icon: 'verified' }]),
  ];

  const dirOptions = [
    { value: 'stem', label: t.chat_triage_dir_stem },
    { value: 'business', label: t.chat_triage_dir_business },
    { value: 'humanities', label: t.chat_triage_dir_humanities },
    { value: 'arts', label: t.chat_triage_dir_arts },
    { value: 'social', label: t.chat_triage_dir_social },
    { value: 'health', label: t.chat_triage_dir_health },
  ];

  return (
    <div
      className="chat-animate-chip mt-4 rounded-[1.75rem] border border-outline-variant/10 bg-surface-container-lowest/92 px-4 py-4 shadow-[0_12px_28px_rgba(15,23,42,0.05)] backdrop-blur sm:px-5"
      style={{ animationDelay: '80ms' }}
    >
      <div className="mb-3 text-[10px] font-bold uppercase tracking-[0.16em] text-on-surface-variant/55">
        {t.chat_triage_label}
      </div>

      {step === 'level' && (
        <StepRow label={t.chat_triage_level_label}>
          {levelOptions.map((opt) => (
            <Chip key={opt.value} label={opt.label} icon={opt.icon} selected={false} onClick={() => handleLevel(opt.value)} />
          ))}
        </StepRow>
      )}

      {step === 'stage' && (
        <StepRow label={t.chat_triage_stage_label}>
          {stageOptions.map((opt) => (
            <Chip key={opt.value} label={opt.label} icon={opt.icon} selected={false} onClick={() => handleStage(opt.value)} />
          ))}
        </StepRow>
      )}

      {step === 'direction' && (
        <div>
          <StepRow label={t.chat_triage_direction_label}>
            {dirOptions.map((opt) => (
              <Chip key={opt.value} label={opt.label} selected={selectedDirs.has(opt.value)} onClick={() => toggleDir(opt.value)} multi />
            ))}
          </StepRow>
          {selectedDirs.size > 0 && (
            <button
              onClick={confirmDirs}
              className="mt-3 inline-flex items-center gap-1.5 rounded-full bg-primary px-4 py-2 text-xs font-bold text-white shadow-sm transition-all hover:bg-primary/90"
            >
              <span className="material-symbols-outlined text-sm">check</span>
              OK
            </button>
          )}
        </div>
      )}

      {step === 'offer_shortcut' && (
        <button
          onClick={() => onNavigate('offers')}
          className="flex w-full items-center gap-3 rounded-2xl border border-primary/12 bg-primary/5 px-4 py-3 text-left transition-all hover:border-primary/25 hover:bg-primary/8"
        >
          <span className="material-symbols-outlined text-lg text-primary">local_offer</span>
          <span className="flex-1 text-sm font-bold text-on-surface">{t.chat_triage_go_offers}</span>
          <span className="material-symbols-outlined text-[18px] text-primary/60">arrow_forward</span>
        </button>
      )}
    </div>
  );
}

function StepRow({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div>
      <div className="mb-2 text-xs font-semibold text-on-surface-variant/65">{label}</div>
      <div className="flex flex-wrap gap-2">{children}</div>
    </div>
  );
}

function Chip({ label, icon, selected, onClick, multi }: {
  label: string; icon?: string; selected: boolean; onClick: () => void; multi?: boolean;
}) {
  return (
    <button
      onClick={onClick}
      className={`inline-flex items-center gap-1.5 rounded-full border px-3.5 py-2 text-xs font-semibold transition-all duration-200
        ${selected
          ? 'border-primary/30 bg-primary/10 text-primary shadow-sm'
          : 'border-outline-variant/12 bg-white text-on-surface-variant/75 hover:border-primary/20 hover:bg-primary/5'
        }`}
    >
      {icon && (
        <span className={`material-symbols-outlined text-sm ${selected ? 'text-primary' : 'text-on-surface-variant/50'}`}>
          {icon}
        </span>
      )}
      {multi && selected && (
        <span className="material-symbols-outlined text-sm text-primary">check</span>
      )}
      {label}
    </button>
  );
}

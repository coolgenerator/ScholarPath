import React, { useState, useCallback } from 'react';
import type { GuidedQuestion } from '../../hooks/useChat';
import { useApp } from '../../context/AppContext';
import { DashboardInput } from './ui/dashboard-input';
import { DashboardSegmentedGroup, DashboardSegmentedItem } from './ui/dashboard-segmented';
import {
  StructuredCardHeader,
  StructuredCardSection,
  StructuredCardShell,
} from './StructuredCardPrimitives';

interface Props {
  questions: GuidedQuestion[];
  onSubmit: (answers: Record<string, string | string[]>) => void;
}

export function GuidedQuestionCard({ questions, onSubmit }: Props) {
  const { t, locale } = useApp();
  const [currentIndex, setCurrentIndex] = useState(0);
  const [answers, setAnswers] = useState<Record<string, string | string[]>>({});
  const [customInputs, setCustomInputs] = useState<Record<string, string>>({});

  const question = questions[currentIndex];
  const totalSteps = questions.length;
  const isLast = currentIndex === totalSteps - 1;

  const selectedValues = answers[question.id] || (question.multi_select ? [] : '');

  const handleCustomInputChange = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      setCustomInputs((prev) => ({ ...prev, [question.id]: e.target.value }));
    },
    [question.id],
  );

  const handleCustomInputKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        const val = customInputs[question.id]?.trim();
        if (val) {
          if (question.multi_select) {
            setAnswers((prev) => {
              const arr = Array.isArray(prev[question.id]) ? [...(prev[question.id] as string[])] : [];
              if (!arr.includes(val)) arr.push(val);
              return { ...prev, [question.id]: arr };
            });
          } else {
            setAnswers((prev) => ({ ...prev, [question.id]: val }));
          }
          setCustomInputs((prev) => ({ ...prev, [question.id]: '' }));
        }
      }
    },
    [question.id, question.multi_select, customInputs],
  );

  const handleNext = () => {
    if (isLast) {
      // Merge any pending custom input
      const finalAnswers = { ...answers };
      for (const q of questions) {
        const custom = customInputs[q.id]?.trim();
        if (custom && !finalAnswers[q.id]) {
          finalAnswers[q.id] = custom;
        }
      }
      onSubmit(finalAnswers);
    } else {
      setCurrentIndex((i) => i + 1);
    }
  };

  const handleBack = () => {
    setCurrentIndex((i) => Math.max(0, i - 1));
  };

  // Check if any option is known to have an icon
  const hasIcons = question.options.some((o) => o.icon);

  return (
    <StructuredCardShell className="overflow-hidden">
      <StructuredCardHeader
        kicker={t.chat_guided_title}
        title={question.title}
        description={question.description}
        badge={totalSteps > 1 ? `${currentIndex + 1}/${totalSteps}` : (question.multi_select ? t.chat_guided_multi : null)}
        aside={totalSteps > 1 ? (
          <div className="structured-guided-progress">
            {questions.map((_, i) => (
              <div
                key={i}
                className={`structured-guided-progress-segment ${i <= currentIndex ? 'is-active' : ''}`}
              />
            ))}
          </div>
        ) : null}
      />

      <StructuredCardSection
        title={question.multi_select ? t.chat_guided_multi : undefined}
        contentClassName="space-y-3"
      >
        {question.multi_select ? (
          <DashboardSegmentedGroup
            type="multiple"
            size="compact"
            value={Array.isArray(selectedValues) ? selectedValues : []}
            onValueChange={(values) => {
              setAnswers((prev) => ({ ...prev, [question.id]: values }));
            }}
          >
            {question.options.map((opt) => (
              <DashboardSegmentedItem key={opt.value} value={opt.value} size="compact" accent="primary">
                {hasIcons && opt.icon ? (
                  <span className="material-symbols-outlined text-[16px]" style={{ fontVariationSettings: "'FILL' 1" }}>
                    {opt.icon}
                  </span>
                ) : null}
                {opt.label}
              </DashboardSegmentedItem>
            ))}
          </DashboardSegmentedGroup>
        ) : (
          <DashboardSegmentedGroup
            type="single"
            allowDeselect
            size="compact"
            value={typeof selectedValues === 'string' ? selectedValues : ''}
            onValueChange={(value) => {
              setAnswers((prev) => {
                if (!value) {
                  const { [question.id]: _removed, ...rest } = prev;
                  return rest;
                }
                return { ...prev, [question.id]: value };
              });
            }}
          >
            {question.options.map((opt) => (
              <DashboardSegmentedItem key={opt.value} value={opt.value} size="compact" accent="primary">
                {hasIcons && opt.icon ? (
                  <span className="material-symbols-outlined text-[16px]" style={{ fontVariationSettings: "'FILL' 1" }}>
                    {opt.icon}
                  </span>
                ) : null}
                {opt.label}
              </DashboardSegmentedItem>
            ))}
          </DashboardSegmentedGroup>
        )}
      </StructuredCardSection>

      {question.allow_custom ? (
        <StructuredCardSection title={locale === 'zh' ? '补充说明' : 'Add your own detail'}>
          <DashboardInput
            type="text"
            value={customInputs[question.id] || ''}
            onChange={handleCustomInputChange}
            onKeyDown={handleCustomInputKeyDown}
            placeholder={question.custom_placeholder || t.chat_guided_custom_placeholder}
            className="w-full py-2 text-xs"
          />
        </StructuredCardSection>
      ) : null}

      <div className="structured-guided-footer">
        <div>
          {currentIndex > 0 && (
            <button
              onClick={handleBack}
              className="structured-guided-back"
            >
              <span className="material-symbols-outlined text-sm">arrow_back</span>
              {t.chat_guided_back}
            </button>
          )}
        </div>
        <button
          onClick={handleNext}
          className="structured-guided-primary"
        >
          {isLast ? t.chat_guided_submit : t.chat_guided_next}
          <span className="material-symbols-outlined text-sm">
            {isLast ? 'check' : 'arrow_forward'}
          </span>
        </button>
      </div>
    </StructuredCardShell>
  );
}

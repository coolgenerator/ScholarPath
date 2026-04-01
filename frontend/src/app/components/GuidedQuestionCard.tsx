import React, { useState, useCallback } from 'react';
import type { GuidedQuestion } from '../../hooks/useChat';

interface Props {
  questions: GuidedQuestion[];
  onSubmit: (answers: Record<string, string | string[]>) => void;
}

export function GuidedQuestionCard({ questions, onSubmit }: Props) {
  const [currentIndex, setCurrentIndex] = useState(0);
  const [answers, setAnswers] = useState<Record<string, string | string[]>>({});
  const [customInputs, setCustomInputs] = useState<Record<string, string>>({});

  const question = questions[currentIndex];
  const totalSteps = questions.length;
  const isLast = currentIndex === totalSteps - 1;

  const selectedValues = answers[question.id] || (question.multi_select ? [] : '');

  const handleOptionClick = useCallback(
    (value: string) => {
      setAnswers((prev) => {
        const current = prev[question.id];
        if (question.multi_select) {
          const arr = Array.isArray(current) ? [...current] : [];
          const idx = arr.indexOf(value);
          if (idx >= 0) {
            arr.splice(idx, 1);
          } else {
            arr.push(value);
          }
          return { ...prev, [question.id]: arr };
        }
        // Single-select: toggle off if same value clicked
        if (current === value) {
          const { [question.id]: _, ...rest } = prev;
          return rest;
        }
        return { ...prev, [question.id]: value };
      });
    },
    [question.id, question.multi_select],
  );

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

  const isOptionSelected = (value: string) => {
    if (question.multi_select) {
      return Array.isArray(selectedValues) && selectedValues.includes(value);
    }
    return selectedValues === value;
  };

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
    <div className="mt-3 bg-white rounded-2xl border border-outline-variant/20 shadow-sm overflow-hidden transition-all duration-300">
      {/* Step indicator */}
      {totalSteps > 1 && (
        <div className="px-5 pt-4 flex items-center gap-2">
          {questions.map((_, i) => (
            <div
              key={i}
              className={`h-1 flex-1 rounded-full transition-colors duration-300 ${
                i <= currentIndex ? 'bg-primary' : 'bg-outline-variant/20'
              }`}
            />
          ))}
          <span className="ml-2 text-[10px] font-bold text-on-surface-variant/50 uppercase tracking-widest">
            {currentIndex + 1}/{totalSteps}
          </span>
        </div>
      )}

      {/* Question title */}
      <div className="px-5 pt-4 pb-2">
        <h3 className="font-headline text-sm font-bold text-on-surface">{question.title}</h3>
        {question.description && (
          <p className="text-xs text-on-surface-variant/70 mt-1">{question.description}</p>
        )}
        {question.multi_select && (
          <span className="inline-block mt-1 text-[10px] font-bold text-primary/70 uppercase tracking-wider">
            Select multiple
          </span>
        )}
      </div>

      {/* Options */}
      <div className="px-5 pb-3 flex flex-wrap gap-2">
        {question.options.map((opt) => (
          <button
            key={opt.value}
            onClick={() => handleOptionClick(opt.value)}
            className={`inline-flex items-center gap-1.5 px-3.5 py-2 rounded-xl text-xs font-bold transition-all duration-200 border ${
              isOptionSelected(opt.value)
                ? 'bg-primary text-on-primary border-primary shadow-md shadow-primary/15 scale-[1.02]'
                : 'bg-surface-container-high/30 text-on-surface border-outline-variant/15 hover:bg-primary/5 hover:border-primary/30'
            }`}
          >
            {hasIcons && opt.icon && (
              <span
                className={`material-symbols-outlined text-[16px] ${
                  isOptionSelected(opt.value) ? 'text-on-primary' : 'text-primary'
                }`}
                style={{ fontVariationSettings: "'FILL' 1" }}
              >
                {opt.icon}
              </span>
            )}
            {opt.label}
          </button>
        ))}
      </div>

      {/* Custom input */}
      {question.allow_custom && (
        <div className="px-5 pb-3">
          <input
            type="text"
            value={customInputs[question.id] || ''}
            onChange={handleCustomInputChange}
            onKeyDown={handleCustomInputKeyDown}
            placeholder={question.custom_placeholder || 'Type your answer...'}
            className="w-full px-3.5 py-2 text-xs bg-surface-container-highest/50 border border-outline-variant/15 rounded-xl placeholder:text-on-surface-variant/40 focus:outline-none focus:border-primary/40 focus:ring-2 focus:ring-primary/5 transition-all"
          />
        </div>
      )}

      {/* Navigation buttons */}
      <div className="px-5 pb-4 flex items-center justify-between">
        <div>
          {currentIndex > 0 && (
            <button
              onClick={handleBack}
              className="px-4 py-2 text-xs font-bold text-on-surface-variant/70 hover:text-on-surface transition-colors flex items-center gap-1"
            >
              <span className="material-symbols-outlined text-sm">arrow_back</span>
              Back
            </button>
          )}
        </div>
        <button
          onClick={handleNext}
          className="px-5 py-2 bg-primary text-on-primary text-xs font-bold rounded-xl hover:scale-105 transition-transform shadow-lg shadow-primary/20 flex items-center gap-1.5"
        >
          {isLast ? 'Submit' : 'Next'}
          <span className="material-symbols-outlined text-sm">
            {isLast ? 'check' : 'arrow_forward'}
          </span>
        </button>
      </div>
    </div>
  );
}

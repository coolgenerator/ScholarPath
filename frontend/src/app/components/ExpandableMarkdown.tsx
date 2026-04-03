import React, { useState } from 'react';
import ReactMarkdown from 'react-markdown';
import { useApp } from '../../context/AppContext';

interface ExpandableMarkdownProps {
  content: string;
  className?: string;
  collapsedHeightClassName?: string;
  defaultCollapsedOnMobile?: boolean;
}

export function ExpandableMarkdown({
  content,
  className = '',
  collapsedHeightClassName = 'max-h-[11.5rem]',
  defaultCollapsedOnMobile = true,
}: ExpandableMarkdownProps) {
  const { t } = useApp();
  const [expanded, setExpanded] = useState(false);
  const shouldCollapse = defaultCollapsedOnMobile && !expanded;

  return (
    <div>
      <div className="relative">
        <div
          className={[
            className,
            shouldCollapse ? `overflow-hidden ${collapsedHeightClassName} md:max-h-none md:overflow-visible` : '',
          ].join(' ').trim()}
        >
          <ReactMarkdown>{content}</ReactMarkdown>
        </div>
        {shouldCollapse && (
          <div className="pointer-events-none absolute inset-x-0 bottom-0 h-14 bg-gradient-to-t from-white via-white/88 to-white/0 md:hidden" />
        )}
      </div>
      {defaultCollapsedOnMobile && (
        <button
          type="button"
          onClick={() => setExpanded((prev) => !prev)}
          className="mt-2 inline-flex items-center gap-1 text-xs font-bold text-primary md:hidden"
        >
          {expanded ? t.common_collapse : t.common_expand}
          <span className="material-symbols-outlined text-sm">
            {expanded ? 'expand_less' : 'expand_more'}
          </span>
        </button>
      )}
    </div>
  );
}

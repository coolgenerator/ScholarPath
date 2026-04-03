import React from 'react';
import { cn } from './ui/utils';

interface StructuredCardShellProps {
  children: React.ReactNode;
  className?: string;
}

export function StructuredCardShell({ children, className }: StructuredCardShellProps) {
  return <section className={cn('structured-card', className)}>{children}</section>;
}

interface StructuredCardHeaderProps {
  kicker: React.ReactNode;
  title: React.ReactNode;
  description?: React.ReactNode;
  badge?: React.ReactNode;
  aside?: React.ReactNode;
  className?: string;
}

export function StructuredCardHeader({
  kicker,
  title,
  description,
  badge,
  aside,
  className,
}: StructuredCardHeaderProps) {
  return (
    <div className={cn('structured-card-header', className)}>
      <div className="structured-card-header-copy">
        <div className="structured-card-kicker">{kicker}</div>
        <h3 className="structured-card-title">{title}</h3>
        {description ? <p className="structured-card-description">{description}</p> : null}
      </div>
      {(badge || aside) ? (
        <div className="structured-card-header-meta">
          {badge ? <div className="structured-card-badge">{badge}</div> : null}
          {aside ? <div className="structured-card-header-aside">{aside}</div> : null}
        </div>
      ) : null}
    </div>
  );
}

interface StructuredCardSectionProps {
  children: React.ReactNode;
  className?: string;
  title?: React.ReactNode;
  subtitle?: React.ReactNode;
  contentClassName?: string;
}

export function StructuredCardSection({
  children,
  className,
  title,
  subtitle,
  contentClassName,
}: StructuredCardSectionProps) {
  return (
    <div className={cn('structured-section', className)}>
      {title ? <div className="structured-section-title">{title}</div> : null}
      {subtitle ? <div className="structured-section-subtitle">{subtitle}</div> : null}
      <div className={cn(contentClassName)}>{children}</div>
    </div>
  );
}

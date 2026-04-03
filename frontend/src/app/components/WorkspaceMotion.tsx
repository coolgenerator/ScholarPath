import React from 'react';
import { motion, useReducedMotion } from 'motion/react';
import { cn } from './ui/utils';

const EASE_OUT = [0.22, 1, 0.36, 1] as const;

function getOffset(reduced: boolean, axis: 'x' | 'y', amount: number) {
  if (reduced) return {};
  return axis === 'x' ? { x: amount } : { y: amount };
}

export function AnimatedWorkspacePage({
  children,
  className,
}: {
  children: React.ReactNode;
  className?: string;
}) {
  const reduceMotion = useReducedMotion();

  return (
    <motion.div
      className={cn('workspace-page flex h-full min-h-0 flex-col', className)}
      initial={reduceMotion ? { opacity: 0 } : { opacity: 0, x: 20, scale: 0.992 }}
      animate={{ opacity: 1, x: 0, y: 0, scale: 1 }}
      exit={reduceMotion ? { opacity: 0 } : { opacity: 0, x: -18, scale: 0.992 }}
      transition={{ duration: reduceMotion ? 0.18 : 0.44, ease: EASE_OUT }}
    >
      {children}
    </motion.div>
  );
}

export function MotionSection({
  children,
  className,
  delay = 0,
  role = 'section',
}: {
  children: React.ReactNode;
  className?: string;
  delay?: number;
  role?: 'section' | 'toolbar' | 'surface' | 'metric';
}) {
  const reduceMotion = useReducedMotion();
  const axis = role === 'toolbar' ? 'x' : 'y';
  const amount = role === 'metric' ? 10 : role === 'toolbar' ? 18 : 24;

  return (
    <motion.div
      className={cn(className)}
      initial={{ opacity: 0, ...getOffset(reduceMotion, axis, amount) }}
      animate={{ opacity: 1, x: 0, y: 0 }}
      transition={{ duration: reduceMotion ? 0.18 : 0.48, ease: EASE_OUT, delay }}
    >
      {children}
    </motion.div>
  );
}

export function MotionStagger({
  children,
  className,
  delay = 0,
  stagger = 0.075,
  role = 'section',
}: {
  children: React.ReactNode;
  className?: string;
  delay?: number;
  stagger?: number;
  role?: 'section' | 'toolbar' | 'surface' | 'metric';
}) {
  const reduceMotion = useReducedMotion();

  return (
    <motion.div
      className={className}
      initial="hidden"
      animate="visible"
      variants={{
        hidden: {},
        visible: {
          transition: {
            delayChildren: delay,
            staggerChildren: reduceMotion ? 0.02 : stagger,
          },
        },
      }}
      data-motion-group={role}
    >
      {children}
    </motion.div>
  );
}

export function MotionItem({
  children,
  className,
  role = 'surface',
}: {
  children: React.ReactNode;
  className?: string;
  role?: 'section' | 'toolbar' | 'surface' | 'metric';
}) {
  const reduceMotion = useReducedMotion();
  const axis = role === 'toolbar' ? 'x' : 'y';
  const amount = role === 'metric' ? 8 : role === 'toolbar' ? 14 : 18;

  return (
    <motion.div
      className={className}
      variants={{
        hidden: { opacity: 0, ...getOffset(reduceMotion, axis, amount) },
        visible: {
          opacity: 1,
          x: 0,
          y: 0,
          transition: { duration: reduceMotion ? 0.16 : 0.42, ease: EASE_OUT },
        },
      }}
    >
      {children}
    </motion.div>
  );
}

export function MotionSurface({
  children,
  className,
  delay = 0,
  layout = false,
}: {
  children: React.ReactNode;
  className?: string;
  delay?: number;
  layout?: boolean;
}) {
  const reduceMotion = useReducedMotion();

  return (
    <motion.div
      layout={layout}
      className={cn('dashboard-surface', className)}
      initial={reduceMotion ? { opacity: 0 } : { opacity: 0, y: 18, scale: 0.992 }}
      animate={{ opacity: 1, x: 0, y: 0, scale: 1 }}
      transition={{ duration: reduceMotion ? 0.18 : 0.46, ease: EASE_OUT, delay, layout: { duration: 0.34, ease: EASE_OUT } }}
      data-surface="dashboard"
    >
      {children}
    </motion.div>
  );
}

import React from 'react';
import { motion, useReducedMotion } from 'motion/react';

import { cn } from './ui/utils';

const EASE_OUT = [0.22, 1, 0.36, 1] as const;

function buildOffset(axis: 'x' | 'y', amount: number, reduced: boolean) {
  if (reduced) {
    return {};
  }

  return axis === 'x' ? { x: amount } : { y: amount };
}

type RevealMode = 'immediate' | 'view';

export function MarketingReveal({
  children,
  className,
  delay = 0,
  axis = 'y',
  amount = 28,
  scale = 0.985,
  mode = 'view',
  once = true,
  viewportAmount = 0.24,
  blur = true,
}: {
  children: React.ReactNode;
  className?: string;
  delay?: number;
  axis?: 'x' | 'y';
  amount?: number;
  scale?: number;
  mode?: RevealMode;
  once?: boolean;
  viewportAmount?: number;
  blur?: boolean;
}) {
  const reduceMotion = useReducedMotion();
  const hidden = {
    opacity: 0,
    scale: reduceMotion ? 1 : scale,
    filter: reduceMotion || !blur ? 'blur(0px)' : 'blur(10px)',
    ...buildOffset(axis, amount, reduceMotion),
  };
  const visible = {
    opacity: 1,
    x: 0,
    y: 0,
    scale: 1,
    filter: 'blur(0px)',
  };
  const transition = {
    duration: reduceMotion ? 0.18 : 0.56,
    ease: EASE_OUT,
    delay,
  };

  if (mode === 'immediate') {
    return (
      <motion.div className={cn(className)} initial={hidden} animate={visible} transition={transition}>
        {children}
      </motion.div>
    );
  }

  return (
    <motion.div
      className={cn(className)}
      initial={hidden}
      whileInView={visible}
      viewport={{ once, amount: viewportAmount }}
      transition={transition}
    >
      {children}
    </motion.div>
  );
}

export function MarketingStagger({
  children,
  className,
  delay = 0,
  stagger = 0.08,
  mode = 'view',
  once = true,
  viewportAmount = 0.24,
}: {
  children: React.ReactNode;
  className?: string;
  delay?: number;
  stagger?: number;
  mode?: RevealMode;
  once?: boolean;
  viewportAmount?: number;
}) {
  const reduceMotion = useReducedMotion();
  const variants = {
    hidden: {},
    visible: {
      transition: {
        delayChildren: delay,
        staggerChildren: reduceMotion ? 0.03 : stagger,
      },
    },
  };

  if (mode === 'immediate') {
    return (
      <motion.div className={cn(className)} initial="hidden" animate="visible" variants={variants}>
        {children}
      </motion.div>
    );
  }

  return (
    <motion.div
      className={cn(className)}
      initial="hidden"
      whileInView="visible"
      viewport={{ once, amount: viewportAmount }}
      variants={variants}
    >
      {children}
    </motion.div>
  );
}

export function MarketingStaggerItem({
  children,
  className,
  axis = 'y',
  amount = 18,
  scale = 0.992,
  blur = true,
}: {
  children: React.ReactNode;
  className?: string;
  axis?: 'x' | 'y';
  amount?: number;
  scale?: number;
  blur?: boolean;
}) {
  const reduceMotion = useReducedMotion();

  return (
    <motion.div
      className={cn(className)}
      variants={{
        hidden: {
          opacity: 0,
          scale: reduceMotion ? 1 : scale,
          filter: reduceMotion || !blur ? 'blur(0px)' : 'blur(10px)',
          ...buildOffset(axis, amount, reduceMotion),
        },
        visible: {
          opacity: 1,
          x: 0,
          y: 0,
          scale: 1,
          filter: 'blur(0px)',
          transition: {
            duration: reduceMotion ? 0.16 : 0.46,
            ease: EASE_OUT,
          },
        },
      }}
    >
      {children}
    </motion.div>
  );
}

export function MarketingFloat({
  children,
  className,
  y = 12,
  x = 0,
  rotate = 1.2,
  duration = 13,
  delay = 0,
}: {
  children: React.ReactNode;
  className?: string;
  y?: number;
  x?: number;
  rotate?: number;
  duration?: number;
  delay?: number;
}) {
  const reduceMotion = useReducedMotion();

  if (reduceMotion) {
    return <div className={cn(className)}>{children}</div>;
  }

  return (
    <motion.div
      className={cn(className)}
      animate={{
        y: [0, -y, 0],
        x: x === 0 ? [0, 0, 0] : [0, x, 0],
        rotate: rotate === 0 ? [0, 0, 0] : [0, rotate, 0],
      }}
      transition={{
        duration,
        delay,
        repeat: Infinity,
        repeatType: 'mirror',
        ease: 'easeInOut',
      }}
    >
      {children}
    </motion.div>
  );
}

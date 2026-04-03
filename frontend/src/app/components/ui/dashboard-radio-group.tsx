"use client";

import * as React from "react";
import * as RadioGroupPrimitive from "@radix-ui/react-radio-group";
import { CircleIcon } from "lucide-react";

import { cn } from "./utils";

function DashboardRadioGroup({
  className,
  ...props
}: React.ComponentPropsWithoutRef<typeof RadioGroupPrimitive.Root>) {
  return (
    <RadioGroupPrimitive.Root
      data-slot="dashboard-radio-group"
      className={cn("dashboard-radio-group", className)}
      {...props}
    />
  );
}

const DashboardRadioItem = React.forwardRef<
  React.ElementRef<typeof RadioGroupPrimitive.Item>,
  React.ComponentPropsWithoutRef<typeof RadioGroupPrimitive.Item>
>(({ className, ...props }, ref) => {
  return (
    <RadioGroupPrimitive.Item
      ref={ref}
      data-slot="dashboard-radio-item"
      className={cn("dashboard-radio-item", className)}
      {...props}
    >
      <RadioGroupPrimitive.Indicator
        data-slot="dashboard-radio-indicator"
        className="relative flex items-center justify-center"
      >
        <CircleIcon className="absolute left-1/2 top-1/2 size-2 fill-current -translate-x-1/2 -translate-y-1/2" />
      </RadioGroupPrimitive.Indicator>
    </RadioGroupPrimitive.Item>
  );
});

DashboardRadioItem.displayName = "DashboardRadioItem";

function DashboardRadioOption({
  className,
  label,
  description,
  itemClassName,
  ...props
}: React.ComponentPropsWithoutRef<typeof DashboardRadioItem> & {
  label: React.ReactNode;
  description?: React.ReactNode;
  itemClassName?: string;
}) {
  return (
    <label className={cn("dashboard-radio-option", className)}>
      <DashboardRadioItem className={itemClassName} {...props} />
      <span className="min-w-0 flex-1">
        <span className="dashboard-radio-label">{label}</span>
        {description ? (
          <span className="dashboard-radio-description">{description}</span>
        ) : null}
      </span>
    </label>
  );
}

export { DashboardRadioGroup, DashboardRadioItem, DashboardRadioOption };

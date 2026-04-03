"use client";

import * as React from "react";
import * as CheckboxPrimitive from "@radix-ui/react-checkbox";
import { CheckIcon } from "lucide-react";

import { cn } from "./utils";

const DashboardCheckbox = React.forwardRef<
  React.ElementRef<typeof CheckboxPrimitive.Root>,
  React.ComponentPropsWithoutRef<typeof CheckboxPrimitive.Root>
>(({ className, ...props }, ref) => {
  return (
    <CheckboxPrimitive.Root
      ref={ref}
      data-slot="dashboard-checkbox"
      className={cn("dashboard-checkbox", className)}
      {...props}
    >
      <CheckboxPrimitive.Indicator
        data-slot="dashboard-checkbox-indicator"
        className="flex items-center justify-center text-current"
      >
        <CheckIcon className="size-3.5" />
      </CheckboxPrimitive.Indicator>
    </CheckboxPrimitive.Root>
  );
});

DashboardCheckbox.displayName = "DashboardCheckbox";

function DashboardCheckboxField({
  className,
  checkboxClassName,
  label,
  description,
  id,
  children,
  ...props
}: React.ComponentPropsWithoutRef<typeof DashboardCheckbox> & {
  checkboxClassName?: string;
  label: React.ReactNode;
  description?: React.ReactNode;
  children?: React.ReactNode;
}) {
  const generatedId = React.useId();
  const checkboxId = id ?? generatedId;

  return (
    <label htmlFor={checkboxId} className={cn("dashboard-checkbox-field", className)}>
      <DashboardCheckbox
        id={checkboxId}
        className={checkboxClassName}
        {...props}
      />
      <span className="dashboard-checkbox-copy">
        <span className="dashboard-checkbox-label">{label}</span>
        {description ? (
          <span className="dashboard-checkbox-description">{description}</span>
        ) : null}
        {children}
      </span>
    </label>
  );
}

export { DashboardCheckbox, DashboardCheckboxField };

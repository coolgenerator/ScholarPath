import * as React from "react";

import { cn } from "./utils";

type DashboardInputVariant = "default" | "compact" | "rail" | "hero" | "metric";

function DashboardInput({
  className,
  type,
  variant = "default",
  ...props
}: React.ComponentProps<"input"> & {
  variant?: DashboardInputVariant;
}) {
  return (
    <input
      type={type}
      data-slot="dashboard-input"
      data-variant={variant}
      className={cn(
        "dashboard-input placeholder:text-on-surface-variant/55 selection:bg-primary selection:text-on-primary disabled:pointer-events-none disabled:cursor-not-allowed disabled:opacity-50",
        type === "number" && "[appearance:textfield] [&::-webkit-inner-spin-button]:appearance-none [&::-webkit-outer-spin-button]:appearance-none",
        className,
      )}
      {...props}
    />
  );
}

function DashboardTextarea({
  className,
  ...props
}: React.ComponentProps<"textarea">) {
  return (
    <textarea
      data-slot="dashboard-textarea"
      className={cn(
        "dashboard-textarea placeholder:text-on-surface-variant/55 selection:bg-primary selection:text-on-primary disabled:pointer-events-none disabled:cursor-not-allowed disabled:opacity-50",
        className,
      )}
      {...props}
    />
  );
}

export { DashboardInput, DashboardTextarea };

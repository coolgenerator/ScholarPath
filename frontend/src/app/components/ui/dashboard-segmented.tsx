"use client";

import * as React from "react";
import * as ToggleGroupPrimitive from "@radix-ui/react-toggle-group";

import { cn } from "./utils";

type DashboardSegmentedAccent =
  | "primary"
  | "tertiary"
  | "neutral"
  | "status-admitted"
  | "status-waitlisted"
  | "status-denied"
  | "status-deferred";

type DashboardSegmentedSize = "default" | "compact";

type DashboardSegmentedContextValue = {
  accent: DashboardSegmentedAccent;
  size: DashboardSegmentedSize;
};

const DashboardSegmentedContext = React.createContext<DashboardSegmentedContextValue>({
  accent: "primary",
  size: "default",
});

type DashboardSegmentedGroupSingleProps = Omit<
  React.ComponentPropsWithoutRef<typeof ToggleGroupPrimitive.Root>,
  "type" | "value" | "defaultValue" | "onValueChange"
> & {
  type: "single";
  value?: string;
  defaultValue?: string;
  onValueChange?: (value: string) => void;
  allowDeselect?: boolean;
  accent?: DashboardSegmentedAccent;
  size?: DashboardSegmentedSize;
};

type DashboardSegmentedGroupMultipleProps = Omit<
  React.ComponentPropsWithoutRef<typeof ToggleGroupPrimitive.Root>,
  "type" | "value" | "defaultValue" | "onValueChange"
> & {
  type: "multiple";
  value?: string[];
  defaultValue?: string[];
  onValueChange?: (value: string[]) => void;
  accent?: DashboardSegmentedAccent;
  size?: DashboardSegmentedSize;
};

type DashboardSegmentedGroupProps =
  | DashboardSegmentedGroupSingleProps
  | DashboardSegmentedGroupMultipleProps;

function DashboardSegmentedGroup({
  className,
  accent = "primary",
  size = "default",
  ...props
}: DashboardSegmentedGroupProps) {
  if (props.type === "single") {
    const { allowDeselect = false, onValueChange, value, ...rest } = props;

    return (
      <DashboardSegmentedContext.Provider value={{ accent, size }}>
        <ToggleGroupPrimitive.Root
          type="single"
          data-slot="dashboard-segmented-group"
          data-size={size}
          className={cn("dashboard-segmented-group", className)}
          value={value}
          onValueChange={(nextValue) => {
            if (!allowDeselect && nextValue === "" && value) return;
            onValueChange?.(nextValue);
          }}
          {...rest}
        />
      </DashboardSegmentedContext.Provider>
    );
  }

  const { onValueChange, value, ...rest } = props;

  return (
    <DashboardSegmentedContext.Provider value={{ accent, size }}>
      <ToggleGroupPrimitive.Root
        type="multiple"
        data-slot="dashboard-segmented-group"
        data-size={size}
        className={cn("dashboard-segmented-group", className)}
        value={value}
        onValueChange={(nextValue) => onValueChange?.(nextValue)}
        {...rest}
      />
    </DashboardSegmentedContext.Provider>
  );
}

function DashboardSegmentedItem({
  className,
  accent,
  size,
  ...props
}: React.ComponentPropsWithoutRef<typeof ToggleGroupPrimitive.Item> & {
  accent?: DashboardSegmentedAccent;
  size?: DashboardSegmentedSize;
}) {
  const context = React.useContext(DashboardSegmentedContext);

  return (
    <ToggleGroupPrimitive.Item
      data-slot="dashboard-segmented-item"
      data-accent={accent ?? context.accent}
      data-size={size ?? context.size}
      className={cn("dashboard-segmented-item", className)}
      {...props}
    />
  );
}

export { DashboardSegmentedGroup, DashboardSegmentedItem };

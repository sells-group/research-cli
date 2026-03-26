import { type ClassValue, clsx } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export type WithoutChildrenOrChild<T> = T extends infer U
  ? Omit<U, "children" | "child">
  : never;

export type WithoutChild<T> = T extends infer U ? Omit<U, "child"> : never;

export type WithElementRef<T> = T & { ref?: HTMLElement | null };

import { isAxiosError } from "axios";

export const errorMessage = (error: unknown, fallback: string): string => {
  const detail = isAxiosError(error) ? (error.response?.data as { detail?: unknown } | undefined)?.detail : undefined;
  return typeof detail === "string" ? detail : fallback;
};

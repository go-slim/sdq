import { formatNumber as formatLocalizedNumber, locale, t } from "../i18n.js";

const durationUnitKeys = {
  d: "unit.dayShort",
  h: "unit.hourShort",
  m: "unit.minuteShort",
  ms: "unit.millisecondShort",
  ns: "unit.nanosecondShort",
  s: "unit.secondShort",
  us: "unit.microsecondShort",
  "µs": "unit.microsecondShort",
};

export function formatDate(value) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "-";
  return new Intl.DateTimeFormat(locale.value, {
    dateStyle: "medium",
    timeStyle: "medium",
  }).format(date);
}

export function formatUptime(value) {
  if (!value) return t("common.unknown");
  if (typeof value === "string") return formatDuration(value);
  const seconds = Number(value);
  if (!Number.isFinite(seconds)) return t("common.unknown");
  const days = Math.floor(seconds / 86400);
  const hours = Math.floor((seconds % 86400) / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const dayPart = `${formatLocalizedNumber(days)}${t("unit.dayShort")}`;
  const hourPart = `${formatLocalizedNumber(hours)}${t("unit.hourShort")}`;
  const minutePart = `${formatLocalizedNumber(minutes)}${t("unit.minuteShort")}`;
  return days > 0 ? `${dayPart} ${hourPart} ${minutePart}` : `${hourPart} ${minutePart}`;
}

export function formatDuration(value) {
  if (typeof value === "number") {
    return `${formatLocalizedNumber(value)}${t("unit.secondShort")}`;
  }

  const input = String(value ?? "").trim();
  const parts = [...input.matchAll(/(\d+(?:\.\d+)?)(ms|us|µs|ns|d|h|m|s)/g)];
  if (parts.length === 0 || parts.map((part) => part[0]).join("") !== input.replaceAll(" ", "")) {
    return input;
  }
  return parts.map((part) => (
    `${formatLocalizedNumber(Number(part[1]))}${t(durationUnitKeys[part[2]])}`
  )).join(" ");
}

export function formatNumber(value) {
  return formatLocalizedNumber(value);
}

export function formatBytes(value) {
  const bytes = Number(value);
  if (!Number.isFinite(bytes) || bytes <= 0) return "0 B";

  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  const index = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
  const amount = bytes / 1024 ** index;
  return `${formatLocalizedNumber(amount, {maximumFractionDigits: index === 0 ? 0 : 2})} ${units[index]}`;
}

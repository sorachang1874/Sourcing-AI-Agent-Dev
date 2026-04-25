const WORKFLOW_TIME_ZONE = "Asia/Shanghai";
const NAIVE_TIMESTAMP_PATTERN =
  /^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2}))?$/;

function parseNaiveTimestamp(value: string): Date | null {
  const match = value.match(NAIVE_TIMESTAMP_PATTERN);
  if (!match) {
    return null;
  }
  const [, year, month, day, hour, minute, second = "00"] = match;
  return new Date(
    Date.UTC(
      Number(year),
      Number(month) - 1,
      Number(day),
      Number(hour),
      Number(minute),
      Number(second),
    ),
  );
}

function isNaiveWorkflowTimestamp(value: string): boolean {
  return NAIVE_TIMESTAMP_PATTERN.test(value.replace("T", " ").trim());
}

export function parseWorkflowTimestamp(value: string): Date {
  const normalized = value.replace("T", " ").trim();
  const naive = parseNaiveTimestamp(normalized);
  if (naive) {
    return naive;
  }
  return new Date(value);
}

export function formatWorkflowTimestamp(value: string): string {
  if (!value) {
    return "--";
  }
  const normalized = value.replace("T", " ").trim();
  if (isNaiveWorkflowTimestamp(normalized)) {
    return normalized;
  }
  const date = parseWorkflowTimestamp(value);
  if (Number.isNaN(date.getTime())) {
    return normalized || value;
  }
  const parts = new Intl.DateTimeFormat("zh-CN", {
    timeZone: WORKFLOW_TIME_ZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).formatToParts(date);
  const partMap = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  const yyyy = partMap.year || "0000";
  const mm = partMap.month || "00";
  const dd = partMap.day || "00";
  const hh = partMap.hour || "00";
  const min = partMap.minute || "00";
  const sec = partMap.second || "00";
  return `${yyyy}-${mm}-${dd} ${hh}:${min}:${sec}`;
}

export function formatWorkflowShortTimestamp(value: string): string {
  if (!value) {
    return "--";
  }
  const normalized = value.replace("T", " ").trim();
  const naiveMatch = normalized.match(NAIVE_TIMESTAMP_PATTERN);
  if (naiveMatch) {
    const [, , month, day, hour, minute] = naiveMatch;
    return `${month}/${day} ${hour}:${minute}`;
  }
  const date = parseWorkflowTimestamp(value);
  if (Number.isNaN(date.getTime())) {
    return normalized || value;
  }
  const parts = new Intl.DateTimeFormat("zh-CN", {
    timeZone: WORKFLOW_TIME_ZONE,
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).formatToParts(date);
  const partMap = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  const mm = partMap.month || "00";
  const dd = partMap.day || "00";
  const hh = partMap.hour || "00";
  const min = partMap.minute || "00";
  return `${mm}/${dd} ${hh}:${min}`;
}

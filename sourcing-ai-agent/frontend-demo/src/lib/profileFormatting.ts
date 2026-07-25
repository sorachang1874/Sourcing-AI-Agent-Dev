import type { Candidate, CandidateDetail } from "../types";

type CandidateLike = Candidate | CandidateDetail;

const ROLE_HINTS = [
  "engineer",
  "scientist",
  "research",
  "manager",
  "director",
  "lead",
  "member of technical staff",
  "mts",
  "pm",
  "product",
  "intern",
  "founder",
  "cto",
  "ceo",
  "architect",
];

const DEGREE_HINTS = [
  "bachelor",
  "master",
  "phd",
  "doctor",
  "mba",
  "bs",
  "ba",
  "ms",
  "ma",
  "b.sc",
  "m.sc",
];

const SCHOOL_HINTS = [
  "university",
  "college",
  "institute",
  "school",
  "academy",
  "大学",
  "学院",
];

function normalizeWhitespace(value: string): string {
  return value.replace(/\s+/g, " ").trim();
}

function normalizeDateRange(value: string): string {
  const text = normalizeWhitespace(value);
  const yearRangeMatch = text.match(/(19|20)\d{2}\s*(?:-|–|—|~|to|至)\s*((19|20)\d{2}|present|current|now|至今)/i);
  if (yearRangeMatch) {
    const start = yearRangeMatch[0].match(/(19|20)\d{2}/)?.[0] || "";
    const endMatch = yearRangeMatch[0].match(/((19|20)\d{2}|present|current|now|至今)$/i)?.[1] || "";
    const end =
      /present|current|now|至今/i.test(endMatch)
        ? "Present"
        : endMatch.match(/(19|20)\d{2}/)?.[0] || endMatch;
    return [start, end].filter(Boolean).join("~");
  }
  const singleYearMatch = text.match(/(19|20)\d{2}/);
  return singleYearMatch?.[0] || "";
}

function removeDateFragments(value: string): string {
  return normalizeWhitespace(
    value.replace(
      /\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s+(19|20)\d{2}\b/gi,
      " ",
    ).replace(/\b(19|20)\d{2}\s*(?:-|–|—|~|to|至)\s*((19|20)\d{2}|present|current|now|至今)\b/gi, " "),
  );
}

function splitCompositeSegments(value: string): string[] {
  return normalizeWhitespace(
    value
      .replace(/\s+at\s+/gi, " · ")
      .replace(/\s*@\s*/g, " · ")
      .replace(/\s*[|•]\s*/g, " · "),
  )
    .split(/\s*·\s*|,\s*/)
    .map((item) => normalizeWhitespace(item))
    .filter(Boolean);
}

function looksLikeRole(value: string): boolean {
  const lower = value.toLowerCase();
  return ROLE_HINTS.some((hint) => lower.includes(hint));
}

function looksLikeDegree(value: string): boolean {
  const lower = value.toLowerCase();
  return DEGREE_HINTS.some((hint) => lower.includes(hint));
}

function looksLikeSchool(value: string): boolean {
  const lower = value.toLowerCase();
  return SCHOOL_HINTS.some((hint) => lower.includes(hint));
}

function cleanProfileLine(value: string): string {
  return normalizeWhitespace(
    value
      .replace(/\s*[-–—]\s*/g, " - ")
      .replace(/\s*·\s*/g, " · "),
  );
}

function formatWorkLine(value: string): string {
  const cleaned = cleanProfileLine(value);
  if (/^(?:\d{4}(?:~(?:\d{4}|Present))?|Present)\b/i.test(cleaned)) {
    return cleaned;
  }
  const date = normalizeDateRange(cleaned);
  const segments = splitCompositeSegments(removeDateFragments(cleaned));
  if (segments.length === 0) {
    return cleaned;
  }
  const role = segments.find(looksLikeRole) || (segments.length > 1 ? segments[segments.length - 1] : "");
  const company = segments.find((segment) => segment !== role) || segments[0];
  const trailing = segments.filter((segment) => segment !== company && segment !== role).slice(0, 2);
  const ordered = [date, company, role, ...trailing].filter(Boolean);
  return ordered.length >= 2 ? ordered.join(", ") : cleaned;
}

function formatEducationLine(value: string): string {
  const cleaned = cleanProfileLine(value);
  if (/^(?:\d{4}(?:~(?:\d{4}|Present))?|Present)\b/i.test(cleaned)) {
    return cleaned;
  }
  const date = normalizeDateRange(cleaned);
  const segments = splitCompositeSegments(removeDateFragments(cleaned));
  if (segments.length === 0) {
    return cleaned;
  }
  const degree = segments.find(looksLikeDegree) || "";
  const school = segments.find((segment) => looksLikeSchool(segment) && segment !== degree) || segments[0];
  const field = segments.find((segment) => segment !== degree && segment !== school) || "";
  const trailing = segments.filter((segment) => segment !== degree && segment !== school && segment !== field).slice(0, 1);
  const ordered = [date, degree, school, field, ...trailing].filter(Boolean);
  return ordered.length >= 2 ? ordered.join(", ") : cleaned;
}

function dedupeLines(values: string[]): string[] {
  return values.filter((value, index) => value && values.indexOf(value) === index);
}

export function getFormattedWorkExperience(candidate: CandidateLike, limit = 4): string[] {
  return dedupeLines(candidate.experience.map(formatWorkLine)).slice(0, limit);
}

export function getFormattedEducationExperience(candidate: CandidateLike, limit = 3): string[] {
  return dedupeLines(candidate.education.map(formatEducationLine)).slice(0, limit);
}

export function getCandidateExperiencePreview(candidate: CandidateLike): string[] {
  const work = getFormattedWorkExperience(candidate, 2);
  const education = getFormattedEducationExperience(candidate, 1);
  return [...work, ...education].slice(0, 3);
}

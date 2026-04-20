import type { Candidate, CandidateDetail, DashboardData, DemoPlan, ManualReviewItem, RunStatusData } from "../types";

export const mockPlan: DemoPlan = {
  planId: "plan_gdm_multimodal_demo",
  rawUserRequest: "Google DeepMind 参与 Nano Banana 和 Veo 3 组多模态的华人",
  targetCompany: "Google DeepMind",
  targetPopulation: "Multimodal Chinese researchers and engineers",
  projectScope: "Google DeepMind all members",
  keywords: ["multimodal", "video generation", "Veo 3", "Nano Banana", "Chinese alias"],
  acquisitionStrategy: "Full company roster scan + targeted search roster",
  searchStrategy: ["relationship_web", "publication_surface", "targeted_people_search"],
  estimatedCostLevel: "high",
  reviewRequired: true,
  status: "pending_review",
};

const mockCandidates: Candidate[] = [
  {
    id: "cand_jeremy_bernstein",
    name: "Jeremy Bernstein",
    headline: "Research Engineer, Google DeepMind",
    avatarUrl:
      "https://images.unsplash.com/photo-1500648767791-00dcc994a43e?auto=format&fit=crop&w=400&q=80",
    team: "Veo 3",
    employmentStatus: "current",
    confidence: "high",
    summary: "Works on multimodal systems and video generation; repeatedly appears in publication and engineering evidence.",
    focusAreas: ["multimodal", "video generation", "training systems"],
    matchReasons: [
      "Matched project scope via Veo 3 evidence",
      "Publication surface references multimodal work",
      "Strong current-employment signals",
    ],
    education: ["Cornell University"],
    experience: ["Google DeepMind", "Robust ML systems", "Large-scale multimodal tooling"],
    evidence: [
      {
        label: "LinkedIn profile",
        type: "linkedin",
        url: "https://www.linkedin.com",
        excerpt: "Current role at Google DeepMind with multimodal focus.",
      },
      {
        label: "Publication mention",
        type: "publication",
        url: "https://scholar.google.com",
        excerpt: "Named on multimodal systems and video-related publications.",
      },
    ],
  },
  {
    id: "cand_horace_he",
    name: "Horace He",
    headline: "ML Systems Lead",
    avatarUrl:
      "https://images.unsplash.com/photo-1506794778202-cad84cf45f1d?auto=format&fit=crop&w=400&q=80",
    team: "Nano Banana",
    employmentStatus: "lead",
    confidence: "medium",
    summary: "High-signal lead with homepage and CV evidence, but still needs stronger team confirmation.",
    focusAreas: ["ML systems", "compiler", "multimodal infra"],
    matchReasons: [
      "Homepage/CV mentions align with target scope",
      "Evidence is strong but team attachment is not fully confirmed",
    ],
    education: ["Cornell University"],
    experience: ["Meta", "Google-adjacent systems work", "Performance engineering"],
    evidence: [
      {
        label: "Personal homepage",
        type: "homepage",
        url: "https://example.com",
        excerpt: "Research and systems profile with relevant multimodal infra overlap.",
      },
      {
        label: "Resume PDF",
        type: "cv",
        url: "https://example.com/resume.pdf",
        excerpt: "Education and work history extracted from PDF artifact.",
      },
    ],
  },
  {
    id: "cand_tianle_li",
    name: "Tianle Li",
    headline: "Research Scientist, Google DeepMind",
    avatarUrl:
      "https://images.unsplash.com/photo-1494790108377-be9c29b29330?auto=format&fit=crop&w=400&q=80",
    team: "Unknown",
    employmentStatus: "current",
    confidence: "lead_only",
    summary: "Appears in roster and publication traces, but still needs deeper LinkedIn and project attribution.",
    focusAreas: ["vision-language", "multimodal", "model evaluation"],
    matchReasons: [
      "Roster hit on DeepMind",
      "Weak but promising multimodal publication overlap",
    ],
    education: ["Tsinghua University"],
    experience: ["Google DeepMind", "Multimodal evaluation", "Research tooling"],
    evidence: [
      {
        label: "Google Scholar",
        type: "publication",
        url: "https://scholar.google.com",
        excerpt: "Publication co-author signals connect to multimodal work.",
      },
      {
        label: "GitHub profile",
        type: "github",
        url: "https://github.com",
        excerpt: "Open-source profile suggests adjacent vision-language research interests.",
      },
    ],
  },
];

export const mockDashboard: DashboardData = {
  title: "Google DeepMind Multimodal Chinese Talent",
  snapshotId: "20260407T163000",
  queryLabel: "Google DeepMind 参与 Nano Banana 和 Veo 3 组多模态的华人",
  totalCandidates: mockCandidates.length,
  totalEvidence: mockCandidates.flatMap((candidate) => candidate.evidence).length,
  manualReviewCount: 2,
  layers: [
    { id: "all", label: "Layer 0 · All Candidates", count: 3 },
    { id: "lead_only", label: "Layer 1 · Lead / Low Confidence", count: 1 },
    { id: "high", label: "Layer 2 · High Confidence", count: 1 },
    { id: "confirmed", label: "Layer 3 · Confirmed Direction", count: 1 },
  ],
  groups: ["All", "Nano Banana", "Veo 3", "Unknown"],
  candidates: mockCandidates,
};

export const mockRunStatus: RunStatusData = {
  jobId: "job_tml_demo_20260407",
  status: "running",
  currentStage: "Enrichment",
  startedAt: "2026-04-07 16:30",
  metrics: [
    { label: "Candidates", value: "55" },
    { label: "Evidence", value: "75" },
    { label: "Manual Review", value: "2" },
    { label: "Profile Backlog", value: "25" },
  ],
  timeline: [
    {
      id: "evt1",
      stage: "Planning",
      title: "Planning completed",
      detail: "Sourcing plan compiled and review gate triggered.",
      status: "completed",
      startedAt: "2026-04-07 16:30",
      completedAt: "2026-04-07 16:30",
      sourceTags: [],
    },
    {
      id: "evt2",
      stage: "Review",
      title: "Review completed",
      detail: "Plan approved for company-wide Thinking Machines Lab asset continuation.",
      status: "completed",
      startedAt: "2026-04-07 16:33",
      completedAt: "2026-04-07 16:33",
      sourceTags: [],
    },
    {
      id: "evt3",
      stage: "Acquisition",
      title: "Acquisition completed",
      detail: "Recovered latest company snapshot and normalized candidate artifacts.",
      status: "completed",
      startedAt: "2026-04-07 16:35",
      completedAt: "2026-04-07 16:35",
      sourceTags: [],
    },
    {
      id: "evt4",
      stage: "Enrichment",
      title: "Enrichment",
      detail: "Processing known profile URLs and unresolved leads.",
      status: "running",
      startedAt: "2026-04-07 16:39",
      completedAt: "",
      sourceTags: [],
    },
  ],
  workers: [
    { id: "wrk_search", lane: "search_planner", status: "completed", budget: "3 / 3 bundles" },
    { id: "wrk_explore", lane: "exploration_specialist", status: "running", budget: "1 / 2 candidates" },
    { id: "wrk_enrich", lane: "enrichment_specialist", status: "running", budget: "6 / 12 profiles" },
  ],
};

export const mockManualReviewItems: ManualReviewItem[] = [
  {
    id: "mr_horace_he",
    candidateId: "cand_horace_he",
    candidateName: "Horace He",
    reviewType: "manual_identity_resolution",
    status: "pending",
    summary: "Homepage and CV evidence are strong, but team attachment to TML remains unresolved.",
    recommendedAction: "Review homepage, resume, and any publication trail before confirming as current, former, or unrelated.",
    evidenceLabels: ["Personal homepage", "Resume PDF"],
  },
  {
    id: "mr_tianle_li",
    candidateId: "cand_tianle_li",
    candidateName: "Tianle Li",
    reviewType: "missing_profile_url",
    status: "pending",
    summary: "Candidate appears in roster and publication traces, but LinkedIn profile detail is incomplete.",
    recommendedAction: "Find stronger profile source or confirm identity via publication and homepage evidence.",
    evidenceLabels: ["Google Scholar", "GitHub profile"],
  },
];

export const mockCandidateDetails: CandidateDetail[] = mockCandidates.map((candidate) => ({
  ...candidate,
  currentCompany: candidate.employmentStatus === "former" ? "Former role" : "Google DeepMind",
  location: "San Francisco Bay Area",
  aliases: [candidate.name],
  sourceSummary: candidate.evidence.map((item) => item.label),
  documents: [
    {
      title: "Candidate Summary",
      body: candidate.summary,
    },
    {
      title: "Why This Candidate Matters",
      body: candidate.matchReasons.join(" "),
    },
  ],
}));

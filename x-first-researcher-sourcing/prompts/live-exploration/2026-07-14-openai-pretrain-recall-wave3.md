You are executing a gap-driven challenger wave for a bounded, read-only, high-recall researcher discovery campaign
using Grok's native X tools.

Goal: find novel public X accounts missed by two prior waves for current or former OpenAI researchers/engineers with
current or historical pre-training experience. Do not repeat only visible leaders. Focus on less-visible individual
contributors and technical subfunctions that rarely use the literal word `pretrain`:

- training-data curation, data mixtures, data quality, synthetic data, and tokenization;
- model architecture, scaling laws, optimization, training stability, and large-scale experiments;
- distributed/model-training systems, kernels, compute, training infrastructure, and reliability;
- multimodal/base-model training and research-engineering contributions;
- technical leads or contributors named in official/team acknowledgements, talks, papers, system cards, launch posts,
  onboarding/farewell posts, and directly attributable colleague replies;
- former OpenAI contributors who moved to another lab but retain relevant training experience.

Use prior handles only as one-hop seeds. Retain a prior handle again only to report a material temporal conflict;
otherwise return novel handles. Search both direct technical terms and semantic paraphrases. A low-authority mention or
list can seed a recall lead, but it must remain `ambiguous` unless another source supports it.

Hard safety boundary:

- Native X keyword, semantic, user, and thread tools only. No generic web search/fetch, browser, shell, filesystem,
  code execution, connectors, memory, or subagents.
- Public professional information only; no writes, follows, likes, messages, contacts, or other mutations.
- Base discovery uses only OpenAI affiliation, role/function, research/publication evidence, and pre-training
  relevance. Never infer or query ethnicity, nationality, race, citizenship, religion, gender, or protected identity.
- Do not impose an arbitrary candidate or observation cutoff. Continue while materially different searches or one-hop
  professional edges add novel evidence-bearing handles. Stop only after all listed technical gaps have been exercised
  and three consecutive materially different expansions yield no new handle. The runner owns the external deadline
  and emergency resource kill switch.
- Deduplicate handles case-insensitively. Never invent an ID, Bio, timestamp, Post URL, excerpt, or claim.

Keep `target_lab_affiliation_state` and `pretraining_experience_state` independent, each one of
`current|historical|ambiguous|unsupported`. Preserve all four complete current/historical combinations; keep incomplete
leads for later hydration. Confidence describes evidence strength only.

Return one JSON object only, without a Markdown fence:

{
  "status": "X_SEARCH_OK | X_SEARCH_PARTIAL | X_SEARCH_BLOCKED",
  "status_reason": "...",
  "native_x_tool_provenance": {
    "tools_reported": ["..."],
    "tool_calls_reported": 0,
    "queries": ["exact query plus [keyword/Latest], [keyword/Top], [semantic], [user_search], or [thread_fetch]"],
    "generic_web_used": false
  },
  "counts": {"observations_inspected_reported": 0, "candidates_retained": 0},
  "candidates": [
    {
      "handle": "...",
      "profile_url": "https://x.com/...",
      "platform_user_id": null,
      "bio_excerpt": null,
      "target_lab_affiliation_state": "current | historical | ambiguous | unsupported",
      "pretraining_experience_state": "current | historical | ambiguous | unsupported",
      "confidence": "high | medium | low",
      "evidence": [
        {
          "kind": "bio | post | mention | thread",
          "relationship": "self | official_lab | colleague_or_team | third_party | historical",
          "author_handle": "...",
          "post_id": null,
          "url": "https://x.com/...",
          "published_at": null,
          "excerpt": "bounded verbatim excerpt, maximum 280 characters",
          "supports": ["target_lab_affiliation_state | pretraining_experience_state"]
        }
      ],
      "caveats": ["..."]
    }
  ],
  "excluded_examples": [{"handle": "...", "reason": "..."}],
  "limitations": ["..."],
  "local_reconciliation": {
    "candidate_records_validated": 0,
    "evidence_items_validated": 0,
    "post_urls_structurally_validated": 0,
    "provider_post_bodies_replayable": false,
    "tool_calls_completed": 0,
    "tool_counts": {"x_keyword_search": 0, "x_semantic_search": 0, "x_user_search": 0, "x_thread_fetch": 0}
  }
}

Omit zero-count tool names from `tool_counts`. Use `X_SEARCH_PARTIAL` when useful public evidence exists but provider
fields remain incomplete. Model prose is a discovery lead, not replayable source truth.

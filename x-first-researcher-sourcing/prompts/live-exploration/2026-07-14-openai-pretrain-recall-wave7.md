You are executing a final coverage-audit challenger wave for a bounded, read-only, high-recall researcher discovery
campaign using Grok's native X tools.

Goal: audit whether materially discoverable public X accounts remain beyond a 97-handle prior union for current or
former OpenAI researchers/engineers with current or historical pre-training experience. This is not a request to force
more candidates. An empty candidate list is correct when distinct remaining searches add no defensible novel handle.

Six prior waves covered prominent and long-tail researchers, early and current teams, direct and adjacent technical
vocabulary, papers/projects/launch acknowledgements, training data/tokenization/scaling/multimodal, systems/compute,
alumni transitions, profile search, and one-hop professional mentions. First perform a native-X coverage gap analysis,
then exercise only materially different residual surfaces:

- combinations of current OpenAI role/profile language with base/frontier-model training functions that earlier
  keyword searches may not have paired;
- official or employee roster-like posts, acknowledgements, recruiting/team threads, conference/talk announcements,
  and low-engagement replies that expose names or handles;
- handle changes, sparse-posting profiles, or alumni accounts discoverable through direct user search and one-hop
  colleague edges;
- overlooked research-engineering functions genuinely connected to large training runs, data, architecture,
  optimization, multimodal base models, distributed systems, compute, reliability, or training-time evaluation/safety.

Do not repeat prior handles unless a material temporal conflict or materially new evidence changes one independent
dimension. Do not include a person merely because they worked at OpenAI, published AI content, or are mentioned near
the lab; both lab affiliation and training relevance must remain separately represented, with ambiguity preserved.

Hard safety boundary:

- Native X keyword, semantic, user, and thread tools only. No generic web search/fetch, browser, shell, filesystem,
  code execution, connectors, memory, or subagents.
- Public professional information only; no writes, follows, likes, messages, contacts, or other mutations.
- Base discovery uses only OpenAI affiliation, role/function, research/publication evidence, and pre-training
  relevance. Never infer or query ethnicity, nationality, race, citizenship, religion, gender, or protected identity.
- Do not impose an arbitrary candidate, observation, or native-X-call cutoff. Continue while a materially different
  residual search adds a novel evidence-bearing handle. Stop after the useful residual surfaces are exercised and
  three consecutive materially different expansions add no novel handle or material state conflict. The external
  runner owns a 30-minute deadline, one-process boundary, and emergency resource kill switch.
- Deduplicate handles case-insensitively. Never invent an ID, Bio, timestamp, Post URL, excerpt, or claim.
- Emit no progress prose. The final response must be exactly one JSON object. A valid zero-candidate result is
  preferable to weak filler.

Keep `target_lab_affiliation_state` and `pretraining_experience_state` independent, each one of
`current|historical|ambiguous|unsupported`. Preserve all complete current/historical combinations; keep incomplete
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

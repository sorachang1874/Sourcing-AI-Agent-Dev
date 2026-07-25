You are executing a current-team precision-frontier challenger wave inside a broader, bounded, read-only, high-recall
researcher discovery campaign using Grok's native X tools.

Goal: discover novel public X accounts beyond a 92-handle prior union, concentrating on people who may be **currently
at OpenAI** and may have **current pre-training/base-model training experience**. The prior union already has broad
historical coverage but only 13 model-classified current/current leads. Do not lower evidence standards merely to
increase that number. Preserve ambiguous dimensions when current affiliation or training relevance is not actually
supported.

Use a fresh, AI-native gap analysis rather than a fixed famous-name list. Explore:

- current OpenAI research scientist, research engineer, ML engineer, member-of-technical-staff, technical lead, and
  training-infrastructure profiles whose Bios or posts use adjacent vocabulary rather than `pretrain`;
- current team discussions around base/frontier models, large training runs, training data/mixtures, tokenization,
  architecture, optimization, scaling, multimodal foundation models, distributed training, reliability, compute, and
  model-training evaluation/safety;
- official/team launch posts, acknowledgements, recruiting threads, colleague mentions/replies, conference or talk
  announcements, and project discussions that expose less-visible individual contributors;
- handle-renamed or low-posting current staff found via user search and one-hop professional graph edges;
- credible current OpenAI affiliation with historical pre-training experience, retained as `current/historical`
  rather than discarded.

Do not repeat a prior account just because it is a useful seed. Retain one only for a material temporal conflict or
materially new evidence. A low-authority mention may seed discovery but remains `ambiguous` until separately
supported.

Hard safety boundary:

- Native X keyword, semantic, user, and thread tools only. No generic web search/fetch, browser, shell, filesystem,
  code execution, connectors, memory, or subagents.
- Public professional information only; no writes, follows, likes, messages, contacts, or other mutations.
- Base discovery uses only OpenAI affiliation, role/function, research/publication evidence, and pre-training
  relevance. Never infer or query ethnicity, nationality, race, citizenship, religion, gender, or protected identity.
- Do not impose an arbitrary candidate, observation, or native-X-call cutoff. Continue while materially different
  searches or one-hop professional edges add novel evidence-bearing handles. Stop only after the current-team gap
  analysis has exercised the useful remaining surfaces and three consecutive materially different expansions add no
  novel handle or material state conflict. The external runner owns a 30-minute deadline, one-process boundary, and
  emergency resource kill switch.
- Deduplicate handles case-insensitively. Never invent an ID, Bio, timestamp, Post URL, excerpt, or claim.
- Emit no progress prose. The final response must be exactly one JSON object.

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

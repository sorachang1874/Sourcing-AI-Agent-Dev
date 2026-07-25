You are executing a graph-and-roster challenger wave for a bounded, read-only, high-recall researcher discovery
campaign using Grok's native X tools.

Goal: discover novel public X accounts beyond a 67-handle prior union for current or former OpenAI researchers and
engineers with current or historical pre-training experience. Four waves have already covered prominent researchers,
direct technical keywords, papers/projects, early model-training contributors, data/tokenization/scaling/multimodal,
and training systems. Do a fresh gap analysis inside native X and explore professional surfaces the prior keyword-heavy
passes are likely to miss:

- official OpenAI and employee posts that name, thank, welcome, congratulate, hire for, or say farewell to technical
  teammates; follow professional mentions and thread participants one hop;
- low-posting or handle-renamed staff found through user-profile search, Bio role/function, colleague interactions,
  conference/talk announcements, author discussions, recruiting posts, and model/research launch threads;
- current and former training-adjacent contributors in data platforms, research engineering, model architecture,
  optimization, distributed systems, compute, reliability, multimodal/base models, synthetic data, evaluation used
  during training, and training safety;
- less-visible authors or acknowledged contributors around GPT-family, Codex, CLIP/DALL-E/Sora, scaling, WebText,
  tokenization, large-scale RL/model training, and internal training infrastructure;
- alumni who moved to Anthropic, xAI, Google DeepMind, Meta, Thinking Machines, SSI, startups, or academia and publicly
  connect their former OpenAI work to model training.

Do not repeat a prior account just because it is a useful seed. Retain one only for a material temporal conflict or
materially new evidence. A low-authority mention may seed discovery but remains `ambiguous` until separately
supported. Let native X evidence drive the next search rather than following a fixed list of famous names.

Hard safety boundary:

- Native X keyword, semantic, user, and thread tools only. No generic web search/fetch, browser, shell, filesystem,
  code execution, connectors, memory, or subagents.
- Public professional information only; no writes, follows, likes, messages, contacts, or other mutations.
- Base discovery uses only OpenAI affiliation, role/function, research/publication evidence, and pre-training
  relevance. Never infer or query ethnicity, nationality, race, citizenship, religion, gender, or protected identity.
- Do not impose an arbitrary candidate, observation, or native-X-call cutoff. Continue while materially different
  searches or one-hop professional edges add novel evidence-bearing handles. Stop only after the gap analysis has
  exercised the useful remaining surfaces and three consecutive materially different expansions add no novel handle
  or material state conflict. The external runner owns a 30-minute deadline, one-process boundary, and emergency
  resource kill switch.
- Deduplicate handles case-insensitively. Never invent an ID, Bio, timestamp, Post URL, excerpt, or claim.
- Emit no progress prose. The final response must be exactly one JSON object.

Keep `target_lab_affiliation_state` and `pretraining_experience_state` independent, each one of
`current|historical|ambiguous|unsupported`. Preserve all four complete current/historical combinations; keep
incomplete leads for later hydration. Confidence describes evidence strength only.

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

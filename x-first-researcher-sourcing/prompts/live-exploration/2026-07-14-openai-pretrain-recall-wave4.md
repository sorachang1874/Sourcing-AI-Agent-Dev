You are executing a convergence-challenger wave for a bounded, read-only, high-recall researcher discovery campaign
using Grok's native X tools.

Goal: find novel public X accounts still missed after three broad waves for current or former OpenAI researchers or
engineers with current or historical pre-training experience. Prior waves already covered prominent researchers,
literal pretraining terms, general author/acknowledgement expansion, training data, tokenization, scaling, multimodal,
kernels, and infrastructure. Do not merely repeat those searches. Look for remaining evidence through different
professional surfaces and vocabulary:

- early OpenAI model-training contributors and alumni from the GPT-1/GPT-2/GPT-3, WebText, sparse-attention,
  scaling-law, CLIP, DALL-E, Codex, and large-scale reinforcement-learning eras;
- less-visible current individual contributors surfaced through launch acknowledgements, technical presentations,
  recruiting threads, colleague congratulations/farewells, paper/project discussions, and team replies;
- training compute, performance, reliability, checkpointing, data-platform, evaluation-for-training, architecture,
  optimization, model-behavior, multimodal, and research-engineering work that may not use the word `pretrain`;
- authors/contributors named in public technical-report, system-card, paper, project, or release conversations whose
  OpenAI affiliation and training relevance can be supported separately;
- former OpenAI contributors now at another lab or company whose historical OpenAI and training experience remains
  useful.

Use prior handles only as one-hop professional seeds. Retain a prior handle again only when you find a material
temporal-state conflict or materially new evidence; otherwise return novel handles. Search direct terms, semantic
paraphrases, user profiles, mentions, and threads. Low-authority mentions may seed a lead but must remain
`ambiguous` unless another source supports the relevant dimension.

Hard safety boundary:

- Native X keyword, semantic, user, and thread tools only. No generic web search/fetch, browser, shell, filesystem,
  code execution, connectors, memory, or subagents.
- Public professional information only; no writes, follows, likes, messages, contacts, or other mutations.
- Base discovery uses only OpenAI affiliation, role/function, research/publication evidence, and pre-training
  relevance. Never infer or query ethnicity, nationality, race, citizenship, religion, gender, or protected identity.
- Do not impose an arbitrary candidate, observation, or native-X-call cutoff. Continue while materially different
  searches or one-hop professional edges add novel evidence-bearing handles. Stop only after every listed gap has
  been exercised and three consecutive materially different expansions yield no novel handle or material state
  conflict. The external runner owns a 30-minute deadline, one-process boundary, and emergency resource kill switch.
- Deduplicate handles case-insensitively. Never invent an ID, Bio, timestamp, Post URL, excerpt, or claim.

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

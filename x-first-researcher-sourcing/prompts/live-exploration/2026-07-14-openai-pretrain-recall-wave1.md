You are executing wave 1 of a bounded, read-only, high-recall researcher discovery experiment using Grok's native X tools.

Goal: build a broad candidate pool of public X accounts that may belong to current or former OpenAI researchers or
engineers with current or historical pre-training experience. This is discovery, not final employment verification.
Do not stop after finding only the most famous people. Seek a genuinely broad set of distinct evidence-bearing
accounts and continue past 20 when X evidence supports more; never fabricate rows merely to reach a target.

Use only these query families in this wave:

1. `official_lab_output`: OpenAI-authored technical posts, launch posts, team acknowledgements, recruiting or joining
   announcements, and replies that name contributors to base-model training.
2. `public_bio_affiliation`: X user/profile searches for people self-describing OpenAI research, model training,
   pre-training, scaling, training data, architecture, optimization, or distributed training work.
3. `first_party_technical_posts`: first-person posts by current or former OpenAI staff about pretraining, pre-training,
   training base/foundation models, scaling laws, data mixtures, tokenization, architecture, optimization, training
   infrastructure, distributed training, multimodal pretraining, or next-token model training.
4. `official_lab_interactions`: bounded replies, quotes, mentions, or acknowledgements from OpenAI or clearly
   attributable OpenAI team members that identify additional candidate accounts.

Query both current and historical experience. Treat direct `pretrain` tokens as only one query family; use the
technical synonyms above so recall is not limited to people who literally write “pretrain” in their Bio. Expand from
promising official/team mentions to the named account with `x_user_search`, but do not perform more than one hop.

Hard execution boundaries:

- Use only native X keyword, semantic, user, and thread tools. Do not use generic web search, web fetch, browser,
  shell, filesystem, code execution, connectors, memory, or subagents.
- Public professional information only. Do not write, follow, like, message, contact, or modify anything.
- Discovery and ranking use only target-lab affiliation, role/function, publication/research evidence, and
  pre-training relevance. Do not infer or query ethnicity, nationality, race, citizenship, religion, gender, or any
  protected identity.
- Do not impose an arbitrary candidate-count or observation-count cutoff. Keep expanding while a query family or
  one-hop professional edge produces novel evidence-bearing handles, subject only to the runner's external deadline
  and emergency resource kill switch.
- Exhaust all four query families. Stop only after every family has been exercised and three consecutive materially
  different expansions add no new evidence-bearing handle. Deduplicate handles case-insensitively.
- A third-party mention may seed a candidate but never confirms employment or pre-training experience.

For each retained candidate return an exact handle and profile URL plus all available source-bound evidence. Keep
current/historical lab affiliation independent from current/historical pre-training experience. All four complete
temporal combinations are valuable. `ambiguous` and `unsupported` remain explicit evidence/hydration cases rather than
being dropped. Confidence is an evidence-strength label only and never changes segment membership.

Return one JSON object only, with no Markdown fence and exactly this top-level shape:

{
  "status": "X_SEARCH_OK | X_SEARCH_PARTIAL | X_SEARCH_BLOCKED",
  "status_reason": "...",
  "native_x_tool_provenance": {
    "tools_reported": ["..."],
    "tool_calls_reported": 0,
    "queries": ["exact query plus [keyword/Latest], [keyword/Top], [semantic], [user_search], or [thread_fetch]"],
    "generic_web_used": false
  },
  "counts": {
    "observations_inspected_reported": 0,
    "candidates_retained": 0
  },
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
  "limitations": ["..."] ,
  "local_reconciliation": {
    "candidate_records_validated": 0,
    "evidence_items_validated": 0,
    "post_urls_structurally_validated": 0,
    "provider_post_bodies_replayable": false,
    "tool_calls_completed": 0,
    "tool_counts": {"x_keyword_search": 0, "x_semantic_search": 0, "x_user_search": 0, "x_thread_fetch": 0}
  }
}

Omit zero-count tool names from `tool_counts`. If native X tools are unavailable, return `X_SEARCH_BLOCKED` with an
empty candidate array. If useful evidence exists but source fields remain unavailable, return `X_SEARCH_PARTIAL` and
state the limitation; do not invent missing IDs, Bios, timestamps, Post URLs, or excerpts.

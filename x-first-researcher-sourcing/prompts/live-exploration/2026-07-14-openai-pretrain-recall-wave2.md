You are executing wave 2 of a bounded, read-only, high-recall researcher discovery experiment using Grok's native X tools.

Goal: add distinct public X accounts that wave 1's direct official/Bio/first-person searches can miss: current or
former OpenAI researchers or engineers with current or historical pre-training experience. This is discovery, not
final employment verification. Continue past 20 when evidence supports more, and prioritize novel handles over
repeating only famous names. Never fabricate rows to reach a target.

Use only these query families in this wave:

1. `paper_conference_linkage`: public X posts that connect authors/contributors of OpenAI base-model reports, system
   cards, scaling/data/architecture/training-infrastructure work, conference talks, or research releases to X handles.
2. `replies_mentions`: bounded professional replies, quotes, team congratulations, onboarding/farewell posts, and
   contributor acknowledgements that reveal less-visible team members.
3. `curated_lists`: public professional lists or threads about OpenAI model-training researchers, used only as
   low-authority seeds that require a separate Bio, first-party, official-lab, or directly attributable team signal.
4. `one_hop_graph_and_conflict_checks`: expand exactly one hop from strong candidates or authoritative team posts,
   then search the candidate profile/posts and preserve conflicting current-versus-former evidence.

Search technical concepts beyond the literal token `pretrain`: base/foundation model training, large-scale training,
scaling laws, training data and data mixtures, tokenizer/tokenization, architecture, optimization, distributed
training, training infrastructure/systems, multimodal pretraining, next-token training, and model-training leadership.
Include historical OpenAI experience and people who later moved to another lab; do not downgrade them to negatives.

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
- Low-authority list/mention evidence can retain a recall lead with `ambiguous` state, but cannot confirm a claim.

For each retained candidate return exact handle/profile/evidence. Keep target-lab affiliation temporality independent
from pre-training-experience temporality. Preserve all four current/historical combinations; keep ambiguous or
unsupported dimensions in the explicit evidence queue. Confidence only describes evidence strength.

Return one JSON object only, with no Markdown fence and exactly the same top-level and candidate/evidence shape below:

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

Omit zero-count tool names from `tool_counts`. If native X tools are unavailable, return `X_SEARCH_BLOCKED` with no
candidates. If useful evidence exists but source fields remain unavailable, return `X_SEARCH_PARTIAL` and state the
limitation. Never invent missing IDs, Bios, timestamps, Post URLs, or excerpts.

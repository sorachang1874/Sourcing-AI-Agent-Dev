# OpenAI zero-prior official discovery v5

Status: offline transfer only. This slice registers and tests an OpenAI adaptation of the reviewed Google DeepMind v5
discovery method. It does not issue a grant, call Grok/X, read OAuth state, or prove live capability.

## Experiment identity

- Target: current or historical OpenAI affiliation crossed independently with current or historical pretraining or
  base-model-training relevance.
- Prior input: `prior_waves=[]`, mechanically enforced by the selected row's
  `require_empty_prior_waves_v1` policy and semantic digest. The prompt receives no prior candidate handles,
  exclusions, denominator, or frozen-union content; a non-empty prior array cannot receive a grant.
- Official-account exception: the effective prompt row permits only `OpenAI`, under
  `discovery_only_official_accounts_no_person_hydration_v2`.
- Discovery surfaces: official/project/model/training-function/colleague graph; Posts, Replies, mentions, quotes, and
  attributable threads. Bio is incidental evidence, not the primary retrieval strategy.
- Strategy obligations: Top and Latest, current/recent-historical/older-historical time shards, four distinct positive
  `filter:replies` cells, semantic and broad user discovery, and attributable thread expansion.
- User-search grammar: `OpenAI` plus at least one of the exact prompt-enumerated runtime terms `researcher`,
  `scientist`, `engineer`, `research`, `pretraining`, `training`, `model`, `scaling`, `tokenization`, `infrastructure`,
  `safety`, `multimodal`, or `robotics`. Bare `OpenAI`, bare `@OpenAI`, and topic-only `data` are not user searches;
  data discovery remains available through keyword and semantic queries.
- Scale: no candidate, observation, query, or native-X-call business cap. A future operator request must still own the
  independent deadline, resource ceiling, private retention, one-shot grant, no-fallback policy, and kill boundary.

## Post-hoc comparison contract

The earlier seven-wave campaign's `98` case-insensitive handles are a frozen exploratory union, not a complete or
independently adjudicated relevant population. It is deliberately withheld from the zero-prior prompt.

After a separately reviewed live run, the only allowed overlap label is **frozen-union conditional coverage**:

`casefold_unique(v5_output_handles intersect frozen_98_union) / 98`

This metric answers how much of that particular historical union the zero-prior method independently rediscovers. It
is never "recall", because the denominator is neither complete population truth nor verified relevance gold. Handles
outside the union are reported separately as novel leads pending evidence review; they do not prove precision. Empty
or partial overlap does not by itself prove failure because time, model, X index, prompt, and evidence-contract drift
remain possible.

## Offline acceptance

The transfer is ready for pinned non-author review when:

1. the production registry owns exactly one OpenAI zero-prior v5 prompt digest;
2. the row binds `official_account_handles=["OpenAI"]`, the reviewed official-discovery v2 policy digest, and the
   `require_empty_prior_waves_v1` semantic digest;
3. shared v5 prompt assertions cover phase boundary, four Reply cells, timestamp normalization, no business caps,
   non-Bio-first evidence, temporal-axis independence, Top and Latest, and time shards;
4. the production policy schema, targeted runner tests, Ruff, and diff-check pass; and
5. no provider call or live grant is issued before a matching pinned independent-review GO.

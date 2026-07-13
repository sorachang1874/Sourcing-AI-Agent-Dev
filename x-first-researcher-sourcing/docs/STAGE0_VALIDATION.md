# Stage 0 fixture validation

Status: fixture-only Stage 0 passed its pinned reproducibility checks. This is not a formal review `GO` and does not
authorize live X/Grok access, researcher identification, ranking, outreach, provider credentials, or canonical product
writes.

## Pinned authority

- Repository commit: `0cfaec6c48b95e57f55f1bce06fe5b3abae0e297`
- `x-first-researcher-sourcing` tree: `07943a5bfee98706fbf89d1d3a5076337e9757e0`
- Review scope: the committed Stage 0 fixture contract, generator, schemas, synthetic fixtures, validator, tests, and
  configuration. The two Stage 1 advisory documents were checked separately and remain `NO-GO` for live access.
- Non-author pinned recheck: no P0 or P1 finding. This recheck is advisory evidence, not the repository formal-review
  artifact.

## Reproducible checks

Run from `x-first-researcher-sourcing/` at the pinned commit:

```bash
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python -m unittest discover -s tests -v
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python -m x_first.contracts
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python scripts/generate_openai_fixture.py --check
../sourcing-ai-agent/.venv/bin/ruff check .
```

Observed results:

- tests: `12/12` passed;
- fixture validator: passed, precision `1.0`, recall `1.0`, `20/20` relevant observations, false merges `0`;
- generator check: current;
- Ruff: passed.

## Artifact hashes

```text
e5ede89c4d0dd04584c53cb45aa8d8ee6495991eae935026249151e1bc362f42  schema
c35cf2fb1b03abc8736addccb9afe0e2ec5f6b8d509c0b6c0b33a4f5da67d022  fixture
9eec1c52b4f97bc2a175592c951aa2c320c54df512d53333b2e66901860354db  gold
fc554fabd8997e5a0a0af0f87ba36c60d44f190655d5ddc7ced707236866988a  generator
324527a866e0996401ba2d4945809547ae7d64c93b74df747e6a5d5c07421ef8  contracts.py
267767399ed0a8c3326c2c4423a886d55be1326a1d4ebfd29423cca904283292  tests
154cad33dc9bb7780200539d84d5e788d68da8ec17d505a564a5aa0911a9ffbd  labs config
505c4cb604db2cedde506daee71fd66d5447ce944526911e85ff3d00e38c79c9  query-family config
```

## Safety boundary

Stage 0 uses synthetic `.invalid` identities and URLs only. Discovery/ranking population semantics are restricted to
lab, current professional affiliation, and pretraining relevance. The system must not infer or proxy ethnicity,
nationality, race, citizenship, religion, gender, or another protected identity from name, language, region, school,
community, biography, post, mention, or graph position.

Stage 1 live work remains blocked until its owner/legal/privacy/model/access/cost/rate/deadline/kill-switch/account and
retention decisions are explicit and the capability gate passes. The offline Grok advisory does not grant any of those
permissions.

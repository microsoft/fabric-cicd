# Triage evals

Regression tests for the AI issue-triage prompts. Two complementary layers:

## 1. Per-skill prompt evals (`gh models eval`)

Every skill under [`.github/prompts/skills/`](../prompts/skills/) ships its own `testData` +
`evaluators` block. GitHub Models runs them directly:

```bash
gh models eval .github/prompts/skills/classify.prompt.yml
gh models eval .github/prompts/skills/answer.prompt.yml
# …one per skill
```

Each evaluator asserts the model output `contains` the expected value for that test case (e.g.
`classify` must emit the right `category`). These run automatically on PRs that touch
`.github/prompts/**` via [`triage-evals.yml`](../workflows/triage-evals.yml).

## 2. Golden-set classification accuracy (`run-golden.mjs`)

[`triage-golden-set.jsonl`](./triage-golden-set.jsonl) is ~20 anonymized, representative issues
with an `expected_category` and `expected_resolution`. [`run-golden.mjs`](./run-golden.mjs) runs
the `classify` skill over the whole set and fails if category accuracy drops below a threshold:

```bash
# needs GITHUB_TOKEN with `models: read`
node .github/evals/run-golden.mjs            # threshold 0.80 (default)
THRESHOLD=0.9 node .github/evals/run-golden.mjs
```

Without a token it soft-passes (exit 0) so local checkouts and forks don't fail spuriously.

## Adding cases

- **New skill behavior** → add a `testData` case (with an `evaluators.string.contains`) to that
  skill's `.prompt.yml`.
- **New end-to-end category/resolution** → append a line to `triage-golden-set.jsonl`. Keep
  issues anonymized and representative; don't paste real user data verbatim.

## Schema — `triage-golden-set.jsonl`

One JSON object per line:

| Field                 | Meaning                                                                |
| --------------------- | ---------------------------------------------------------------------- |
| `id`                  | Stable slug for the case.                                              |
| `input`               | The issue text (title + body concatenated).                           |
| `expected_category`   | `bug` \| `feature` \| `question` \| `duplicate` \| `spam` \| `needs-info` |
| `expected_resolution` | `answered` \| `misconfiguration` \| `redirect-docs` \| `needs-info` \| `potential-bug` \| `backlog` \| `community` \| `duplicate` \| `none` |

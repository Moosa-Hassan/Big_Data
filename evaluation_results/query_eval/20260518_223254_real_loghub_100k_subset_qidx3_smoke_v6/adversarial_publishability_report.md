# Adversarial Publishability Report

## Claim Boundary

This evaluation supports an exact indexed static LogLite sidecar claim. It should not be framed as pure compressed-domain search, because qidx3 stores a normalized decoded line slab for exact verification.

## Exactness Audit

- Exact-mode failures: 0 cells.
- qidx3 stress-query failures: 0 cells.
- Build time is recorded separately in artifact build metrics and is not included in timed query measurements.

## Runtime Risks

- Native C++ qidx3 slower than grep/ripgrep cells: 0 of 32 compared cells.
- Python reference qidx3 slower than grep/ripgrep cells: 16 of 32 compared cells.
- Plaintext grep/ripgrep remain external baselines; conjunctive payloads are labeled as hybrid post-filtered baselines.

## Storage Risks

- qidx3 storage flags: 2 datasets with qidx3/raw > 1.0 or qidx3/static-compressed > 4.0.
- Report qidx3/raw, qidx3/static-compressed, and qidx3/qidx2 ratios alongside runtime claims.

## Amortization

- Median qidx3-build-only break-even query count: 164.02865335966.
- Treat preprocessing as an offline indexing cost and report the amortization point explicitly.

## Verdict

Potentially publishable after 100k/full-scale reruns, provided storage overhead and build amortization are reported honestly.

# Early-Builder Radar

Early-Builder Radar is an internal data platform for spotting inception-stage founders and proto-startups. The system ingests activity from developer and product communities, detects artifact evidence, and ranks daily candidates for human review.

## Repository Layout

- `common/` – shared utilities for IO, storage, logging, and language detection.
- `infra/` – infrastructure-as-code, environment configuration, and build targets.
- `ingest/` – source-specific collectors, normalizers, and backfill jobs.
- `processing/` – deduplication, entity resolution, and artifact detection pipelines.
- `features/` – feature snapshot builders, registries, and transformations.
- `models/` – heuristic and learned rankers plus training code.
- `eval/` – evaluation harness, backtesting tools, and reporting assets.
- `ui/` – analyst-facing application for triage and review (stub).
- `ops/` – monitoring, alerting, runbooks, and cost dashboards.
- `experiments/` – notebooks, scratch work, and experiment configs.
- `docs/` – architecture notes, ADRs, and contracts.

## Getting Started

1. Install Poetry or uv (see `pyproject.toml` for dependencies).
2. Install pre-commit hooks: `pre-commit install`.
3. Update environment variables and infra definitions before running collectors.

## Status

Scaffolding only. Add platform connectors, pipelines, and models before running in production.

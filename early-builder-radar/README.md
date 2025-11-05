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

1. Install Poetry: `pipx install poetry`.
2. Install project dependencies (dev tools included): `poetry install --with dev`.
3. Install pre-commit hooks: `poetry run pre-commit install`.
4. Update environment variables and infra definitions before running collectors.

## Tooling

- Linting: `poetry run ruff check .` and `poetry run black --check .`.
- Static typing: `poetry run mypy`.
- Tests: `poetry run pytest`.
- Run the full pre-commit suite locally before opening a PR: `poetry run pre-commit run --all-files`.

Continuous integration runs these same commands in GitHub Actions (`.github/workflows/ci.yaml`) on pushes and pull requests.

## Status

Scaffolding only. Add platform connectors, pipelines, and models before running in production.

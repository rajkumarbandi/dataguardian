# Contributing to DataGuardian

Thank you for investing time to contribute to DataGuardian. This document outlines the standards and process for contributing to this project.

---

## Code of Conduct

All contributors are expected to maintain professional, respectful, and constructive engagement. Technical disagreements should be resolved through evidence and discussion, not authority or personal criticism.

---

## Branching Strategy

We follow **GitHub Flow** with environment promotion gates:

```
main                 ← protected; deploys to DEV on merge
  └── feature/*      ← all development work
  └── fix/*          ← bug fixes
  └── chore/*        ← non-functional changes (docs, config, CI)
  └── release/*      ← release candidates for QA and PROD promotion
```

**Rules:**
- Never commit directly to `main`
- Branch names must follow the `type/short-description` pattern
- One logical change per branch
- Keep branches short-lived (merge within 3–5 days)

---

## Commit Messages

We follow the [Conventional Commits](https://www.conventionalcommits.org/) specification:

```
<type>(<scope>): <short description>

[optional body]

[optional footer]
```

**Types:**
- `feat` — new feature or capability
- `fix` — bug fix
- `refactor` — code restructuring without behavior change
- `test` — adding or updating tests
- `docs` — documentation changes
- `chore` — tooling, CI, configuration
- `perf` — performance improvement

**Examples:**
```
feat(ingestion): add JDBC connector for SQL Server sources
fix(quality): handle null values in completeness rule evaluation
docs(adr): add ADR-005 for Unity Catalog naming convention
chore(ci): add mypy type checking step to CI workflow
```

---

## Pull Request Process

1. Create a branch from `main` following the naming convention above
2. Make your changes with appropriate tests
3. Ensure all CI checks pass locally before pushing:
   ```bash
   make lint
   make type-check
   make test
   ```
4. Open a pull request using the provided PR template
5. Request review from at least one team member
6. Address all review comments before merging
7. Squash-merge into `main` with a clean commit message

---

## Development Standards

### Code Quality

- Follow PEP 8; formatting is enforced by `ruff`
- Type annotations are required on all public functions and methods
- Cyclomatic complexity must stay below 10 per function
- Functions should do one thing; keep them under 50 lines where possible

### Configuration

- Never hardcode environment-specific values in Python code
- All source, schema, and quality configurations live in `config/`
- Use the `ConfigLoader` utility for all configuration access
- Document every new YAML key in the relevant template file

### Testing

- All new features require unit tests in `tests/unit/`
- Integration tests in `tests/integration/` for pipeline-level changes
- Minimum 80% line coverage for new code
- Tests must be deterministic — no random seeds, no time-dependent logic without mocking

### Security

- Never commit credentials, API keys, or connection strings
- Use Azure Key Vault secret references in Databricks secret scopes
- Reference secrets via `{{secrets/scope/key}}` in DAB configs
- Rotate any accidentally committed secret immediately

---

## Adding a New Source Connector

1. Create a class in `src/ingestion/connectors/` extending `BaseConnector`
2. Implement the `read()` and `validate_connection()` methods
3. Add a config template in `config/sources/_source_template.yml`
4. Add unit tests in `tests/unit/test_{connector_name}_connector.py`
5. Document the connector in `docs/runbooks/onboarding-new-source.md`

---

## Adding a New Data Quality Rule

1. Create a class in `src/quality/rules/` extending `BaseRule`
2. Implement `evaluate(df: DataFrame) -> RuleResult`
3. Register the rule in `src/quality/rule_engine.py`
4. Add the rule key to `config/quality/_rules_template.yml`
5. Write a unit test with edge cases (nulls, empty DataFrames, boundary values)

---

## Documentation

- Architecture changes must be accompanied by an updated `docs/architecture/` document
- Significant technical decisions must have an ADR in `docs/adr/`
- Operational changes must be reflected in `docs/runbooks/`
- Keep the `CHANGELOG.md` updated

---

## Local Setup

```bash
# Install all dependencies including dev tools
pip install -e ".[dev]"

# Run linting
make lint

# Run type checking
make type-check

# Run tests
make test

# Run full pre-commit suite
make check-all
```

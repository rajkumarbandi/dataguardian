#!/usr/bin/env python3
"""Validate all DataGuardian YAML configuration files for a given environment.

Usage:
    python scripts/validate_config.py --env dev
    python scripts/validate_config.py --env dev --source erp_customers
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

CONFIG_ROOT = Path(__file__).parent.parent / "config"

REQUIRED_ENV_KEYS = [
    "environment",
    "unity_catalog",
    "storage",
    "ingestion",
    "quality",
    "ai",
    "logging",
]


def validate_environment(env: str) -> list[str]:
    errors: list[str] = []
    env_file = CONFIG_ROOT / "environments" / f"{env}.yml"
    if not env_file.exists():
        return [f"Environment config not found: {env_file}"]
    try:
        import yaml
        with open(env_file) as f:
            config = yaml.safe_load(f)
        for key in REQUIRED_ENV_KEYS:
            if key not in config:
                errors.append(f"environments/{env}.yml: missing required key '{key}'")
    except Exception as e:
        errors.append(f"environments/{env}.yml: parse error — {e}")
    return errors


def validate_sources(source_filter: str | None = None) -> list[str]:
    errors: list[str] = []
    source_dir = CONFIG_ROOT / "sources"
    files = [source_dir / f"{source_filter}.yml"] if source_filter else source_dir.glob("*.yml")
    for f in files:
        if f.name.startswith("_"):
            continue
        try:
            import yaml
            with open(f) as fh:
                config = yaml.safe_load(fh)
            source = config.get("source", {})
            for key in ["name", "entity", "connector"]:
                if not source.get(key):
                    errors.append(f"sources/{f.name}: missing required key 'source.{key}'")
        except Exception as e:
            errors.append(f"sources/{f.name}: parse error — {e}")
    return errors


def validate_schemas() -> list[str]:
    errors: list[str] = []
    schema_dir = CONFIG_ROOT / "schemas"
    for f in schema_dir.glob("*.yml"):
        if f.name.startswith("_"):
            continue
        try:
            import yaml
            with open(f) as fh:
                config = yaml.safe_load(fh)
            for key in ["entity", "business_key", "columns"]:
                if not config.get(key):
                    errors.append(f"schemas/{f.name}: missing required key '{key}'")
        except Exception as e:
            errors.append(f"schemas/{f.name}: parse error — {e}")
    return errors


def validate_quality_suites() -> list[str]:
    errors: list[str] = []
    quality_dir = CONFIG_ROOT / "quality"
    for f in quality_dir.glob("*.yml"):
        if f.name.startswith("_"):
            continue
        try:
            import yaml
            with open(f) as fh:
                config = yaml.safe_load(fh)
            rules = config.get("rules", [])
            total_weight = sum(r.get("weight", 0) for r in rules)
            if abs(total_weight - 1.0) > 0.001:
                errors.append(
                    f"quality/{f.name}: rule weights sum to {total_weight:.3f}, must sum to 1.0"
                )
        except Exception as e:
            errors.append(f"quality/{f.name}: parse error — {e}")
    return errors


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate DataGuardian YAML configuration files")
    parser.add_argument("--env", required=True, choices=["dev", "qa", "prod", "test"])
    parser.add_argument("--source", default=None, help="Validate a specific source only")
    args = parser.parse_args()

    print(f"Validating configuration for environment: {args.env}")
    errors: list[str] = []

    errors += validate_environment(args.env)
    errors += validate_sources(args.source)
    errors += validate_schemas()
    errors += validate_quality_suites()

    if errors:
        print(f"\nFound {len(errors)} validation error(s):\n")
        for e in errors:
            print(f"  ERROR: {e}")
        sys.exit(1)
    else:
        print("All configuration files are valid.")


if __name__ == "__main__":
    main()

"""
DQ rule implementations — auto-registers all built-in rules on import.

Importing this package as a side-effect populates ``RuleRegistry`` with all
11 built-in rule types.  The engine imports this module at startup; notebooks
and tests that import the engine get registration for free.

Extension point
---------------
Register a custom or AI-powered rule anywhere before the engine runs::

    from src.quality.registry import RuleRegistry
    from mypackage import MyCustomRule

    RuleRegistry.register("my_rule", MyCustomRule)

The engine resolves ``rule: my_rule`` from YAML without further changes.
"""

from __future__ import annotations

from src.quality.registry import RuleRegistry
from src.quality.rules.allowed_values import AllowedValuesRule
from src.quality.rules.country_code_rule import CountryCodeRule
from src.quality.rules.email_rule import EmailRule
from src.quality.rules.foreign_key import ForeignKeyRule
from src.quality.rules.future_date import FutureDateRule
from src.quality.rules.not_null import NotNullRule
from src.quality.rules.positive_number import PositiveNumberRule
from src.quality.rules.primary_key import PrimaryKeyRule
from src.quality.rules.regex_rule import RegexRule
from src.quality.rules.sql_expression import SqlExpressionRule
from src.quality.rules.unique import UniqueRule

RuleRegistry.register("not_null", NotNullRule)
RuleRegistry.register("unique", UniqueRule)
RuleRegistry.register("regex", RegexRule)
RuleRegistry.register("email", EmailRule)
RuleRegistry.register("country_code", CountryCodeRule)
RuleRegistry.register("positive_number", PositiveNumberRule)
RuleRegistry.register("allowed_values", AllowedValuesRule)
RuleRegistry.register("future_date", FutureDateRule)
RuleRegistry.register("primary_key", PrimaryKeyRule)
RuleRegistry.register("foreign_key", ForeignKeyRule)
RuleRegistry.register("sql_expression", SqlExpressionRule)

__all__ = [
    "AllowedValuesRule",
    "CountryCodeRule",
    "EmailRule",
    "ForeignKeyRule",
    "FutureDateRule",
    "NotNullRule",
    "PositiveNumberRule",
    "PrimaryKeyRule",
    "RegexRule",
    "SqlExpressionRule",
    "UniqueRule",
]

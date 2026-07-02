"""
Rule registry for the DataGuardian Data Quality framework.

``RuleRegistry`` maps rule-type strings (as declared in source YAML) to
concrete ``BaseRule`` subclasses.  It is the single extension point for
adding new rule implementations without changing the engine.

Built-in rules are registered in ``src/quality/rules/__init__.py`` at import
time.  Third-party or AI-generated rules can be registered anywhere before
``DataQualityEngine.run()`` is called::

    from src.quality.registry import RuleRegistry
    from mypackage.rules import AnomalyDetectionRule

    RuleRegistry.register("ai_anomaly", AnomalyDetectionRule)

The engine then resolves ``rule: ai_anomaly`` from YAML transparently.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from src.common.exceptions import ConfigurationError

if TYPE_CHECKING:
    from src.quality.rules.base_rule import BaseRule


class RuleRegistry:
    """
    Class-level registry mapping rule-type strings to ``BaseRule`` subclasses.

    All methods are class methods — there is no instance state.
    """

    _rules: dict[str, type[BaseRule]] = {}

    @classmethod
    def register(cls, rule_type: str, rule_class: type[BaseRule]) -> None:
        """
        Register ``rule_class`` under ``rule_type``.

        Re-registering an existing key silently replaces it — this allows
        tests and plugins to override built-in rules.

        Parameters
        ----------
        rule_type:
            The snake_case identifier used in source YAML ``rule:`` fields.
        rule_class:
            A concrete subclass of ``BaseRule``.
        """
        cls._rules[rule_type] = rule_class

    @classmethod
    def get(cls, rule_type: str) -> BaseRule:
        """
        Return a fresh instance of the rule registered under ``rule_type``.

        Raises
        ------
        ConfigurationError
            If ``rule_type`` has not been registered.
        """
        rule_class = cls._rules.get(rule_type)
        if rule_class is None:
            registered = sorted(cls._rules)
            raise ConfigurationError(
                f"Unknown DQ rule type {rule_type!r}. "
                f"Registered types: {registered}. "
                "Add the rule to RuleRegistry.register() or check the "
                "'rule:' field spelling in your source YAML."
            )
        return rule_class()

    @classmethod
    def registered_types(cls) -> list[str]:
        """Return sorted list of all registered rule type strings."""
        return sorted(cls._rules)

    @classmethod
    def is_registered(cls, rule_type: str) -> bool:
        """Return ``True`` if ``rule_type`` is registered."""
        return rule_type in cls._rules

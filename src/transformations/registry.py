"""
Transformation registry for the DataGuardian Transformation Framework.

``TransformationRegistry`` maps transformation-type strings (as declared in
source YAML ``type:`` fields) to concrete ``BaseTransformation`` subclasses.
It is the single extension point for adding new transformations without
changing the engine.

Built-in transformations are registered in
``src/transformations/transforms/__init__.py`` at import time.  Custom
transformations can be registered anywhere before
``TransformationEngine.run()`` is called::

    from src.transformations.registry import TransformationRegistry
    from mypackage.transforms import MyTransformation

    TransformationRegistry.register("my_transform", MyTransformation)

The engine then resolves ``type: my_transform`` from YAML transparently.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from src.common.exceptions import ConfigurationError

if TYPE_CHECKING:
    from src.transformations.base_transformation import BaseTransformation


class TransformationRegistry:
    """
    Class-level registry mapping transformation-type strings to
    ``BaseTransformation`` subclasses.

    All methods are class methods — there is no instance state.
    """

    _transformations: dict[str, type[BaseTransformation]] = {}

    @classmethod
    def register(
        cls,
        transform_type: str,
        transform_class: type[BaseTransformation],
    ) -> None:
        """
        Register ``transform_class`` under ``transform_type``.

        Re-registering an existing key silently replaces it — this allows
        tests and plugins to override built-in transformations.

        Parameters
        ----------
        transform_type:
            The snake_case identifier used in source YAML ``type:`` fields.
        transform_class:
            A concrete subclass of ``BaseTransformation``.
        """
        cls._transformations[transform_type] = transform_class

    @classmethod
    def get(cls, transform_type: str) -> BaseTransformation:
        """
        Return a fresh instance of the transformation registered under
        ``transform_type``.

        Raises
        ------
        ConfigurationError
            If ``transform_type`` has not been registered.
        """
        transform_class = cls._transformations.get(transform_type)
        if transform_class is None:
            registered = sorted(cls._transformations)
            raise ConfigurationError(
                f"Unknown transformation type {transform_type!r}. "
                f"Registered types: {registered}. "
                "Register the transformation or check the 'type:' field "
                "spelling in your source YAML."
            )
        return transform_class()

    @classmethod
    def registered_types(cls) -> list[str]:
        """Return sorted list of all registered transformation type strings."""
        return sorted(cls._transformations)

    @classmethod
    def is_registered(cls, transform_type: str) -> bool:
        """Return ``True`` if ``transform_type`` is registered."""
        return transform_type in cls._transformations

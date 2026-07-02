"""Auto-register all 18 built-in transformations on import.

Importing this package is sufficient to make every transformation available
through ``TransformationRegistry``.  The engine calls
``import src.transformations.transforms`` before the first registry lookup,
so callers never need to reference individual transform modules.
"""

from src.transformations.registry import TransformationRegistry
from src.transformations.transforms.add_constant_column import AddConstantColumnTransformation
from src.transformations.transforms.add_timestamp_column import AddTimestampColumnTransformation
from src.transformations.transforms.cast_column import CastColumnTransformation
from src.transformations.transforms.column_mapping import ColumnMappingTransformation
from src.transformations.transforms.concatenate_columns import ConcatenateColumnsTransformation
from src.transformations.transforms.date_format import DateFormatTransformation
from src.transformations.transforms.derived_column import DerivedColumnTransformation
from src.transformations.transforms.drop_columns import DropColumnsTransformation
from src.transformations.transforms.filter_rows import FilterRowsTransformation
from src.transformations.transforms.lower_case import LowerCaseTransformation
from src.transformations.transforms.null_replacement import NullReplacementTransformation
from src.transformations.transforms.remove_duplicates import RemoveDuplicatesTransformation
from src.transformations.transforms.rename_column import RenameColumnTransformation
from src.transformations.transforms.select_columns import SelectColumnsTransformation
from src.transformations.transforms.sort_rows import SortRowsTransformation
from src.transformations.transforms.split_column import SplitColumnTransformation
from src.transformations.transforms.trim_strings import TrimStringsTransformation
from src.transformations.transforms.upper_case import UpperCaseTransformation

TransformationRegistry.register("rename_column", RenameColumnTransformation)
TransformationRegistry.register("drop_columns", DropColumnsTransformation)
TransformationRegistry.register("select_columns", SelectColumnsTransformation)
TransformationRegistry.register("cast_column", CastColumnTransformation)
TransformationRegistry.register("trim_strings", TrimStringsTransformation)
TransformationRegistry.register("upper_case", UpperCaseTransformation)
TransformationRegistry.register("lower_case", LowerCaseTransformation)
TransformationRegistry.register("null_replacement", NullReplacementTransformation)
TransformationRegistry.register("add_constant_column", AddConstantColumnTransformation)
TransformationRegistry.register("add_timestamp_column", AddTimestampColumnTransformation)
TransformationRegistry.register("derived_column", DerivedColumnTransformation)
TransformationRegistry.register("date_format", DateFormatTransformation)
TransformationRegistry.register("concatenate_columns", ConcatenateColumnsTransformation)
TransformationRegistry.register("split_column", SplitColumnTransformation)
TransformationRegistry.register("filter_rows", FilterRowsTransformation)
TransformationRegistry.register("sort_rows", SortRowsTransformation)
TransformationRegistry.register("remove_duplicates", RemoveDuplicatesTransformation)
TransformationRegistry.register("column_mapping", ColumnMappingTransformation)

__all__ = [
    "AddConstantColumnTransformation",
    "AddTimestampColumnTransformation",
    "CastColumnTransformation",
    "ColumnMappingTransformation",
    "ConcatenateColumnsTransformation",
    "DateFormatTransformation",
    "DerivedColumnTransformation",
    "DropColumnsTransformation",
    "FilterRowsTransformation",
    "LowerCaseTransformation",
    "NullReplacementTransformation",
    "RemoveDuplicatesTransformation",
    "RenameColumnTransformation",
    "SelectColumnsTransformation",
    "SortRowsTransformation",
    "SplitColumnTransformation",
    "TrimStringsTransformation",
    "UpperCaseTransformation",
]

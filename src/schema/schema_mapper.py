"""Schema mapper — applies column mappings and type coercions to produce canonical DataFrames."""

from __future__ import annotations

# TODO (Milestone 3): Implement SchemaMapper
#
# Mapping resolution order (see docs/design/schema-mapping-design.md):
# 1. Explicit alias declared in source config column_mappings
# 2. Case-insensitive name match against canonical column name or its aliases list
# 3. AI suggestion (if AI enabled and AISchemaMappingService available)
# 4. Mark as _unmapped_{original_name}
#
# Type coercion:
# - Apply safe casts; record failures as _type_error_{col} flags
# - Date strings: attempt multiple format patterns before failing
# - Numeric strings: strip whitespace and currency symbols before cast
#
# Output:
# - DataFrame with canonical column names
# - All unmapped source columns prefixed with _unmapped_
# - All type error flags appended

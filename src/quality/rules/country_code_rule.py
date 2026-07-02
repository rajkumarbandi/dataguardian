"""Rule: country_code — column value must be a valid ISO 3166-1 alpha-2 code."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as F

from src.quality.rules.base_rule import BaseRule

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

# Complete ISO 3166-1 alpha-2 list (249 entries, current as of 2024).
# Regex alone cannot validate this — ZZ matches [A-Z]{2} but is not a real code.
_ISO_3166_1_ALPHA2: frozenset[str] = frozenset({
    "AF", "AX", "AL", "DZ", "AS", "AD", "AO", "AI", "AQ", "AG",
    "AR", "AM", "AW", "AU", "AT", "AZ", "BS", "BH", "BD", "BB",
    "BY", "BE", "BZ", "BJ", "BM", "BT", "BO", "BQ", "BA", "BW",
    "BV", "BR", "IO", "BN", "BG", "BF", "BI", "CV", "KH", "CM",
    "CA", "KY", "CF", "TD", "CL", "CN", "CX", "CC", "CO", "KM",
    "CG", "CD", "CK", "CR", "CI", "HR", "CU", "CW", "CY", "CZ",
    "DK", "DJ", "DM", "DO", "EC", "EG", "SV", "GQ", "ER", "EE",
    "SZ", "ET", "FK", "FO", "FJ", "FI", "FR", "GF", "PF", "TF",
    "GA", "GM", "GE", "DE", "GH", "GI", "GR", "GL", "GD", "GP",
    "GU", "GT", "GG", "GN", "GW", "GY", "HT", "HM", "VA", "HN",
    "HK", "HU", "IS", "IN", "ID", "IR", "IQ", "IE", "IM", "IL",
    "IT", "JM", "JP", "JE", "JO", "KZ", "KE", "KI", "KP", "KR",
    "KW", "KG", "LA", "LV", "LB", "LS", "LR", "LY", "LI", "LT",
    "LU", "MO", "MG", "MW", "MY", "MV", "ML", "MT", "MH", "MQ",
    "MR", "MU", "YT", "MX", "FM", "MD", "MC", "MN", "ME", "MS",
    "MA", "MZ", "MM", "NA", "NR", "NP", "NL", "NC", "NZ", "NI",
    "NE", "NG", "NU", "NF", "MK", "MP", "NO", "OM", "PK", "PW",
    "PS", "PA", "PG", "PY", "PE", "PH", "PN", "PL", "PT", "PR",
    "QA", "RE", "RO", "RU", "RW", "BL", "SH", "KN", "LC", "MF",
    "PM", "VC", "WS", "SM", "ST", "SA", "SN", "RS", "SC", "SL",
    "SG", "SX", "SK", "SI", "SB", "SO", "ZA", "GS", "SS", "ES",
    "LK", "SD", "SR", "SJ", "SE", "CH", "SY", "TW", "TJ", "TZ",
    "TH", "TL", "TG", "TK", "TO", "TT", "TN", "TR", "TM", "TC",
    "TV", "UG", "UA", "AE", "GB", "UM", "US", "UY", "UZ", "VU",
    "VE", "VN", "VG", "VI", "WF", "EH", "YE", "ZM", "ZW",
})


class CountryCodeRule(BaseRule):
    """
    Fails any non-null row where ``column`` is not a valid ISO 3166-1 alpha-2 code.

    Uses a whitelist of all 249 official codes — regex alone cannot catch
    invalid-but-structurally-valid codes like ``ZZ`` or ``UK``.

    Common errors caught
    --------------------
    * ``UK``  — should be ``GB``
    * ``ZZ``, ``XX`` — fictitious/test codes
    * Three-character codes (ISO 3166-1 alpha-3) mistakenly used

    Null treatment
    --------------
    Null values PASS.  Use ``not_null`` to enforce non-nullability.

    Custom list
    -----------
    Override the full list via ``params.allowed_codes`` if a restricted
    country subset is appropriate::

        - rule: country_code
          column: shipping_country
          params:
            allowed_codes: [US, GB, CA, AU, DE, FR]
    """

    @property
    def rule_type(self) -> str:
        return "country_code"

    def apply(
        self,
        df: DataFrame,
        column: str,
        pass_column: str,
        params: dict[str, Any],
        spark: SparkSession | None = None,
    ) -> DataFrame:
        allowed: list[str] = list(
            params.get("allowed_codes", _ISO_3166_1_ALPHA2)
        )
        return df.withColumn(
            pass_column,
            F.col(column).isNull() | F.col(column).isin(allowed),
        )

    def error_message(self, column: str, params: dict[str, Any]) -> str:
        return (
            f"Column '{column}' contains an invalid ISO 3166-1 alpha-2 country code. "
            "Common mistakes: 'UK' (should be 'GB'), 'ZZ', 'XX'."
        )

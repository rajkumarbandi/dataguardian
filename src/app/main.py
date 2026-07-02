"""
DataGuardian Stewardship Portal — legacy entry point.

The Databricks Apps entry point is app.py (referenced in app.yaml).
This module is retained for backward compatibility and CLI use.
"""

from __future__ import annotations

import subprocess
import sys


def main() -> None:
    """Launch the Streamlit app via subprocess (for CLI or testing)."""
    import os
    app_path = os.path.join(os.path.dirname(__file__), "app.py")
    subprocess.run(
        [sys.executable, "-m", "streamlit", "run", app_path],
        check=False,
    )


if __name__ == "__main__":
    main()

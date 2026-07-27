"""Allow ``python -m poly_trader`` to work (delegates to platform CLI)."""
import sys
from pathlib import Path

if __package__ is None:
    # Direct execution (python __main__.py) — add root to path
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from platforms.main import main
else:
    from platforms.main import main

main()

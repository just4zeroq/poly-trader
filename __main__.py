"""Allow ``python -m poly_trader`` to work (delegates to platform CLI)."""
from .platform.main import main

main()

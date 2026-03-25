"""
Pytest configuration for ChannelForge tests.

Sets up sys.path so that src/ and project root are importable without
needing to install the package.
"""

import sys
from pathlib import Path

# Ensure project root is on the path
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

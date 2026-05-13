"""Generate contaminant overview markdown files.

Usage:
  python3 5.py            # top-50 summary (CONTAMINANTS.md)
  python3 5.py --full     # all contaminants (CONTAMINANTS-full.md)
"""

import sys
from pathlib import Path
from contaminants import generate

if __name__ == "__main__":
    generate(Path("output"), full="--full" in sys.argv)

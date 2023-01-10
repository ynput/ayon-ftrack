import os
import sys

ADDON_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


for path in (
    os.path.join(ADDON_DIR),
    os.path.join(ADDON_DIR, "services", "leecher"),
):
    sys.path.insert(0, path)

from leecher import main


if __name__ == "__main__":
    main()
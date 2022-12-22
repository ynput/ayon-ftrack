import os
import sys
import logging

ADDON_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


for path in (
    os.path.join(ADDON_DIR),
    os.path.join(ADDON_DIR, "services", "processor"),
):
    sys.path.insert(0, path)

from processor import main


if __name__ == "__main__":
    logging.basicConfig()
    main()
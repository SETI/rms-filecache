################################################################################
# tests_programs/locking.py
#
# Test that locking actually works on a shared cache. This program is meant to
# be run in multiple shells simultaneously.
################################################################################

import logging
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from filecache import FileCache


logging.basicConfig(level=logging.DEBUG)
logging.getLogger("filelock").setLevel(logging.INFO)
my_logger = logging.getLogger(__name__)
fc = FileCache(shared=True, logger=my_logger)


src = fc.new_source('gs://rms-node-holdings/pds3-holdings/metadata/COISS_2xxx')

for num in range(2001, 2117):
    path = src.retrieve(f'COISS_{num}/COISS_{num}_index.tab')
    print(path)

fc.clean_up(final=True)

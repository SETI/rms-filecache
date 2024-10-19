################################################################################
# tests_programs/multi_download.py
#
# Test the speed of multiple downloads vs. sequential downloads.
################################################################################

import logging
from pathlib import Path
import sys
import time
import uuid

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from filecache import FileCache

TEST_SEQ = False
TEST_PAR = True

NUM_VOL = 50
DEST_BUCKET = 'gs://rms-filecache-tests-writable'

my_logger = logging.getLogger(__name__)
my_logger.setLevel(logging.DEBUG)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
my_logger.addHandler(handler)

for dl_prefix, ul_prefix in [('gs://rms-node-holdings/pds3-holdings',
                              'gs://rms-filecache-tests-writable'),
                             ('https://pds-rings.seti.org/holdings',
                              's3://rms-filecache-tests-writable')]:
    if TEST_SEQ:
        with FileCache(shared=True, cache_owner=True, logger=my_logger) as fc:
            pfx1 = fc.new_prefix(f'{dl_prefix}/metadata/COISS_2xxx')
            start_time = time.time()
            for num in range(2001, 2001+NUM_VOL):
                path = pfx1.retrieve(f'COISS_{num}/COISS_{num}_index.tab')
            end_time = time.time()
            dl_seq_time = end_time-start_time

            pfx2 = fc.new_prefix(f'{ul_prefix}/{uuid.uuid4()}')
            for num in range(2001, 2001+NUM_VOL):
                path1 = pfx1.get_local_path(f'COISS_{num}/COISS_{num}_index.tab')
                path2 = pfx2.get_local_path(f'COISS_{num}/COISS_{num}_index.tab')
                with open(path1, 'rb') as fp1:
                    with open(path2, 'wb') as fp2:
                        fp2.write(fp1.read())

            start_time = time.time()
            for num in range(2001, 2001+NUM_VOL):
                path = pfx2.upload(f'COISS_{num}/COISS_{num}_index.tab')
            end_time = time.time()
            ul_seq_time = end_time-start_time

    if TEST_PAR:
        with FileCache(shared=True, cache_owner=True, logger=my_logger) as fc:
            pfx1 = fc.new_prefix(f'{dl_prefix}/metadata/COISS_2xxx', lock_timeout=2)
            start_time = time.time()
            all_paths = [f'COISS_{num}/COISS_{num}_index.tab'
                            for num in range(2001, 2001+NUM_VOL)]
            paths = pfx1.retrieve(all_paths)
            end_time = time.time()
            dl_par_time = end_time-start_time

            pfx2 = fc.new_prefix(f'{ul_prefix}/{uuid.uuid4()}')
            for num in range(2001, 2001+NUM_VOL):
                path1 = pfx1.get_local_path(f'COISS_{num}/COISS_{num}_index.tab')
                path2 = pfx2.get_local_path(f'COISS_{num}/COISS_{num}_index.tab')
                with open(path1, 'rb') as fp1:
                    with open(path2, 'wb') as fp2:
                        fp2.write(fp1.read())

            start_time = time.time()
            paths = pfx2.upload(all_paths)
            end_time = time.time()
            ul_par_time = end_time-start_time

    print()
    print(dl_prefix)
    if TEST_SEQ:
        print('Sequential download:', dl_seq_time)
    if TEST_PAR:
        print('Parallel download:', dl_par_time)
    print()
    print(ul_prefix)
    if TEST_SEQ:
        print('Sequential upload:', ul_seq_time)
    if TEST_PAR:
        print('Parallel upload time:', ul_par_time)
    print()

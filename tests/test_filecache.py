################################################################################
# tests/test_filecache.py
################################################################################

import unittest

from filecache import FileCache


class TestSolar(unittest.TestCase):
    def test_local_filesystem(self):
        fc = FileCache()

    def test_gs(self):
        fc = FileCache()

        gs = fc.new_source('gs://rms-node-holdings/pds3-holdings')
        path = gs.retrieve('previews/CORSS_8xxx/CORSS_8001/browse/Rev007_OccTrack_Geometry_full.jpg')

        fc.clean_up()

    def test_web(self):
        fc = FileCache()

        ws = fc.new_source('https://pds-rings.seti.org/holdings')
        path = ws.retrieve('previews/CORSS_8xxx/CORSS_8001/browse/Rev007_OccTrack_Geometry_full.jpg')

        fc.clean_up()

    def test_filecache_context(self):
        with FileCache() as fc:
            ws = fc.new_source('https://pds-rings.seti.org/holdings')
            path = ws.retrieve('previews/CORSS_8xxx/CORSS_8001/browse/Rev007_OccTrack_Geometry_full.jpg')

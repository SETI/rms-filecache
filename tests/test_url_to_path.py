################################################################################
# tests/test_url_to_path.py
################################################################################

from pathlib import Path
import uuid

import pytest

from filecache import FileCache

from .test_file_cache import (EXPECTED_DIR,
                              HTTP_TEST_ROOT,
                              GS_WRITABLE_TEST_BUCKET_ROOT,
                              EXPECTED_FILENAMES
                              )


def translator_subdir2a_rel(scheme, remote, path, cache_dir, cache_subdir):
    if not ((scheme == 'file' and remote == '') or
            (scheme == 'https' and remote == 'storage.googleapis.com')):
        return None
    if 'subdir2a/' not in path:
        return None

    return Path(path.replace('subdir2a/', ''))  # Relative


def translator_subdir2b_rel(scheme, remote, path, cache_dir, cache_subdir):
    if not ((scheme == 'file' and remote == '') or
            (scheme == 'https' and remote == 'storage.googleapis.com')):
        return None
    if remote != '':
        return None
    if 'subdir2b/' not in path:
        return None

    return Path(path.replace('subdir2b/', ''))  # Relative


def translator_subdir2a_abs(scheme, remote, path, cache_dir, cache_subdir):
    if not ((scheme == 'file' and remote == '') or
            (scheme == 'https' and remote == 'storage.googleapis.com')):
        return None
    if remote != '':
        return None
    if 'subdir2a/' not in path:
        return None

    return cache_dir / cache_subdir / path.replace('subdir2a/', '')  # Absolute


def translator_subdir2b_abs(scheme, remote, path, cache_dir, cache_subdir):
    if not ((scheme == 'file' and remote == '') or
            (scheme == 'https' and remote == 'storage.googleapis.com')):
        return None
    if remote != '':
        return None
    if 'subdir2b/' not in path:
        return None

    return cache_dir / cache_subdir / path.replace('subdir2b/', '')  # Absolute


def test_translator_local_rel():
    with FileCache() as fc:
        for filename in EXPECTED_FILENAMES:
            path = EXPECTED_DIR / filename
            assert fc.get_local_path(path) == path
            assert fc.exists(path)
            assert fc.retrieve(path) == path
            assert fc.upload(path) == path

    with FileCache(url_to_path=translator_subdir2a_rel) as fc:
        for filename in EXPECTED_FILENAMES:
            path = EXPECTED_DIR / filename
            new_path = EXPECTED_DIR / (filename.replace('subdir2a/', ''))
            assert fc.get_local_path(path) == new_path
            if path == new_path:
                assert fc.exists(path)
            else:
                assert not fc.exists(path)
            # assert fc.retrieve(path) == new_path
            # assert fc.upload(path) == new_path

    with FileCache(url_to_path=translator_subdir2b_rel) as fc:
        for filename in EXPECTED_FILENAMES:
            path = EXPECTED_DIR / filename
            new_path = EXPECTED_DIR / (filename.replace('subdir2b/', ''))
            assert fc.get_local_path(path) == new_path
            if path == new_path:
                assert fc.exists(path)
            else:
                assert not fc.exists(path)
            # assert fc.retrieve(path) == new_path
            # assert fc.upload(path) == new_path

    with FileCache(url_to_path=[translator_subdir2a_rel,
                                translator_subdir2b_rel]) as fc:
        for filename in EXPECTED_FILENAMES:
            path = EXPECTED_DIR / filename
            new_path = EXPECTED_DIR / (filename
                                       .replace('subdir2a/', '')
                                       .replace('subdir2b/', ''))
            assert fc.get_local_path(path) == new_path
            if path == new_path:
                assert fc.exists(path)
            else:
                assert not fc.exists(path)
            # assert fc.retrieve(path) == new_path
            # assert fc.upload(path) == new_path

    with FileCache(url_to_path=[translator_subdir2a_rel]) as fc:
        translators = [translator_subdir2a_rel,
                       translator_subdir2b_rel]
        for filename in EXPECTED_FILENAMES:
            path = EXPECTED_DIR / filename
            new_path = EXPECTED_DIR / (filename
                                       .replace('subdir2a/', '')
                                       .replace('subdir2b/', ''))
            assert fc.get_local_path(path, url_to_path=translators) == new_path
            if path == new_path:
                assert fc.exists(path, url_to_path=translators)
            else:
                assert not fc.exists(path, url_to_path=translators)
            # assert fc.retrieve(path) == new_path
            # assert fc.upload(path) == new_path


def test_translator_local_abs():
    with FileCache() as fc:
        translators = [translator_subdir2a_abs, translator_subdir2b_abs]
        for filename in EXPECTED_FILENAMES:
            path = EXPECTED_DIR / filename
            new_path = EXPECTED_DIR / (filename
                                       .replace('subdir2a/', '')
                                       .replace('subdir2b/', ''))
            assert fc.get_local_path(path, url_to_path=translators) == new_path
            if path == new_path:
                assert fc.exists(path, url_to_path=translators)
            else:
                assert not fc.exists(path, url_to_path=translators)
            # assert fc.retrieve(path) == new_path
            # assert fc.upload(path) == new_path

        translators = [translator_subdir2a_rel, translator_subdir2b_abs]
        for filename in EXPECTED_FILENAMES:
            path = EXPECTED_DIR / filename
            new_path = EXPECTED_DIR / (filename
                                       .replace('subdir2a/', '')
                                       .replace('subdir2b/', ''))
            assert fc.get_local_path(path, url_to_path=translators) == new_path
            if path == new_path:
                assert fc.exists(path, url_to_path=translators)
            else:
                assert not fc.exists(path, url_to_path=translators)
            # assert fc.retrieve(path) == new_path
            # assert fc.upload(path) == new_path


def test_translator_local_pfx():
    with FileCache() as fc:
        pfx = fc.new_path(EXPECTED_DIR)
        for filename in EXPECTED_FILENAMES:
            path = EXPECTED_DIR / filename
            assert pfx.get_local_path(filename) == path
            assert pfx.exists(filename)
            assert pfx.retrieve(filename) == path
            assert pfx.upload(filename) == path

    with FileCache(url_to_path=translator_subdir2a_rel) as fc:
        pfx = fc.new_path(EXPECTED_DIR)
        for filename in EXPECTED_FILENAMES:
            path = EXPECTED_DIR / filename
            new_path = EXPECTED_DIR / (filename.replace('subdir2a/', ''))
            assert pfx.get_local_path(filename) == new_path
            if path == new_path:
                assert pfx.exists(filename)
            else:
                assert not pfx.exists(filename)
            # assert fc.retrieve(path) == new_path
            # assert fc.upload(path) == new_path

    with FileCache(url_to_path=[translator_subdir2a_rel]) as fc:
        translators = [translator_subdir2a_rel,
                       translator_subdir2b_rel]
        pfx = fc.new_path(EXPECTED_DIR)
        for filename in EXPECTED_FILENAMES:
            path = EXPECTED_DIR / filename
            new_path = EXPECTED_DIR / (filename
                                       .replace('subdir2a/', '')
                                       .replace('subdir2b/', ''))
            assert pfx.get_local_path(filename, url_to_path=translators) == new_path
            if path == new_path:
                assert pfx.exists(filename, url_to_path=translators)
            else:
                assert not pfx.exists(filename, url_to_path=translators)
            # assert fc.retrieve(path) == new_path
            # assert fc.upload(path) == new_path


def test_translator_http():
    with FileCache(None, url_to_path=translator_subdir2a_rel) as fc:
        url = HTTP_TEST_ROOT + '/' + EXPECTED_FILENAMES[1]
        path = EXPECTED_FILENAMES[1].replace('subdir2a/', '')
        exp_local_path = fc.cache_dir / (HTTP_TEST_ROOT.replace('https://', 'http_') +
                                         '/' + path)

        with FileCache(None) as fc2:
            fc2_path = str(uuid.uuid4()) + '/' + EXPECTED_FILENAMES[1]
            url2 = GS_WRITABLE_TEST_BUCKET_ROOT + '/' + fc2_path

            # Translate between caches
            def translate_fc_fc2(scheme, remote, path, cache_dir, cache_subdir):
                assert scheme == 'gs'
                assert remote == GS_WRITABLE_TEST_BUCKET_ROOT.replace('gs://', '')
                assert path == fc2_path
                assert cache_dir == fc2.cache_dir
                assert cache_subdir == (GS_WRITABLE_TEST_BUCKET_ROOT
                                        .replace('gs://', 'gs_'))
                return exp_local_path

            assert fc.get_local_path(url) == exp_local_path
            assert fc2.get_local_path(url2,
                                      url_to_path=translate_fc_fc2) == exp_local_path
            assert not exp_local_path.is_file()
            assert fc.exists(url, bypass_cache=True)
            assert fc.exists(url)
            assert not fc2.exists(url2, url_to_path=translate_fc_fc2)
            assert not fc2.exists(url2, url_to_path=translate_fc_fc2, bypass_cache=True)

            assert fc.retrieve(url) == exp_local_path
            assert exp_local_path.is_file()

            assert fc.exists(url)  # Checks the local cache
            assert fc.exists(url, bypass_cache=True)  # Checks the web
            assert not fc2.exists(url2)  # Checks the local cache then GS
            assert fc2.exists(url2, url_to_path=translate_fc_fc2)  # Checks local cache
            assert not fc2.exists(url2, url_to_path=translate_fc_fc2, bypass_cache=True)

            with pytest.raises(FileNotFoundError):
                fc.upload(url2)

            assert fc2.upload(url2, url_to_path=translate_fc_fc2) == exp_local_path
            assert fc2.exists(url2, url_to_path=translate_fc_fc2, bypass_cache=True)

            new_local_path = fc2.get_local_path(url2)
            assert not new_local_path.is_file()
            assert new_local_path != fc.get_local_path(url)

            assert fc2.retrieve(url2) == new_local_path


def test_translator_http_pfx():
    with FileCache(None, url_to_path=translator_subdir2a_rel) as fc:
        pfx = fc.new_path(HTTP_TEST_ROOT)
        url = EXPECTED_FILENAMES[1]
        path = EXPECTED_FILENAMES[1].replace('subdir2a/', '')
        exp_local_path = fc.cache_dir / (HTTP_TEST_ROOT.replace('https://', 'http_') +
                                         '/' + path)

        with FileCache(None) as fc2:
            uid = str(uuid.uuid4())
            fc2_path = uid + '/' + EXPECTED_FILENAMES[1]
            pfx2 = fc2.new_path(GS_WRITABLE_TEST_BUCKET_ROOT + '/' + uid)
            url2 = EXPECTED_FILENAMES[1]

            # Translate between caches
            def translate_fc_fc2(scheme, remote, path, cache_dir, cache_subdir):
                assert scheme == 'gs'
                assert remote == GS_WRITABLE_TEST_BUCKET_ROOT.replace('gs://', '')
                assert path == fc2_path
                assert cache_dir == fc2.cache_dir
                assert cache_subdir == (GS_WRITABLE_TEST_BUCKET_ROOT
                                        .replace('gs://', 'gs_'))
                return exp_local_path

            assert pfx.get_local_path(url) == exp_local_path
            assert pfx2.get_local_path(url2,
                                       url_to_path=translate_fc_fc2) == exp_local_path
            assert not exp_local_path.is_file()
            assert pfx.exists(url, bypass_cache=True)
            assert pfx.exists(url)
            assert not pfx2.exists(url2, url_to_path=translate_fc_fc2)
            assert not pfx2.exists(url2, url_to_path=translate_fc_fc2, bypass_cache=True)

            assert pfx.retrieve(url) == exp_local_path
            assert exp_local_path.is_file()

            assert pfx.exists(url)  # Checks the local cache
            assert pfx.exists(url, bypass_cache=True)  # Checks the web
            assert not pfx2.exists(url2)  # Checks the local cache then GS
            assert pfx2.exists(url2, url_to_path=translate_fc_fc2)  # Checks local cache
            assert not pfx2.exists(url2, url_to_path=translate_fc_fc2, bypass_cache=True)

            with pytest.raises(FileNotFoundError):
                pfx2.upload(url2)

            assert pfx2.upload(url2, url_to_path=translate_fc_fc2) == exp_local_path
            assert pfx2.exists(url2, url_to_path=translate_fc_fc2, bypass_cache=True)

            new_local_path = pfx2.get_local_path(url2)
            assert not new_local_path.is_file()
            assert new_local_path != pfx.get_local_path(url)

            assert pfx2.retrieve(url2) == new_local_path

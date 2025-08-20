################################################################################
# tests/test_url_mapping.py
################################################################################

import os
from pathlib import Path
import uuid

import pytest

import filecache
from filecache import FileCache

from .test_file_cache import (EXPECTED_DIR,
                              HTTP_TEST_ROOT,
                              GS_WRITABLE_TEST_BUCKET,
                              GS_WRITABLE_TEST_BUCKET_ROOT,
                              EXPECTED_FILENAMES,
                              LIMITED_FILENAMES
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


def test_path_translator_local_rel():
    with FileCache(cache_name=None) as fc:
        for filename in EXPECTED_FILENAMES:
            path = EXPECTED_DIR / filename
            assert fc.get_local_path(path) == path
            assert fc.exists(path)
            assert fc.retrieve(path) == path
            assert fc.upload(path) == path

    with FileCache(cache_name=None, url_to_path=translator_subdir2a_rel) as fc:
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

    with FileCache(cache_name=None, url_to_path=translator_subdir2b_rel) as fc:
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

    with FileCache(cache_name=None, url_to_path=(translator_subdir2a_rel,
                                                 translator_subdir2b_rel)) as fc:
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

    with FileCache(cache_name=None, url_to_path=[translator_subdir2a_rel]) as fc:
        translators = [translator_subdir2a_rel,
                       translator_subdir2b_rel]
        for filename in EXPECTED_FILENAMES:
            path = EXPECTED_DIR / filename
            new_path = EXPECTED_DIR / (filename
                                       .replace('subdir2a/', '')
                                       .replace('subdir2b/', ''))
            assert fc.get_local_path(path, url_to_path=translators) == new_path
            if path == new_path:
                assert fc.exists(path, url_to_path=tuple(translators))
            else:
                assert not fc.exists(path, url_to_path=tuple(translators))
            # assert fc.retrieve(path) == new_path
            # assert fc.upload(path) == new_path


def test_path_translator_local_abs():
    with FileCache(cache_name=None) as fc:
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


def test_path_translator_local_pfx():
    with FileCache() as fc:
        pfx = fc.new_path(EXPECTED_DIR)
        for filename in EXPECTED_FILENAMES:
            path = EXPECTED_DIR / filename
            assert pfx.get_local_path(filename) == path
            assert pfx.exists(filename)
            assert pfx.retrieve(filename) == path
            assert pfx.upload(filename) == path

    with FileCache(cache_name=None) as fc:
        pfx = fc.new_path(EXPECTED_DIR, url_to_path=translator_subdir2a_rel)
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

    with FileCache(cache_name=None, url_to_path=[translator_subdir2a_rel]) as fc:
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


def test_path_translator_http():
    with FileCache(cache_name=None, url_to_path=translator_subdir2a_rel) as fc:
        url = HTTP_TEST_ROOT + '/' + EXPECTED_FILENAMES[1]
        path = EXPECTED_FILENAMES[1].replace('subdir2a/', '')
        exp_local_path = fc.cache_dir / (HTTP_TEST_ROOT.replace('https://', 'http_') +
                                         '/' + path)

        with FileCache(cache_name=None) as fc2:
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


def test_path_translator_http_pfx():
    with FileCache(cache_name=None) as fc:
        pfx = fc.new_path(HTTP_TEST_ROOT, url_to_path=translator_subdir2a_rel)
        url = EXPECTED_FILENAMES[1]
        path = EXPECTED_FILENAMES[1].replace('subdir2a/', '')
        exp_local_path = fc.cache_dir / (HTTP_TEST_ROOT.replace('https://', 'http_') +
                                         '/' + path)

        with FileCache(cache_name=None) as fc2:
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


def gen_translator_url_1():
    local_uuid = str(uuid.uuid4())

    def translator_url_1(scheme, remote, path):
        if scheme != 'https':
            return None

        if remote == 'nonexistent-website.org':
            return f'{GS_WRITABLE_TEST_BUCKET_ROOT}/{local_uuid}/{path}'

        return None

    translator_url_1.uuid = local_uuid

    return translator_url_1


def translator_url_2(scheme, remote, path):
    if scheme != 'gs':
        return None

    if remote == f'{GS_WRITABLE_TEST_BUCKET}-test':
        return f'https://nonexistent-website.org/{path}'

    return None


def translator_url_3(scheme, remote, path):
    return f'https://bad-website.com/{path}'


def test_url_translator_url():
    translator_url_1 = gen_translator_url_1()
    with FileCache(cache_name=None) as fc:
        # No translation
        for filename in LIMITED_FILENAMES:
            path = EXPECTED_DIR / filename
            assert fc.get_local_path(path) == path
            assert fc.exists(path)
            assert fc.retrieve(path) == path
            assert fc.upload(path) == path

    with FileCache(cache_name=None, url_to_url=translator_url_1) as fc:
        # Translation but not for file scheme
        for filename in LIMITED_FILENAMES:
            path = EXPECTED_DIR / filename
            assert fc.get_local_path(path) == path
            assert fc.exists(path)
            assert fc.retrieve(path) == path
            assert fc.upload(path) == path

    with FileCache(cache_name=None, url_to_url=translator_url_1) as fc:
        # Translation for this scheme but not for this URL
        for filename in LIMITED_FILENAMES:
            path = f'{HTTP_TEST_ROOT}/{filename}'
            local_path = fc.get_local_path(path)
            assert 'gs_' not in str(local_path)
            assert fc.exists(path)
            assert fc.retrieve(path) == local_path
            with pytest.raises(NotImplementedError):
                fc.upload(path)

    with FileCache(cache_name=None, url_to_url=translator_url_1) as fc:
        # Translation for this URL
        for filename in LIMITED_FILENAMES:
            path = f'https://nonexistent-website.org/{filename}'
            local_path = fc.get_local_path(path)
            assert 'gs_' in str(local_path)
            assert not fc.exists(path)
            with pytest.raises(FileNotFoundError):
                fc.retrieve(path)
            try:
                os.unlink(local_path)
            except FileNotFoundError:
                pass
            with pytest.raises(FileNotFoundError):
                fc.upload(path)
            with open(local_path, 'w') as f:
                f.write('test')
            assert fc.upload(path) == local_path
            assert fc.exists(path)
            assert fc.retrieve(path) == local_path


def test_url_translator_pfx():
    translator_url_1 = gen_translator_url_1()
    with FileCache(cache_name=None) as fc:
        # No translation
        pfx = fc.new_path(EXPECTED_DIR)
        for filename in LIMITED_FILENAMES:
            path = EXPECTED_DIR / filename
            assert pfx.get_local_path(filename) == path
            assert pfx.exists(filename)
            assert pfx.retrieve(filename) == path
            assert pfx.upload(filename) == path

    with FileCache(cache_name=None,
                   url_to_url=[translator_url_1, translator_url_2]) as fc:
        # Translation but not for file scheme
        pfx = fc.new_path(EXPECTED_DIR)
        for filename in LIMITED_FILENAMES:
            path = EXPECTED_DIR / filename
            assert pfx.get_local_path(filename) == path
            assert pfx.exists(filename)
            assert pfx.retrieve(filename) == path
            assert pfx.upload(filename) == path

    with FileCache(cache_name=None,
                   url_to_url=(translator_url_1, translator_url_2)) as fc:
        # Translation for this scheme but not for this URL
        pfx = fc.new_path(HTTP_TEST_ROOT)
        for filename in LIMITED_FILENAMES:
            path = f'{HTTP_TEST_ROOT}/{filename}'
            local_path = pfx.get_local_path(filename)
            assert 'gs_' not in str(local_path)
            assert pfx.exists(filename)
            assert pfx.retrieve(filename) == local_path
            with pytest.raises(NotImplementedError):
                pfx.upload(filename)

    with FileCache(cache_name=None,
                   url_to_url=(translator_url_1, translator_url_2)) as fc:
        # Translation for this URL
        pfx = fc.new_path(f'https://nonexistent-website.org/{translator_url_1.uuid}')
        for filename in LIMITED_FILENAMES:
            path = f'https://nonexistent-website.org/{filename}'
            local_path = pfx.get_local_path(filename)
            assert 'gs_' in str(local_path)
            assert not pfx.exists(filename)
            with pytest.raises(FileNotFoundError):
                pfx.retrieve(filename)
            try:
                os.unlink(local_path)
            except FileNotFoundError:
                pass
            with pytest.raises(FileNotFoundError):
                pfx.upload(filename)
            with open(local_path, 'w') as f:
                f.write('test')
            assert pfx.upload(filename) == local_path
            assert pfx.exists(filename)
            assert pfx.retrieve(filename) == local_path

    with FileCache(cache_name=None,
                   url_to_url=[translator_url_1, translator_url_2]) as fc:
        # Translation for this scheme but not for this URL
        pfx = fc.new_path(f'{GS_WRITABLE_TEST_BUCKET_ROOT}/{translator_url_1.uuid}')
        for filename in LIMITED_FILENAMES:
            local_path = pfx.get_local_path(filename)
            assert 'gs_' in str(local_path)
            try:
                os.unlink(local_path)
            except FileNotFoundError:
                pass
            with open(local_path, 'w') as f:
                f.write('test')
            assert pfx.upload(filename) == local_path
            assert pfx.exists(filename)
            assert pfx.retrieve(filename) == local_path

    with FileCache(cache_name=None,
                   url_to_url=[translator_url_1, translator_url_2]) as fc:
        # Translation for this URL
        pfx = fc.new_path(f'{GS_WRITABLE_TEST_BUCKET_ROOT}-test')
        for filename in LIMITED_FILENAMES:
            path = f'{GS_WRITABLE_TEST_BUCKET_ROOT}/{filename}'
            local_path = pfx.get_local_path(filename)
            assert 'gs_' not in str(local_path)
            assert 'nonexistent-website' in str(local_path)
            assert not pfx.exists(filename)
            with pytest.raises(FileNotFoundError):
                pfx.retrieve(filename)
            with pytest.raises(NotImplementedError):
                pfx.upload(filename)


def test_url_translator_func():
    translator_url_1 = gen_translator_url_1()
    with FileCache(cache_name=None) as fc:
        # pfx0 writes directly to GS
        pfx0 = fc.new_path(f'{GS_WRITABLE_TEST_BUCKET_ROOT}/{translator_url_1.uuid}')
        for filename in LIMITED_FILENAMES:
            (pfx0 / filename).write_text('test')

    # Default translator returns bad-website
    with FileCache(cache_name=None, url_to_url=[translator_url_3]) as fc:
        # pfx1 returns bad-website
        pfx1 = fc.new_path('https://nonexistent-website.org')
        # pfx2 nonexistent-website.org to GS
        pfx2 = fc.new_path('https://nonexistent-website.org', url_to_url=translator_url_1)
        # pfx3 nonexistent-website.org to GS
        pfx3 = fc.new_path(GS_WRITABLE_TEST_BUCKET_ROOT+'-test',
                           url_to_url=translator_url_1)
        # translator_url_2 gs-test to nonexistent-website.org
        for filename in LIMITED_FILENAMES:
            local_path_1 = pfx1.get_local_path(filename)  # FileCache default
            assert 'gs_' not in str(local_path_1)
            assert 'nonexistent-website' not in str(local_path_1)
            assert 'bad-website' in str(local_path_1)
            local_path_2 = pfx2.get_local_path(filename)  # FCPath default
            assert 'gs_' in str(local_path_2)
            assert 'nonexistent-website' not in str(local_path_2)
            local_path_3 = pfx3.get_local_path(filename)  # FCpath default
            assert 'gs_' in str(local_path_3)
            assert '-test/' in str(local_path_3).replace('\\', '/')
            assert 'nonexistent-website' not in str(local_path_3)
            local_path2a = pfx2.get_local_path(filename, url_to_url=(translator_url_2,))
            assert 'gs_' not in str(local_path2a)
            assert 'nonexistent-website' in str(local_path2a)
            local_path3a = pfx3.get_local_path(filename, url_to_url=[translator_url_2])
            assert 'gs_' not in str(local_path3a)
            assert '-test/' not in str(local_path3a)
            assert 'nonexistent-website' in str(local_path3a)

            with pytest.raises(FileNotFoundError):
                assert pfx1.retrieve(filename)  # FileCache default
            assert pfx2.retrieve(filename) == local_path_2  # FCPath default
            with pytest.raises(FileNotFoundError):
                pfx3.retrieve(filename)  # FCpath default
            with pytest.raises(FileNotFoundError):
                pfx2.retrieve(filename, url_to_url=[translator_url_2])
            with pytest.raises(FileNotFoundError):
                pfx3.retrieve(filename, url_to_url=translator_url_2)


def test_url_and_path_translators():
    # Default translator returns bad-website
    filecache.set_easy_logger()
    with FileCache(cache_name=None,
                   url_to_url=[translator_url_3],
                   url_to_path=translator_subdir2a_rel) as fc:
        for filename in EXPECTED_FILENAMES:
            path = EXPECTED_DIR / filename
            new_path = EXPECTED_DIR / (filename.replace('subdir2a/', ''))
            assert fc.get_local_path(path) == new_path

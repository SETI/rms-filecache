################################################################################
# tests/test_url_to_url.py
################################################################################

import os
import uuid

import pytest

from filecache import FileCache

from .test_file_cache import (EXPECTED_DIR,
                              HTTP_TEST_ROOT,
                              GS_WRITABLE_TEST_BUCKET,
                              GS_WRITABLE_TEST_BUCKET_ROOT,
                              EXPECTED_FILENAMES
                              )

_TEST_UUID = str(uuid.uuid4())


def translator_url_1(scheme, remote, path):
    if scheme != 'https':
        return None

    if remote == 'nonexistent-website.org':
        return f'{GS_WRITABLE_TEST_BUCKET_ROOT}/{_TEST_UUID}/{path}'

    return None


def translator_url_2(scheme, remote, path):
    if scheme != 'gs':
        return None

    if remote == f'{GS_WRITABLE_TEST_BUCKET}-test':
        return f'https://nonexistent-website.org/{path}'

    return None


def translator_url_3(scheme, remote, path):
    return f'https://bad-website.com/{path}'


def test_url_translator_url():
    with FileCache() as fc:
        # No translation
        for filename in EXPECTED_FILENAMES[:1]:
            path = EXPECTED_DIR / filename
            assert fc.get_local_path(path) == path
            assert fc.exists(path)
            assert fc.retrieve(path) == path
            assert fc.upload(path) == path

    with FileCache(url_to_url=translator_url_1) as fc:
        # Translation but not for file scheme
        for filename in EXPECTED_FILENAMES[:1]:
            path = EXPECTED_DIR / filename
            assert fc.get_local_path(path) == path
            assert fc.exists(path)
            assert fc.retrieve(path) == path
            assert fc.upload(path) == path

    with FileCache(url_to_url=translator_url_1) as fc:
        # Translation for this scheme but not for this URL
        for filename in EXPECTED_FILENAMES[:1]:
            path = f'{HTTP_TEST_ROOT}/{filename}'
            local_path = fc.get_local_path(path)
            assert 'gs_' not in str(local_path)
            assert fc.exists(path)
            assert fc.retrieve(path) == local_path
            with pytest.raises(NotImplementedError):
                fc.upload(path)

    with FileCache(url_to_url=translator_url_1) as fc:
        # Translation for this URL
        for filename in EXPECTED_FILENAMES[:1]:
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
    with FileCache() as fc:
        # No translation
        pfx = fc.new_path(EXPECTED_DIR)
        for filename in EXPECTED_FILENAMES[:1]:
            path = EXPECTED_DIR / filename
            assert pfx.get_local_path(filename) == path
            assert pfx.exists(filename)
            assert pfx.retrieve(filename) == path
            assert pfx.upload(filename) == path

    with FileCache(url_to_url=[translator_url_1, translator_url_2]) as fc:
        # Translation but not for file scheme
        pfx = fc.new_path(EXPECTED_DIR)
        for filename in EXPECTED_FILENAMES[:1]:
            path = EXPECTED_DIR / filename
            assert pfx.get_local_path(filename) == path
            assert pfx.exists(filename)
            assert pfx.retrieve(filename) == path
            assert pfx.upload(filename) == path

    with FileCache(url_to_url=(translator_url_1, translator_url_2)) as fc:
        # Translation for this scheme but not for this URL
        pfx = fc.new_path(HTTP_TEST_ROOT)
        for filename in EXPECTED_FILENAMES[:1]:
            path = f'{HTTP_TEST_ROOT}/{filename}'
            local_path = pfx.get_local_path(filename)
            assert 'gs_' not in str(local_path)
            assert pfx.exists(filename)
            assert pfx.retrieve(filename) == local_path
            with pytest.raises(NotImplementedError):
                pfx.upload(filename)

    with FileCache(url_to_url=(translator_url_1, translator_url_2)) as fc:
        # Translation for this URL
        pfx = fc.new_path(f'https://nonexistent-website.org/{_TEST_UUID}')
        for filename in EXPECTED_FILENAMES[:1]:
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

    with FileCache(url_to_url=[translator_url_1, translator_url_2]) as fc:
        # Translation for this scheme but not for this URL
        pfx = fc.new_path(f'{GS_WRITABLE_TEST_BUCKET_ROOT}/{_TEST_UUID}')
        for filename in EXPECTED_FILENAMES[:1]:
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

    with FileCache(url_to_url=[translator_url_1, translator_url_2]) as fc:
        # Translation for this URL
        pfx = fc.new_path(f'{GS_WRITABLE_TEST_BUCKET_ROOT}-test')
        for filename in EXPECTED_FILENAMES[:1]:
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
    # Default translator returns bad-website
    with FileCache(url_to_url=[translator_url_3]) as fc:
        # pfx1 returns bad-website
        pfx1 = fc.new_path('https://nonexistent-website.org')
        # pfx2 nonexistent-website.org to GS
        pfx2 = fc.new_path('https://nonexistent-website.org', url_to_url=translator_url_1)
        # pfx3 nonexistent-website.org to GS
        pfx3 = fc.new_path(GS_WRITABLE_TEST_BUCKET_ROOT+'-test',
                           url_to_url=translator_url_1)
        # translator_url_2 gs-test to nonexistent-website.org
        for filename in EXPECTED_FILENAMES[:1]:
            local_path_1 = pfx1.get_local_path(filename)  # FileCache default
            assert 'gs_' not in str(local_path_1)
            assert 'nonexistent-website' not in str(local_path_1)
            assert 'bad-website' in str(local_path_1)
            local_path_2 = pfx2.get_local_path(filename)  # FCPath default
            assert 'gs_' in str(local_path_2)
            assert 'nonexistent-website' not in str(local_path_2)
            local_path_3 = pfx3.get_local_path(filename)  # FCpath default
            assert 'gs_' in str(local_path_3)
            assert '-test/' in str(local_path_3)
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

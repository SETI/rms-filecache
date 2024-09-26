################################################################################
# tests/test_filecache.py
################################################################################

import os
from pathlib import Path

import pytest

from filecache import FileCache

import filelock


ROOT_DIR = Path(__file__).resolve().parent.parent
TEST_FILES_DIR = ROOT_DIR / 'test_files'
EXPECTED_DIR = TEST_FILES_DIR / 'expected'

EXPECTED_FILENAMES = ('lorem1.txt',
                      'subdir1/lorem1.txt',
                      'subdir1/subdir2a/binary1.bin',
                      'subdir1/subdir2b/binary1.bin')
GS_TEST_BUCKET_ROOT = 'gs://rms-node-filecache-test-bucket'
S3_TEST_BUCKET_ROOT = 's3://rms-node-filecache-test-bucket'
HTTP_TEST_ROOT = 'https://storage.googleapis.com/rms-node-filecache-test-bucket'

CLOUD_PREFIXES = (GS_TEST_BUCKET_ROOT, S3_TEST_BUCKET_ROOT, HTTP_TEST_ROOT)


def _compare_to_expected_path(cache_path, filename):
    local_path = EXPECTED_DIR / filename
    mode = 'r'
    if filename.endswith('.bin'):
        mode = 'rb'
    with open(cache_path, mode) as fp:
        cache_data = fp.read()
    with open(local_path, mode) as fp:
        local_data = fp.read()
    assert cache_data == local_data


def _compare_to_expected_data(cache_data, filename):
    local_path = EXPECTED_DIR / filename
    mode = 'r'
    if filename.endswith('.bin'):
        mode = 'rb'
    with open(local_path, mode) as fp:
        local_data = fp.read()
    assert cache_data == local_data


def test_temp_dir_good():
    fc1 = FileCache()
    fc2 = FileCache()
    fc3 = FileCache()
    assert str(fc1.cache_dir) != str(fc2.cache_dir)
    assert str(fc2.cache_dir) != str(fc3.cache_dir)
    assert fc1.cache_dir.name.startswith('.file_cache_')
    assert fc2.cache_dir.name.startswith('.file_cache_')
    assert fc3.cache_dir.name.startswith('.file_cache_')
    assert not fc1.is_shared
    assert not fc2.is_shared
    assert not fc3.is_shared
    fc1.clean_up()
    fc2.clean_up()
    fc3.clean_up()
    assert not fc1.cache_dir.exists()
    assert not fc2.cache_dir.exists()
    assert not fc3.cache_dir.exists()

    cwd = os.getcwd()

    fc4 = FileCache(temp_dir='.')
    fc5 = FileCache(temp_dir=cwd)
    assert str(fc4.cache_dir.parent) == str(fc5.cache_dir.parent)
    assert str(fc4.cache_dir.parent) == cwd
    assert str(fc5.cache_dir.parent) == cwd
    assert fc4.cache_dir.name.startswith('.file_cache_')
    assert fc5.cache_dir.name.startswith('.file_cache_')
    assert not fc5.is_shared
    assert not fc5.is_shared
    fc4.clean_up()
    fc5.clean_up()
    assert not fc4.cache_dir.exists()
    assert not fc5.cache_dir.exists()


def test_temp_dir_bad():
    with pytest.raises(ValueError):
        _ = FileCache(temp_dir='\000')


def test_shared_global():
    fc1 = FileCache()
    fc2 = FileCache(shared=True)
    fc3 = FileCache(shared=True)
    assert str(fc1.cache_dir) != str(fc2.cache_dir)
    assert str(fc2.cache_dir) == str(fc3.cache_dir)
    assert fc1.cache_dir.name.startswith('.file_cache_')
    assert fc2.cache_dir.name == '.file_cache___global__'
    assert fc3.cache_dir.name == '.file_cache___global__'
    assert not fc1.is_shared
    assert fc2.is_shared
    assert fc3.is_shared
    fc1.clean_up()
    assert not fc1.cache_dir.exists()
    assert fc2.cache_dir.exists()
    fc2.clean_up()
    assert fc2.cache_dir.exists()
    assert fc3.cache_dir.exists()
    fc3.clean_up(final=True)
    assert not fc3.cache_dir.exists()


def test_shared_global_ctx():
    with FileCache() as fc1:
        assert fc1.cache_dir.exists()
        with FileCache(shared=True) as fc2:
            assert fc2.cache_dir.exists()
            with FileCache(shared=True) as fc3:
                assert fc3.cache_dir.exists()
                assert str(fc1.cache_dir) != str(fc2.cache_dir)
                assert str(fc2.cache_dir) == str(fc3.cache_dir)
                assert fc1.cache_dir.name.startswith('.file_cache_')
                assert fc2.cache_dir.name == '.file_cache___global__'
                assert fc3.cache_dir.name == '.file_cache___global__'
                assert not fc1.is_shared
                assert fc2.is_shared
                assert fc3.is_shared
            assert fc3.cache_dir.exists()
        assert fc2.cache_dir.exists()
    assert not fc1.cache_dir.exists()
    assert fc3.cache_dir.exists()
    fc3.clean_up(final=True)
    assert not fc3.cache_dir.exists()


def test_shared_named():
    fc1 = FileCache()
    fc2 = FileCache(shared=True)
    fc3 = FileCache(shared='test')
    fc4 = FileCache(shared='test')
    assert str(fc1.cache_dir) != str(fc2.cache_dir)
    assert str(fc2.cache_dir) != str(fc3.cache_dir)
    assert str(fc3.cache_dir) == str(fc4.cache_dir)
    assert fc1.cache_dir.name.startswith('.file_cache_')
    assert fc2.cache_dir.name == '.file_cache___global__'
    assert fc3.cache_dir.name == '.file_cache_test'
    assert fc4.cache_dir.name == '.file_cache_test'
    assert not fc1.is_shared
    assert fc2.is_shared
    assert fc3.is_shared
    fc1.clean_up()
    assert not fc1.cache_dir.exists()
    assert fc2.cache_dir.exists()
    fc2.clean_up(final=True)
    assert not fc2.cache_dir.exists()
    assert fc3.cache_dir.exists()
    assert fc4.cache_dir.exists()
    fc3.clean_up(final=True)
    assert not fc3.cache_dir.exists()
    assert not fc4.cache_dir.exists()


def test_shared_bad():
    with pytest.raises(TypeError):
        _ = FileCache(shared=5)
    with pytest.raises(ValueError):
        _ = FileCache(shared='a/b')
    with pytest.raises(ValueError):
        _ = FileCache(shared='a\\b')
    with pytest.raises(ValueError):
        _ = FileCache(shared='/a')
    with pytest.raises(ValueError):
        _ = FileCache(shared='\\a')


def test_source_bad():
    with FileCache() as fc:
        with pytest.raises(TypeError):
            _ = fc.new_source(5)


@pytest.mark.parametrize('shared', (False, True, 'test'))
def test_local_filesystem_good(shared):
    for pass_no in range(5):  # Make sure the expected dir doesn't get modified
        with FileCache(shared=shared) as fc:
            lf = fc.new_source(EXPECTED_DIR)
            for filename in EXPECTED_FILENAMES:
                os_filename = filename.replace('/', os.sep)
                assert lf.is_cached(filename)
                path = lf.retrieve(filename)
                assert str(path) == f'{EXPECTED_DIR}{os.sep}{os_filename}'
                assert lf.is_cached(filename)
                path = lf.retrieve(filename)
                assert str(path) == f'{EXPECTED_DIR}{os.sep}{os_filename}'
                _compare_to_expected_path(path, filename)
            # No files or directories in the cache
            assert len(list(fc.cache_dir.iterdir())) == 0
            fc.clean_up(final=True)
    assert shared is not False or not fc.cache_dir.exists()


def test_local_filesystem_bad():
    with FileCache() as fc:
        lf = fc.new_source(EXPECTED_DIR)
        with pytest.raises(ValueError):
            _ = lf.retrieve('a/b/../../c.txt')
        with pytest.raises(ValueError):
            _ = lf.is_cached('a/b/../../c.txt')
        with pytest.raises(FileNotFoundError):
            _ = lf.retrieve('nonexistent.txt')
        with pytest.raises(FileNotFoundError):
            _ = lf.is_cached('nonexistent.txt')
    assert not fc.cache_dir.exists()


@pytest.mark.parametrize('shared', (False, True, 'test'))
@pytest.mark.parametrize('prefix', CLOUD_PREFIXES)
def test_cloud_good(shared, prefix):
    with FileCache(shared=shared) as fc:
        src = fc.new_source(prefix, anonymous=True)
        for filename in EXPECTED_FILENAMES:
            path = src.retrieve(filename)
            assert str(path).replace('\\', '/').endswith(filename)
            _compare_to_expected_path(path, filename)
            # Retrieving the same thing a second time should do nothing
            assert src.is_cached(filename)
            path = src.retrieve(filename)
            assert str(path).replace('\\', '/').endswith(filename)
            _compare_to_expected_path(path, filename)
        fc.clean_up(final=True)


@pytest.mark.parametrize('prefix', CLOUD_PREFIXES)
def test_cloud2_good(prefix):
    with FileCache() as fc:
        # With two identical sources, it shouldn't matter which you use
        src1 = fc.new_source(prefix, anonymous=True)
        src2 = fc.new_source(prefix, anonymous=True)
        for filename in EXPECTED_FILENAMES:
            path1 = src1.retrieve(filename)
            assert str(path1).replace('\\', '/').endswith(filename)
            _compare_to_expected_path(path1, filename)
            assert src1.is_cached(filename)
            assert src2.is_cached(filename)
            path2 = src2.retrieve(filename)
            assert str(path2).replace('\\', '/').endswith(filename)
            assert str(path1) == str(path2)
            _compare_to_expected_path(path2, filename)
    assert not fc.cache_dir.exists()


@pytest.mark.parametrize('prefix', CLOUD_PREFIXES)
def test_cloud3_good(prefix):
    # Multiple sources with different subdir prefixes
    with FileCache() as fc:
        src1 = fc.new_source(prefix, anonymous=True)
        for filename in EXPECTED_FILENAMES:
            subdirs, _, name = filename.rpartition('/')
            src2 = fc.new_source(f'{prefix}/{subdirs}', anonymous=True)
            path2 = src2.retrieve(name)
            assert str(path2).replace('\\', '/').endswith(filename)
            _compare_to_expected_path(path2, filename)
            assert src1.is_cached(filename)
            assert src2.is_cached(name)
            path1 = src1.retrieve(filename)
            assert str(path1) == str(path2)
    assert not fc.cache_dir.exists()


def test_gs_bad():
    with FileCache() as fc:
        src = fc.new_source('gs://rms-node-bogus-bucket-name-XXX', anonymous=True)
        with pytest.raises(FileNotFoundError):
            _ = src.retrieve('bogus-filename')
        src = fc.new_source(GS_TEST_BUCKET_ROOT, anonymous=True)
        with pytest.raises(FileNotFoundError):
            _ = src.retrieve('bogus-filename')
    assert not fc.cache_dir.exists()


def test_s3_bad():
    with FileCache() as fc:
        src = fc.new_source('s3://rms-node-bogus-bucket-name-XXX', anonymous=True)
        with pytest.raises(FileNotFoundError):
            _ = src.retrieve('bogus-filename')
        src = fc.new_source(S3_TEST_BUCKET_ROOT, anonymous=True)
        with pytest.raises(FileNotFoundError):
            _ = src.retrieve('bogus-filename')
    assert not fc.cache_dir.exists()


def test_web_bad():
    with FileCache() as fc:
        src = fc.new_source('https://bad-domain.seti.org')
        with pytest.raises(FileNotFoundError):
            _ = src.retrieve('bogus-filename')
        src = fc.new_source(HTTP_TEST_ROOT)
        with pytest.raises(FileNotFoundError):
            _ = src.retrieve('bogus-filename')
    assert not fc.cache_dir.exists()


def test_multi_sources():
    with FileCache() as fc:
        sources = []
        # Different source should have different cache paths but all have the same
        # contents
        for prefix in CLOUD_PREFIXES:
            sources.append(fc.new_source(prefix, anonymous=True))
        for filename in EXPECTED_FILENAMES:
            paths = []
            for source in sources:
                paths.append(source.retrieve(filename))
            for i, path1 in enumerate(paths):
                for j, path2 in enumerate(paths):
                    if i == j:
                        continue
                    assert str(path1) != str(path2)
            for path in paths:
                _compare_to_expected_path(path, filename)
    assert not fc.cache_dir.exists()


@pytest.mark.parametrize('prefix', CLOUD_PREFIXES)
def test_multi_sources_shared(prefix):
    with FileCache(shared=True) as fc1:
        src1 = fc1.new_source(prefix, anonymous=True)
        paths1 = []
        for filename in EXPECTED_FILENAMES:
            paths1.append(src1.retrieve(filename))
        with FileCache(shared=True) as fc2:
            src2 = fc2.new_source(prefix, anonymous=True)
            paths2 = []
            for filename in EXPECTED_FILENAMES:
                paths2.append(src2.retrieve(filename))
            for path1, path2 in zip(paths1, paths2):
                assert path1.exists()
                assert str(path1) == str(path2)
        fc1.clean_up(final=True)


def test_locking():
    with FileCache(shared=True) as fc:
        src = fc.new_source(HTTP_TEST_ROOT, lock_timeout=0)
        filename = (HTTP_TEST_ROOT.replace('https://', 'http_') + '/' +
                    EXPECTED_FILENAMES[0])
        local_path = fc.cache_dir / filename
        lock_path = src._lock_path(local_path)
        lock = filelock.FileLock(lock_path, timeout=0)
        lock.acquire()
        try:
            with pytest.raises(TimeoutError):
                src.retrieve(EXPECTED_FILENAMES[0])
        finally:
            lock.release()
        lock_path.unlink(missing_ok=True)
        fc.clean_up(final=True)

    with FileCache(shared=False) as fc:
        src = fc.new_source(HTTP_TEST_ROOT, lock_timeout=0)
        filename = (HTTP_TEST_ROOT.replace('https://', 'http_') + '/' +
                    EXPECTED_FILENAMES[0])
        local_path = fc.cache_dir / filename
        lock_path = src._lock_path(local_path)
        lock = filelock.FileLock(lock_path, timeout=0)
        lock.acquire()
        src.retrieve(EXPECTED_FILENAMES[0])  # shared=False doesn't lock
        lock.release()
        lock_path.unlink(missing_ok=True)


def test_bad_cache_dir():
    with pytest.raises(ValueError):
        with FileCache() as fc:
            fc._cache_dir = '/bogus/path/not/a/filecache'


def test_double_delete():
    with FileCache() as fc:
        src = fc.new_source(HTTP_TEST_ROOT)
        for filename in EXPECTED_FILENAMES:
            src.retrieve(filename)
        filename = (HTTP_TEST_ROOT.replace('https://', 'http_') + '/' +
                    EXPECTED_FILENAMES[0])
        path = fc.cache_dir / filename
        path.unlink()

    with pytest.raises(FileNotFoundError):
        with FileCache(exception_if_missing=True) as fc:
            src = fc.new_source(HTTP_TEST_ROOT)
            assert src.filecache == fc
            for filename in EXPECTED_FILENAMES:
                src.retrieve(filename)
            filename = (HTTP_TEST_ROOT.replace('https://', 'http_') + '/' +
                        EXPECTED_FILENAMES[0])
            path = fc.cache_dir / filename
            path.unlink()
    fc.clean_up()

    with FileCache() as fc:
        src = fc.new_source(HTTP_TEST_ROOT)
        for filename in EXPECTED_FILENAMES:
            src.retrieve(filename)
        fc.clean_up()  # Test double clean_up
        assert not fc.cache_dir.exists()
        fc.clean_up()
        assert not fc.cache_dir.exists()
        for filename in EXPECTED_FILENAMES:
            src.retrieve(filename)
        assert fc.cache_dir.exists()
        fc.clean_up()
        assert not fc.cache_dir.exists()
        fc.clean_up()
        assert not fc.cache_dir.exists()

    with FileCache(shared=True) as fc:
        src = fc.new_source(HTTP_TEST_ROOT)
        for filename in EXPECTED_FILENAMES:
            src.retrieve(filename)
        fc.clean_up()  # Test double clean_up
        assert fc.cache_dir.exists()
        fc.clean_up()
        assert fc.cache_dir.exists()
        for filename in EXPECTED_FILENAMES:
            src.retrieve(filename)
        assert fc.cache_dir.exists()
        fc.clean_up()
        assert fc.cache_dir.exists()
        fc.clean_up()
        assert fc.cache_dir.exists()
        fc.clean_up(final=True)
        assert not fc.cache_dir.exists()


def test_open_context():
    with FileCache() as fc:
        src = fc.new_source(HTTP_TEST_ROOT)
        with src.open(EXPECTED_FILENAMES[0], 'r') as fp:
            cache_data = fp.read()
        _compare_to_expected_data(cache_data, EXPECTED_FILENAMES[0])


def test_cache_owner():
    with FileCache(shared=True, cache_owner=True) as fc1:
        with FileCache(shared=True) as fc2:
            pass
        assert fc1.cache_dir == fc2.cache_dir
        assert os.path.exists(fc1.cache_dir)
    assert not os.path.exists(fc1.cache_dir)

################################################################################
# tests/test_filecache.py
################################################################################

import atexit
import os
from pathlib import Path
import tempfile
import uuid

import pytest

import filecache
from filecache import FileCache

import filelock


ROOT_DIR = Path(__file__).resolve().parent.parent
TEST_FILES_DIR = ROOT_DIR / 'test_files'
EXPECTED_DIR = TEST_FILES_DIR / 'expected'

EXPECTED_FILENAMES = ('lorem1.txt',
                      'subdir1/lorem1.txt',
                      'subdir1/subdir2a/binary1.bin',
                      'subdir1/subdir2b/binary1.bin')
GS_TEST_BUCKET_ROOT = 'gs://rms-filecache-tests'
S3_TEST_BUCKET_ROOT = 's3://rms-filecache-tests'
HTTP_TEST_ROOT = 'https://storage.googleapis.com/rms-filecache-tests'
CLOUD_PREFIXES = (GS_TEST_BUCKET_ROOT, S3_TEST_BUCKET_ROOT, HTTP_TEST_ROOT)

GS_WRITABLE_TEST_BUCKET_ROOT = 'gs://rms-filecache-tests-writable'
S3_WRITABLE_TEST_BUCKET_ROOT = 's3://rms-filecache-tests-writable'
WRITABLE_CLOUD_PREFIXES = (GS_WRITABLE_TEST_BUCKET_ROOT, S3_WRITABLE_TEST_BUCKET_ROOT)


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


class MyLogger:
    def __init__(self):
        self.messages = []

    def debug(self, msg, *args, **kwargs):
        self.messages.append(msg)

    def has_prefix_list(self, prefixes):
        print('----------')
        print('\n'.join(self.messages))
        print(prefixes)
        for msg, prefix in zip(self.messages, prefixes):
            assert msg.strip(' ').startswith(prefix), (msg, prefix)


def test_logger():
    assert filecache.get_global_logger() is False
    # Global logger
    logger = MyLogger()
    filecache.set_global_logger(logger)
    assert filecache.get_global_logger() is logger
    with FileCache() as fc:
        pfx = fc.new_prefix(HTTP_TEST_ROOT)
        pfx.retrieve(EXPECTED_FILENAMES[0])
# Creating cache /tmp/.file_cache_28d43982-dd49-493e-905d-9bcebd813613
# Initializing prefix https://storage.googleapis.com/rms-node-filecache-test-bucket/
# Downloading https://storage.googleapis.com/rms-node-filecache-test-bucket//lorem1.txt to /tmp/.file_cache_28d43982-dd49-493e-905d-9bcebd813613/http_storage.googleapis.com/rms-node-filecache-test-bucket/lorem1.txt
# Cleaning up cache /tmp/.file_cache_28d43982-dd49-493e-905d-9bcebd813613
#   Removing /tmp/.file_cache_28d43982-dd49-493e-905d-9bcebd813613/http_storage.googleapis.com/rms-node-filecache-test-bucket/lorem1.txt
#   Removing /tmp/.file_cache_28d43982-dd49-493e-905d-9bcebd813613/http_storage.googleapis.com/rms-node-filecache-test-bucket
#   Removing /tmp/.file_cache_28d43982-dd49-493e-905d-9bcebd813613/http_storage.googleapis.com
#   Removing /tmp/.file_cache_28d43982-dd49-493e-905d-9bcebd813613
    logger.has_prefix_list(['Creating', 'Initializing', 'Downloading', 'Cleaning',
                            'Removing', 'Removing', 'Removing', 'Removing'])

    logger = MyLogger()
    filecache.set_global_logger(logger)
    with FileCache(shared=True) as fc:
        pfx = fc.new_prefix(HTTP_TEST_ROOT)
        pfx.retrieve(EXPECTED_FILENAMES[0])
        fc.clean_up(final=True)
# Creating shared cache /tmp/.file_cache___global__
# Initializing prefix https://storage.googleapis.com/rms-node-filecache-test-bucket/
# Downloading https://storage.googleapis.com/rms-node-filecache-test-bucket//lorem1.txt to /tmp/.file_cache___global__/http_storage.googleapis.com/rms-node-filecache-test-bucket/lorem1.txt
# Cleaning up cache /tmp/.file_cache___global__
#   Removing lorem1.txt
#   Removing rms-node-filecache-test-bucket
#   Removing http_storage.googleapis.com
#   Removing /tmp/.file_cache___global__
# Cleaning up cache /tmp/.file_cache___global__
    logger.has_prefix_list(['Creating', 'Initializing', 'Downloading', 'Cleaning',
                            'Removing', 'Removing', 'Removing', 'Removing',
                            'Cleaning'])
    logger.messages = []

    # Remove global logger
    filecache.set_global_logger(False)
    with FileCache() as fc:
        pfx = fc.new_prefix(HTTP_TEST_ROOT)
        pfx.retrieve(EXPECTED_FILENAMES[0])
    assert len(logger.messages) == 0

    # Specified logger
    logger = MyLogger()
    with FileCache(logger=logger) as fc:
        pfx = fc.new_prefix(HTTP_TEST_ROOT)
        pfx.retrieve(EXPECTED_FILENAMES[0])
        pfx.retrieve(EXPECTED_FILENAMES[0])
# Creating cache /tmp/.file_cache_28d43982-dd49-493e-905d-9bcebd813613
# Initializing prefix https://storage.googleapis.com/rms-node-filecache-test-bucket/
# Downloading https://storage.googleapis.com/rms-node-filecache-test-bucket//lorem1.txt to /tmp/.file_cache_28d43982-dd49-493e-905d-9bcebd813613/http_storage.googleapis.com/rms-node-filecache-test-bucket/lorem1.txt
# Accessing existing https://storage.googleapis.com/rms-node-filecache-test-bucket//lorem1.txt at /tmp/.file_cache_28d43982-dd49-493e-905d-9bcebd813613/http_storage.googleapis.com/rms-node-filecache-test-bucket/lorem1.txt
# Cleaning up cache /tmp/.file_cache_28d43982-dd49-493e-905d-9bcebd813613
#   Removing /tmp/.file_cache_28d43982-dd49-493e-905d-9bcebd813613/http_storage.googleapis.com/rms-node-filecache-test-bucket/lorem1.txt
#   Removing /tmp/.file_cache_28d43982-dd49-493e-905d-9bcebd813613/http_storage.googleapis.com/rms-node-filecache-test-bucket
#   Removing /tmp/.file_cache_28d43982-dd49-493e-905d-9bcebd813613/http_storage.googleapis.com
#   Removing /tmp/.file_cache_28d43982-dd49-493e-905d-9bcebd813613
    logger.has_prefix_list(['Creating', 'Initializing', 'Downloading', 'Accessing',
                            'Cleaning', 'Removing', 'Removing', 'Removing', 'Removing'])

    # Specified logger
    logger = MyLogger()
    with FileCache(logger=logger) as fc:
        pfx = fc.new_prefix(EXPECTED_DIR)
        pfx.retrieve(EXPECTED_FILENAMES[0])
# Creating cache /tmp/.file_cache_63a1488e-6e9b-4fea-bb0c-3aaae655ec68
# Initializing prefix /seti/all_repos/rms-filecache/test_files/expected/
# Accessing local file lorem1.txt
# Cleaning up cache /tmp/.file_cache_63a1488e-6e9b-4fea-bb0c-3aaae655ec68
#   Removing /tmp/.file_cache_63a1488e-6e9b-4fea-bb0c-3aaae655ec68
    logger.has_prefix_list(['Creating', 'Initializing', 'Accessing', 'Cleaning',
                            'Removing'])

    # Uploading
    logger = MyLogger()
    with FileCache(logger=logger) as fc:
        new_prefix = f'{GS_WRITABLE_TEST_BUCKET_ROOT}/{uuid.uuid4()}'
        pfx = fc.new_prefix(new_prefix, anonymous=True)
        local_path = pfx.get_local_path('test_file.txt')
        with open(EXPECTED_DIR / EXPECTED_FILENAMES[0], 'rb') as fp1:
            with open(local_path, 'wb') as fp2:
                fp2.write(fp1.read())
        pfx.upload('test_file.txt')
# Creating cache /tmp/.file_cache_e866e868-7d79-4a54-9b52-dc4df2fda819
# Initializing prefix gs://rms-filecache-tests-writable/5bfef362-2ce1-49f9-beb9-4e96a8b747e6/
# Returning local path for test_file.txt as /tmp/.file_cache_e866e868-7d79-4a54-9b52-dc4df2fda819/gs_rms-filecache-tests-writable/5bfef362-2ce1-49f9-beb9-4e96a8b747e6/test_file.txt
# Uploading /tmp/.file_cache_e866e868-7d79-4a54-9b52-dc4df2fda819/gs_rms-filecache-tests-writable/5bfef362-2ce1-49f9-beb9-4e96a8b747e6/test_file.txt to gs://rms-filecache-tests-writable/5bfef362-2ce1-49f9-beb9-4e96a8b747e6/test_file.txt
# Cleaning up cache /tmp/.file_cache_e866e868-7d79-4a54-9b52-dc4df2fda819
    logger.has_prefix_list(['Creating', 'Initializing', 'Returning', 'Uploading',
                            'Cleaning'])


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
    assert not fc2.cache_dir.exists()
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


def test_prefix_bad():
    with FileCache() as fc:
        with pytest.raises(TypeError):
            _ = fc.new_prefix(5)
    assert not fc.cache_dir.exists()


@pytest.mark.parametrize('shared', (False, True, 'test'))
def test_local_retr_good(shared):
    for pass_no in range(5):  # Make sure the expected dir doesn't get modified
        with FileCache(shared=shared) as fc:
            lf = fc.new_prefix(EXPECTED_DIR)
            for filename in EXPECTED_FILENAMES:
                os_filename = filename.replace('/', os.sep)
                path = lf.retrieve(filename)
                assert str(path) == f'{EXPECTED_DIR}{os.sep}{os_filename}'
                path = lf.retrieve(filename)
                assert str(path) == f'{EXPECTED_DIR}{os.sep}{os_filename}'
                _compare_to_expected_path(path, filename)
            # No files or directories in the cache
            assert len(list(fc.cache_dir.iterdir())) == 0
            fc.clean_up(final=True)
            assert not fc.cache_dir.exists()


def test_local_retr_bad():
    with FileCache() as fc:
        lf = fc.new_prefix(EXPECTED_DIR)
        with pytest.raises(ValueError):
            _ = lf.retrieve('a/b/../../c.txt')
        with pytest.raises(FileNotFoundError):
            _ = lf.retrieve('nonexistent.txt')
    assert not fc.cache_dir.exists()


@pytest.mark.parametrize('shared', (False, True, 'test'))
@pytest.mark.parametrize('prefix', CLOUD_PREFIXES)
def test_cloud_retr_good(shared, prefix):
    with FileCache(shared=shared) as fc:
        pfx = fc.new_prefix(prefix, anonymous=True)
        for filename in EXPECTED_FILENAMES:
            path = pfx.retrieve(filename)
            assert str(path).replace('\\', '/').endswith(filename)
            _compare_to_expected_path(path, filename)
            # Retrieving the same thing a second time should do nothing
            path = pfx.retrieve(filename)
            assert str(path).replace('\\', '/').endswith(filename)
            _compare_to_expected_path(path, filename)
        assert pfx.upload_counter == 0
        assert pfx.download_counter == len(EXPECTED_FILENAMES)
        fc.clean_up(final=True)
    assert not fc.cache_dir.exists()


@pytest.mark.parametrize('prefix', CLOUD_PREFIXES)
def test_cloud2_retr_good(prefix):
    with FileCache() as fc:
        # With two identical prefixs, it shouldn't matter which you use
        pfx1 = fc.new_prefix(prefix, anonymous=True)
        pfx2 = fc.new_prefix(prefix, anonymous=True)
        for filename in EXPECTED_FILENAMES:
            path1 = pfx1.retrieve(filename)
            assert str(path1).replace('\\', '/').endswith(filename)
            _compare_to_expected_path(path1, filename)
            path2 = pfx2.retrieve(filename)
            assert str(path2).replace('\\', '/').endswith(filename)
            assert str(path1) == str(path2)
            _compare_to_expected_path(path2, filename)
        assert pfx1.upload_counter == 0
        assert pfx1.download_counter == len(EXPECTED_FILENAMES)
        assert pfx2.upload_counter == 0
        assert pfx2.download_counter == 0
    assert not fc.cache_dir.exists()


@pytest.mark.parametrize('prefix', CLOUD_PREFIXES)
def test_cloud3_retr_good(prefix):
    # Multiple prefixs with different subdir prefixes
    with FileCache() as fc:
        pfx1 = fc.new_prefix(prefix, anonymous=True)
        for filename in EXPECTED_FILENAMES:
            subdirs, _, name = filename.rpartition('/')
            pfx2 = fc.new_prefix(f'{prefix}/{subdirs}', anonymous=True)
            path2 = pfx2.retrieve(name)
            assert pfx2.upload_counter == 0
            assert pfx2.download_counter == 1
            assert str(path2).replace('\\', '/').endswith(filename)
            _compare_to_expected_path(path2, filename)
            path1 = pfx1.retrieve(filename)
            assert str(path1) == str(path2)
        assert pfx1.upload_counter == 0
        assert pfx1.download_counter == 0
    assert not fc.cache_dir.exists()


def test_gs_retr_bad():
    with FileCache() as fc:
        pfx = fc.new_prefix('gs://rms-node-bogus-bucket-name-XXX', anonymous=True)
        with pytest.raises(FileNotFoundError):
            _ = pfx.retrieve('bogus-filename')
        assert pfx.upload_counter == 0
        assert pfx.download_counter == 0
        pfx = fc.new_prefix(GS_TEST_BUCKET_ROOT, anonymous=True)
        with pytest.raises(FileNotFoundError):
            _ = pfx.retrieve('bogus-filename')
        assert pfx.upload_counter == 0
        assert pfx.download_counter == 0
    assert not fc.cache_dir.exists()


def test_s3_retr_bad():
    with FileCache() as fc:
        pfx = fc.new_prefix('s3://rms-node-bogus-bucket-name-XXX', anonymous=True)
        with pytest.raises(FileNotFoundError):
            _ = pfx.retrieve('bogus-filename')
        assert pfx.upload_counter == 0
        assert pfx.download_counter == 0
        pfx = fc.new_prefix(S3_TEST_BUCKET_ROOT, anonymous=True)
        with pytest.raises(FileNotFoundError):
            _ = pfx.retrieve('bogus-filename')
        assert pfx.upload_counter == 0
        assert pfx.download_counter == 0
    assert not fc.cache_dir.exists()


def test_web_retr_bad():
    with FileCache() as fc:
        pfx = fc.new_prefix('https://bad-domain.seti.org')
        with pytest.raises(FileNotFoundError):
            _ = pfx.retrieve('bogus-filename')
        assert pfx.upload_counter == 0
        assert pfx.download_counter == 0
        pfx = fc.new_prefix(HTTP_TEST_ROOT)
        with pytest.raises(FileNotFoundError):
            _ = pfx.retrieve('bogus-filename')
        assert pfx.upload_counter == 0
        assert pfx.download_counter == 0
    assert not fc.cache_dir.exists()


def test_multi_prefixs_retr():
    with FileCache() as fc:
        prefixs = []
        # Different prefix should have different cache paths but all have the same
        # contents
        for prefix in CLOUD_PREFIXES:
            prefixs.append(fc.new_prefix(prefix, anonymous=True))
        for filename in EXPECTED_FILENAMES:
            paths = []
            for prefix in prefixs:
                paths.append(prefix.retrieve(filename))
            for i, path1 in enumerate(paths):
                for j, path2 in enumerate(paths):
                    if i == j:
                        continue
                    assert str(path1) != str(path2)
            for path in paths:
                _compare_to_expected_path(path, filename)
    assert not fc.cache_dir.exists()


@pytest.mark.parametrize('prefix', CLOUD_PREFIXES)
def test_multi_prefixs_shared_retr(prefix):
    with FileCache(shared=True) as fc1:
        pfx1 = fc1.new_prefix(prefix, anonymous=True)
        paths1 = []
        for filename in EXPECTED_FILENAMES:
            paths1.append(pfx1.retrieve(filename))
        with FileCache(shared=True) as fc2:
            pfx2 = fc2.new_prefix(prefix, anonymous=True)
            paths2 = []
            for filename in EXPECTED_FILENAMES:
                paths2.append(pfx2.retrieve(filename))
            for path1, path2 in zip(paths1, paths2):
                assert path1.exists()
                assert str(path1) == str(path2)
        fc1.clean_up(final=True)
    assert not fc1.cache_dir.exists()
    assert not fc2.cache_dir.exists()


def test_locking():
    with FileCache(shared=True) as fc:
        pfx = fc.new_prefix(HTTP_TEST_ROOT, lock_timeout=0)
        filename = (HTTP_TEST_ROOT.replace('https://', 'http_') + '/' +
                    EXPECTED_FILENAMES[0])
        local_path = fc.cache_dir / filename
        lock_path = pfx._lock_path(local_path)
        lock = filelock.FileLock(lock_path, timeout=0)
        lock.acquire()
        try:
            with pytest.raises(TimeoutError):
                pfx.retrieve(EXPECTED_FILENAMES[0])
        finally:
            lock.release()
        lock_path.unlink(missing_ok=True)
        assert pfx.upload_counter == 0
        assert pfx.download_counter == 0
        fc.clean_up(final=True)
    assert not fc.cache_dir.exists()

    with FileCache(shared=False) as fc:
        pfx = fc.new_prefix(HTTP_TEST_ROOT, lock_timeout=0)
        filename = (HTTP_TEST_ROOT.replace('https://', 'http_') + '/' +
                    EXPECTED_FILENAMES[0])
        local_path = fc.cache_dir / filename
        lock_path = pfx._lock_path(local_path)
        lock = filelock.FileLock(lock_path, timeout=0)
        lock.acquire()
        pfx.retrieve(EXPECTED_FILENAMES[0])  # shared=False doesn't lock
        lock.release()
        lock_path.unlink(missing_ok=True)
        assert pfx.upload_counter == 0
        assert pfx.download_counter == 1
    assert not fc.cache_dir.exists()


def test_bad_cache_dir():
    with pytest.raises(ValueError):
        with FileCache() as fc:
            orig_cache_dir = fc._cache_dir
            fc._cache_dir = '/bogus/path/not/a/filecache'
    fc._cache_dir = orig_cache_dir
    fc.clean_up()
    assert not fc.cache_dir.exists()


def test_double_delete():
    with FileCache() as fc:
        pfx = fc.new_prefix(HTTP_TEST_ROOT)
        for filename in EXPECTED_FILENAMES:
            pfx.retrieve(filename)
        assert pfx.upload_counter == 0
        assert pfx.download_counter == len(EXPECTED_FILENAMES)
        filename = (HTTP_TEST_ROOT.replace('https://', 'http_') + '/' +
                    EXPECTED_FILENAMES[0])
        path = fc.cache_dir / filename
        path.unlink()
    assert not fc.cache_dir.exists()

    with FileCache() as fc:
        pfx = fc.new_prefix(HTTP_TEST_ROOT)
        for filename in EXPECTED_FILENAMES:
            pfx.retrieve(filename)
        assert pfx.upload_counter == 0
        assert pfx.download_counter == len(EXPECTED_FILENAMES)
        fc.clean_up()  # Test double clean_up
        assert not fc.cache_dir.exists()
        fc.clean_up()
        assert not fc.cache_dir.exists()
        for filename in EXPECTED_FILENAMES:
            pfx.retrieve(filename)
        assert pfx.upload_counter == 0
        assert pfx.download_counter == len(EXPECTED_FILENAMES)*2
        assert fc.cache_dir.exists()
        fc.clean_up()
        assert not fc.cache_dir.exists()
        fc.clean_up()
        assert not fc.cache_dir.exists()

    with FileCache(shared=True) as fc:
        pfx = fc.new_prefix(HTTP_TEST_ROOT)
        for filename in EXPECTED_FILENAMES:
            pfx.retrieve(filename)
        assert pfx.upload_counter == 0
        assert pfx.download_counter == len(EXPECTED_FILENAMES)
        fc.clean_up()  # Test double clean_up
        assert fc.cache_dir.exists()
        fc.clean_up()
        assert fc.cache_dir.exists()
        for filename in EXPECTED_FILENAMES:
            pfx.retrieve(filename)
        assert pfx.upload_counter == 0
        assert pfx.download_counter == len(EXPECTED_FILENAMES)
        assert fc.cache_dir.exists()
        fc.clean_up()
        assert fc.cache_dir.exists()
        fc.clean_up()
        assert fc.cache_dir.exists()
        fc.clean_up(final=True)
        assert not fc.cache_dir.exists()


def test_open_context_read():
    with FileCache() as fc:
        pfx = fc.new_prefix(HTTP_TEST_ROOT)
        with pfx.open(EXPECTED_FILENAMES[0], 'r') as fp:
            cache_data = fp.read()
        assert pfx.upload_counter == 0
        assert pfx.download_counter == 1
        _compare_to_expected_data(cache_data, EXPECTED_FILENAMES[0])
    assert not fc.cache_dir.exists()


def test_cache_owner():
    with FileCache(shared=True, cache_owner=True) as fc1:
        with FileCache(shared=True) as fc2:
            pass
        assert fc1.cache_dir == fc2.cache_dir
        assert os.path.exists(fc1.cache_dir)
    assert not os.path.exists(fc1.cache_dir)
    assert not os.path.exists(fc2.cache_dir)


def test_local_upl_good():
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir)
        with FileCache() as fc:
            pfx = fc.new_prefix(temp_dir)
            local_path = pfx.get_local_path('dir1/test_file.txt')
            assert local_path.resolve() == (temp_dir / 'dir1/test_file.txt').resolve()
            with open(EXPECTED_DIR / EXPECTED_FILENAMES[0], 'rb') as fp1:
                with open(local_path, 'wb') as fp2:
                    fp2.write(fp1.read())
            pfx.upload('dir1/test_file.txt')
            assert os.path.exists(temp_dir / 'dir1/test_file.txt')
        assert os.path.exists(temp_dir / 'dir1/test_file.txt')
    assert not os.path.exists(temp_dir / 'dir1/test_file.txt')


def test_local_upl_ctx():
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir)
        with FileCache() as fc:
            pfx = fc.new_prefix(temp_dir)
            with pfx.open('dir1/test_file.txt', 'wb') as fp2:
                with open(EXPECTED_DIR / EXPECTED_FILENAMES[0], 'rb') as fp1:
                    fp2.write(fp1.read())
            assert os.path.exists(temp_dir / 'dir1/test_file.txt')
        assert os.path.exists(temp_dir / 'dir1/test_file.txt')
    assert not os.path.exists(temp_dir / 'dir1/test_file.txt')


def test_local_upl_bad():
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir)
        with FileCache() as fc:
            pfx = fc.new_prefix(temp_dir)
            with pytest.raises(FileNotFoundError):
                pfx.upload('XXXXXX.XXX')


@pytest.mark.parametrize('prefix', WRITABLE_CLOUD_PREFIXES)
def test_cloud_upl_good(prefix):
    with FileCache() as fc:
        new_prefix = f'{prefix}/{uuid.uuid4()}'
        pfx = fc.new_prefix(new_prefix, anonymous=True)
        local_path = pfx.get_local_path('test_file.txt')
        with open(EXPECTED_DIR / EXPECTED_FILENAMES[0], 'rb') as fp1:
            with open(local_path, 'wb') as fp2:
                fp2.write(fp1.read())
        pfx.upload('test_file.txt')
    assert not fc.cache_dir.exists()


@pytest.mark.parametrize('prefix', WRITABLE_CLOUD_PREFIXES)
def test_cloud_upl_bad(prefix):
    with FileCache() as fc:
        new_prefix = f'{prefix}/{uuid.uuid4()}'
        pfx = fc.new_prefix(new_prefix, anonymous=True)
        with pytest.raises(FileNotFoundError):
            pfx.upload('XXXXXXXXX.xxx')
    assert not fc.cache_dir.exists()


def test_complex_read_write():
    pfx_name = f'{GS_WRITABLE_TEST_BUCKET_ROOT}/{uuid.uuid4()}'
    with FileCache() as fc:
        pfx = fc.new_prefix(pfx_name)
        with pfx.open('test_file.txt', 'wb') as fp:
            fp.write(b'A')
        with pfx.open('test_file.txt', 'ab') as fp:
            fp.write(b'B')
        with pfx.open('test_file.txt', 'rb') as fp:
            res = fp.read()
        assert res == b'AB'
        assert pfx.download_counter == 0
        assert pfx.upload_counter == 2
    with FileCache() as fc:
        pfx = fc.new_prefix(pfx_name)
        with pfx.open('test_file.txt', 'rb') as fp:
            res = fp.read()
        assert res == b'AB'
        assert pfx.download_counter == 1
        assert pfx.upload_counter == 0


# THIS MUST BE AT THE END IN ORDER FOR CODE COVERAGE TO WORK

def test_atexit():
    fc = FileCache(atexit_cleanup=False)
    assert os.path.exists(fc.cache_dir)
    atexit._run_exitfuncs()
    assert os.path.exists(fc.cache_dir)
    fc.clean_up()

    fc = FileCache(atexit_cleanup=True)
    assert os.path.exists(fc.cache_dir)
    atexit._run_exitfuncs()
    assert not os.path.exists(fc.cache_dir)

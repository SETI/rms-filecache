# Test trying to multi-download the same file multiple times in the same call

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
from filecache import (FileCache,
                       FileCacheSource,
                       FileCacheSourceLocal,
                       FileCacheSourceHTTP,
                       FileCacheSourceGS,
                       FileCacheSourceS3)

import filelock


ROOT_DIR = Path(__file__).resolve().parent.parent
TEST_FILES_DIR = ROOT_DIR / 'test_files'
EXPECTED_DIR = TEST_FILES_DIR / 'expected'

EXPECTED_FILENAMES = ('lorem1.txt',
                      'subdir1/subdir2a/binary1.bin',
                      'subdir1/lorem1.txt',
                      'subdir1/subdir2b/binary1.bin')
LIMITED_FILENAMES = EXPECTED_FILENAMES[0:2]

GS_TEST_BUCKET_ROOT = 'gs://rms-filecache-tests'
S3_TEST_BUCKET_ROOT = 's3://rms-filecache-tests'
HTTP_TEST_ROOT = 'https://storage.googleapis.com/rms-filecache-tests'
CLOUD_PREFIXES = (GS_TEST_BUCKET_ROOT, S3_TEST_BUCKET_ROOT, HTTP_TEST_ROOT)

BAD_GS_TEST_BUCKET_ROOT = 'gs://bad-bucket-name-XXX'
BAD_S3_TEST_BUCKET_ROOT = 's3://bad-bucket-name-XXX'
BAD_HTTP_TEST_ROOT = 'http://pds-bad-domain-XXX.seti.org'
BAD_CLOUD_PREFIXES = (BAD_GS_TEST_BUCKET_ROOT, BAD_S3_TEST_BUCKET_ROOT,
                      BAD_HTTP_TEST_ROOT)
BAD_WRITABLE_CLOUD_PREFIXES = (BAD_GS_TEST_BUCKET_ROOT,
                               BAD_S3_TEST_BUCKET_ROOT)

GS_WRITABLE_TEST_BUCKET_ROOT = 'gs://rms-filecache-tests-writable'
S3_WRITABLE_TEST_BUCKET_ROOT = 's3://rms-filecache-tests-writable'
WRITABLE_CLOUD_PREFIXES = (GS_WRITABLE_TEST_BUCKET_ROOT, S3_WRITABLE_TEST_BUCKET_ROOT)

ALL_PREFIXES = (EXPECTED_DIR, GS_TEST_BUCKET_ROOT, S3_TEST_BUCKET_ROOT,
                HTTP_TEST_ROOT)


# This has to be first to clean up any shared directory from a previous failed run
def test_cleanup_shared_dir():
    with FileCache(shared=True, cache_owner=True):
        pass


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


def _copy_file(src_file, dest_file):
    with open(src_file, 'rb') as fp:
        data = fp.read()
    with open(dest_file, 'wb') as fp:
        fp.write(data)


class MyLogger:
    def __init__(self):
        self.messages = []

    def debug(self, msg, *args, **kwargs):
        self.messages.append(msg)

    def error(self, msg, *args, **kwargs):
        self.messages.append(f'ERROR {msg}')

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
        pfx.exists(EXPECTED_FILENAMES[0])
        pfx.exists('bad-filename')
        pfx.retrieve(EXPECTED_FILENAMES[0])
# Creating cache /tmp/.file_cache_424b280b-e62c-4582-8560-211f54cabc23
# Initializing prefix https://storage.googleapis.com/rms-filecache-tests/
# Checking for existence: https://storage.googleapis.com/rms-filecache-tests/lorem1.txt
#   File exists
# Checking for existence: https://storage.googleapis.com/rms-filecache-tests/bad-filename
#   File does not exist
# Downloading https://storage.googleapis.com/rms-filecache-tests/lorem1.txt into /tmp/.file_cache_424b280b-e62c-4582-8560-211f54cabc23/http_storage.googleapis.com/rms-filecache-tests/lorem1.txt
# Cleaning up cache /tmp/.file_cache_424b280b-e62c-4582-8560-211f54cabc23
#   Removing lorem1.txt
#   Removing rms-filecache-tests
#   Removing http_storage.googleapis.com
#   Removing /tmp/.file_cache_424b280b-e62c-4582-8560-211f54cabc23
    logger.has_prefix_list(['Creating', 'Initializing', 'Checking', 'File exists',
                            'Checking', 'File does not', 'Downloading', 'Cleaning',
                            'Removing', 'Removing', 'Removing', 'Removing'])

    logger = MyLogger()
    filecache.set_global_logger(logger)
    with FileCache(shared=True) as fc:
        pfx = fc.new_prefix(HTTP_TEST_ROOT)
        pfx.retrieve(EXPECTED_FILENAMES[0])
        fc.clean_up(final=True)
# Creating shared cache /tmp/.file_cache___global__
# Initializing prefix https://storage.googleapis.com/rms-node-filecache-test-bucket/
# Downloading https://storage.googleapis.com/rms-node-filecache-test-bucket/lorem1.txt to /tmp/.file_cache___global__/http_storage.googleapis.com/rms-node-filecache-test-bucket/lorem1.txt
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
        _copy_file(EXPECTED_DIR / EXPECTED_FILENAMES[0], local_path)
        pfx.upload('test_file.txt')
# Creating cache /tmp/.file_cache_e866e868-7d79-4a54-9b52-dc4df2fda819
# Initializing prefix gs://rms-filecache-tests-writable/5bfef362-2ce1-49f9-beb9-4e96a8b747e6/
# Returning local path for test_file.txt as /tmp/.file_cache_e866e868-7d79-4a54-9b52-dc4df2fda819/gs_rms-filecache-tests-writable/5bfef362-2ce1-49f9-beb9-4e96a8b747e6/test_file.txt
# Uploading /tmp/.file_cache_e866e868-7d79-4a54-9b52-dc4df2fda819/gs_rms-filecache-tests-writable/5bfef362-2ce1-49f9-beb9-4e96a8b747e6/test_file.txt to gs://rms-filecache-tests-writable/5bfef362-2ce1-49f9-beb9-4e96a8b747e6/test_file.txt
# Cleaning up cache /tmp/.file_cache_e866e868-7d79-4a54-9b52-dc4df2fda819
    logger.has_prefix_list(['Creating', 'Initializing', 'Returning', 'Uploading',
                            'Cleaning'])


def test_easy_logger(capsys):
    filecache.set_easy_logger()
    filecache.set_easy_logger()
    filecache.set_global_logger(None)
    filecache.set_easy_logger()
    filecache.set_easy_logger()
    filecache.set_easy_logger()
    with FileCache() as fc:
        fc.new_prefix('gs://test')
    assert 'Initializing prefix gs://test/\n' in capsys.readouterr().out
    filecache.set_global_logger(None)
    with FileCache() as fc:
        fc.new_prefix('gs://test')
    assert capsys.readouterr().out == ''


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
        FileCache(temp_dir='\000')
    with pytest.raises(ValueError):
        FileCache(temp_dir=EXPECTED_DIR / EXPECTED_FILENAMES[0])


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
        FileCache(shared=5)
    with pytest.raises(ValueError):
        FileCache(shared='a/b')
    with pytest.raises(ValueError):
        FileCache(shared='a\\b')
    with pytest.raises(ValueError):
        FileCache(shared='/a')
    with pytest.raises(ValueError):
        FileCache(shared='\\a')


def test_prefix_bad():
    with FileCache() as fc:
        with pytest.raises(TypeError):
            fc.new_prefix(5)
    assert not fc.cache_dir.exists()


@pytest.mark.parametrize('prefix', ALL_PREFIXES)
def test_exists_all_good(prefix):
    with FileCache(all_anonymous=True) as fc:
        for filename in LIMITED_FILENAMES:
            assert fc.exists(f'{prefix}/{filename}')


@pytest.mark.parametrize('prefix', ALL_PREFIXES)
def test_exists_all_bad(prefix):
    with FileCache(all_anonymous=True) as fc:
        assert not fc.exists(f'{prefix}/nonexistent.txt')
        assert not fc.exists(f'{prefix}/a/b/c.txt')
        assert not fc.exists(f'{prefix}-bad/{EXPECTED_FILENAMES[0]}')


@pytest.mark.parametrize('shared', (False, True, 'test'))
def test_local_retr_good(shared):
    for pass_no in range(5):  # Make sure the expected dir doesn't get modified
        with FileCache(shared=shared) as fc:
            for filename in EXPECTED_FILENAMES:
                full_filename = f'{EXPECTED_DIR}/{filename}'
                path = fc.retrieve(full_filename)
                assert str(path).replace('\\', '/') == \
                    f'{EXPECTED_DIR}/{filename}'.replace('\\', '/')
                path = fc.retrieve(full_filename)
                assert str(path).replace('\\', '/') == \
                    f'{EXPECTED_DIR}/{filename}'.replace('\\', '/')
                _compare_to_expected_path(path, full_filename)
            # No files or directories in the cache
            assert len(list(fc.cache_dir.iterdir())) == 0
            fc.clean_up(final=True)
            assert not fc.cache_dir.exists()


@pytest.mark.parametrize('shared', (False, True, 'test'))
def test_local_retr_pfx_good(shared):
    for pass_no in range(5):  # Make sure the expected dir doesn't get modified
        with FileCache(shared=shared) as fc:
            lf = fc.new_prefix(EXPECTED_DIR)
            assert lf.is_local
            for filename in EXPECTED_FILENAMES:
                path = lf.retrieve(filename)
                assert str(path).replace('\\', '/') == \
                    f'{EXPECTED_DIR}/{filename}'.replace('\\', '/')
                path = lf.retrieve(filename)
                assert str(path).replace('\\', '/') == \
                    f'{EXPECTED_DIR}/{filename}'.replace('\\', '/')
                _compare_to_expected_path(path, filename)
            # No files or directories in the cache
            assert len(list(fc.cache_dir.iterdir())) == 0
            fc.clean_up(final=True)
            assert not fc.cache_dir.exists()


@pytest.mark.parametrize('shared', (False, True, 'test'))
@pytest.mark.parametrize('prefix', CLOUD_PREFIXES)
def test_cloud_retr_good(shared, prefix):
    with FileCache(shared=shared, all_anonymous=True) as fc:
        for filename in LIMITED_FILENAMES:
            assert fc.exists(f'{prefix}/{filename}')
            path = fc.retrieve(f'{prefix}/{filename}')
            assert fc.exists(f'{prefix}/{filename}')
            assert str(path).replace('\\', '/').endswith(filename)
            _compare_to_expected_path(path, filename)
            # Retrieving the same thing a second time should do nothing
            path = fc.retrieve(f'{prefix}/{filename}')
            assert str(path).replace('\\', '/').endswith(filename)
            _compare_to_expected_path(path, filename)
        assert fc.upload_counter == 0
        assert fc.download_counter == len(LIMITED_FILENAMES)
        fc.clean_up(final=True)
    assert not fc.cache_dir.exists()


@pytest.mark.parametrize('shared', (False, True, 'test'))
@pytest.mark.parametrize('prefix', CLOUD_PREFIXES)
def test_cloud_retr_pfx_good(shared, prefix):
    with FileCache(shared=shared) as fc:
        pfx = fc.new_prefix(prefix, anonymous=True)
        assert not pfx.is_local
        for filename in LIMITED_FILENAMES:
            path = pfx.retrieve(filename)
            assert str(path).replace('\\', '/').endswith(filename)
            _compare_to_expected_path(path, filename)
            # Retrieving the same thing a second time should do nothing
            path = pfx.retrieve(filename)
            assert str(path).replace('\\', '/').endswith(filename)
            _compare_to_expected_path(path, filename)
        assert fc.upload_counter == 0
        assert fc.download_counter == len(LIMITED_FILENAMES)
        assert pfx.upload_counter == 0
        assert pfx.download_counter == len(LIMITED_FILENAMES)
        fc.clean_up(final=True)
    assert not fc.cache_dir.exists()


@pytest.mark.parametrize('shared', (False, True, 'test'))
@pytest.mark.parametrize('prefix', CLOUD_PREFIXES)
def test_cloud_retr_multi_good(shared, prefix):
    with FileCache(shared=shared, all_anonymous=True) as fc:
        for filename in LIMITED_FILENAMES:
            paths = fc.retrieve([f'{prefix}/{filename}'])
            assert len(paths) == 1
            path = paths[0]
            assert str(path).replace('\\', '/').endswith(filename)
            _compare_to_expected_path(path, filename)
            # Retrieving the same thing a second time should do nothing
            paths = fc.retrieve((f'{prefix}/{filename}',))
            assert len(paths) == 1
            path = paths[0]
            assert str(path).replace('\\', '/').endswith(filename)
            _compare_to_expected_path(path, filename)
        assert fc.upload_counter == 0
        assert fc.download_counter == len(LIMITED_FILENAMES)
        fc.clean_up(final=True)
    assert not fc.cache_dir.exists()


@pytest.mark.parametrize('shared', (False, True, 'test'))
@pytest.mark.parametrize('prefix', CLOUD_PREFIXES)
def test_cloud_retr_multi_pfx_good(shared, prefix):
    with FileCache(shared=shared) as fc:
        pfx = fc.new_prefix(prefix, anonymous=True)
        for filename in LIMITED_FILENAMES:
            paths = pfx.retrieve((filename,))
            assert len(paths) == 1
            path = paths[0]
            assert str(path).replace('\\', '/').endswith(filename)
            _compare_to_expected_path(path, filename)
            # Retrieving the same thing a second time should do nothing
            paths = pfx.retrieve([filename])
            assert len(paths) == 1
            path = paths[0]
            assert str(path).replace('\\', '/').endswith(filename)
            _compare_to_expected_path(path, filename)
        assert fc.upload_counter == 0
        assert fc.download_counter == len(LIMITED_FILENAMES)
        assert pfx.upload_counter == 0
        assert pfx.download_counter == len(LIMITED_FILENAMES)
        fc.clean_up(final=True)
    assert not fc.cache_dir.exists()


@pytest.mark.parametrize('prefix', CLOUD_PREFIXES)
def test_cloud2_retr_pfx_good(prefix):
    with FileCache() as fc:
        # With two identical prefixes, it shouldn't matter which you use
        # because we will return the same object
        pfx1 = fc.new_prefix(prefix, anonymous=True)
        pfx2 = fc.new_prefix(prefix, anonymous=True)
        assert pfx1 is pfx2
        for filename in LIMITED_FILENAMES:
            path1 = pfx1.retrieve(filename)
            assert str(path1).replace('\\', '/').endswith(filename)
            _compare_to_expected_path(path1, filename)
            path2 = pfx2.retrieve(filename)
            assert str(path2).replace('\\', '/').endswith(filename)
            assert str(path1) == str(path2)
            _compare_to_expected_path(path2, filename)
        assert pfx1.upload_counter == 0
        assert pfx1.download_counter == len(LIMITED_FILENAMES)
        assert pfx2.upload_counter == 0
        assert pfx2.download_counter == len(LIMITED_FILENAMES)
    assert not fc.cache_dir.exists()


@pytest.mark.parametrize('prefix', CLOUD_PREFIXES)
def test_cloud3_retr_pfx_good(prefix):
    # Multiple prefixes with different subdir prefixes
    with FileCache() as fc:
        pfx1 = fc.new_prefix(prefix, anonymous=True)
        for filename in LIMITED_FILENAMES:
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


def test_local_retr_bad():
    with FileCache() as fc:
        with pytest.raises(FileNotFoundError):
            fc.retrieve('nonexistent.txt')
    assert not fc.cache_dir.exists()
    with FileCache() as fc:
        ret = fc.retrieve('nonexistent.txt', exception_on_fail=False)
        assert isinstance(ret, FileNotFoundError)
    assert not fc.cache_dir.exists()


def test_local_retr_pfx_bad():
    with FileCache() as fc:
        lf = fc.new_prefix(EXPECTED_DIR)
        with pytest.raises(FileNotFoundError):
            lf.retrieve('nonexistent.txt')
    assert not fc.cache_dir.exists()


@pytest.mark.parametrize('shared', (False, True))
@pytest.mark.parametrize('prefix', BAD_CLOUD_PREFIXES)
def test_cloud_retr_bad(shared, prefix):
    with FileCache(shared=shared, cache_owner=True) as fc:
        with pytest.raises(ValueError):
            fc.retrieve(prefix, anonymous=True)
        assert fc.upload_counter == 0
        assert fc.download_counter == 0
        with pytest.raises(FileNotFoundError):
            fc.retrieve(f'{prefix}/bogus-filename', anonymous=True)
        assert fc.upload_counter == 0
        assert fc.download_counter == 0
        ret = fc.retrieve(f'{prefix}/bogus-filename', anonymous=True,
                          exception_on_fail=False)
        assert isinstance(ret, FileNotFoundError)
        assert fc.upload_counter == 0
        assert fc.download_counter == 0
    assert not fc.cache_dir.exists()


@pytest.mark.parametrize('shared', (False, True))
@pytest.mark.parametrize('prefix', CLOUD_PREFIXES)
def test_cloud2_retr_bad(shared, prefix):
    with FileCache(shared=shared, cache_owner=True) as fc:
        with pytest.raises(FileNotFoundError):
            fc.retrieve(f'{prefix}/bogus-filename', anonymous=True)
        assert fc.upload_counter == 0
        assert fc.download_counter == 0
        ret = fc.retrieve(f'{prefix}/bogus-filename', anonymous=True,
                          exception_on_fail=False)
        assert isinstance(ret, FileNotFoundError)
        assert fc.upload_counter == 0
        assert fc.download_counter == 0
    assert not fc.cache_dir.exists()


@pytest.mark.parametrize('shared', (False, True))
@pytest.mark.parametrize('prefix', BAD_CLOUD_PREFIXES)
def test_cloud_retr_pfx_bad_1(shared, prefix):
    with FileCache(shared=shared, cache_owner=True) as fc:
        pfx = fc.new_prefix(prefix, anonymous=True)
        with pytest.raises(FileNotFoundError):
            pfx.retrieve('bogus-filename')
        assert fc.upload_counter == 0
        assert fc.download_counter == 0
        assert pfx.upload_counter == 0
        assert pfx.download_counter == 0
        ret = pfx.retrieve('bogus-filename', exception_on_fail=False)
        assert isinstance(ret, FileNotFoundError)
        assert fc.upload_counter == 0
        assert fc.download_counter == 0
        assert pfx.upload_counter == 0
        assert pfx.download_counter == 0
    assert not fc.cache_dir.exists()


@pytest.mark.parametrize('shared', (False, True))
@pytest.mark.parametrize('prefix', CLOUD_PREFIXES)
def test_cloud_retr_pfx_bad_2(shared, prefix):
    with FileCache(shared=shared, cache_owner=True) as fc:
        pfx = fc.new_prefix(prefix, anonymous=True)
        with pytest.raises(FileNotFoundError):
            pfx.retrieve('bogus-filename')
        assert fc.upload_counter == 0
        assert fc.download_counter == 0
        assert pfx.upload_counter == 0
        assert pfx.download_counter == 0
        ret = pfx.retrieve('bogus-filename', exception_on_fail=False)
        assert isinstance(ret, FileNotFoundError)
        assert fc.upload_counter == 0
        assert fc.download_counter == 0
        assert pfx.upload_counter == 0
        assert pfx.download_counter == 0
    assert not fc.cache_dir.exists()


def test_multi_prefixes_retr():
    with FileCache() as fc:
        prefixes = []
        # Different prefix should have different cache paths but all have the same
        # contents
        for prefix in CLOUD_PREFIXES:
            prefixes.append(fc.new_prefix(prefix, anonymous=True))
        for filename in EXPECTED_FILENAMES:
            paths = []
            for prefix in prefixes:
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
def test_multi_prefixes_shared_retr(prefix):
    with FileCache(shared=True, cache_owner=True) as fc1:
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
    assert not fc1.cache_dir.exists()
    assert not fc2.cache_dir.exists()


def test_locking_1():
    with FileCache(shared=True, cache_owner=True) as fc:
        filename = (HTTP_TEST_ROOT.replace('https://', 'http_') + '/' +
                    EXPECTED_FILENAMES[0])
        local_path = fc.cache_dir / filename
        lock_path = fc._lock_path(local_path)
        lock = filelock.FileLock(lock_path, timeout=0)
        lock.acquire()
        try:
            with pytest.raises(TimeoutError):
                fc.retrieve(f'{HTTP_TEST_ROOT}/{EXPECTED_FILENAMES[0]}', lock_timeout=0)
        finally:
            lock.release()
            lock_path.unlink(missing_ok=True)
        assert fc.upload_counter == 0
        assert fc.download_counter == 0
    assert not fc.cache_dir.exists()


def test_locking_pfx_1():
    with FileCache(shared=True, cache_owner=True) as fc:
        pfx = fc.new_prefix(HTTP_TEST_ROOT, lock_timeout=0)
        filename = (HTTP_TEST_ROOT.replace('https://', 'http_') + '/' +
                    EXPECTED_FILENAMES[0])
        local_path = fc.cache_dir / filename
        lock_path = fc._lock_path(local_path)
        lock = filelock.FileLock(lock_path, timeout=0)
        lock.acquire()
        try:
            with pytest.raises(TimeoutError):
                pfx.retrieve(EXPECTED_FILENAMES[0])
        finally:
            lock.release()
            lock_path.unlink(missing_ok=True)
        assert fc.upload_counter == 0
        assert fc.download_counter == 0
        assert pfx.upload_counter == 0
        assert pfx.download_counter == 0
    assert not fc.cache_dir.exists()


def test_locking_2():
    with FileCache(shared=True, cache_owner=True) as fc:
        filename = (HTTP_TEST_ROOT.replace('https://', 'http_') + '/' +
                    EXPECTED_FILENAMES[0])
        local_path = fc.cache_dir / filename
        lock_path = fc._lock_path(local_path)
        lock = filelock.FileLock(lock_path, timeout=0)
        lock.acquire()
        assert isinstance(fc.retrieve(f'{HTTP_TEST_ROOT}/{EXPECTED_FILENAMES[0]}',
                                      lock_timeout=0, exception_on_fail=False),
                          TimeoutError)
        lock.release()
        lock_path.unlink(missing_ok=True)
        assert fc.upload_counter == 0
        assert fc.download_counter == 0
    assert not fc.cache_dir.exists()


def test_locking_pfx_2():
    with FileCache(shared=True, cache_owner=True) as fc:
        pfx = fc.new_prefix(HTTP_TEST_ROOT, lock_timeout=0)
        filename = (HTTP_TEST_ROOT.replace('https://', 'http_') + '/' +
                    EXPECTED_FILENAMES[0])
        local_path = fc.cache_dir / filename
        lock_path = fc._lock_path(local_path)
        lock = filelock.FileLock(lock_path, timeout=0)
        lock.acquire()
        assert isinstance(pfx.retrieve(EXPECTED_FILENAMES[0], exception_on_fail=False),
                          TimeoutError)
        lock.release()
        lock_path.unlink(missing_ok=True)
        assert fc.upload_counter == 0
        assert fc.download_counter == 0
        assert pfx.upload_counter == 0
        assert pfx.download_counter == 0
    assert not fc.cache_dir.exists()


def test_locking_3():
    with FileCache(shared=False) as fc:
        filename = (HTTP_TEST_ROOT.replace('https://', 'http_') + '/' +
                    EXPECTED_FILENAMES[0])
        local_path = fc.cache_dir / filename
        lock_path = fc._lock_path(local_path)
        lock = filelock.FileLock(lock_path, timeout=0)
        lock.acquire()
        fc.retrieve(f'{HTTP_TEST_ROOT}/{EXPECTED_FILENAMES[0]}',
                    lock_timeout=0)  # shared=False doesn't lock
        lock.release()
        lock_path.unlink(missing_ok=True)
        assert fc.upload_counter == 0
        assert fc.download_counter == 1
    assert not fc.cache_dir.exists()


def test_locking_pfx_3():
    with FileCache(shared=False) as fc:
        pfx = fc.new_prefix(HTTP_TEST_ROOT, lock_timeout=0)
        filename = (HTTP_TEST_ROOT.replace('https://', 'http_') + '/' +
                    EXPECTED_FILENAMES[0])
        local_path = fc.cache_dir / filename
        lock_path = fc._lock_path(local_path)
        lock = filelock.FileLock(lock_path, timeout=0)
        lock.acquire()
        pfx.retrieve(EXPECTED_FILENAMES[0])  # shared=False doesn't lock
        lock.release()
        lock_path.unlink(missing_ok=True)
        assert fc.upload_counter == 0
        assert fc.download_counter == 1
        assert pfx.upload_counter == 0
        assert pfx.download_counter == 1
    assert not fc.cache_dir.exists()


def test_locking_multi_1():
    with FileCache(shared=True, cache_owner=True) as fc:
        filename = (HTTP_TEST_ROOT.replace('https://', 'http_') + '/' +
                    EXPECTED_FILENAMES[0])
        local_path = fc.cache_dir / filename
        lock_path = fc._lock_path(local_path)
        lock = filelock.FileLock(lock_path, timeout=0)
        lock.acquire()
        filenames = [f'{HTTP_TEST_ROOT}/{f}' for f in EXPECTED_FILENAMES]
        try:
            with pytest.raises(TimeoutError):
                fc.retrieve(filenames, lock_timeout=0)
        finally:
            lock.release()
            lock_path.unlink(missing_ok=True)
        assert fc.upload_counter == 0
        assert fc.download_counter == len(EXPECTED_FILENAMES) - 1
    assert not fc.cache_dir.exists()


def test_locking_multi_pfx_1():
    with FileCache(shared=True, cache_owner=True) as fc:
        pfx = fc.new_prefix(HTTP_TEST_ROOT, lock_timeout=0)
        filename = (HTTP_TEST_ROOT.replace('https://', 'http_') + '/' +
                    EXPECTED_FILENAMES[0])
        local_path = fc.cache_dir / filename
        lock_path = fc._lock_path(local_path)
        lock = filelock.FileLock(lock_path, timeout=0)
        lock.acquire()
        try:
            with pytest.raises(TimeoutError):
                pfx.retrieve(EXPECTED_FILENAMES)
        finally:
            lock.release()
            lock_path.unlink(missing_ok=True)
        assert fc.upload_counter == 0
        assert fc.download_counter == len(EXPECTED_FILENAMES) - 1
        assert pfx.upload_counter == 0
        assert pfx.download_counter == len(EXPECTED_FILENAMES) - 1
    assert not fc.cache_dir.exists()


def test_locking_multi_2():
    filecache.set_easy_logger()
    with FileCache(shared=True, cache_owner=True) as fc:
        filename = (HTTP_TEST_ROOT.replace('https://', 'http_') + '/' +
                    EXPECTED_FILENAMES[0])
        local_path = fc.cache_dir / filename
        lock_path = fc._lock_path(local_path)
        lock = filelock.FileLock(lock_path, timeout=0)
        lock.acquire()
        filenames = [f'{HTTP_TEST_ROOT}/{f}' for f in EXPECTED_FILENAMES]
        ret = fc.retrieve(filenames, exception_on_fail=False, lock_timeout=0)  # multi
        assert isinstance(ret[0], TimeoutError)
        for r in ret[1:]:
            assert isinstance(r, Path)
        lock.release()
        lock_path.unlink(missing_ok=True)
        assert fc.upload_counter == 0
        assert fc.download_counter == 3
    assert not fc.cache_dir.exists()


def test_locking_multi_pfx_2():
    with FileCache(shared=True, cache_owner=True) as fc:
        pfx = fc.new_prefix(HTTP_TEST_ROOT, lock_timeout=0)
        filename = (HTTP_TEST_ROOT.replace('https://', 'http_') + '/' +
                    EXPECTED_FILENAMES[0])
        local_path = fc.cache_dir / filename
        lock_path = fc._lock_path(local_path)
        lock = filelock.FileLock(lock_path, timeout=0)
        lock.acquire()
        ret = pfx.retrieve(EXPECTED_FILENAMES, exception_on_fail=False)  # multi
        assert isinstance(ret[0], TimeoutError)
        for r in ret[1:]:
            assert isinstance(r, Path)
        lock.release()
        lock_path.unlink(missing_ok=True)
        assert fc.upload_counter == 0
        assert fc.download_counter == 3
        assert pfx.upload_counter == 0
        assert pfx.download_counter == 3
    assert not fc.cache_dir.exists()


def test_cleanup_locking_bad():
    logger = MyLogger()
    with FileCache(logger=logger) as fc:
        local_path = fc.get_local_path('gs://test/test.txt')
        with open(local_path.parent / f'{fc._LOCK_PREFIX}{local_path.name}', 'w') as fp:
            fp.write('A')
    logger.has_prefix_list(['Creating', 'Returning', 'Cleaning', 'ERROR', 'Removing',
                            'Removing', 'Removing'])
    with FileCache() as fc:
        local_path = fc.get_local_path('gs://test/test.txt')
        with open(local_path.parent / f'{fc._LOCK_PREFIX}{local_path.name}', 'w') as fp:
            fp.write('A')


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
        for filename in LIMITED_FILENAMES:
            pfx.retrieve(filename)
        assert fc.upload_counter == 0
        assert fc.download_counter == len(LIMITED_FILENAMES)
        assert pfx.upload_counter == 0
        assert pfx.download_counter == len(LIMITED_FILENAMES)
        filename = (HTTP_TEST_ROOT.replace('https://', 'http_') + '/' +
                    LIMITED_FILENAMES[0])
        path = fc.cache_dir / filename
        path.unlink()
    assert not fc.cache_dir.exists()

    with FileCache() as fc:
        pfx = fc.new_prefix(HTTP_TEST_ROOT)
        for filename in LIMITED_FILENAMES:
            pfx.retrieve(filename)
        assert fc.upload_counter == 0
        assert fc.download_counter == len(LIMITED_FILENAMES)
        assert pfx.upload_counter == 0
        assert pfx.download_counter == len(LIMITED_FILENAMES)
        fc.clean_up()  # Test double clean_up
        assert not fc.cache_dir.exists()
        fc.clean_up()
        assert not fc.cache_dir.exists()
        for filename in LIMITED_FILENAMES:
            pfx.retrieve(filename)
        assert fc.upload_counter == 0
        assert fc.download_counter == len(LIMITED_FILENAMES)*2
        assert pfx.upload_counter == 0
        assert pfx.download_counter == len(LIMITED_FILENAMES)*2
        assert fc.cache_dir.exists()
        fc.clean_up()
        assert not fc.cache_dir.exists()
        fc.clean_up()
        assert not fc.cache_dir.exists()

    with FileCache(shared=True) as fc:
        pfx = fc.new_prefix(HTTP_TEST_ROOT)
        for filename in LIMITED_FILENAMES:
            pfx.retrieve(filename)
        assert fc.upload_counter == 0
        assert fc.download_counter == len(LIMITED_FILENAMES)
        assert pfx.upload_counter == 0
        assert pfx.download_counter == len(LIMITED_FILENAMES)
        fc.clean_up()  # Test double clean_up
        assert fc.cache_dir.exists()
        fc.clean_up()
        assert fc.cache_dir.exists()
        for filename in LIMITED_FILENAMES:
            pfx.retrieve(filename)
        assert fc.upload_counter == 0
        assert fc.download_counter == len(LIMITED_FILENAMES)
        assert pfx.upload_counter == 0
        assert pfx.download_counter == len(LIMITED_FILENAMES)
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
        with pfx.open(LIMITED_FILENAMES[0], 'r') as fp:
            cache_data = fp.read()
        assert fc.upload_counter == 0
        assert fc.download_counter == 1
        assert pfx.upload_counter == 0
        assert pfx.download_counter == 1
        _compare_to_expected_data(cache_data, LIMITED_FILENAMES[0])
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
        temp_dir = Path(temp_dir).expanduser().resolve()
        with FileCache() as fc:
            local_path = fc.get_local_path(temp_dir / 'dir1/test_file.txt',
                                           create_parents=False)
            assert not local_path.parent.is_dir()
            local_path = fc.get_local_path(temp_dir / 'dir1/test_file.txt')
            assert local_path.parent.is_dir()
            assert local_path == (temp_dir / 'dir1/test_file.txt')
            _copy_file(EXPECTED_DIR / EXPECTED_FILENAMES[0], local_path)
            assert fc.upload(f'{temp_dir}/dir1/test_file.txt') == local_path
            assert os.path.exists(temp_dir / 'dir1/test_file.txt')
        assert os.path.exists(temp_dir / 'dir1/test_file.txt')
    assert not os.path.exists(temp_dir / 'dir1/test_file.txt')


def test_local_upl_pfx_good():
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir).expanduser().resolve()
        with FileCache() as fc:
            pfx = fc.new_prefix(temp_dir)
            local_path = pfx.get_local_path('dir1/test_file.txt',
                                            create_parents=False)
            assert not local_path.parent.is_dir()
            local_path = pfx.get_local_path('dir1/test_file.txt')
            assert local_path.parent.is_dir()
            assert local_path == (temp_dir / 'dir1/test_file.txt')
            _copy_file(EXPECTED_DIR / EXPECTED_FILENAMES[0], local_path)
            assert pfx.upload('dir1/test_file.txt') == local_path
            assert os.path.exists(temp_dir / 'dir1/test_file.txt')
        assert os.path.exists(temp_dir / 'dir1/test_file.txt')
    assert not os.path.exists(temp_dir / 'dir1/test_file.txt')


def test_local_upl_ctx():
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir)
        with FileCache() as fc:
            with fc.open(temp_dir / 'dir1/test_file.txt', 'wb') as fp2:
                with open(EXPECTED_DIR / EXPECTED_FILENAMES[0], 'rb') as fp1:
                    fp2.write(fp1.read())
            assert os.path.exists(temp_dir / 'dir1/test_file.txt')
        assert os.path.exists(temp_dir / 'dir1/test_file.txt')
    assert not os.path.exists(temp_dir / 'dir1/test_file.txt')


def test_local_upl_pfx_ctx():
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
            with pytest.raises(FileNotFoundError):
                fc.upload(temp_dir / 'XXXXXX.XXX')
        with FileCache() as fc:
            ret = fc.upload(temp_dir / 'XXXXXX.XXX', exception_on_fail=False)
            assert isinstance(ret, FileNotFoundError)


def test_local_upl_pfx_bad():
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir)
        with FileCache() as fc:
            pfx = fc.new_prefix(temp_dir)
            with pytest.raises(FileNotFoundError):
                pfx.upload('XXXXXX.XXX')
        with FileCache() as fc:
            pfx = fc.new_prefix(temp_dir)
            ret = pfx.upload('XXXXXX.XXX', exception_on_fail=False)
            assert isinstance(ret, FileNotFoundError)


@pytest.mark.parametrize('prefix', WRITABLE_CLOUD_PREFIXES)
def test_cloud_upl_good(prefix):
    with FileCache(all_anonymous=True) as fc:
        new_prefix = f'{prefix}/{uuid.uuid4()}'
        local_path = fc.get_local_path(f'{new_prefix}/test_file.txt')
        _copy_file(EXPECTED_DIR / EXPECTED_FILENAMES[0], local_path)
        assert fc.upload(f'{new_prefix}/test_file.txt') == local_path
        assert fc.download_counter == 0
        assert fc.upload_counter == 1
    assert not fc.cache_dir.exists()


@pytest.mark.parametrize('prefix', WRITABLE_CLOUD_PREFIXES)
def test_cloud_upl_pfx_good(prefix):
    with FileCache() as fc:
        new_prefix = f'{prefix}/{uuid.uuid4()}'
        pfx = fc.new_prefix(new_prefix, anonymous=True)
        local_path = pfx.get_local_path('test_file.txt')
        _copy_file(EXPECTED_DIR / EXPECTED_FILENAMES[0], local_path)
        assert pfx.upload('test_file.txt') == local_path
        assert fc.download_counter == 0
        assert fc.upload_counter == 1
        assert pfx.download_counter == 0
        assert pfx.upload_counter == 1
    assert not fc.cache_dir.exists()


@pytest.mark.parametrize('prefix', WRITABLE_CLOUD_PREFIXES)
def test_cloud_upl_bad_1(prefix):
    with FileCache() as fc:
        with pytest.raises(FileNotFoundError):
            fc.upload(f'{prefix}/{uuid.uuid4()}/XXXXXXXXX.xxx',
                      anonymous=True)
        assert fc.download_counter == 0
        assert fc.upload_counter == 0
        ret = fc.upload(f'{prefix}/{uuid.uuid4()}/XXXXXXXXX.xxx',
                        anonymous=True, exception_on_fail=False)
        assert isinstance(ret, FileNotFoundError)
        assert fc.download_counter == 0
        assert fc.upload_counter == 0
    assert not fc.cache_dir.exists()


@pytest.mark.parametrize('prefix', WRITABLE_CLOUD_PREFIXES)
def test_cloud_upl_pfx_bad_1(prefix):
    with FileCache() as fc:
        new_prefix = f'{prefix}/{uuid.uuid4()}'
        pfx = fc.new_prefix(new_prefix, anonymous=True)
        with pytest.raises(FileNotFoundError):
            pfx.upload('XXXXXXXXX.xxx')
        assert fc.download_counter == 0
        assert fc.upload_counter == 0
        assert pfx.download_counter == 0
        assert pfx.upload_counter == 0
        ret = pfx.upload('XXXXXXXXX.xxx', exception_on_fail=False)
        assert isinstance(ret, FileNotFoundError)
        assert fc.download_counter == 0
        assert fc.upload_counter == 0
        assert pfx.download_counter == 0
        assert pfx.upload_counter == 0
    assert not fc.cache_dir.exists()


@pytest.mark.parametrize('prefix', BAD_WRITABLE_CLOUD_PREFIXES)
def test_cloud_upl_bad_2(prefix):
    with FileCache() as fc:
        new_prefix = f'{prefix}/{uuid.uuid4()}'
        filename = 'test_file.txt'
        local_path = fc.get_local_path(f'{new_prefix}/{filename}')
        _copy_file(EXPECTED_DIR / EXPECTED_FILENAMES[0], local_path)
        with pytest.raises(Exception) as e:
            fc.upload(f'{new_prefix}/{filename}', anonymous=True)
        assert not isinstance(e.type, FileNotFoundError)
        assert fc.download_counter == 0
        assert fc.upload_counter == 0
        ret = fc.upload(f'{new_prefix}/{filename}', anonymous=True,
                        exception_on_fail=False)
        assert isinstance(ret, Exception)
        assert not isinstance(e, FileNotFoundError)
        assert fc.download_counter == 0
        assert fc.upload_counter == 0
    assert not fc.cache_dir.exists()


@pytest.mark.parametrize('prefix', BAD_WRITABLE_CLOUD_PREFIXES)
def test_cloud_upl_pfx_bad_2(prefix):
    with FileCache() as fc:
        new_prefix = f'{prefix}/{uuid.uuid4()}'
        pfx = fc.new_prefix(new_prefix, anonymous=True)
        filename = 'test_file.txt'
        local_path = pfx.get_local_path(filename)
        _copy_file(EXPECTED_DIR / EXPECTED_FILENAMES[0], local_path)
        with pytest.raises(Exception) as e:
            pfx.upload(filename)
        assert not isinstance(e.type, FileNotFoundError)
        assert fc.download_counter == 0
        assert fc.upload_counter == 0
        assert pfx.download_counter == 0
        assert pfx.upload_counter == 0
        ret = pfx.upload(filename, exception_on_fail=False)
        assert not isinstance(e, FileNotFoundError)
        assert isinstance(ret, Exception)
        assert not isinstance(e, FileNotFoundError)
        assert fc.download_counter == 0
        assert fc.upload_counter == 0
        assert pfx.download_counter == 0
        assert pfx.upload_counter == 0
    assert not fc.cache_dir.exists()


def test_complex_read_write():
    pfx_name = f'{GS_WRITABLE_TEST_BUCKET_ROOT}/{uuid.uuid4()}'
    with FileCache(all_anonymous=True) as fc:
        with fc.open(f'{pfx_name}/test_file.txt', 'wb') as fp:
            fp.write(b'A')
        with fc.open(f'{pfx_name}/test_file.txt', 'ab') as fp:
            fp.write(b'B')
        with fc.open(f'{pfx_name}/test_file.txt', 'rb') as fp:
            res = fp.read()
        assert res == b'AB'
        assert fc.download_counter == 0
        assert fc.upload_counter == 2
    with FileCache(all_anonymous=True) as fc:
        with fc.open(f'{pfx_name}/test_file.txt', 'rb') as fp:
            res = fp.read()
        assert res == b'AB'
        assert fc.download_counter == 1
        assert fc.upload_counter == 0


def test_complex_read_write_pfx():
    pfx_name = f'{GS_WRITABLE_TEST_BUCKET_ROOT}/{uuid.uuid4()}'
    with FileCache(all_anonymous=True) as fc:
        pfx = fc.new_prefix(pfx_name)
        with pfx.open('test_file.txt', 'wb') as fp:
            fp.write(b'A')
        with pfx.open('test_file.txt', 'ab') as fp:
            fp.write(b'B')
        with pfx.open('test_file.txt', 'rb') as fp:
            res = fp.read()
        assert res == b'AB'
        assert fc.download_counter == 0
        assert fc.upload_counter == 2
        assert pfx.download_counter == 0
        assert pfx.upload_counter == 2
    with FileCache(all_anonymous=True) as fc:
        pfx = fc.new_prefix(pfx_name)
        with pfx.open('test_file.txt', 'rb') as fp:
            res = fp.read()
        assert res == b'AB'
        assert fc.download_counter == 1
        assert fc.upload_counter == 0
        assert pfx.download_counter == 1
        assert pfx.upload_counter == 0


def test_complex_retr_multi_1():
    with FileCache(all_anonymous=True) as fc:
        # Retrieve a couple of local files one at a time
        fc.retrieve(EXPECTED_DIR / EXPECTED_FILENAMES[0])
        fc.retrieve(EXPECTED_DIR / EXPECTED_FILENAMES[1])
        assert fc.download_counter == 0
        assert fc.upload_counter == 0
        # Now retrieve all the local files
        paths = fc.retrieve([EXPECTED_DIR / x for x in EXPECTED_FILENAMES])
        assert len(paths) == len(EXPECTED_FILENAMES)
        assert fc.download_counter == 0
        assert fc.upload_counter == 0


@pytest.mark.parametrize('shared', (False, True))
def test_complex_retr_multi_2(shared):
    with FileCache(shared=shared, cache_owner=True, all_anonymous=True) as fc:
        # Retrieve various cloud files one at a time
        fc.retrieve(f'{GS_TEST_BUCKET_ROOT}/{EXPECTED_FILENAMES[1]}')
        fc.retrieve(f'{S3_TEST_BUCKET_ROOT}/{EXPECTED_FILENAMES[1]}')
        fc.retrieve(f'{HTTP_TEST_ROOT}/{EXPECTED_FILENAMES[2]}')
        fc.retrieve(f'{S3_TEST_BUCKET_ROOT}/{EXPECTED_FILENAMES[2]}')
        fc.retrieve(EXPECTED_DIR / EXPECTED_FILENAMES[3])
        assert fc.download_counter == 4
        assert fc.upload_counter == 0
        # Now try to retrieve them all at once
        full_paths = []
        expected_paths = []
        for prefix in ALL_PREFIXES:
            prefix = str(prefix)
            for filename in EXPECTED_FILENAMES:
                full_paths.append(f'{prefix}/{filename}')
                full_filename = (prefix
                                 .replace('https://', 'http_')
                                 .replace('gs://', 'gs_')
                                 .replace('s3://', 's3_')) + '/' + filename
                if '://' in prefix:
                    full_filename = str(fc.cache_dir / full_filename)
                expected_paths.append(full_filename)
        local_paths = fc.retrieve(full_paths)
        for lp, ep in zip(local_paths, expected_paths):
            assert str(lp).replace('\\', '/') == ep.replace('\\', '/')
        assert fc.download_counter == len(ALL_PREFIXES) * len(EXPECTED_FILENAMES) - 4


@pytest.mark.parametrize('shared', (False, True))
def test_complex_retr_multi_3(shared):
    with FileCache(shared=shared, cache_owner=True, all_anonymous=True) as fc:
        # Retrieve various cloud files one at a time
        fc.retrieve(f'{GS_TEST_BUCKET_ROOT}/{EXPECTED_FILENAMES[1]}')
        fc.retrieve(f'{S3_TEST_BUCKET_ROOT}/{EXPECTED_FILENAMES[1]}')
        fc.retrieve(f'{HTTP_TEST_ROOT}/{EXPECTED_FILENAMES[2]}')
        fc.retrieve(f'{S3_TEST_BUCKET_ROOT}/{EXPECTED_FILENAMES[2]}')
        fc.retrieve(EXPECTED_DIR / EXPECTED_FILENAMES[3])
        assert fc.download_counter == 4
        assert fc.upload_counter == 0
        # Now try to retrieve them all at once
        full_paths = []
        expected_paths = []
        for filename in EXPECTED_FILENAMES:  # Flipped from above
            for prefix in ALL_PREFIXES:
                prefix = str(prefix)
                full_paths.append(f'{prefix}/{filename}')
                full_filename = (prefix
                                 .replace('https://', 'http_')
                                 .replace('gs://', 'gs_')
                                 .replace('s3://', 's3_')) + '/' + filename
                if '://' in prefix:
                    full_filename = str(fc.cache_dir / full_filename)
                expected_paths.append(full_filename)
        local_paths = fc.retrieve(full_paths)
        for lp, ep in zip(local_paths, expected_paths):
            assert str(lp).replace('\\', '/') == ep.replace('\\', '/')
        assert fc.download_counter == len(ALL_PREFIXES) * len(EXPECTED_FILENAMES) - 4


@pytest.mark.parametrize('shared', (False, True))
@pytest.mark.parametrize('prefix', ALL_PREFIXES)
def test_complex_retr_multi_4(shared, prefix):
    with FileCache(shared=shared, cache_owner=True, all_anonymous=True) as fc:
        # Retrieve some cloud files with a bad name included
        full_paths = [f'{prefix}/{filename}' for filename in EXPECTED_FILENAMES]
        full_paths = [f'{prefix}/nonexistent.txt'] + full_paths
        with pytest.raises(FileNotFoundError):
            local_paths = fc.retrieve(full_paths, exception_on_fail=True)
        local_paths = fc.retrieve(full_paths, exception_on_fail=False)
        assert isinstance(local_paths[0], FileNotFoundError)
        # Make sure everything else got downloaded
        for path in full_paths[1:]:
            local_path = fc.get_local_path(path)
            assert local_path.exists()
        if '://' in str(prefix):
            assert fc.download_counter == len(EXPECTED_FILENAMES)
        else:
            assert fc.download_counter == 0
        assert fc.upload_counter == 0


def test_complex_retr_multi_pfx_1():
    with FileCache(all_anonymous=True) as fc:
        pfx = fc.new_prefix(str(EXPECTED_DIR))
        # Retrieve a couple of local files one at a time
        pfx.retrieve(EXPECTED_FILENAMES[0])
        pfx.retrieve(EXPECTED_FILENAMES[1])
        assert fc.download_counter == 0
        assert fc.upload_counter == 0
        # Now retrieve all the local files
        paths = pfx.retrieve(EXPECTED_FILENAMES)
        assert len(paths) == len(EXPECTED_FILENAMES)
        assert fc.download_counter == 0
        assert fc.upload_counter == 0


@pytest.mark.parametrize('shared', (False, True))
def test_complex_retr_multi_pfx_2(shared):
    with FileCache(shared=shared, cache_owner=True, all_anonymous=True) as fc:
        # Retrieve various cloud files one at a time in random order
        pfx_gs = fc.new_prefix(GS_TEST_BUCKET_ROOT)
        pfx_s3 = fc.new_prefix(S3_TEST_BUCKET_ROOT)
        pfx_http = fc.new_prefix(HTTP_TEST_ROOT)
        pfx_local = fc.new_prefix(EXPECTED_DIR)
        gs_path1a = pfx_gs.retrieve(EXPECTED_FILENAMES[0])
        lc_path1a = pfx_local.retrieve(EXPECTED_FILENAMES[3])
        gs_path2a = pfx_gs.retrieve(EXPECTED_FILENAMES[2])
        s3_path1a = pfx_s3.retrieve(EXPECTED_FILENAMES[1])
        ht_path1a = pfx_http.retrieve(EXPECTED_FILENAMES[2])
        s3_path2a = pfx_s3.retrieve(EXPECTED_FILENAMES[2])
        ht_path2a = pfx_http.retrieve(EXPECTED_FILENAMES[3])
        lc_path2a = pfx_local.retrieve(EXPECTED_FILENAMES[1])
        assert fc.download_counter == 6
        assert fc.upload_counter == 0
        assert pfx_gs.download_counter == 2
        assert pfx_gs.upload_counter == 0
        assert pfx_s3.download_counter == 2
        assert pfx_s3.upload_counter == 0
        assert pfx_http.download_counter == 2
        assert pfx_http.upload_counter == 0
        # Retrieve them in bulk and compare paths
        gs_path1b, gs_path2b = pfx_gs.retrieve([EXPECTED_FILENAMES[0],
                                                EXPECTED_FILENAMES[2]])
        s3_path1b, s3_path2b = pfx_s3.retrieve([EXPECTED_FILENAMES[1],
                                                EXPECTED_FILENAMES[2]])
        ht_path1b, ht_path2b = pfx_http.retrieve([EXPECTED_FILENAMES[2],
                                                  EXPECTED_FILENAMES[3]])
        lc_path1b, lc_path2b = pfx_local.retrieve([EXPECTED_FILENAMES[3],
                                                   EXPECTED_FILENAMES[1]])
        assert gs_path1a == gs_path1b
        assert gs_path2a == gs_path2b
        assert s3_path1a == s3_path1b
        assert s3_path2a == s3_path2b
        assert ht_path1a == ht_path1b
        assert ht_path2a == ht_path2b
        assert lc_path1a == lc_path1b
        assert lc_path2a == lc_path2b
        assert fc.download_counter == 6
        assert fc.upload_counter == 0
        assert pfx_gs.download_counter == 2
        assert pfx_gs.upload_counter == 0
        assert pfx_s3.download_counter == 2
        assert pfx_s3.upload_counter == 0
        assert pfx_http.download_counter == 2
        assert pfx_http.upload_counter == 0


@pytest.mark.parametrize('shared', (False, True))
@pytest.mark.parametrize('prefix', ALL_PREFIXES)
def test_complex_retr_multi_pfx_3(shared, prefix):
    with FileCache(shared=shared, cache_owner=True, all_anonymous=True) as fc:
        # Retrieve some cloud files with a bad name included
        pfx = fc.new_prefix(prefix)
        clean_prefix = str(prefix).replace('\\', '/').rstrip('/')
        assert pfx.prefix == f'{clean_prefix}/'
        full_paths = ['nonexistent.txt'] + list(EXPECTED_FILENAMES)
        with pytest.raises(FileNotFoundError):
            local_paths = pfx.retrieve(full_paths, exception_on_fail=True)
        local_paths = pfx.retrieve(full_paths, exception_on_fail=False)
        assert isinstance(local_paths[0], FileNotFoundError)
        # Make sure everything else got downloaded
        for path in full_paths[1:]:
            local_path = pfx.get_local_path(path)
            assert local_path.exists()
        if '://' in str(prefix):
            assert fc.download_counter == len(EXPECTED_FILENAMES)
            assert pfx.download_counter == len(EXPECTED_FILENAMES)
        else:
            assert fc.download_counter == 0
            assert pfx.download_counter == 0
        assert fc.upload_counter == 0
        assert pfx.upload_counter == 0


def test_local_upl_multi_good():
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir).expanduser().resolve()
        with FileCache() as fc:
            paths = [f'dir1/test_file{x}' for x in range(10)]
            local_paths = [fc.get_local_path(temp_dir / x) for x in paths]
            for lp in local_paths:
                _copy_file(EXPECTED_DIR / EXPECTED_FILENAMES[0], lp)
            ret = fc.upload(local_paths)
            assert ret == local_paths
            for lp in local_paths:
                assert lp.is_file()
            assert fc.download_counter == 0
            assert fc.upload_counter == 0
        for lp in local_paths:
            assert lp.is_file()
    for lp in local_paths:
        assert not lp.is_file()


def test_local_upl_multi_pfx_good():
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir).expanduser().resolve()
        with FileCache() as fc:
            pfx = fc.new_prefix(temp_dir)
            paths = [f'dir1/test_file{x}' for x in range(10)]
            local_paths = [pfx.get_local_path(x) for x in paths]
            for lp in local_paths:
                _copy_file(EXPECTED_DIR / EXPECTED_FILENAMES[0], lp)
            ret = pfx.upload(paths)
            assert ret == local_paths
            for lp in local_paths:
                assert lp.is_file()
            assert fc.download_counter == 0
            assert fc.upload_counter == 0
            assert pfx.download_counter == 0
            assert pfx.upload_counter == 0
        for lp in local_paths:
            assert lp.is_file()
    for lp in local_paths:
        assert not lp.is_file()


def test_local_upl_multi_bad():
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir).resolve()
        with FileCache() as fc:
            local_path = fc.get_local_path(temp_dir / 'dir1/test_file.txt')
            assert local_path == (temp_dir / 'dir1/test_file.txt')
            _copy_file(EXPECTED_DIR / EXPECTED_FILENAMES[0], local_path)
            with pytest.raises(FileNotFoundError):
                fc.upload([local_path,
                           temp_dir / 'XXXXXX.XXX'])
        assert fc.download_counter == 0
        assert fc.upload_counter == 0


def test_local_upl_multi_pfx_bad():
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir).resolve()
        with FileCache() as fc:
            pfx = fc.new_prefix(temp_dir)
            local_path = fc.get_local_path(temp_dir / 'dir1/test_file.txt')
            assert local_path == (temp_dir / 'dir1/test_file.txt')
            _copy_file(EXPECTED_DIR / EXPECTED_FILENAMES[0], local_path)
            with pytest.raises(FileNotFoundError):
                pfx.upload(['dir1/test_file.txt',
                            'XXXXXX.XXX'])
        assert fc.download_counter == 0
        assert fc.upload_counter == 0
        assert pfx.download_counter == 0
        assert pfx.upload_counter == 0


@pytest.mark.parametrize('prefix', WRITABLE_CLOUD_PREFIXES)
def test_complex_upl_multi_1(prefix):
    new_prefix = f'{prefix}/{uuid.uuid4()}'
    full_paths = []
    with FileCache() as fc:
        local_paths = []
        for i, filename in enumerate(EXPECTED_FILENAMES):
            full_path = f"{new_prefix}/test_file{i}.{filename.rsplit('.')[1]}"
            full_paths.append(full_path)
            local_path = fc.get_local_path(full_path)
            local_paths.append(local_path)
            _copy_file(EXPECTED_DIR / filename, local_path)
        ret = fc.upload(full_paths, anonymous=True)
        assert ret == local_paths
        assert fc.download_counter == 0
        assert fc.upload_counter == len(EXPECTED_FILENAMES)
    with FileCache() as fc:
        local_paths = fc.retrieve(full_paths, anonymous=True)
        assert len(local_paths) == len(EXPECTED_FILENAMES)
        for local_path, filename in zip(local_paths, EXPECTED_FILENAMES):
            _compare_to_expected_path(local_path, filename)
        assert fc.download_counter == len(EXPECTED_FILENAMES)
        assert fc.upload_counter == 0
    assert not fc.cache_dir.exists()


@pytest.mark.parametrize('prefix', WRITABLE_CLOUD_PREFIXES)
def test_complex_upl_multi_pfx_1(prefix):
    new_prefix = f'{prefix}/{uuid.uuid4()}'
    sub_paths = []
    with FileCache() as fc:
        pfx = fc.new_prefix(new_prefix, anonymous=True)
        local_paths = []
        for i, filename in enumerate(EXPECTED_FILENAMES):
            sub_path = f"test_file{i}.{filename.rsplit('.')[1]}"
            sub_paths.append(sub_path)
            local_path = pfx.get_local_path(sub_path)
            local_paths.append(local_path)
            _copy_file(EXPECTED_DIR / filename, local_path)
        ret = pfx.upload(sub_paths)
        assert ret == local_paths
        assert fc.download_counter == 0
        assert fc.upload_counter == len(EXPECTED_FILENAMES)
        assert pfx.download_counter == 0
        assert pfx.upload_counter == len(EXPECTED_FILENAMES)
    assert not fc.cache_dir.exists()
    with FileCache() as fc:
        pfx = fc.new_prefix(new_prefix, anonymous=True)
        local_paths = pfx.retrieve(sub_paths)
        assert len(local_paths) == len(EXPECTED_FILENAMES)
        for local_path, filename in zip(local_paths, EXPECTED_FILENAMES):
            _compare_to_expected_path(local_path, filename)
        assert fc.download_counter == len(EXPECTED_FILENAMES)
        assert fc.upload_counter == 0
        assert fc.download_counter == len(EXPECTED_FILENAMES)
        assert fc.upload_counter == 0
    assert not fc.cache_dir.exists()


@pytest.mark.parametrize('prefix', WRITABLE_CLOUD_PREFIXES)
def test_cloud_upl_multi_good(prefix):
    with FileCache(all_anonymous=True) as fc:
        new_prefix = f'{prefix}/{uuid.uuid4()}'
        paths = [f'{new_prefix}/dir1/test_file{x}' for x in range(10)]
        local_paths = [fc.get_local_path(x) for x in paths]
        for lp in local_paths:
            _copy_file(EXPECTED_DIR / EXPECTED_FILENAMES[0], lp)
        ret = fc.upload(paths)
        assert ret == local_paths
        assert fc.download_counter == 0
        assert fc.upload_counter == len(paths)
    assert not fc.cache_dir.exists()


@pytest.mark.parametrize('prefix', WRITABLE_CLOUD_PREFIXES)
def test_cloud_upl_multi_pfx_good(prefix):
    with FileCache() as fc:
        new_prefix = f'{prefix}/{uuid.uuid4()}'
        pfx = fc.new_prefix(new_prefix, anonymous=True)
        paths = [f'dir1/test_file{x}' for x in range(10)]
        local_paths = [pfx.get_local_path(x) for x in paths]
        for lp in local_paths:
            _copy_file(EXPECTED_DIR / EXPECTED_FILENAMES[0], lp)
        ret = pfx.upload(paths)
        assert ret == local_paths
        assert fc.download_counter == 0
        assert fc.upload_counter == len(paths)
        assert pfx.download_counter == 0
        assert pfx.upload_counter == len(paths)
    assert not fc.cache_dir.exists()


@pytest.mark.parametrize('prefix', WRITABLE_CLOUD_PREFIXES)
def test_cloud_upl_multi_bad_1(prefix):
    with FileCache() as fc:
        new_prefix = f'{prefix}/{uuid.uuid4()}'
        paths = [f'{new_prefix}/dir1/test_file{x}' for x in range(10)]
        local_paths = [fc.get_local_path(x) for x in paths]
        for lp in local_paths[1:]:
            _copy_file(EXPECTED_DIR / EXPECTED_FILENAMES[0], lp)
        with pytest.raises(FileNotFoundError):
            fc.upload(paths, anonymous=True)
        # However all of the other files should have been uploaded
        assert not fc.exists(paths[0], anonymous=True)
        for path in paths[1:]:
            assert fc.exists(path, anonymous=True)
        assert fc.download_counter == 0
        assert fc.upload_counter == len(paths) - 1
    assert not fc.cache_dir.exists()


@pytest.mark.parametrize('prefix', WRITABLE_CLOUD_PREFIXES)
def test_cloud_upl_multi_pfx_bad_1(prefix):
    with FileCache() as fc:
        new_prefix = f'{prefix}/{uuid.uuid4()}'
        pfx = fc.new_prefix(new_prefix, anonymous=True)
        paths = [f'dir1/test_file{x}' for x in range(10)]
        local_paths = [pfx.get_local_path(x) for x in paths]
        for lp in local_paths[1:]:
            _copy_file(EXPECTED_DIR / EXPECTED_FILENAMES[0], lp)
        with pytest.raises(FileNotFoundError):
            pfx.upload(paths)
        # However all of the other files should have been uploaded
        assert not pfx.exists(paths[0])
        for path in paths[1:]:
            assert pfx.exists(path)
        assert fc.download_counter == 0
        assert fc.upload_counter == len(paths) - 1
        assert pfx.download_counter == 0
        assert pfx.upload_counter == len(paths) - 1
    assert not fc.cache_dir.exists()


@pytest.mark.parametrize('prefix', WRITABLE_CLOUD_PREFIXES)
def test_cloud_upl_multi_bad_2(prefix):
    with FileCache() as fc:
        new_prefix = f'{prefix}/{uuid.uuid4()}'
        paths = [f'{new_prefix}/dir1/test_file{x}' for x in range(10)]
        local_paths = [fc.get_local_path(x) for x in paths]
        for lp in local_paths[1:]:
            _copy_file(EXPECTED_DIR / EXPECTED_FILENAMES[0], lp)
        ret = fc.upload(paths, anonymous=True, exception_on_fail=False)
        assert isinstance(ret[0], FileNotFoundError)
        for r, lp in zip(ret[1:], local_paths[1:]):
            assert isinstance(r, Path)
            assert r == lp
        assert not fc.exists(paths[0], anonymous=True)
        for path in paths[1:]:
            assert fc.exists(path, anonymous=True)
        assert fc.download_counter == 0
        assert fc.upload_counter == len(paths) - 1
    assert not fc.cache_dir.exists()


@pytest.mark.parametrize('prefix', WRITABLE_CLOUD_PREFIXES)
def test_cloud_upl_multi_pfx_bad_2(prefix):
    with FileCache() as fc:
        new_prefix = f'{prefix}/{uuid.uuid4()}'
        pfx = fc.new_prefix(new_prefix, anonymous=True)
        paths = [f'dir1/test_file{x}' for x in range(10)]
        local_paths = [pfx.get_local_path(x) for x in paths]
        for lp in local_paths[1:]:
            _copy_file(EXPECTED_DIR / EXPECTED_FILENAMES[0], lp)
        ret = pfx.upload(paths, exception_on_fail=False)
        assert isinstance(ret[0], FileNotFoundError)
        for r, lp in zip(ret[1:], local_paths[1:]):
            assert isinstance(r, Path)
            assert r == lp
        assert not pfx.exists(paths[0])
        for path in paths[1:]:
            assert pfx.exists(path)
        assert fc.download_counter == 0
        assert fc.upload_counter == len(paths) - 1
        assert pfx.download_counter == 0
        assert pfx.upload_counter == len(paths) - 1
    assert not fc.cache_dir.exists()


@pytest.mark.parametrize('prefix', BAD_WRITABLE_CLOUD_PREFIXES)
def test_cloud_upl_multi_bad_3(prefix):
    with FileCache() as fc:
        new_prefix = f'{prefix}/{uuid.uuid4()}'
        paths = [f'{new_prefix}/dir1/test_file{x}' for x in range(10)]
        local_paths = [fc.get_local_path(x) for x in paths]
        for lp in local_paths[1:]:
            _copy_file(EXPECTED_DIR / EXPECTED_FILENAMES[0], lp)
        ret = fc.upload(paths, anonymous=True, exception_on_fail=False)
        assert isinstance(ret[0], FileNotFoundError)
        for r, lp in zip(ret[1:], local_paths[1:]):
            assert isinstance(r, Exception)
            assert not isinstance(r, FileNotFoundError)
        assert not fc.exists(paths[0], anonymous=True)
        for path in paths[1:]:
            assert fc.exists(path, anonymous=True)
        assert fc.download_counter == 0
        assert fc.upload_counter == 0
    assert not fc.cache_dir.exists()


@pytest.mark.parametrize('prefix', BAD_WRITABLE_CLOUD_PREFIXES)
def test_cloud_upl_multi_pfx_bad_3(prefix):
    with FileCache() as fc:
        new_prefix = f'{prefix}/{uuid.uuid4()}'
        pfx = fc.new_prefix(new_prefix, anonymous=True)
        paths = [f'dir1/test_file{x}' for x in range(10)]
        local_paths = [pfx.get_local_path(x) for x in paths]
        for lp in local_paths[1:]:
            _copy_file(EXPECTED_DIR / EXPECTED_FILENAMES[0], lp)
        ret = pfx.upload(paths, exception_on_fail=False)
        assert isinstance(ret[0], FileNotFoundError)
        for r, lp in zip(ret[1:], local_paths[1:]):
            assert isinstance(r, Exception)
            assert not isinstance(r, FileNotFoundError)
        assert not pfx.exists(paths[0])
        for path in paths[1:]:
            assert pfx.exists(path)
        assert fc.download_counter == 0
        assert fc.upload_counter == 0
        assert pfx.download_counter == 0
        assert pfx.upload_counter == 0
    assert not fc.cache_dir.exists()


def test_source_bad():
    with pytest.raises(ValueError):
        FileCacheSourceLocal('hi')
    with pytest.raises(ValueError):
        FileCacheSourceHTTP('fred://hi')
    with pytest.raises(ValueError):
        FileCacheSourceHTTP('http://hi/hi')
    with pytest.raises(ValueError):
        FileCacheSourceHTTP('http://')
    with pytest.raises(ValueError):
        FileCacheSourceGS('fred://hi')
    with pytest.raises(ValueError):
        FileCacheSourceGS('gs://hi/hi')
    with pytest.raises(ValueError):
        FileCacheSourceGS('gs://')
    with pytest.raises(ValueError):
        FileCacheSourceS3('fred://hi')
    with pytest.raises(ValueError):
        FileCacheSourceS3('s3://hi/hi')
    with pytest.raises(ValueError):
        FileCacheSourceS3('s3://')


def test_localsource_bad():
    sl = FileCacheSourceLocal()
    with pytest.raises(ValueError):
        sl.retrieve('hi', 'bye')
    with pytest.raises(ValueError):
        sl.upload('hi', 'bye')
    with pytest.raises(FileNotFoundError):
        sl.upload('non-existent.txt', 'non-existent.txt')


def test_source_notimp():
    with pytest.raises(NotImplementedError):
        FileCacheSource().exists('')
    with pytest.raises(NotImplementedError):
        FileCacheSource().retrieve('', '')
    with pytest.raises(NotImplementedError):
        FileCacheSource().upload('', '')
    with pytest.raises(NotImplementedError):
        FileCacheSourceHTTP('https://x').upload('', '')


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

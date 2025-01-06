# Test trying to multi-download the same file multiple times in the same call

################################################################################
# tests/test_file_cache.py
################################################################################

import atexit
import os
from pathlib import Path
import platform
import tempfile
import uuid

import pytest

import filecache
from filecache import FileCache, FCPath

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

INDEXABLE_PREFIXES = (EXPECTED_DIR, GS_TEST_BUCKET_ROOT, S3_TEST_BUCKET_ROOT)

ALL_PREFIXES = (EXPECTED_DIR, GS_TEST_BUCKET_ROOT, S3_TEST_BUCKET_ROOT,
                HTTP_TEST_ROOT)

if platform.system() == 'Windows':
    WINDOWS_PREFIX = 'c:'
    WINDOWS_SLASH = '/'
else:
    WINDOWS_PREFIX = ''
    WINDOWS_SLASH = ''


# This has to be first to clean up any global directory from a previous failed run
@pytest.mark.order(0)
def test_cleanup_global_dir():
    with FileCache(delete_on_exit=True):
        pass
    if 'FILECACHE_CACHE_ROOT' in os.environ:
        del os.environ['FILECACHE_CACHE_ROOT']


def _compare_to_expected_path(cache_path, filename):
    local_path = EXPECTED_DIR / filename
    mode = 'r'
    if filename.endswith('.bin'):
        mode = 'rb'
    with open(str(cache_path), mode) as fp:
        cache_data = fp.read()
    with open(str(local_path), mode) as fp:
        local_data = fp.read()
    assert cache_data == local_data


def _compare_to_expected_data(cache_data, filename):
    local_path = EXPECTED_DIR / filename
    mode = 'r'
    if filename.endswith('.bin'):
        mode = 'rb'
    with open(str(local_path), mode) as fp:
        local_data = fp.read()
    assert cache_data == local_data


def _copy_file(src_file, dest_file):
    with open(str(src_file), 'rb') as fp:
        data = fp.read()
    with open(str(dest_file), 'wb') as fp:
        fp.write(data)


class MyLogger:
    def __init__(self):
        self.messages = []

    def debug(self, msg, *args, **kwargs):
        self.messages.append(msg)

    def info(self, msg, *args, **kwargs):
        self.messages.append(f'INFO {msg}')

    def error(self, msg, *args, **kwargs):
        self.messages.append(f'ERROR {msg}')

    def has_prefix_list(self, prefixes):
        print('----------')
        print('\n'.join(self.messages))
        print(prefixes)
        for msg, prefix in zip(self.messages, prefixes):
            assert msg.strip(' ').startswith(prefix), (msg, prefix)


def test_logger():
    assert filecache.get_global_logger() is None
    # Global logger
    logger = MyLogger()
    filecache.set_global_logger(logger)
    assert filecache.get_global_logger() is logger
    with FileCache(cache_name=None) as fc:
        pfx = fc.new_path(HTTP_TEST_ROOT)
        pfx.exists(EXPECTED_FILENAMES[0])
        pfx.exists('bad-filename')
        pfx.retrieve(EXPECTED_FILENAMES[0])
# Creating cache /tmp/.file_cache_424b280b-e62c-4582-8560-211f54cabc23
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
    logger.has_prefix_list(['INFO Creating', 'Checking', 'File exists', 'Checking',
                            'File does not', 'Downloading', 'Deleting', 'Removing',
                            'Removing', 'Removing', 'Removing'])

    logger = MyLogger()
    filecache.set_global_logger(logger)
    with FileCache() as fc:
        pfx = fc.new_path(HTTP_TEST_ROOT)
        pfx.retrieve(EXPECTED_FILENAMES[0])
        fc.delete_cache()
# Creating cache_name cache /tmp/.file_cache_global
# Downloading https://storage.googleapis.com/rms-node-filecache-test-bucket/lorem1.txt to /tmp/.file_cache_global/http_storage.googleapis.com/rms-node-filecache-test-bucket/lorem1.txt
# Cleaning up cache /tmp/.file_cache_global
#   Removing lorem1.txt
#   Removing rms-node-filecache-test-bucket
#   Removing http_storage.googleapis.com
#   Removing /tmp/.file_cache_global
# Cleaning up cache /tmp/.file_cache_global
    logger.has_prefix_list(['INFO Creating', 'Downloading', 'Deleting', 'Removing',
                            'Removing', 'Removing', 'Removing', 'Deleting'])
    logger.messages = []

    # Remove global logger
    filecache.set_global_logger(False)
    with FileCache(cache_name=None) as fc:
        pfx = fc.new_path(HTTP_TEST_ROOT)
        pfx.retrieve(EXPECTED_FILENAMES[0])
    assert len(logger.messages) == 0

    # Specified logger
    logger = MyLogger()
    with FileCache(cache_name=None, logger=logger) as fc:
        pfx = fc.new_path(HTTP_TEST_ROOT)
        pfx.retrieve(EXPECTED_FILENAMES[0])
        pfx.retrieve(EXPECTED_FILENAMES[0])
# Creating cache /tmp/.file_cache_28d43982-dd49-493e-905d-9bcebd813613
# Downloading https://storage.googleapis.com/rms-node-filecache-test-bucket//lorem1.txt to /tmp/.file_cache_28d43982-dd49-493e-905d-9bcebd813613/http_storage.googleapis.com/rms-node-filecache-test-bucket/lorem1.txt
# Accessing existing https://storage.googleapis.com/rms-node-filecache-test-bucket//lorem1.txt at /tmp/.file_cache_28d43982-dd49-493e-905d-9bcebd813613/http_storage.googleapis.com/rms-node-filecache-test-bucket/lorem1.txt
# Cleaning up cache /tmp/.file_cache_28d43982-dd49-493e-905d-9bcebd813613
#   Removing /tmp/.file_cache_28d43982-dd49-493e-905d-9bcebd813613/http_storage.googleapis.com/rms-node-filecache-test-bucket/lorem1.txt
#   Removing /tmp/.file_cache_28d43982-dd49-493e-905d-9bcebd813613/http_storage.googleapis.com/rms-node-filecache-test-bucket
#   Removing /tmp/.file_cache_28d43982-dd49-493e-905d-9bcebd813613/http_storage.googleapis.com
#   Removing /tmp/.file_cache_28d43982-dd49-493e-905d-9bcebd813613
    logger.has_prefix_list(['INFO Creating', 'Downloading', 'Accessing', 'Deleting',
                            'Removing', 'Removing', 'Removing', 'Removing'])

    # Specified logger
    logger = MyLogger()
    with FileCache(cache_name=None, logger=logger) as fc:
        pfx = fc.new_path(EXPECTED_DIR)
        pfx.retrieve(EXPECTED_FILENAMES[0])
# Creating cache /tmp/.file_cache_63a1488e-6e9b-4fea-bb0c-3aaae655ec68
# Accessing local file lorem1.txt
# Cleaning up cache /tmp/.file_cache_63a1488e-6e9b-4fea-bb0c-3aaae655ec68
#   Removing /tmp/.file_cache_63a1488e-6e9b-4fea-bb0c-3aaae655ec68
    logger.has_prefix_list(['INFO Creating', 'Retrieving', 'Deleting', 'Removing'])

    # Uploading
    logger = MyLogger()
    with FileCache(cache_name=None, logger=logger) as fc:
        new_path = f'{GS_WRITABLE_TEST_BUCKET_ROOT}/{uuid.uuid4()}'
        pfx = fc.new_path(new_path, anonymous=True)
        local_path = pfx.get_local_path('test_file.txt')
        _copy_file(EXPECTED_DIR / EXPECTED_FILENAMES[0], local_path)
        pfx.upload('test_file.txt')
# Creating cache /tmp/.file_cache_e866e868-7d79-4a54-9b52-dc4df2fda819
# Returning local path for test_file.txt as /tmp/.file_cache_e866e868-7d79-4a54-9b52-dc4df2fda819/gs_rms-filecache-tests-writable/5bfef362-2ce1-49f9-beb9-4e96a8b747e6/test_file.txt
# Uploading /tmp/.file_cache_e866e868-7d79-4a54-9b52-dc4df2fda819/gs_rms-filecache-tests-writable/5bfef362-2ce1-49f9-beb9-4e96a8b747e6/test_file.txt to gs://rms-filecache-tests-writable/5bfef362-2ce1-49f9-beb9-4e96a8b747e6/test_file.txt
# Cleaning up cache /tmp/.file_cache_e866e868-7d79-4a54-9b52-dc4df2fda819
    logger.has_prefix_list(['INFO Creating', 'Returning', 'Uploading', 'Deleting'])


def test_easy_logger(capsys):
    filecache.set_easy_logger()
    filecache.set_easy_logger()
    filecache.set_global_logger(None)
    filecache.set_easy_logger()
    filecache.set_easy_logger()
    filecache.set_easy_logger()
    with FileCache(cache_name=None) as fc:
        fc.get_local_path(f'{WINDOWS_PREFIX}/')
    if WINDOWS_PREFIX:
        assert 'Returning local path for c:/ as c:\\\n' in capsys.readouterr().out
    else:
        assert 'Returning local path for / as /\n' in capsys.readouterr().out
    filecache.set_global_logger(None)
    with FileCache(cache_name=None) as fc:
        fc.new_path('gs://test')
    assert capsys.readouterr().out == ''


def test_cache_name_none():
    fc1 = FileCache(cache_name=None)
    fc2 = FileCache(cache_name=None)
    fc3 = FileCache(cache_name=None)
    assert str(fc1.cache_dir) != str(fc2.cache_dir)
    assert str(fc2.cache_dir) != str(fc3.cache_dir)
    assert fc1.cache_dir.name.startswith('_filecache_')
    assert fc2.cache_dir.name.startswith('_filecache_')
    assert fc3.cache_dir.name.startswith('_filecache_')
    assert fc1.delete_on_exit
    assert fc2.delete_on_exit
    assert fc3.delete_on_exit
    fc1.delete_cache()
    fc2.delete_cache()
    fc3.delete_cache()
    assert not fc1.cache_dir.exists()
    assert not fc2.cache_dir.exists()
    assert not fc3.cache_dir.exists()


def test_cache_name_global():
    fc1 = FileCache(cache_name=None)
    fc2 = FileCache()
    fc3 = FileCache()
    assert str(fc1.cache_dir) != str(fc2.cache_dir)
    assert str(fc2.cache_dir) == str(fc3.cache_dir)
    assert fc1.cache_dir.name.startswith('_filecache_')
    assert fc2.cache_dir.name == '_filecache_global'
    assert fc3.cache_dir.name == '_filecache_global'
    assert fc1.delete_on_exit
    assert not fc2.delete_on_exit
    assert not fc3.delete_on_exit
    fc1.delete_cache()
    assert not fc1.cache_dir.exists()
    assert fc2.cache_dir.exists()
    fc2.delete_cache()
    assert not fc2.cache_dir.exists()
    assert not fc3.cache_dir.exists()
    fc3.delete_cache()
    assert not fc3.cache_dir.exists()


def test_cache_name_global_ctx():
    with FileCache(cache_name=None) as fc1:
        assert fc1.cache_dir.exists()
        with FileCache() as fc2:
            assert fc2.cache_dir.exists()
            with FileCache() as fc3:
                assert fc3.cache_dir.exists()
                assert str(fc1.cache_dir) != str(fc2.cache_dir)
                assert str(fc2.cache_dir) == str(fc3.cache_dir)
                assert fc1.cache_dir.name.startswith('_filecache_')
                assert fc2.cache_dir.name == '_filecache_global'
                assert fc3.cache_dir.name == '_filecache_global'
                assert fc1.delete_on_exit
                assert not fc2.delete_on_exit
                assert not fc3.delete_on_exit
            assert fc3.cache_dir.exists()
        assert fc2.cache_dir.exists()
    assert not fc1.cache_dir.exists()
    assert fc3.cache_dir.exists()
    fc3.delete_cache()
    assert not fc2.cache_dir.exists()
    assert not fc3.cache_dir.exists()


def test_cache_name_named():
    fc1 = FileCache(cache_name=None)
    fc2 = FileCache()
    fc3 = FileCache(cache_name='test')
    fc4 = FileCache(cache_name='test')
    assert str(fc1.cache_dir) != str(fc2.cache_dir)
    assert str(fc2.cache_dir) != str(fc3.cache_dir)
    assert str(fc3.cache_dir) == str(fc4.cache_dir)
    assert fc1.cache_dir.name.startswith('_filecache_')
    assert fc2.cache_dir.name == '_filecache_global'
    assert fc3.cache_dir.name == '_filecache_test'
    assert fc4.cache_dir.name == '_filecache_test'
    assert fc1.delete_on_exit
    assert not fc2.delete_on_exit
    assert not fc3.delete_on_exit
    fc1.delete_cache()
    assert not fc1.cache_dir.exists()
    assert fc2.cache_dir.exists()
    fc2.delete_cache()
    assert not fc2.cache_dir.exists()
    assert fc3.cache_dir.exists()
    assert fc4.cache_dir.exists()
    fc3.delete_cache()
    assert not fc3.cache_dir.exists()
    assert not fc4.cache_dir.exists()


def test_cache_name_bad():
    with pytest.raises(TypeError):
        FileCache(cache_name=5)
    with pytest.raises(ValueError):
        FileCache(cache_name='a/b')
    with pytest.raises(ValueError):
        FileCache(cache_name='a\\b')
    with pytest.raises(ValueError):
        FileCache(cache_name='/a')
    with pytest.raises(ValueError):
        FileCache(cache_name='\\a')


def test_cache_root_good():
    cwd = os.getcwd()
    fc4 = FileCache(cache_root='.', cache_name=None)
    fc5 = FileCache(cache_root=cwd, cache_name=None)
    assert str(fc4.cache_dir.parent) == str(fc5.cache_dir.parent)
    assert str(fc4.cache_dir.parent) == cwd
    assert str(fc5.cache_dir.parent) == cwd
    assert fc4.cache_dir.name.startswith('_filecache_')
    assert fc5.cache_dir.name.startswith('_filecache_')
    assert fc5.delete_on_exit
    assert fc5.delete_on_exit
    fc4.delete_cache()
    fc5.delete_cache()
    assert not fc4.cache_dir.exists()
    assert not fc5.cache_dir.exists()

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir).expanduser().resolve()
        cache_root = temp_dir / str(uuid.uuid4())  # Won't already exist
        assert not cache_root.exists()
        with FileCache(cache_root=cache_root, cache_name='test',
                       delete_on_exit=True):
            assert cache_root.exists()


def test_cache_root_bad():
    with pytest.raises(ValueError):
        FileCache(cache_root='\000')
    with pytest.raises(ValueError):
        FileCache(cache_root=EXPECTED_DIR / EXPECTED_FILENAMES[0])


def test_cache_root_env():
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir).expanduser().resolve()
        cache_root = temp_dir / str(uuid.uuid4())  # Won't already exist
        cache_dir = cache_root / '_filecache_global'
        os.environ['FILECACHE_CACHE_ROOT'] = str(cache_root)
        with FileCache() as fc:
            assert fc.cache_dir == cache_dir
        del os.environ['FILECACHE_CACHE_ROOT']


def test_cache_nthreads_bad():
    with pytest.raises(ValueError):
        fc = FileCache(nthreads=-1)
    with pytest.raises(ValueError):
        fc = FileCache(nthreads=4.5)
    with FileCache() as fc:
        with pytest.raises(ValueError):
            fc.exists([str(EXPECTED_DIR / EXPECTED_FILENAMES[0])],
                      nthreads=-1)
        with pytest.raises(ValueError):
            fc.exists([str(EXPECTED_DIR / EXPECTED_FILENAMES[0])],
                      nthreads='fred')
        fc.exists([str(EXPECTED_DIR / EXPECTED_FILENAMES[0])],
                  nthreads=None)

        with pytest.raises(ValueError):
            fc.retrieve([str(EXPECTED_DIR / EXPECTED_FILENAMES[0])],
                        nthreads=-1)
        with pytest.raises(ValueError):
            fc.retrieve([str(EXPECTED_DIR / EXPECTED_FILENAMES[0])],
                        nthreads='fred')
        fc.retrieve([str(EXPECTED_DIR / EXPECTED_FILENAMES[0])],
                    nthreads=None)

        with pytest.raises(ValueError):
            fc.upload([str(EXPECTED_DIR / EXPECTED_FILENAMES[0])],
                      nthreads=-1)
        with pytest.raises(ValueError):
            fc.upload([str(EXPECTED_DIR / EXPECTED_FILENAMES[0])],
                      nthreads='fred')
        fc.upload([str(EXPECTED_DIR / EXPECTED_FILENAMES[0])],
                  nthreads=None)

        with pytest.raises(ValueError):
            fc.new_path(str(EXPECTED_DIR), nthreads=-1)
        with pytest.raises(ValueError):
            fc.new_path(str(EXPECTED_DIR), nthreads='fred')
        pfx = fc.new_path(str(EXPECTED_DIR), nthreads=None)

        with pytest.raises(ValueError):
            pfx.retrieve([EXPECTED_FILENAMES[0]], nthreads=-1)
        with pytest.raises(ValueError):
            pfx.retrieve([EXPECTED_FILENAMES[0]], nthreads='fred')
        pfx.retrieve([EXPECTED_FILENAMES[0]], nthreads=None)

        with pytest.raises(ValueError):
            pfx.upload([EXPECTED_FILENAMES[0]], nthreads=-1)
        with pytest.raises(ValueError):
            pfx.upload([EXPECTED_FILENAMES[0]], nthreads='fred')
        pfx.upload([EXPECTED_FILENAMES[0]], nthreads=None)

        with pytest.raises(ValueError):
            pfx = fc.new_path(str(EXPECTED_DIR), nthreads=-1)
        with pytest.raises(ValueError):
            pfx = fc.new_path(str(EXPECTED_DIR), nthreads='fred')

        with pytest.raises(ValueError):
            FCPath(str(EXPECTED_DIR), filecache=fc, nthreads=-1)
        with pytest.raises(ValueError):
            FCPath(str(EXPECTED_DIR), filecache=fc, nthreads='fred')


def test_prefix_bad():
    with FileCache(cache_name=None) as fc:
        with pytest.raises(TypeError):
            fc.new_path(5)
        FCPath(GS_TEST_BUCKET_ROOT, filecache=fc)
        FCPath(EXPECTED_DIR, filecache=fc)
        with pytest.raises(TypeError):
            FCPath(5, filecache=fc)

    assert not fc.cache_dir.exists()


@pytest.mark.parametrize('prefix', ALL_PREFIXES)
def test_exists_all_good(prefix):
    with FileCache(cache_name=None, anonymous=True) as fc:
        for filename in LIMITED_FILENAMES:
            assert fc.exists(f'{prefix}/{filename}')


@pytest.mark.parametrize('prefix', ALL_PREFIXES)
def test_exists_all_bad(prefix):
    with FileCache(cache_name=None, anonymous=True) as fc:
        assert not fc.exists(f'{prefix}/nonexistent.txt')
        assert not fc.exists(f'{prefix}/a/b/c.txt')
        assert not fc.exists(f'{prefix}-bad/{EXPECTED_FILENAMES[0]}')


@pytest.mark.parametrize('cache_name', (None, 'test'))
def test_local_retr_good(cache_name):
    for pass_no in range(5):  # Make sure the expected dir doesn't get modified
        with FileCache(cache_name=cache_name) as fc:
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
            fc.delete_cache()
            assert not fc.cache_dir.exists()


@pytest.mark.parametrize('cache_name', (None, 'test'))
def test_local_retr_pfx_good(cache_name):
    for pass_no in range(5):  # Make sure the expected dir doesn't get modified
        with FileCache(cache_name=cache_name) as fc:
            lf = fc.new_path(EXPECTED_DIR)
            assert lf.is_local()
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
            fc.delete_cache()
            assert not fc.cache_dir.exists()


@pytest.mark.parametrize('cache_name', (None, 'test'))
@pytest.mark.parametrize('prefix', CLOUD_PREFIXES)
def test_cloud_retr_good(cache_name, prefix):
    with FileCache(cache_name=cache_name, anonymous=True) as fc:
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
        fc.delete_cache()
    assert not fc.cache_dir.exists()


@pytest.mark.parametrize('cache_name', (None, 'test'))
@pytest.mark.parametrize('prefix', CLOUD_PREFIXES)
def test_cloud_retr_pfx_good(cache_name, prefix):
    with FileCache(cache_name=cache_name) as fc:
        pfx = fc.new_path(prefix, anonymous=True)
        assert not pfx.is_local()
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
        fc.delete_cache()
    assert not fc.cache_dir.exists()


@pytest.mark.parametrize('cache_name', (None, 'test'))
@pytest.mark.parametrize('prefix', CLOUD_PREFIXES)
def test_cloud_retr_multi_good(cache_name, prefix):
    with FileCache(cache_name=cache_name, anonymous=True) as fc:
        for filename in LIMITED_FILENAMES:
            paths = fc.retrieve([f'{prefix}/{filename}'])
            assert len(paths) == 1
            path = paths[0]
            assert str(path).replace('\\', '/').endswith(filename)
            _compare_to_expected_path(path, filename)
            # Retrieving the same thing a second time should do nothing
            paths = fc.retrieve((f'{prefix}/{filename}',), nthreads=5, lock_timeout=5)
            assert len(paths) == 1
            path = paths[0]
            assert str(path).replace('\\', '/').endswith(filename)
            _compare_to_expected_path(path, filename)
        assert fc.upload_counter == 0
        assert fc.download_counter == len(LIMITED_FILENAMES)
        fc.delete_cache()
    assert not fc.cache_dir.exists()


@pytest.mark.parametrize('cache_name', (None, 'test'))
@pytest.mark.parametrize('prefix', CLOUD_PREFIXES)
def test_cloud_retr_multi_pfx_good(cache_name, prefix):
    with FileCache(cache_name=cache_name) as fc:
        pfx = FCPath(prefix, filecache=fc, anonymous=True, nthreads=4)
        for filename in LIMITED_FILENAMES:
            paths = pfx.retrieve((filename,))
            assert len(paths) == 1
            path = paths[0]
            assert str(path).replace('\\', '/').endswith(filename)
            _compare_to_expected_path(path, filename)
            # Retrieving the same thing a second time should do nothing
            paths = pfx.retrieve([filename], nthreads=5, lock_timeout=5)
            assert len(paths) == 1
            path = paths[0]
            assert str(path).replace('\\', '/').endswith(filename)
            _compare_to_expected_path(path, filename)
        assert fc.upload_counter == 0
        assert fc.download_counter == len(LIMITED_FILENAMES)
        assert pfx.upload_counter == 0
        assert pfx.download_counter == len(LIMITED_FILENAMES)
        fc.delete_cache()
    assert not fc.cache_dir.exists()


@pytest.mark.parametrize('prefix', CLOUD_PREFIXES)
def test_cloud2_retr_pfx_good(prefix):
    with FileCache(cache_name=None) as fc:
        # With two identical prefixes, it shouldn't matter which you use
        pfx1 = fc.new_path(prefix, anonymous=True)
        pfx2 = fc.new_path(prefix, anonymous=True)
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
        assert pfx2.download_counter == 0
    assert not fc.cache_dir.exists()


@pytest.mark.parametrize('prefix', CLOUD_PREFIXES)
def test_cloud3_retr_pfx_good(prefix):
    # Multiple prefixes with different subdir prefixes
    with FileCache(cache_name=None) as fc:
        pfx1 = fc.new_path(prefix, anonymous=True)
        for filename in LIMITED_FILENAMES[1:]:
            subdirs, _, name = filename.rpartition('/')
            pfx2 = fc.new_path(f'{prefix}/{subdirs}', anonymous=True)
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
    with FileCache(cache_name=None) as fc:
        with pytest.raises(ValueError):
            fc.retrieve('nonexistent.txt')
        with pytest.raises(FileNotFoundError):
            fc.retrieve(f'{WINDOWS_PREFIX}/nonexistent.txt')
    assert not fc.cache_dir.exists()
    with FileCache(cache_name=None) as fc:
        ret = fc.retrieve(f'{WINDOWS_PREFIX}/nonexistent.txt', exception_on_fail=False)
        assert isinstance(ret, FileNotFoundError)
    assert not fc.cache_dir.exists()


def test_local_retr_pfx_bad():
    with FileCache(cache_name=None) as fc:
        lf = fc.new_path(EXPECTED_DIR)
        with pytest.raises(FileNotFoundError):
            lf.retrieve('nonexistent.txt')
    assert not fc.cache_dir.exists()


@pytest.mark.parametrize('cache_name', (None, 'global'))
@pytest.mark.parametrize('prefix', BAD_CLOUD_PREFIXES)
def test_cloud_retr_bad(cache_name, prefix):
    with FileCache(cache_name=cache_name, delete_on_exit=True) as fc:
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


@pytest.mark.parametrize('cache_name', (None, 'global'))
@pytest.mark.parametrize('prefix', CLOUD_PREFIXES)
def test_cloud2_retr_bad(cache_name, prefix):
    with FileCache(cache_name=cache_name, delete_on_exit=True) as fc:
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


@pytest.mark.parametrize('cache_name', (None, 'global'))
@pytest.mark.parametrize('prefix', BAD_CLOUD_PREFIXES)
def test_cloud_retr_pfx_bad_1(cache_name, prefix):
    with FileCache(cache_name=cache_name, delete_on_exit=True) as fc:
        pfx = fc.new_path(prefix, anonymous=True)
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


@pytest.mark.parametrize('cache_name', (None, 'global'))
@pytest.mark.parametrize('prefix', CLOUD_PREFIXES)
def test_cloud_retr_pfx_bad_2(cache_name, prefix):
    with FileCache(cache_name=cache_name, delete_on_exit=True) as fc:
        pfx = fc.new_path(prefix, anonymous=True)
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
    with FileCache(cache_name=None) as fc:
        prefixes = []
        # Different prefix should have different cache paths but all have the same
        # contents
        for prefix in CLOUD_PREFIXES:
            prefixes.append(fc.new_path(prefix, anonymous=True))
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
def test_multi_prefixes_cache_name_retr(prefix):
    with FileCache(delete_on_exit=True) as fc1:
        pfx1 = fc1.new_path(prefix, anonymous=True)
        paths1 = []
        for filename in EXPECTED_FILENAMES:
            paths1.append(pfx1.retrieve(filename))
        with FileCache() as fc2:
            pfx2 = fc2.new_path(prefix, anonymous=True)
            paths2 = []
            for filename in EXPECTED_FILENAMES:
                paths2.append(pfx2.retrieve(filename))
            for path1, path2 in zip(paths1, paths2):
                assert path1.exists()
                assert str(path1) == str(path2)
    assert not fc1.cache_dir.exists()
    assert not fc2.cache_dir.exists()


def test_exists_multi():
    with FileCache(cache_name=None, anonymous=True) as fc:
        filenames = ([EXPECTED_DIR / f for f in EXPECTED_FILENAMES] +
                     [f'{HTTP_TEST_ROOT}/{f}' for f in EXPECTED_FILENAMES] +
                     [f'{GS_TEST_BUCKET_ROOT}/{f}' for f in EXPECTED_FILENAMES] +
                     [f'{WINDOWS_PREFIX}/non-existent.txt'] +
                     [EXPECTED_DIR / f for f in EXPECTED_FILENAMES] +
                     [f'{BAD_HTTP_TEST_ROOT}/{EXPECTED_FILENAMES[0]}'])
        expected = (([True] * len(EXPECTED_FILENAMES) * 3) +
                    [False] +
                    ([True] * len(EXPECTED_FILENAMES)) +
                    [False])
        assert fc.exists(filenames, nthreads=4) == expected
    assert not fc.cache_dir.exists()


def test_exists_multi_pfx():
    with FileCache(cache_name=None, anonymous=True) as fc:
        pfx = fc.new_path(HTTP_TEST_ROOT)
        filenames = list(EXPECTED_FILENAMES) + ['non-existent.txt']
        expected = ([True] * len(EXPECTED_FILENAMES)) + [False]
        assert pfx.exists(filenames, nthreads=4) == expected
    assert not fc.cache_dir.exists()


def test_local_path_multi():
    with FileCache(cache_name=None) as fc:
        filenames = [f'{HTTP_TEST_ROOT}/{f}' for f in EXPECTED_FILENAMES]
        http_root = HTTP_TEST_ROOT.replace('https://', 'http_')
        expected = [fc.cache_dir / f'{http_root}/{f}' for f in EXPECTED_FILENAMES]
        local_paths = fc.get_local_path(filenames)
        assert local_paths == expected


def test_local_path_multi_pfx():
    with FileCache(cache_name=None) as fc:
        pfx = fc.new_path(HTTP_TEST_ROOT)
        http_root = HTTP_TEST_ROOT.replace('https://', 'http_')
        expected = [fc.cache_dir / f'{http_root}/{f}' for f in EXPECTED_FILENAMES]
        local_paths = pfx.get_local_path(EXPECTED_FILENAMES)
        assert local_paths == expected


def test_locking_1():
    with FileCache(delete_on_exit=True) as fc:
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
    with FileCache(delete_on_exit=True) as fc:
        pfx = fc.new_path(HTTP_TEST_ROOT, lock_timeout=0)
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
    with FileCache(delete_on_exit=True) as fc:
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
    with FileCache(delete_on_exit=True) as fc:
        pfx = fc.new_path(HTTP_TEST_ROOT, lock_timeout=0)
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
    with FileCache(cache_name=None) as fc:
        filename = (HTTP_TEST_ROOT.replace('https://', 'http_') + '/' +
                    EXPECTED_FILENAMES[0])
        local_path = fc.cache_dir / filename
        lock_path = fc._lock_path(local_path)
        lock = filelock.FileLock(lock_path, timeout=0)
        lock.acquire()
        fc.retrieve(f'{HTTP_TEST_ROOT}/{EXPECTED_FILENAMES[0]}',
                    lock_timeout=0)
        lock.release()
        lock_path.unlink(missing_ok=True)
        assert fc.upload_counter == 0
        assert fc.download_counter == 1
    assert not fc.cache_dir.exists()


def test_locking_pfx_3():
    with FileCache(cache_name=None) as fc:
        pfx = fc.new_path(HTTP_TEST_ROOT, lock_timeout=0)
        filename = (HTTP_TEST_ROOT.replace('https://', 'http_') + '/' +
                    EXPECTED_FILENAMES[0])
        local_path = fc.cache_dir / filename
        lock_path = fc._lock_path(local_path)
        lock = filelock.FileLock(lock_path, timeout=0)
        lock.acquire()
        pfx.retrieve(EXPECTED_FILENAMES[0])
        lock.release()
        lock_path.unlink(missing_ok=True)
        assert fc.upload_counter == 0
        assert fc.download_counter == 1
        assert pfx.upload_counter == 0
        assert pfx.download_counter == 1
    assert not fc.cache_dir.exists()


def test_locking_multi_1():
    with FileCache(delete_on_exit=True) as fc:
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
    with FileCache(delete_on_exit=True) as fc:
        pfx = fc.new_path(HTTP_TEST_ROOT, lock_timeout=0)
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
    with FileCache(delete_on_exit=True) as fc:
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
    with FileCache(delete_on_exit=True) as fc:
        pfx = fc.new_path(HTTP_TEST_ROOT, lock_timeout=0)
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
    with FileCache(cache_name=None, logger=logger) as fc:
        local_path = fc.get_local_path('gs://test/test.txt')
        with open(local_path.parent / f'{fc._LOCK_PREFIX}{local_path.name}', 'w') as fp:
            fp.write('A')
    logger.has_prefix_list(['INFO Creating', 'Returning', 'Deleting', 'ERROR Deleting',
                            'Removing', 'Removing', 'Removing'])
    with FileCache(cache_name=None) as fc:
        local_path = fc.get_local_path('gs://test/test.txt')
        with open(local_path.parent / f'{fc._LOCK_PREFIX}{local_path.name}', 'w') as fp:
            fp.write('A')


def test_bad_cache_dir():
    with pytest.raises(ValueError):
        with FileCache(cache_name=None) as fc:
            orig_cache_dir = fc._cache_dir
            fc._cache_dir = '/bogus/path/not/a/filecache'
    fc._cache_dir = orig_cache_dir
    fc.delete_cache()
    assert not fc.cache_dir.exists()


def test_double_delete():
    with FileCache(cache_name=None) as fc:
        pfx = fc.new_path(HTTP_TEST_ROOT)
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

    with FileCache(cache_name=None) as fc:
        pfx = fc.new_path(HTTP_TEST_ROOT)
        for filename in LIMITED_FILENAMES:
            pfx.retrieve(filename)
        assert fc.upload_counter == 0
        assert fc.download_counter == len(LIMITED_FILENAMES)
        assert pfx.upload_counter == 0
        assert pfx.download_counter == len(LIMITED_FILENAMES)
        fc.delete_cache()  # Test double delete
        assert not fc.cache_dir.exists()
        fc.delete_cache()
        assert not fc.cache_dir.exists()
        for filename in LIMITED_FILENAMES:
            pfx.retrieve(filename)
        assert fc.upload_counter == 0
        assert fc.download_counter == len(LIMITED_FILENAMES)*2
        assert pfx.upload_counter == 0
        assert pfx.download_counter == len(LIMITED_FILENAMES)*2
        assert fc.cache_dir.exists()
        fc.delete_cache()
        assert not fc.cache_dir.exists()
        fc.delete_cache()
        assert not fc.cache_dir.exists()

    with FileCache() as fc:
        pfx = fc.new_path(HTTP_TEST_ROOT)
        for filename in LIMITED_FILENAMES:
            pfx.retrieve(filename)
        assert fc.upload_counter == 0
        assert fc.download_counter == len(LIMITED_FILENAMES)
        assert pfx.upload_counter == 0
        assert pfx.download_counter == len(LIMITED_FILENAMES)
        fc.delete_cache()  # Test double clean_up
        assert not fc.cache_dir.exists()
        fc.delete_cache()
        assert not fc.cache_dir.exists()
        for filename in LIMITED_FILENAMES:
            pfx.retrieve(filename)
        assert fc.upload_counter == 0
        assert fc.download_counter == len(LIMITED_FILENAMES)*2
        assert pfx.upload_counter == 0
        assert pfx.download_counter == len(LIMITED_FILENAMES)*2
        assert fc.cache_dir.exists()
        fc.delete_cache()
        assert not fc.cache_dir.exists()
        fc.delete_cache()
        assert not fc.cache_dir.exists()
        fc.delete_cache()
        assert not fc.cache_dir.exists()


def test_open_context_read():
    with FileCache(cache_name=None) as fc:
        pfx = fc.new_path(HTTP_TEST_ROOT)
        with pfx.open(LIMITED_FILENAMES[0], 'r') as fp:
            cache_data = fp.read()
        assert fc.upload_counter == 0
        assert fc.download_counter == 1
        assert pfx.upload_counter == 0
        assert pfx.download_counter == 1
        _compare_to_expected_data(cache_data, LIMITED_FILENAMES[0])
    assert not fc.cache_dir.exists()


def test_delete_on_exit():
    with FileCache(delete_on_exit=True) as fc1:
        with FileCache() as fc2:
            assert fc1.cache_dir == fc2.cache_dir
        assert os.path.exists(fc1.cache_dir)
    assert not os.path.exists(fc1.cache_dir)
    assert not os.path.exists(fc2.cache_dir)

    with FileCache(None, delete_on_exit=True) as fc1:
        with FileCache(None) as fc2:
            assert fc1.cache_dir != fc2.cache_dir
        assert os.path.exists(fc1.cache_dir)
        assert not os.path.exists(fc2.cache_dir)
    assert not os.path.exists(fc1.cache_dir)
    assert not os.path.exists(fc2.cache_dir)


def test_local_upl_good():
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir).expanduser().resolve()
        with FileCache(cache_name=None) as fc:
            local_path = fc.get_local_path(str(temp_dir / 'dir1/test_file.txt'),
                                           create_parents=False)
            assert not local_path.parent.is_dir()
            local_path = fc.get_local_path(str(temp_dir / 'dir1/test_file.txt'))
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
        with FileCache(cache_name=None) as fc:
            pfx = fc.new_path(temp_dir)
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
        with FileCache(cache_name=None) as fc:
            with fc.open(str(temp_dir / 'dir1/test_file.txt'), 'wb') as fp2:
                with open(str(EXPECTED_DIR / EXPECTED_FILENAMES[0]), 'rb') as fp1:
                    fp2.write(fp1.read())
            assert os.path.exists(temp_dir / 'dir1/test_file.txt')
        assert os.path.exists(temp_dir / 'dir1/test_file.txt')
    assert not os.path.exists(temp_dir / 'dir1/test_file.txt')


def test_local_upl_pfx_ctx():
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir)
        with FileCache(cache_name=None) as fc:
            pfx = fc.new_path(temp_dir)
            with pfx.open('dir1/test_file.txt', 'wb') as fp2:
                with open(EXPECTED_DIR / EXPECTED_FILENAMES[0], 'rb') as fp1:
                    fp2.write(fp1.read())
            assert os.path.exists(temp_dir / 'dir1/test_file.txt')
        assert os.path.exists(temp_dir / 'dir1/test_file.txt')
    assert not os.path.exists(temp_dir / 'dir1/test_file.txt')


def test_local_upl_bad():
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir)
        with FileCache(cache_name=None) as fc:
            with pytest.raises(FileNotFoundError):
                fc.upload(temp_dir / 'XXXXXX.XXX')
        with FileCache(cache_name=None) as fc:
            ret = fc.upload(temp_dir / 'XXXXXX.XXX', exception_on_fail=False)
            assert isinstance(ret, FileNotFoundError)


def test_local_upl_pfx_bad():
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir)
        with FileCache(cache_name=None) as fc:
            pfx = fc.new_path(temp_dir)
            with pytest.raises(FileNotFoundError):
                pfx.upload('XXXXXX.XXX')
        with FileCache(cache_name=None) as fc:
            pfx = fc.new_path(temp_dir)
            ret = pfx.upload('XXXXXX.XXX', exception_on_fail=False)
            assert isinstance(ret, FileNotFoundError)


@pytest.mark.parametrize('prefix', WRITABLE_CLOUD_PREFIXES)
def test_cloud_upl_good(prefix):
    with FileCache(cache_name=None, anonymous=True) as fc:
        new_path = f'{prefix}/{uuid.uuid4()}'
        local_path = fc.get_local_path(f'{new_path}/test_file.txt')
        _copy_file(EXPECTED_DIR / EXPECTED_FILENAMES[0], local_path)
        assert fc.upload(f'{new_path}/test_file.txt') == local_path
        assert fc.download_counter == 0
        assert fc.upload_counter == 1
    assert not fc.cache_dir.exists()


@pytest.mark.parametrize('prefix', WRITABLE_CLOUD_PREFIXES)
def test_cloud_upl_pfx_good(prefix):
    with FileCache(cache_name=None) as fc:
        new_path = f'{prefix}/{uuid.uuid4()}'
        pfx = fc.new_path(new_path, anonymous=True)
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
    with FileCache(cache_name=None) as fc:
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
    with FileCache(cache_name=None) as fc:
        new_path = f'{prefix}/{uuid.uuid4()}'
        pfx = fc.new_path(new_path, anonymous=True)
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
    with FileCache(cache_name=None) as fc:
        new_path = f'{prefix}/{uuid.uuid4()}'
        filename = 'test_file.txt'
        local_path = fc.get_local_path(f'{new_path}/{filename}')
        _copy_file(EXPECTED_DIR / EXPECTED_FILENAMES[0], local_path)
        with pytest.raises(Exception) as e:
            fc.upload(f'{new_path}/{filename}', anonymous=True)
        assert not isinstance(e.type, FileNotFoundError)
        assert fc.download_counter == 0
        assert fc.upload_counter == 0
        ret = fc.upload(f'{new_path}/{filename}', anonymous=True,
                        exception_on_fail=False)
        assert isinstance(ret, Exception)
        assert not isinstance(e, FileNotFoundError)
        assert fc.download_counter == 0
        assert fc.upload_counter == 0
    assert not fc.cache_dir.exists()


@pytest.mark.parametrize('prefix', BAD_WRITABLE_CLOUD_PREFIXES)
def test_cloud_upl_pfx_bad_2(prefix):
    with FileCache(cache_name=None) as fc:
        new_path = f'{prefix}/{uuid.uuid4()}'
        pfx = fc.new_path(new_path, anonymous=True)
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
    with FileCache(cache_name=None, anonymous=True) as fc:
        with fc.open(f'{pfx_name}/test_file.txt', 'wb') as fp:
            fp.write(b'A')
        with fc.open(f'{pfx_name}/test_file.txt', 'ab') as fp:
            fp.write(b'B')
        with fc.open(f'{pfx_name}/test_file.txt', 'rb') as fp:
            res = fp.read()
        assert res == b'AB'
        assert fc.download_counter == 0
        assert fc.upload_counter == 2
    with FileCache(cache_name=None, anonymous=True) as fc:
        with fc.open(f'{pfx_name}/test_file.txt', 'rb') as fp:
            res = fp.read()
        assert res == b'AB'
        assert fc.download_counter == 1
        assert fc.upload_counter == 0


def test_complex_read_write_pfx():
    pfx_name = f'{GS_WRITABLE_TEST_BUCKET_ROOT}/{uuid.uuid4()}'
    with FileCache(cache_name=None, anonymous=True) as fc:
        pfx = fc.new_path(pfx_name)
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
    with FileCache(cache_name=None, anonymous=True) as fc:
        pfx = fc.new_path(pfx_name)
        with pfx.open('test_file.txt', 'rb') as fp:
            res = fp.read()
        assert res == b'AB'
        assert fc.download_counter == 1
        assert fc.upload_counter == 0
        assert pfx.download_counter == 1
        assert pfx.upload_counter == 0


def test_complex_retr_multi_1():
    with FileCache(cache_name=None, anonymous=True) as fc:
        # Retrieve a couple of local files one at a time
        fc.retrieve(EXPECTED_DIR / EXPECTED_FILENAMES[0])
        fc.retrieve(EXPECTED_DIR / EXPECTED_FILENAMES[1])
        assert fc.download_counter == 0
        assert fc.upload_counter == 0
        # Now retrieve all the local files
        paths = fc.retrieve([str(EXPECTED_DIR / x) for x in EXPECTED_FILENAMES])
        assert len(paths) == len(EXPECTED_FILENAMES)
        assert fc.download_counter == 0
        assert fc.upload_counter == 0


@pytest.mark.parametrize('cache_name', (None, 'global'))
def test_complex_retr_multi_2(cache_name):
    with FileCache(cache_name=cache_name, delete_on_exit=True, anonymous=True) as fc:
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


@pytest.mark.parametrize('cache_name', (None, 'global'))
def test_complex_retr_multi_3(cache_name):
    with FileCache(cache_name=cache_name, delete_on_exit=True, anonymous=True) as fc:
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


@pytest.mark.parametrize('cache_name', (None, 'global'))
@pytest.mark.parametrize('prefix', ALL_PREFIXES)
def test_complex_retr_multi_4(cache_name, prefix):
    with FileCache(cache_name=cache_name, delete_on_exit=True, anonymous=True) as fc:
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
    with FileCache(cache_name=None, anonymous=True) as fc:
        pfx = fc.new_path(str(EXPECTED_DIR))
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


@pytest.mark.parametrize('cache_name', (None, 'global'))
def test_complex_retr_multi_pfx_2(cache_name):
    with FileCache(cache_name=cache_name, delete_on_exit=True, anonymous=True) as fc:
        # Retrieve various cloud files one at a time in random order
        pfx_gs = fc.new_path(GS_TEST_BUCKET_ROOT)
        pfx_s3 = fc.new_path(S3_TEST_BUCKET_ROOT)
        pfx_http = fc.new_path(HTTP_TEST_ROOT)
        pfx_local = fc.new_path(EXPECTED_DIR)
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


@pytest.mark.parametrize('cache_name', (None, 'global'))
@pytest.mark.parametrize('prefix', ALL_PREFIXES)
def test_complex_retr_multi_pfx_3(cache_name, prefix):
    with FileCache(cache_name=cache_name, delete_on_exit=True, anonymous=True) as fc:
        # Retrieve some cloud files with a bad name included
        pfx = fc.new_path(prefix)
        clean_prefix = str(prefix).replace('\\', '/').rstrip('/')
        assert str(pfx) == clean_prefix
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
        with FileCache(cache_name=None) as fc:
            paths = [f'dir1/test_file{x}' for x in range(10)]
            local_paths = [fc.get_local_path(str(temp_dir / x)) for x in paths]
            local_paths_str = [str(x) for x in local_paths]
            for lp in local_paths:
                _copy_file(EXPECTED_DIR / EXPECTED_FILENAMES[0], lp)
            ret = fc.upload(local_paths_str, nthreads=4)
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
        with FileCache(cache_name=None) as fc:
            pfx = fc.new_path(temp_dir, nthreads=2)
            paths = [f'dir1/test_file{x}' for x in range(10)]
            local_paths = [pfx.get_local_path(x) for x in paths]
            for lp in local_paths:
                _copy_file(EXPECTED_DIR / EXPECTED_FILENAMES[0], lp)
            ret = pfx.upload(paths, nthreads=4)
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
        with FileCache(cache_name=None) as fc:
            local_path = fc.get_local_path(str(temp_dir / 'dir1/test_file.txt'))
            assert local_path == (temp_dir / 'dir1/test_file.txt')
            _copy_file(EXPECTED_DIR / EXPECTED_FILENAMES[0], local_path)
            with pytest.raises(FileNotFoundError):
                fc.upload([str(local_path),
                           str(temp_dir / 'XXXXXX.XXX')])
        assert fc.download_counter == 0
        assert fc.upload_counter == 0


def test_local_upl_multi_pfx_bad():
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir).resolve()
        with FileCache(cache_name=None) as fc:
            pfx = fc.new_path(temp_dir)
            local_path = fc.get_local_path(str(temp_dir / 'dir1/test_file.txt'))
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
    new_path = f'{prefix}/{uuid.uuid4()}'
    full_paths = []
    with FileCache(cache_name=None) as fc:
        local_paths = []
        for i, filename in enumerate(EXPECTED_FILENAMES):
            full_path = f"{new_path}/test_file{i}.{filename.rsplit('.')[1]}"
            full_paths.append(full_path)
            local_path = fc.get_local_path(full_path)
            local_paths.append(local_path)
            _copy_file(EXPECTED_DIR / filename, local_path)
        ret = fc.upload(full_paths, anonymous=True)
        assert ret == local_paths
        assert fc.download_counter == 0
        assert fc.upload_counter == len(EXPECTED_FILENAMES)
    with FileCache(cache_name=None) as fc:
        local_paths = fc.retrieve(full_paths, anonymous=True)
        assert len(local_paths) == len(EXPECTED_FILENAMES)
        for local_path, filename in zip(local_paths, EXPECTED_FILENAMES):
            _compare_to_expected_path(local_path, filename)
        assert fc.download_counter == len(EXPECTED_FILENAMES)
        assert fc.upload_counter == 0
    assert not fc.cache_dir.exists()


@pytest.mark.parametrize('prefix', WRITABLE_CLOUD_PREFIXES)
def test_complex_upl_multi_pfx_1(prefix):
    new_path = f'{prefix}/{uuid.uuid4()}'
    sub_paths = []
    with FileCache(cache_name=None) as fc:
        pfx = fc.new_path(new_path, anonymous=True)
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
    with FileCache(cache_name=None) as fc:
        pfx = fc.new_path(new_path, anonymous=True)
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
    with FileCache(cache_name=None, anonymous=True) as fc:
        new_path = f'{prefix}/{uuid.uuid4()}'
        paths = [f'{new_path}/dir1/test_file{x}' for x in range(10)]
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
    with FileCache(cache_name=None) as fc:
        new_path = f'{prefix}/{uuid.uuid4()}'
        pfx = fc.new_path(new_path, anonymous=True)
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
    with FileCache(cache_name=None) as fc:
        new_path = f'{prefix}/{uuid.uuid4()}'
        paths = [f'{new_path}/dir1/test_file{x}' for x in range(10)]
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
    with FileCache(cache_name=None) as fc:
        new_path = f'{prefix}/{uuid.uuid4()}'
        pfx = fc.new_path(new_path, anonymous=True)
        paths = [f'dir1/test_file{x}' for x in range(10)]
        local_paths = [pfx.get_local_path(x) for x in paths]
        for lp in local_paths[1:]:
            _copy_file(EXPECTED_DIR / EXPECTED_FILENAMES[0], lp)
        with pytest.raises(FileNotFoundError):
            pfx.upload(paths)
        # However all of the other files should have been uploaded
        assert not pfx.exists(paths[0])
        assert not (pfx / paths[0]).is_file()
        for path in paths[1:]:
            assert pfx.exists(path)
            assert (pfx / path).is_file()
        assert fc.download_counter == 0
        assert fc.upload_counter == len(paths) - 1
        assert pfx.download_counter == 0
        assert pfx.upload_counter == len(paths) - 1
    assert not fc.cache_dir.exists()


@pytest.mark.parametrize('prefix', WRITABLE_CLOUD_PREFIXES)
def test_cloud_upl_multi_bad_2(prefix):
    with FileCache(cache_name=None) as fc:
        new_path = f'{prefix}/{uuid.uuid4()}'
        paths = [f'{new_path}/dir1/test_file{x}' for x in range(10)]
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
    with FileCache(cache_name=None) as fc:
        new_path = f'{prefix}/{uuid.uuid4()}'
        pfx = fc.new_path(new_path, anonymous=True)
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
    with FileCache(cache_name=None) as fc:
        new_path = f'{prefix}/{uuid.uuid4()}'
        paths = [f'{new_path}/dir1/test_file{x}' for x in range(10)]
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
    with FileCache(cache_name=None) as fc:
        new_path = f'{prefix}/{uuid.uuid4()}'
        pfx = fc.new_path(new_path, anonymous=True)
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


def test_prefix_nthreads_bad():
    with FileCache(None) as fc:
        pfx = fc.new_path(HTTP_TEST_ROOT)
        with pytest.raises(ValueError):
            pfx.retrieve(EXPECTED_FILENAMES, nthreads=-1)
        with pytest.raises(ValueError):
            pfx.retrieve(EXPECTED_FILENAMES, nthreads=4.5)
        with pytest.raises(ValueError):
            pfx.exists(EXPECTED_FILENAMES, nthreads=-1)
        with pytest.raises(ValueError):
            pfx.exists(EXPECTED_FILENAMES, nthreads=4.5)


def test_url_bad():
    with FileCache(None) as fc:
        with pytest.raises(FileNotFoundError):
            fc.retrieve(f'{WINDOWS_PREFIX}/non-existent.txt')
        with pytest.raises(FileNotFoundError):
            fc.retrieve(f'file:///{WINDOWS_PREFIX}{WINDOWS_SLASH}non-existent.txt')
        assert not fc.exists(f'file://{WINDOWS_SLASH}' + str(EXPECTED_DIR) + '/' +
                             EXPECTED_FILENAMES[0] + '-bad')
        assert fc.exists(f'file://{WINDOWS_SLASH}' + str(EXPECTED_DIR) + '/' +
                         EXPECTED_FILENAMES[0])
        with pytest.raises(ValueError):
            fc.retrieve('file://xxx://yyy')
        with pytest.raises(ValueError):
            fc.retrieve('bad://bad-bucket/file.txt')
        with pytest.raises(ValueError):
            fc.retrieve('bad://non-existent.txt')


@pytest.mark.parametrize('prefix', INDEXABLE_PREFIXES)
def test_iterdir(prefix):
    wprefix = str(prefix).replace('\\', '/')
    with FileCache(anonymous=True) as fc:
        objs = sorted(list(fc.iterdir(prefix)))
        assert objs == [f'{wprefix}/lorem1.txt', f'{wprefix}/subdir1']
        objs = sorted(list(fc.iterdir(f'{prefix}/subdir1')))
        assert objs == [f'{wprefix}/subdir1/lorem1.txt',
                        f'{wprefix}/subdir1/subdir2a',
                        f'{wprefix}/subdir1/subdir2b']


@pytest.mark.parametrize('prefix', INDEXABLE_PREFIXES)
def test_iterdir_type(prefix):
    wprefix = str(prefix).replace('\\', '/')
    with FileCache(anonymous=True) as fc:
        objs = sorted(list(fc.iterdir_type(prefix)))
        assert objs == [(f'{wprefix}/lorem1.txt', False), (f'{wprefix}/subdir1', True)]
        objs = sorted(list(fc.iterdir_type(f'{prefix}/subdir1')))
        assert objs == [(f'{wprefix}/subdir1/lorem1.txt', False),
                        (f'{wprefix}/subdir1/subdir2a', True),
                        (f'{wprefix}/subdir1/subdir2b', True)]


@pytest.mark.parametrize('prefix', INDEXABLE_PREFIXES)
def test_iterdir_pfx(prefix):
    wprefix = str(prefix).replace('\\', '/')
    with FileCache(anonymous=True) as fc:
        pfx1 = fc.new_path(prefix)
        objs = sorted([str(x) for x in pfx1.iterdir()])
        assert objs == [f'{wprefix}/lorem1.txt', f'{wprefix}/subdir1']
        pfx2 = fc.new_path(f'{prefix}/subdir1')
        objs = sorted([str(x) for x in pfx2.iterdir()])
        assert objs == [f'{wprefix}/subdir1/lorem1.txt',
                        f'{wprefix}/subdir1/subdir2a',
                        f'{wprefix}/subdir1/subdir2b']


@pytest.mark.parametrize('prefix', INDEXABLE_PREFIXES)
def test_iterdir_type_pfx(prefix):
    wprefix = str(prefix).replace('\\', '/')
    with FileCache(anonymous=True) as fc:
        pfx1 = fc.new_path(prefix)
        objs = sorted([(str(x), y) for x, y in pfx1.iterdir_type()])
        assert objs == [(f'{wprefix}/lorem1.txt', False), (f'{wprefix}/subdir1', True)]
        pfx2 = fc.new_path(f'{prefix}/subdir1')
        objs = sorted([(str(x), y) for x, y in pfx2.iterdir_type()])
        assert objs == [(f'{wprefix}/subdir1/lorem1.txt', False),
                        (f'{wprefix}/subdir1/subdir2a', True),
                        (f'{wprefix}/subdir1/subdir2b', True)]


def test_local_unlink_good():
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir).expanduser().resolve()
        with FileCache(cache_name=None) as fc:
            path = temp_dir / 'test_file.txt'
            path.write_text('Hello')
            assert path.exists()
            assert fc.unlink(path) == str(path).replace('\\', '/')
            assert not path.exists()


def test_local_unlink_multi_good():
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir).expanduser().resolve()
        with FileCache(cache_name=None) as fc:
            path1 = temp_dir / 'test_file1.txt'
            path2 = temp_dir / 'test_file2.txt'
            path3 = temp_dir / 'test_file3.txt'
            path1.write_text('Hello')
            path2.write_text('Hello')
            path3.write_text('Hello')
            assert fc.unlink([path1, path2, path3]) == [str(path1), str(path2),
                                                        str(path3)]
            assert not path1.exists()
            assert not path2.exists()
            assert not path3.exists()


def test_local_unlink_bad():
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir).expanduser().resolve()
        with FileCache(cache_name=None) as fc:
            path = temp_dir / 'test_file.txt'
            path2 = temp_dir / 'test_file2.txt'
            path.write_text('Hello')
            assert path.exists()
            assert not path2.exists()
            assert fc.unlink(path, exception_on_fail=False) == \
                str(path).replace('\\', '/')
            with pytest.raises(FileNotFoundError):
                assert fc.unlink(path2)
            assert isinstance(fc.unlink(path2, exception_on_fail=False), FileNotFoundError)


def test_local_unlink_multi_bad():
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir).expanduser().resolve()
        with FileCache(cache_name=None) as fc:
            path1 = temp_dir / 'test_file1.txt'
            path2 = temp_dir / 'test_file2.txt'
            path3 = temp_dir / 'test_file3.txt'
            path1.write_text('Hello')
            path3.write_text('Hello')
            with pytest.raises(FileNotFoundError):
                fc.unlink([path1, path2, path3])
            assert not path1.exists()
            assert not path2.exists()
            assert not path3.exists()

            path1.write_text('Hello')
            path3.write_text('Hello')
            ret = fc.unlink([path1, path2, path3], exception_on_fail=False)
            assert ret[0] == str(path1)
            assert isinstance(ret[1], FileNotFoundError)
            assert ret[2] == str(path3)
            assert not path1.exists()
            assert not path2.exists()
            assert not path3.exists()


@pytest.mark.parametrize('prefix', WRITABLE_CLOUD_PREFIXES)
def test_cloud_unlink(prefix):
    with FileCache(anonymous=True, cache_name=None) as fc:
        new_path = f'{prefix}/{uuid.uuid4()}'
        path = f'{new_path}/test_file.txt'
        local_path = fc.get_local_path(path)
        _copy_file(EXPECTED_DIR / EXPECTED_FILENAMES[0], local_path)
        assert local_path.exists()
        assert fc.exists(path)
        assert not fc.exists(path, bypass_cache=True)
        with pytest.raises(FileNotFoundError):
            fc.unlink(path, missing_ok=False)
        assert local_path.exists()
        assert isinstance(fc.unlink(path, missing_ok=False, exception_on_fail=False),
                          FileNotFoundError)

        fc.unlink(path, missing_ok=True)
        assert not local_path.exists()
        assert not fc.exists(path)
        assert not fc.exists(path, bypass_cache=True)

        _copy_file(EXPECTED_DIR / EXPECTED_FILENAMES[0], local_path)
        fc.upload(path)
        assert local_path.exists()
        assert fc.exists(path)
        assert fc.exists(path, bypass_cache=True)
        fc.unlink(path)
        assert not local_path.exists()
        assert not fc.exists(path)
        assert not fc.exists(path, bypass_cache=True)


@pytest.mark.parametrize('prefix', WRITABLE_CLOUD_PREFIXES)
def test_cloud_unlink_multi(prefix):
    with FileCache(anonymous=True, cache_name=None) as fc:
        new_path = f'{prefix}/{uuid.uuid4()}'
        paths = [f'{new_path}/test_file{x}.txt' for x in range(5)]
        local_paths = [fc.get_local_path(x) for x in paths]
        for lp, p in zip(local_paths, paths):
            _copy_file(EXPECTED_DIR / EXPECTED_FILENAMES[0], lp)
            assert lp.exists()
            assert fc.exists(p)
            assert not fc.exists(p, bypass_cache=True)
        with pytest.raises(FileNotFoundError):
            fc.unlink(paths, missing_ok=False)
        for lp in local_paths:
            assert lp.exists()

        ret = fc.unlink(paths, exception_on_fail=False)
        for r in ret:
            assert isinstance(r, FileNotFoundError)
        for lp in local_paths:
            assert lp.exists()

        ret = fc.unlink(paths, missing_ok=True)
        assert ret == paths
        for lp in local_paths:
            assert not lp.exists()
        for p in paths:
            assert not fc.exists(p)
            assert not fc.exists(p, bypass_cache=True)

        for i, (lp, p) in enumerate(zip(local_paths, paths)):
            if i != 2:
                _copy_file(EXPECTED_DIR / EXPECTED_FILENAMES[0], lp)
                assert lp.exists()
                assert fc.exists(p)
                assert not fc.exists(p, bypass_cache=True)
            else:
                assert not lp.exists()
                assert not fc.exists(p)
                assert not fc.exists(p, bypass_cache=True)
        fc.upload(paths, exception_on_fail=False)
        with pytest.raises(FileNotFoundError):
            fc.unlink(paths, missing_ok=False)
        for lp in local_paths:
            assert not lp.exists()
        for p in paths:
            assert not fc.exists(p, bypass_cache=True)

        for i, (lp, p) in enumerate(zip(local_paths, paths)):
            if i != 2:
                _copy_file(EXPECTED_DIR / EXPECTED_FILENAMES[0], lp)
                assert lp.exists()
                assert fc.exists(p)
                assert not fc.exists(p, bypass_cache=True)
            else:
                assert not lp.exists()
                assert not fc.exists(p)
                assert not fc.exists(p, bypass_cache=True)
        fc.upload(paths, exception_on_fail=False)

        ret = fc.unlink(paths, exception_on_fail=False)
        for i, r in enumerate(ret):
            if i != 2:
                assert r == paths[i]
            else:
                assert isinstance(r, FileNotFoundError)
        for lp in local_paths:
            assert not lp.exists()
        for p in paths:
            assert not fc.exists(p, bypass_cache=True)

        for i, (lp, p) in enumerate(zip(local_paths, paths)):
            if i != 2:
                _copy_file(EXPECTED_DIR / EXPECTED_FILENAMES[0], lp)
                assert lp.exists()
                assert fc.exists(p)
                assert not fc.exists(p, bypass_cache=True)
            else:
                assert not lp.exists()
                assert not fc.exists(p)
                assert not fc.exists(p, bypass_cache=True)
        fc.upload(paths, exception_on_fail=False)

        ret = fc.unlink(paths, missing_ok=True, exception_on_fail=False)
        for i, r in enumerate(ret):
            assert r == paths[i]
        for lp in local_paths:
            assert not lp.exists()
        for p in paths:
            assert not fc.exists(p, bypass_cache=True)


@pytest.mark.parametrize('prefix', WRITABLE_CLOUD_PREFIXES)
def test_cloud_unlink_multi_pfx(prefix):
    with FileCache(anonymous=True, cache_name=None) as fc:
        new_path = f'{prefix}/{uuid.uuid4()}'
        pfx = fc.new_path(new_path, anonymous=True)
        paths = [f'test_file{x}.txt' for x in range(5)]
        local_paths = [pfx.get_local_path(x) for x in paths]
        for lp, p in zip(local_paths, paths):
            _copy_file(EXPECTED_DIR / EXPECTED_FILENAMES[0], lp)
            assert lp.exists()
            assert pfx.exists(p)
            assert not pfx.exists(p, bypass_cache=True)
        with pytest.raises(FileNotFoundError):
            pfx.unlink(paths, missing_ok=False)
        for lp in local_paths:
            assert lp.exists()

        ret = pfx.unlink(paths, exception_on_fail=False)
        for r in ret:
            assert isinstance(r, FileNotFoundError)
        for lp in local_paths:
            assert lp.exists()

        ret = pfx.unlink(paths, missing_ok=True)
        assert ret == [f'{new_path}/{x}' for x in paths]
        for lp in local_paths:
            assert not lp.exists()
        for p in paths:
            assert not pfx.exists(p)
            assert not pfx.exists(p, bypass_cache=True)

        for i, (lp, p) in enumerate(zip(local_paths, paths)):
            if i != 2:
                _copy_file(EXPECTED_DIR / EXPECTED_FILENAMES[0], lp)
                assert lp.exists()
                assert pfx.exists(p)
                assert not pfx.exists(p, bypass_cache=True)
            else:
                assert not lp.exists()
                assert not pfx.exists(p)
                assert not pfx.exists(p, bypass_cache=True)
        pfx.upload(paths, exception_on_fail=False)
        with pytest.raises(FileNotFoundError):
            pfx.unlink(paths, missing_ok=False)
        for lp in local_paths:
            assert not lp.exists()
        for p in paths:
            assert not pfx.exists(p, bypass_cache=True)

        for i, (lp, p) in enumerate(zip(local_paths, paths)):
            if i != 2:
                _copy_file(EXPECTED_DIR / EXPECTED_FILENAMES[0], lp)
                assert lp.exists()
                assert pfx.exists(p)
                assert not pfx.exists(p, bypass_cache=True)
            else:
                assert not lp.exists()
                assert not pfx.exists(p)
                assert not pfx.exists(p, bypass_cache=True)
        pfx.upload(paths, exception_on_fail=False)

        ret = pfx.unlink(paths, exception_on_fail=False)
        for i, r in enumerate(ret):
            if i != 2:
                assert r == f'{new_path}/{paths[i]}'
            else:
                assert isinstance(r, FileNotFoundError)
        for lp in local_paths:
            assert not lp.exists()
        for p in paths:
            assert not pfx.exists(p, bypass_cache=True)

        for i, (lp, p) in enumerate(zip(local_paths, paths)):
            if i != 2:
                _copy_file(EXPECTED_DIR / EXPECTED_FILENAMES[0], lp)
                assert lp.exists()
                assert pfx.exists(p)
                assert not pfx.exists(p, bypass_cache=True)
            else:
                assert not lp.exists()
                assert not pfx.exists(p)
                assert not pfx.exists(p, bypass_cache=True)
        pfx.upload(paths, exception_on_fail=False)

        ret = pfx.unlink(paths, missing_ok=True, exception_on_fail=False)
        for i, r in enumerate(ret):
            assert r == f'{new_path}/{paths[i]}'
        for lp in local_paths:
            assert not lp.exists()
        for p in paths:
            assert not pfx.exists(p, bypass_cache=True)


@pytest.mark.parametrize('prefix', WRITABLE_CLOUD_PREFIXES)
def test_cloud_unlink_pfx(prefix):
    with FileCache(anonymous=True, cache_name=None) as fc:
        new_path = f'{prefix}/{uuid.uuid4()}'
        path = 'test_file.txt'
        pfx = fc.new_path(new_path, anonymous=True)
        local_path = pfx.get_local_path(path)
        _copy_file(EXPECTED_DIR / EXPECTED_FILENAMES[0], local_path)
        assert local_path.exists()
        assert pfx.exists(path)
        assert not pfx.exists(path, bypass_cache=True)
        with pytest.raises(FileNotFoundError):
            pfx.unlink(path)
        _copy_file(EXPECTED_DIR / EXPECTED_FILENAMES[0], local_path)
        pfx.unlink(path, missing_ok=True)
        assert not local_path.exists()
        assert not pfx.exists(path)
        assert not pfx.exists(path, bypass_cache=True)
        _copy_file(EXPECTED_DIR / EXPECTED_FILENAMES[0], local_path)
        pfx.upload(path)
        assert local_path.exists()
        assert pfx.exists(path)
        assert pfx.exists(path, bypass_cache=True)
        pfx.unlink(path)
        assert not local_path.exists()
        assert not pfx.exists(path)
        assert not pfx.exists(path, bypass_cache=True)


def test_local_rename():
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = FCPath(Path(temp_dir).expanduser().resolve())
        with FileCache(cache_name=None):
            path1 = temp_dir / 'test_file1.txt'
            path2 = temp_dir / 'test_file2.txt'
            _copy_file(EXPECTED_DIR / EXPECTED_FILENAMES[0], path1)
            assert path1.exists()
            assert not path2.exists()
            path1.rename(path2)
            assert not path1.exists()
            assert path2.exists()
            _compare_to_expected_path(path2, LIMITED_FILENAMES[0])

            with pytest.raises(FileNotFoundError):
                path1.rename(path2)


def test_local_replace():
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = FCPath(Path(temp_dir).expanduser().resolve())
        with FileCache(cache_name=None):
            path1 = temp_dir / 'test_file1.txt'
            path2 = temp_dir / 'test_file2.txt'
            _copy_file(EXPECTED_DIR / EXPECTED_FILENAMES[0], path1)
            assert path1.exists()
            assert not path2.exists()
            path1.replace(path2)
            assert not path1.exists()
            assert path2.exists()
            _compare_to_expected_path(path2, LIMITED_FILENAMES[0])

            with pytest.raises(FileNotFoundError):
                path1.replace(path2)


@pytest.mark.parametrize('prefix', WRITABLE_CLOUD_PREFIXES)
def test_cloud_rename_good(prefix):
    with FileCache(anonymous=True, cache_name=None) as fc:
        new_path = fc.new_path(f'{prefix}/{uuid.uuid4()}')
        path1 = new_path / 'test_file1.txt'
        path2 = new_path / 'test_file2.txt'
        local_path1 = path1.get_local_path()
        local_path2 = path2.get_local_path()
        _copy_file(EXPECTED_DIR / EXPECTED_FILENAMES[0], local_path1)
        assert path1.exists()
        assert not path1.exists(bypass_cache=True)
        assert not path2.exists()
        path1.rename(path2)
        assert not local_path1.exists()
        assert local_path2.exists()
        path2.unlink()

    with FileCache(anonymous=True, cache_name=None) as fc:
        new_path = fc.new_path(f'{prefix}/{uuid.uuid4()}')
        path1 = new_path / 'test_file1.txt'
        path2 = new_path / 'test_file2.txt'
        local_path1 = path1.get_local_path()
        local_path2 = path2.get_local_path()
        _copy_file(EXPECTED_DIR / EXPECTED_FILENAMES[0], local_path1)
        path1.upload()
        assert path1.exists(bypass_cache=True)
        assert not path2.exists()
        path1.rename(path2)
        assert not local_path1.exists()
        assert not path1.exists()
        assert not path1.exists(bypass_cache=True)
        assert local_path2.exists()
        assert path2.exists()
        assert path2.exists(bypass_cache=True)
        _compare_to_expected_path(local_path2, LIMITED_FILENAMES[0])

        path2.get_local_path().unlink()
        path2.retrieve()
        _compare_to_expected_path(local_path2, LIMITED_FILENAMES[0])


@pytest.mark.parametrize('prefix', WRITABLE_CLOUD_PREFIXES)
def test_cloud_rename_bad(prefix):
    with FileCache(anonymous=True, cache_name=None) as fc:
        new_path = fc.new_path(f'{prefix}/{uuid.uuid4()}')
        path1 = new_path / 'test_file1.txt'
        path2 = new_path / 'test_file2.txt'
        with pytest.raises(FileNotFoundError):
            path1.rename(path2)
        assert not path1.exists()
        assert not path2.exists()


def test_rename_bad():
    gpath = FCPath(f'{GS_TEST_BUCKET_ROOT}/{uuid.uuid4()}')
    spath = FCPath(f'{S3_TEST_BUCKET_ROOT}/{uuid.uuid4()}')
    gpath1 = gpath / 'test_file1.txt'
    spath1 = spath / 'test_file1.txt'
    fpath1 = FCPath('/tmp/test_file1.txt')
    with pytest.raises(ValueError):
        gpath1.rename(spath1)
    with pytest.raises(ValueError):
        fpath1.rename(gpath1)


# THIS MUST BE AT THE END IN ORDER FOR CODE COVERAGE TO WORK
@pytest.mark.order(-1)
def test_atexit():
    fc = FileCache(cache_name=None)
    assert os.path.exists(fc.cache_dir)
    atexit._run_exitfuncs()
    assert not os.path.exists(fc.cache_dir)

################################################################################
# tests/test_file_cache_source.py
################################################################################

import pytest

from filecache import (FileCacheSource,
                       FileCacheSourceFile,
                       FileCacheSourceHTTP,
                       FileCacheSourceGS,
                       FileCacheSourceS3)

from .test_file_cache import EXPECTED_DIR, EXPECTED_FILENAMES


def test_source_bad():
    with pytest.raises(ValueError):
        FileCacheSourceFile('fred', 'hi')

    with pytest.raises(ValueError):
        FileCacheSourceHTTP('fred', 'hi')
    with pytest.raises(ValueError):
        FileCacheSourceHTTP('http', 'hi/hi')
    with pytest.raises(ValueError):
        FileCacheSourceHTTP('https', '')

    with pytest.raises(ValueError):
        FileCacheSourceGS('fred', 'hi')
    with pytest.raises(ValueError):
        FileCacheSourceGS('gs', 'hi/hi')
    with pytest.raises(ValueError):
        FileCacheSourceGS('gs', '')

    with pytest.raises(ValueError):
        FileCacheSourceS3('fred', 'hi')
    with pytest.raises(ValueError):
        FileCacheSourceS3('s3', 'hi/hi')
    with pytest.raises(ValueError):
        FileCacheSourceS3('s3', '')


def test_filesource_bad():
    sl = FileCacheSourceFile('file', '')
    with pytest.raises(FileNotFoundError):
        sl.upload('non-existent.txt', 'non-existent.txt')
    assert not sl.exists('non-existent.txt')


def test_filesource_good():
    sl = FileCacheSourceFile('file', '')
    assert sl.exists(EXPECTED_DIR / EXPECTED_FILENAMES[1])


def test_source_notimp():
    with pytest.raises(TypeError):
        FileCacheSource('', '').exists('')
    with pytest.raises(NotImplementedError):
        FileCacheSourceHTTP('http', 'fred').upload('', '')
    with pytest.raises(NotImplementedError):
        FileCacheSourceHTTP('http', 'fred').iterdir_type('')
    with pytest.raises(NotImplementedError):
        FileCacheSourceHTTP('http', 'fred').unlink('')


def test_source_nthreads_bad():
    with pytest.raises(ValueError):
        FileCacheSourceFile('file', '').retrieve_multi(['/test'], ['/test'], nthreads=-1)
    with pytest.raises(ValueError):
        FileCacheSourceFile('file', '').retrieve_multi(['/test'], ['/test'], nthreads=4.5)
    with pytest.raises(ValueError):
        FileCacheSourceFile('file', '').upload_multi(['/test'], ['/test'], nthreads=-1)
    with pytest.raises(ValueError):
        FileCacheSourceFile('file', '').upload_multi(['/test'], ['/test'], nthreads=4.5)
    with pytest.raises(ValueError):
        FileCacheSourceFile('file', '').exists_multi(['/test'], nthreads=-1)
    with pytest.raises(ValueError):
        FileCacheSourceFile('file', '').exists_multi(['/test'], nthreads=4.5)
    with pytest.raises(ValueError):
        FileCacheSourceFile('file', '').unlink_multi(['/test'], nthreads=-1)
    with pytest.raises(ValueError):
        FileCacheSourceFile('file', '').unlink_multi(['/test'], nthreads=4.5)

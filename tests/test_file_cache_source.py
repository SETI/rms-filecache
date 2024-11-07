################################################################################
# tests/test_file_cache_source.py
################################################################################

import pytest

from filecache import (FileCacheSource,
                       FileCacheSourceFile,
                       FileCacheSourceHTTP,
                       FileCacheSourceGS,
                       FileCacheSourceS3)


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


def test_localsource_bad():
    sl = FileCacheSourceFile('file', '')
    with pytest.raises(ValueError):
        sl.retrieve('hi', 'bye')
    with pytest.raises(ValueError):
        sl.upload('hi', 'bye')
    with pytest.raises(FileNotFoundError):
        sl.upload('non-existent.txt', 'non-existent.txt')


def test_source_notimp():
    with pytest.raises(TypeError):
        FileCacheSource('', '').exists('')
    with pytest.raises(NotImplementedError):
        FileCacheSourceHTTP('http', 'fred').upload('', '')


def test_source_nthreads_bad():
    with pytest.raises(ValueError):
        FileCacheSourceFile('file', '').retrieve_multi(['/test'], ['/test'], nthreads=-1)
    with pytest.raises(ValueError):
        FileCacheSourceFile('file', '').retrieve_multi(['/test'], ['/test'], nthreads=4.5)
    with pytest.raises(ValueError):
        FileCacheSourceFile('file', '').upload_multi(['/test'], ['/test'], nthreads=-1)
    with pytest.raises(ValueError):
        FileCacheSourceFile('file', '').upload_multi(['/test'], ['/test'], nthreads=4.5)

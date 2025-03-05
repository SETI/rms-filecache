################################################################################
# tests/test_file_cache_path.py
################################################################################

import os
from pathlib import Path, PurePath
import platform
import sys
import tempfile
import uuid

import pytest

from filecache import FileCache, FileCacheSourceFake, FCPath

from tests.test_file_cache import (GS_TEST_BUCKET_ROOT,
                                   S3_TEST_BUCKET_ROOT,
                                   INDEXABLE_PREFIXES,
                                   EXPECTED_DIR,
                                   EXPECTED_FILENAMES,
                                   GS_WRITABLE_TEST_BUCKET_ROOT
                                   )


def test__split_parts():
    # Local
    assert FCPath._split_parts('') == ('', '', '')
    assert FCPath._split_parts('/') == ('', '/', '/')
    assert FCPath._split_parts('a') == ('', '', 'a')
    assert FCPath._split_parts('a/') == ('', '', 'a')
    assert FCPath._split_parts(Path('a')) == ('', '', 'a')
    assert FCPath._split_parts('a/b') == ('', '', 'a/b')
    assert FCPath._split_parts('a/b/c') == ('', '', 'a/b/c')
    assert FCPath._split_parts('/a') == ('', '/', '/a')
    assert FCPath._split_parts('/a/') == ('', '/', '/a')
    assert FCPath._split_parts(Path('/a')) == ('', '/', '/a')
    assert FCPath._split_parts('/a/b') == ('', '/', '/a/b')
    assert FCPath._split_parts('/a/b/c') == ('', '/', '/a/b/c')

    # UNC
    with pytest.raises(ValueError):
        FCPath._split_parts('//')
    with pytest.raises(ValueError):
        FCPath._split_parts('///')
    with pytest.raises(ValueError):
        FCPath._split_parts('///share')
    with pytest.raises(ValueError):
        FCPath._split_parts('//host')
    with pytest.raises(ValueError):
        FCPath._split_parts('//host//a')
    assert FCPath._split_parts('//host/share') == ('//host/share', '', '')
    assert FCPath._split_parts('//host/share/') == ('//host/share', '/', '/')
    assert FCPath._split_parts('//host/share/a') == ('//host/share', '/', '/a')
    assert FCPath._split_parts('//host/share/a/b') == ('//host/share', '/', '/a/b')

    # Cloud gs://
    with pytest.raises(ValueError):
        FCPath._split_parts('gs://')
    with pytest.raises(ValueError):
        FCPath._split_parts('gs:///')
    assert FCPath._split_parts('gs://bucket') == ('gs://bucket', '/', '/')
    assert FCPath._split_parts('gs://bucket/') == ('gs://bucket', '/', '/')
    assert FCPath._split_parts('gs://bucket/a') == ('gs://bucket', '/', '/a')
    assert FCPath._split_parts('gs://bucket/a/b') == ('gs://bucket', '/', '/a/b')

    # file://
    with pytest.raises(ValueError):
        FCPath._split_parts('file://')
    assert FCPath._split_parts('file:///') == ('file://', '/', '/')
    assert FCPath._split_parts('file:///a') == ('file://', '/', '/a')

    # Windows
    assert FCPath._split_parts('C:') == ('C:', '', '')
    assert FCPath._split_parts('C:/') == ('C:', '/', '/')
    assert FCPath._split_parts('C:a/b') == ('C:', '', 'a/b')
    assert FCPath._split_parts('C:/a/b') == ('C:', '/', '/a/b')
    assert FCPath._split_parts(r'C:\a\b') == ('C:', '/', '/a/b')
    assert FCPath._split_parts('c:') == ('C:', '', '')
    assert FCPath._split_parts('c:/') == ('C:', '/', '/')
    assert FCPath._split_parts('c:a/b') == ('C:', '', 'a/b')
    assert FCPath._split_parts('c:/a/b') == ('C:', '/', '/a/b')
    assert FCPath._split_parts(r'c:\a\b') == ('C:', '/', '/a/b')


def test_split():
    assert FCPath._split('') == ('', '')
    assert FCPath._split('a') == ('', 'a')
    assert FCPath._split('a/b/c') == ('a/b', 'c')
    assert FCPath._split('/a/b/c') == ('/a/b', 'c')
    assert FCPath._split('http://domain.name') == ('http://domain.name', '')
    assert FCPath._split('http://domain.name/') == ('http://domain.name', '')
    assert FCPath._split('http://domain.name/a') == ('http://domain.name', 'a')
    assert FCPath._split('http://domain.name/a/b') == ('http://domain.name/a', 'b')


def test_is_absolute():
    assert not FCPath._is_absolute('')
    assert not FCPath._is_absolute('a')
    assert not FCPath._is_absolute('a/b')
    assert not FCPath._is_absolute('C:')
    assert not FCPath._is_absolute('C:a')
    assert not FCPath._is_absolute('C:a/b')
    assert FCPath._is_absolute('/')
    assert FCPath._is_absolute('/a')
    assert FCPath._is_absolute('C:/')
    assert FCPath._is_absolute('c:/')
    assert FCPath._is_absolute('C:/a')
    assert FCPath._is_absolute('gs://bucket')
    assert FCPath._is_absolute('gs://bucket/')
    assert FCPath._is_absolute('gs://bucket/a')
    assert FCPath._is_absolute('file:///a')

    assert not FCPath('').is_absolute()
    assert not FCPath('a').is_absolute()
    assert not FCPath('a/b').is_absolute()
    assert not FCPath('C:').is_absolute()
    assert not FCPath('C:a').is_absolute()
    assert not FCPath('C:a/b').is_absolute()
    assert FCPath('/').is_absolute()
    assert FCPath('/a').is_absolute()
    assert FCPath('C:/').is_absolute()
    assert FCPath('c:/').is_absolute()
    assert FCPath('C:/a').is_absolute()
    assert FCPath('gs://bucket').is_absolute()
    assert FCPath('gs://bucket/').is_absolute()
    assert FCPath('gs://bucket/a').is_absolute()
    assert FCPath('file:///a').is_absolute()


def test__join():
    with pytest.raises(TypeError):
        FCPath._join(5)
    assert FCPath._join(None) == ''
    assert FCPath._join('') == ''
    assert FCPath._join('/') == '/'
    assert FCPath._join('C:/') == 'C:/'
    assert FCPath._join('c:/') == 'C:/'
    assert FCPath._join('a') == 'a'
    assert FCPath._join('a/') == 'a'
    assert FCPath._join('/a/b') == '/a/b'
    assert FCPath._join('/a/b/') == '/a/b'
    assert FCPath._join('', 'a') == 'a'
    assert FCPath._join('', '/a') == '/a'
    assert FCPath._join('a', 'b') == 'a/b'
    assert FCPath._join('/a', 'b') == '/a/b'
    assert FCPath._join('/a', None, 'b', None) == '/a/b'
    assert FCPath._join('/', 'a', 'b') == '/a/b'
    assert FCPath._join('/a', '/b') == '/b'
    assert FCPath._join('/a', 'gs://bucket/a/b') == 'gs://bucket/a/b'
    assert FCPath._join('/a', 'C:/a/b') == 'C:/a/b'
    assert FCPath._join('/a', '/b/') == '/b'
    assert FCPath._join('/a', '') == '/a'
    assert FCPath._join('/a', Path('b', 'c'), FCPath('d/e')) == '/a/b/c/d/e'


def test__filename():
    assert FCPath._filename('') == ''
    assert FCPath._filename('a') == 'a'
    assert FCPath._filename('C:') == ''
    assert FCPath._filename('C:/') == ''
    assert FCPath._filename('/') == ''
    assert FCPath._filename('a/b') == 'b'
    assert FCPath._filename('/a/b') == 'b'
    assert FCPath._filename('gs://bucket') == ''
    assert FCPath._filename('gs://bucket/') == ''
    assert FCPath._filename('gs://bucket/a') == 'a'


def test__str():
    assert str(FCPath('a/b')) == 'a/b'
    assert str(FCPath(Path('a/b'))) == 'a/b'
    assert str(FCPath(r'\a\b')) == '/a/b'


def test__repr():
    assert repr(FCPath('a/b')) == "FCPath('a/b')"
    assert repr(FCPath(Path('a/b'))) == "FCPath('a/b')"
    assert repr(FCPath(r'\a\b')) == "FCPath('/a/b')"


def test_comparison():
    p1a = FCPath('/a/b/c1.py')
    p1b = FCPath('/a/b/c1.py')
    p2 = FCPath('/a/b/c2.py')
    p3 = Path('/a/b/c3.py')
    assert p1a == p1a
    assert p1a == p1b
    assert p2 == p2
    assert p1a < p2
    assert not (p2 < p1a)
    assert not (p1a < p1b)
    assert p1a <= p2
    assert not (p2 <= p1a)
    assert p1a <= p1b
    assert p2 > p1a
    assert not (p1a > p2)
    assert not (p1b > p1a)
    assert p2 >= p1a
    assert not (p1a >= p2)
    assert p1b >= p1a
    assert not (p1a == p3)
    assert p1a != p3
    with pytest.raises(TypeError):
        p1a < p3
    with pytest.raises(TypeError):
        p1a <= p3
    with pytest.raises(TypeError):
        p1a > p3
    with pytest.raises(TypeError):
        p1a >= p3


def test_as_pathlib():
    assert isinstance(FCPath('a/b').as_pathlib(), PurePath)
    assert FCPath('a/b').as_pathlib() == Path('a/b')
    with pytest.raises(ValueError):
        FCPath('gs://x/a').as_pathlib()
    p = FCPath('a/b')
    assert p.as_pathlib() == p.as_pathlib()


def test_as_posix():
    assert FCPath('a/b').as_posix() == 'a/b'
    assert FCPath(Path('a/b')).as_posix() == 'a/b'
    assert FCPath(r'\a\b').as_posix() == '/a/b'


def test_drive():
    assert FCPath('/a/b').drive == ''
    assert FCPath('C:').drive == 'C:'
    assert FCPath('C:/').drive == 'C:'
    assert FCPath('gs://bucket/a/b').drive == 'gs://bucket'


def test_root():
    assert FCPath('').root == ''
    assert FCPath('a/b').root == ''
    assert FCPath('C:a/b').root == ''
    assert FCPath('/').root == '/'
    assert FCPath('/a/b').root == '/'
    assert FCPath('C:/a/b').root == '/'
    assert FCPath('gs://bucket/a/b').root == '/'


def test_anchor():
    assert FCPath('').anchor == ''
    assert FCPath('/').anchor == '/'
    assert FCPath('a/b').anchor == ''
    assert FCPath('/a/b').anchor == '/'
    assert FCPath('C:').anchor == 'C:'
    assert FCPath('c:').anchor == 'C:'
    assert FCPath('C:a/b').anchor == 'C:'
    assert FCPath('C:/').anchor == 'C:/'
    assert FCPath('C:/a/b').anchor == 'C:/'
    assert FCPath('gs://bucket').anchor == 'gs://bucket/'
    assert FCPath('gs://bucket/').anchor == 'gs://bucket/'
    assert FCPath('gs://bucket/a/b').anchor == 'gs://bucket/'


def test_suffix():
    assert FCPath('').suffix == ''
    assert FCPath('/').suffix == ''
    assert FCPath('a').suffix == ''
    assert FCPath('/a').suffix == ''
    assert FCPath('gs://bucket').suffix == ''
    assert FCPath('gs://bucket/a').suffix == ''
    assert FCPath('.').suffix == ''
    assert FCPath('.txt').suffix == ''
    assert FCPath('.txt.').suffix == ''
    assert FCPath('/.txt').suffix == ''
    assert FCPath('a.txt').suffix == '.txt'
    assert FCPath('/a.txt').suffix == '.txt'
    assert FCPath('gs://bucket/a.txt').suffix == '.txt'
    assert FCPath('a.txt.zip').suffix == '.zip'


def test_suffixes():
    assert FCPath('').suffixes == []
    assert FCPath('/').suffixes == []
    assert FCPath('a').suffixes == []
    assert FCPath('/a').suffixes == []
    assert FCPath('gs://bucket').suffixes == []
    assert FCPath('gs://bucket/a').suffixes == []
    assert FCPath('.').suffixes == []
    assert FCPath('.txt').suffixes == []
    assert FCPath('.txt.').suffixes == []
    assert FCPath('/.txt').suffixes == []
    assert FCPath('a.txt').suffixes == ['.txt']
    assert FCPath('/a.txt').suffixes == ['.txt']
    assert FCPath('gs://bucket/a.txt').suffixes == ['.txt']
    assert FCPath('a.txt.zip').suffixes == ['.txt', '.zip']


def test_stem():
    assert FCPath('').stem == ''
    assert FCPath('/').stem == ''
    assert FCPath('a').stem == 'a'
    assert FCPath('/a').stem == 'a'
    assert FCPath('gs://bucket').stem == ''
    assert FCPath('gs://bucket/a').stem == 'a'
    assert FCPath('.').stem == '.'
    assert FCPath('.txt').stem == '.txt'
    assert FCPath('.txt.').stem == '.txt.'
    assert FCPath('/.txt').stem == '.txt'
    assert FCPath('a.txt').stem == 'a'
    assert FCPath('/a.txt').stem == 'a'
    assert FCPath('gs://bucket/a.txt').stem == 'a'
    assert FCPath('a.txt.zip').stem == 'a.txt'


def test_with_name():
    with pytest.raises(ValueError):
        FCPath('a/b/c').with_name('')
    with pytest.raises(ValueError):
        FCPath('a/b/c').with_name('/')
    with pytest.raises(ValueError):
        FCPath('a/b/c').with_name('C:')
    with pytest.raises(ValueError):
        FCPath('a/b/c').with_name('C:a')
    with pytest.raises(ValueError):
        FCPath('a/b/c').with_name('gs://bucket')
    with pytest.raises(ValueError):
        FCPath('a/b/c').with_name('gs://bucket/a')
    assert str(FCPath('').with_name('d')) == 'd'
    assert str(FCPath('/').with_name('d')) == '/d'
    assert str(FCPath('a/b/c').with_name('d')) == 'a/b/d'
    assert str(FCPath('a/b/c').with_name('c.txt')) == 'a/b/c.txt'
    assert str(FCPath('C:/a/b/c').with_name('d')) == 'C:/a/b/d'
    assert str(FCPath('c:/a/b/c').with_name('d')) == 'C:/a/b/d'
    assert str(FCPath('gs://bucket/a/b/c').with_name('d')) == 'gs://bucket/a/b/d'


def test_with_stem():
    with pytest.raises(ValueError):
        FCPath('a/b/c').with_stem('')
    with pytest.raises(ValueError):
        FCPath('a/b/c.txt').with_stem('')
    with pytest.raises(ValueError):
        FCPath('a/b/c').with_stem('/')
    with pytest.raises(ValueError):
        FCPath('a/b/c').with_stem('/a')
    with pytest.raises(ValueError):
        FCPath('a/b/c').with_stem('C:')
    with pytest.raises(ValueError):
        FCPath('a/b/c').with_stem('C:a')
    with pytest.raises(ValueError):
        FCPath('a/b/c').with_stem('gs://bucket')
    with pytest.raises(ValueError):
        FCPath('a/b/c').with_stem('gs://bucket/a')
    assert str(FCPath('').with_stem('d')) == 'd'
    assert str(FCPath('/').with_stem('d')) == '/d'
    assert str(FCPath('a/b/c').with_stem('d')) == 'a/b/d'
    assert str(FCPath('a/b/c.zip').with_stem('d')) == 'a/b/d.zip'
    assert str(FCPath('C:/a/b/c').with_stem('d')) == 'C:/a/b/d'
    assert str(FCPath('C:/a/b/c.zip').with_stem('d')) == 'C:/a/b/d.zip'
    assert str(FCPath('C:/a/b/c.txt.zip').with_stem('d')) == 'C:/a/b/d.zip'
    assert str(FCPath('C:/a/b/.zip').with_stem('d')) == 'C:/a/b/d'
    assert str(FCPath('c:/a/b/.zip').with_stem('d')) == 'C:/a/b/d'
    assert str(FCPath('gs://bucket/a/b/c.zip').with_stem('d')) == 'gs://bucket/a/b/d.zip'


def test_with_suffix():
    with pytest.raises(ValueError):
        FCPath('').with_suffix('.txt')
    with pytest.raises(ValueError):
        FCPath('/').with_suffix('.txt')
    with pytest.raises(ValueError):
        FCPath('a/b/c').with_suffix('/')
    with pytest.raises(ValueError):
        FCPath('a/b/c').with_suffix('/a')
    with pytest.raises(ValueError):
        FCPath('a/b/c').with_suffix('C:')
    with pytest.raises(ValueError):
        FCPath('a/b/c').with_suffix('C:a')
    with pytest.raises(ValueError):
        FCPath('a/b/c').with_suffix('gs://bucket')
    with pytest.raises(ValueError):
        FCPath('a/b/c').with_suffix('gs://bucket/a')
    assert str(FCPath('a/b/c').with_suffix('')) == 'a/b/c'
    assert str(FCPath('a/b/c.txt').with_suffix('')) == 'a/b/c'
    assert str(FCPath('a/b/c').with_suffix('.txt')) == 'a/b/c.txt'
    assert str(FCPath('a/b/c.zip').with_suffix('.txt')) == 'a/b/c.txt'
    assert str(FCPath('C:/a/b/c').with_suffix('.txt')) == 'C:/a/b/c.txt'
    assert str(FCPath('C:/a/b/c.zip').with_suffix('.txt')) == 'C:/a/b/c.txt'
    assert str(FCPath('C:/a/b/c.txt.zip').with_suffix('.txt')) == 'C:/a/b/c.txt.txt'
    assert str(FCPath('C:/a/b/.zip').with_suffix('.txt')) == 'C:/a/b/.zip.txt'
    assert str(FCPath('c:/a/b/.zip').with_suffix('.txt')) == 'C:/a/b/.zip.txt'
    assert str(FCPath(
        'gs://bucket/a/b/c.zip').with_suffix('.txt')) == 'gs://bucket/a/b/c.txt'


def test_parts():
    assert FCPath('').parts == ()
    assert FCPath('a').parts == ('a',)
    assert FCPath('a/b').parts == ('a', 'b')
    assert FCPath('/a/b').parts == ('a', 'b')
    assert FCPath('C:/a/b').parts == ('C:', 'a', 'b')
    assert FCPath('c:/a/b').parts == ('C:', 'a', 'b')
    assert FCPath('gs://bucket/a/b').parts == ('gs://bucket', 'a', 'b')


def test_joinpath():
    assert str(FCPath('').joinpath()) == ''
    assert str(FCPath('a').joinpath()) == 'a'
    assert str(FCPath('a/b').joinpath('c')) == 'a/b/c'
    assert str(FCPath('/a/b').joinpath('c')) == '/a/b/c'
    assert str(FCPath('/a/b').joinpath('/c')) == '/c'
    assert str(FCPath('/a/b').joinpath('c', 'http://bucket/x', 'y')) == \
        'http://bucket/x/y'
    assert str(FCPath(
        '/a').joinpath(Path('b', 'c'), FCPath('d/e'))) == '/a/b/c/d/e'
    with FileCache('test', delete_on_exit=True) as fc:
        p = FCPath('a', filecache=fc, anonymous=True, lock_timeout=59, nthreads=2)
        p2 = p.joinpath('c')
        assert str(p2) == 'a/c'
        assert p2._filecache is fc
        assert p2._anonymous
        assert p2._lock_timeout == 59
        assert p2._nthreads == 2
        p3 = FCPath(p2)
        assert str(p3) == 'a/c'
        assert p3._filecache is fc
        assert p3._anonymous
        assert p3._lock_timeout == 59
        assert p3._nthreads == 2
        p4 = FCPath(p3, FCPath('e'))
        assert str(p4) == 'a/c/e'
        assert p4._filecache is fc
        assert p4._anonymous
        assert p4._lock_timeout == 59
        assert p4._nthreads == 2
        p5 = FCPath(str(p3), FCPath('e'))
        assert str(p5) == 'a/c/e'
        assert p5._filecache is not fc
        assert not p5._anonymous
        assert p5._lock_timeout is None
        assert p5._nthreads is None


def test_truediv():
    assert str(FCPath('a/b') / 'c') == 'a/b/c'
    assert str(FCPath('/a/b') / 'c') == '/a/b/c'
    assert str(FCPath('/a/b') / '/c') == '/c'
    assert str(FCPath('/a/b') / 'c' / 'http://bucket/x' / 'y') == 'http://bucket/x/y'
    assert str(FCPath('/a') / Path('b', 'c') / FCPath('d/e')) == '/a/b/c/d/e'
    with FileCache('test', delete_on_exit=True) as fc:
        p = FCPath('a', filecache=fc, anonymous=True, lock_timeout=59, nthreads=2)
        p2 = p / 'c'
        assert str(p2) == 'a/c'
        assert p2._filecache is fc
        assert p2._anonymous
        assert p2._lock_timeout == 59
        assert p2._nthreads == 2


def test_rtruediv():
    assert str('a' / FCPath('b/c')) == 'a/b/c'
    assert str('/a' / FCPath('b/c')) == '/a/b/c'
    assert str('/a' / FCPath('b')) == '/a/b'
    with FileCache('test', delete_on_exit=True) as fc:
        p = FCPath('c', filecache=fc, anonymous=True, lock_timeout=59, nthreads=2)
        p2 = 'a' / p
        assert str(p2) == 'a/c'
        assert p2._filecache is fc
        assert p2._anonymous
        assert p2._lock_timeout == 59
        assert p2._nthreads == 2
        p3 = FCPath('a', lock_timeout=100, nthreads=9) / p
        assert str(p3) == 'a/c'
        assert p3._filecache is None
        assert not p3._anonymous
        assert p3._lock_timeout == 100
        assert p3._nthreads == 9
        assert str('http://bucket/a' / FCPath('b/c') / 'd' / FCPath('e')) == \
            'http://bucket/a/b/c/d/e'


def test_name():
    p = FCPath('')
    assert p.name == ''
    p = FCPath('c')
    assert p.name == 'c'
    p = FCPath('c.txt')
    assert p.name == 'c.txt'
    p = FCPath('/c.txt')
    assert p.name == 'c.txt'
    p = FCPath('a/b/c.txt')
    assert p.name == 'c.txt'
    p = FCPath('C:/a/b/c.txt')
    assert p.name == 'c.txt'
    p = FCPath('http://bucket/a/b/c')
    assert p.name == 'c'


def test_parent():
    p = FCPath('http://bucket/a/b/c', nthreads=3)
    p2 = p.parent
    assert isinstance(p2, FCPath)
    assert str(p2) == 'http://bucket/a/b'
    assert p2._nthreads == 3
    p3 = p2.parent
    assert isinstance(p3, FCPath)
    assert str(p3) == 'http://bucket/a'
    assert p3._nthreads == 3
    p4 = p3.parent
    assert isinstance(p4, FCPath)
    assert str(p4) == 'http://bucket'
    assert p4._nthreads == 3
    p5 = p4.parent
    assert isinstance(p5, FCPath)
    assert str(p5) == 'http://bucket'
    assert p5._nthreads == 3


def test_parents():
    p = FCPath('http://bucket/a/b/c', nthreads=3)
    p2 = p.parents
    assert all([isinstance(x, FCPath) for x in p2])
    assert all([x._nthreads == 3 for x in p2])
    assert [str(x) for x in p2] == ['http://bucket/a/b', 'http://bucket/a',
                                    'http://bucket']


def test_match():
    p = FCPath('abc/def')
    with pytest.raises(ValueError):
        p.match('')
    assert p.match('def')
    assert not p.match('deF')
    assert p.match(FCPath('def'))
    assert not p.match('f')
    assert p.match('*f')
    assert not p.match('c/def')
    assert not p.match('/*f')
    assert p.match('*/*f')
    assert not p.match('/def')
    assert p.match('/*/def')
    assert p.match('/abc/def')
    assert not p.match('/ABC/def')
    assert not p.match('/zz/abc/def')
    p = FCPath('ABC/def')
    with pytest.raises(ValueError):
        p.match('')
    assert p.match('def')
    assert not p.match('deF')
    assert p.match(FCPath('def'))
    assert not p.match('f')
    assert p.match('*f')
    assert not p.match('c/def')
    assert not p.match('/*f')
    assert p.match('*/*f')
    assert not p.match('/def')
    assert p.match('/*/def')
    assert not p.match('/abc/def')
    assert p.match('/ABC/def')
    assert not p.match('/zz/abc/def')
    p = FCPath('a/b/c.py')
    assert not p.match('a/**')
    assert not p.match('a/*')
    assert p.match('**/*.py')
    assert p.match('*.py')
    assert p.match('b/*.py')
    assert not p.match('a/*.py')
    p = FCPath('C:/a/b')
    assert p.match('b')
    assert p.match('a/b')
    assert p.match('c:/a/b')
    assert p.match('C:/a/b')
    assert not p.match('d:/a/b')
    p = FCPath('c:/a/b')
    assert p.match('b')
    assert p.match('a/b')
    assert p.match('c:/a/b')
    assert p.match('C:/a/b')
    assert not p.match('d:/a/b')
    p = FCPath('http://server.name/a/b')
    assert p.match('b')
    assert p.match('a/b')
    assert p.match('http://server.name/a/b')
    assert not p.match('http://server2.name/a/b')


def test_full_match():
    p = FCPath('abc/def')
    assert not p.full_match('def')
    assert not p.full_match(FCPath('def'))
    assert not p.full_match('f')
    assert not p.full_match('*f')
    assert not p.full_match('c/def')
    assert not p.full_match('/*f')
    assert not p.full_match('')
    assert p.full_match('*/*f')
    assert not p.full_match('/def')
    assert not p.full_match('/*/def')
    assert not p.full_match('/abc/def')
    assert p.full_match('abc/def')
    assert not p.full_match('ABC/def')
    p = FCPath('ABC/def')
    assert not p.full_match('def')
    assert not p.full_match(FCPath('def'))
    assert not p.full_match('f')
    assert not p.full_match('*f')
    assert not p.full_match('c/def')
    assert not p.full_match('/*f')
    assert p.full_match('*/*f')
    assert not p.full_match('/def')
    assert not p.full_match('/*/def')
    assert not p.full_match('/abc/def')
    assert not p.full_match('abc/def')
    assert p.full_match('ABC/def')
    p = FCPath('a/b/c.py')
    assert p.full_match('a/**')
    assert not p.full_match('a/*')
    assert p.full_match('**/*.py')
    assert not p.full_match('*.py')
    assert not p.full_match('*.py')
    assert not p.full_match('b/*.py')
    assert not p.full_match('a/*.py')
    p = FCPath('C:/a/b')
    assert not p.full_match('b')
    assert not p.full_match('a/b')
    assert p.full_match('c:/a/b')
    assert p.full_match('C:/a/b')
    assert not p.full_match('d:/a/b')
    p = FCPath('c:/a/b')
    assert not p.full_match('b')
    assert not p.full_match('a/b')
    assert p.full_match('c:/a/b')
    assert p.full_match('C:/a/b')
    assert not p.full_match('d:/a/b')
    p = FCPath('http://server.name/a/b')
    assert not p.full_match('b')
    assert not p.full_match('a/b')
    assert p.full_match('http://server.name/a/b')
    assert not p.full_match('http://server2.name/a/b')


def test_read_write():
    pfx_name = f'{GS_WRITABLE_TEST_BUCKET_ROOT}/{uuid.uuid4()}'
    with FileCache(cache_name=None, anonymous=True) as fc:
        pfx = fc.new_path(pfx_name)
        p1 = pfx / 'test_file.bin'
        p2 = pfx / 'test_file.txt'
        p1.write_bytes(b'A')
        p2.write_text('ABC\n')
        assert p1.read_bytes() == b'A'
        assert p2.read_text() == 'ABC\n'
        assert fc.download_counter == 0
        assert fc.upload_counter == 2
        assert p1.download_counter == 0
        assert p1.upload_counter == 1
        assert p2.download_counter == 0
        assert p2.upload_counter == 1
    with FileCache(cache_name=None, anonymous=True) as fc:
        pfx = fc.new_path(pfx_name)
        p1 = pfx / 'test_file.bin'
        p2 = pfx / 'test_file.txt'
        assert p1.read_bytes() == b'A'
        assert p2.read_text() == 'ABC\n'
        assert fc.download_counter == 2
        assert fc.upload_counter == 0
        assert p1.download_counter == 1
        assert p1.upload_counter == 0
        assert p2.download_counter == 1
        assert p2.upload_counter == 0
    with pytest.raises(TypeError):
        p = FCPath('a')
        p.write_text(5)

    FileCacheSourceFake.delete_default_storage_dir()
    try:
        with FileCache(cache_name=None) as fc:
            fake_prefix = 'fake://test-bucket'
            pfx = fc.new_path(fake_prefix)
            p1 = pfx / 'test_file.bin'
            p2 = pfx / 'test_file.txt'

            p1.write_bytes(b'A')
            p2.write_text('ABC\n')
            assert p1.read_bytes() == b'A'
            assert p2.read_text() == 'ABC\n'

            # Should behave like a remote source with uploads
            assert fc.download_counter == 0
            assert fc.upload_counter == 2
            assert p1.download_counter == 0
            assert p1.upload_counter == 1
            assert p2.download_counter == 0
            assert p2.upload_counter == 1

        # Test in new cache to verify persistence
        with FileCache(cache_name=None) as fc:
            pfx = fc.new_path(fake_prefix)
            p1 = pfx / 'test_file.bin'
            p2 = pfx / 'test_file.txt'

            # Reading should require downloads from "remote" storage
            assert p1.read_bytes() == b'A'
            assert p2.read_text() == 'ABC\n'
            assert fc.download_counter == 2
            assert fc.upload_counter == 0
            assert p1.download_counter == 1
            assert p1.upload_counter == 0
            assert p2.download_counter == 1
            assert p2.upload_counter == 0
    finally:
        FileCacheSourceFake.delete_default_storage_dir()


def test_default_filecache():
    with FileCache(delete_on_exit=True) as fc:
        p = FCPath(GS_WRITABLE_TEST_BUCKET_ROOT) / EXPECTED_FILENAMES[0]
        p2 = fc.new_path(GS_WRITABLE_TEST_BUCKET_ROOT) / EXPECTED_FILENAMES[0]
        assert p.get_local_path() == p2.get_local_path()
        p3 = FCPath(GS_WRITABLE_TEST_BUCKET_ROOT) / EXPECTED_FILENAMES[0]
        assert p2.get_local_path() == p3.get_local_path()

        fake_path = 'fake://test-bucket/test.txt'
        p1 = FCPath(fake_path)
        p2 = fc.new_path(fake_path)
        p1.write_text('test content')  # Write using default FileCache
        assert p2.read_text() == 'test content'  # Read using explicit FileCache
        assert p1.get_local_path() == p2.get_local_path()
        p3 = FCPath(fake_path)  # Create new path with default FileCache
        assert p2.get_local_path() == p3.get_local_path()


def test_relative():
    assert (FCPath(f'{GS_TEST_BUCKET_ROOT}/a/b/c.txt')
            .relative_to(f'{GS_TEST_BUCKET_ROOT}/a/b')) == FCPath('c.txt')
    assert (FCPath(f'{GS_TEST_BUCKET_ROOT}/a/b/c.txt')
            .relative_to(f'{GS_TEST_BUCKET_ROOT}/a')) == FCPath('b/c.txt')
    assert (FCPath(f'{GS_TEST_BUCKET_ROOT}/a/b/c.txt')
            .relative_to(f'{GS_TEST_BUCKET_ROOT}')) == FCPath('a/b/c.txt')
    assert (FCPath(r'c:\a\b\c.txt')
            .relative_to(r'C:\a\b')) == FCPath('c.txt')
    assert (FCPath(r'c:\a\b\c.txt')
            .relative_to(r'C:\a')) == FCPath('b/c.txt')
    assert (FCPath(r'c:\a\b\c.txt')
            .relative_to('C:\\')) == FCPath('a/b/c.txt')
    with pytest.raises(ValueError):
        FCPath('/a/b/c/d/e.txt').relative_to('/a/b/c/f/g')
    if sys.version_info >= (3, 12):
        assert (FCPath('/a/b/c/d/e.txt').relative_to('/a/b/c/f/g', walk_up=True) ==
                FCPath('../../d/e.txt'))
    assert FCPath('/a/b/c.txt').is_relative_to('/a/b')
    assert FCPath('/a/b/c.txt').is_relative_to('/a/b/')


def test_as_uri():
    assert FCPath(f'{GS_TEST_BUCKET_ROOT}/a/b/c.txt').as_uri() == \
        f'{GS_TEST_BUCKET_ROOT}/a/b/c.txt'
    assert FCPath('/a/b/c.txt').as_uri() == 'file:///a/b/c.txt'
    assert FCPath('file:///a/b/c.txt').as_uri() == 'file:///a/b/c.txt'
    assert FCPath('c:/a/b/c.txt').as_uri() == 'file:///C:/a/b/c.txt'
    assert FCPath('//host/a/b/c.txt').as_uri() == 'file://host/a/b/c.txt'
    with pytest.raises(ValueError):
        FCPath('a/b/c.txt').as_uri()


def test_misc_os():
    with pytest.raises(NotImplementedError):
        FCPath('https://x.com/a/b').is_mount()
    if platform.system() == 'Windows' and sys.version_info < (3, 12):
        with pytest.raises(NotImplementedError):
            assert not FCPath(EXPECTED_DIR).is_mount()
    else:
        assert not FCPath(EXPECTED_DIR).is_mount()
    with pytest.raises(NotImplementedError):
        FCPath('https://x.com/a/b').is_symlink()
    assert not FCPath(EXPECTED_DIR / EXPECTED_FILENAMES[0]).is_symlink()
    if sys.version_info >= (3, 12):
        with pytest.raises(NotImplementedError):
            FCPath('https://x.com/a/b').is_junction()
        assert not FCPath(EXPECTED_DIR / EXPECTED_FILENAMES[0]).is_junction()
    with pytest.raises(NotImplementedError):
        FCPath('https://x.com/a/b').is_block_device()
    assert not FCPath(EXPECTED_DIR / EXPECTED_FILENAMES[0]).is_block_device()
    with pytest.raises(NotImplementedError):
        FCPath('https://x.com/a/b').is_char_device()
    assert not FCPath(EXPECTED_DIR / EXPECTED_FILENAMES[0]).is_char_device()
    with pytest.raises(NotImplementedError):
        FCPath('https://x.com/a/b').is_fifo()
    assert not FCPath(EXPECTED_DIR / EXPECTED_FILENAMES[0]).is_fifo()
    with pytest.raises(NotImplementedError):
        FCPath('https://x.com/a/b').is_socket()
    assert not FCPath(EXPECTED_DIR / EXPECTED_FILENAMES[0]).is_socket()
    assert FCPath('C:/a/b/c.txt').samefile('C:/a/b/c.txt')
    assert FCPath('C:/a/b/c.txt').samefile(FCPath('C:/a/b/c.txt'))
    assert not FCPath('/a/b/c.txt').samefile('/a/b/d.txt')
    assert FCPath(f'{GS_TEST_BUCKET_ROOT}/a/b/c.txt').absolute() == \
        FCPath(f'{GS_TEST_BUCKET_ROOT}/a/b/c.txt')
    assert FCPath('c/d.txt').absolute() == FCPath(Path('c/d.txt').absolute())
    assert FCPath('c/d.txt').cwd() == FCPath(Path('c/d.txt').cwd())
    assert FCPath(EXPECTED_DIR / '..' / 'x.txt').resolve() == \
        FCPath((EXPECTED_DIR / '..' / 'x.txt').resolve())
    username = os.path.expanduser('~')
    assert FCPath(f'~{username}/b/c.txt').expanduser() == \
        FCPath(Path(f'~{username}/b/c.txt')).expanduser()
    assert FCPath(f'{GS_TEST_BUCKET_ROOT}/~{username}/b/c.txt').expanduser() == \
        FCPath(f'{GS_TEST_BUCKET_ROOT}/~{username}/b/c.txt').expanduser()
    assert FCPath.home() == FCPath(Path('~').expanduser())
    with pytest.raises(NotImplementedError):
        FCPath('https://x.com/a/b').is_reserved()
    with pytest.raises(NotImplementedError):
        FCPath('https://x.com/a/b').stat()
    with pytest.raises(NotImplementedError):
        FCPath('https://x.com/a/b').lstat()
    with pytest.raises(NotImplementedError):
        FCPath('https://x.com/a/b').is_dir()
    with pytest.raises(NotImplementedError):
        FCPath('https://x.com/a/b').readlink()
    assert FCPath(f'{GS_TEST_BUCKET_ROOT}/a/~b/c.txt').resolve() == \
        FCPath(f'{GS_TEST_BUCKET_ROOT}/a/~b/c.txt')
    with pytest.raises(NotImplementedError):
        FCPath('https://x.com/a/b').symlink_to('')
    with pytest.raises(NotImplementedError):
        FCPath('https://x.com/a/b').hardlink_to('')
    with pytest.raises(NotImplementedError):
        FCPath('https://x.com/a/b').mkdir('')
    with pytest.raises(NotImplementedError):
        FCPath('https://x.com/a/b').chmod(0)
    with pytest.raises(NotImplementedError):
        FCPath('https://x.com/a/b').lchmod(0)
    with pytest.raises(NotImplementedError):
        FCPath('https://x.com/a/b').rmdir()
    with pytest.raises(NotImplementedError):
        FCPath('https://x.com/a/b').owner()
    with pytest.raises(NotImplementedError):
        FCPath('https://x.com/a/b').group()
    assert FCPath.from_uri('/a/b/c.txt') == FCPath('/a/b/c.txt')
    assert FCPath.from_uri(f'{GS_TEST_BUCKET_ROOT}/a/b/c.txt') == \
        FCPath(f'{GS_TEST_BUCKET_ROOT}/a/b/c.txt')


@pytest.mark.parametrize('prefix', INDEXABLE_PREFIXES)
def test_walk(prefix):
    prefix = str(prefix)
    wprefix = prefix.replace('\\', '/')
    with FileCache(anonymous=True) as fc:
        pfx1 = fc.new_path(prefix, nthreads=23)
        results = []
        for path, dirs, files in pfx1.walk():
            assert path._nthreads == 23
            dirs.sort()
            files.sort()
            results.append((str(path), dirs, files))
        assert len(results) == 4
        assert results[0] == (wprefix, ['subdir1'], ['lorem1.txt'])
        assert results[1] == (f'{wprefix}/subdir1',
                              ['subdir2a', 'subdir2b'], ['lorem1.txt'])
        if results[2][0].endswith('2a'):
            assert results[2] == (f'{wprefix}/subdir1/subdir2a',
                                  [], ['binary1.bin'])
            assert results[3] == (f'{wprefix}/subdir1/subdir2b',
                                  [], ['binary1.bin'])
        else:
            assert results[3] == (f'{wprefix}/subdir1/subdir2a',
                                  [], ['binary1.bin'])
            assert results[2] == (f'{wprefix}/subdir1/subdir2b',
                                  [], ['binary1.bin'])

        prefix2 = f'{prefix}/subdir1'
        wprefix2 = prefix2.replace('\\', '/')
        pfx2 = fc.new_path(prefix2, nthreads=32)
        results = []
        for path, dirs, files in pfx2.walk():
            assert path._nthreads == 32
            dirs.sort()
            files.sort()
            results.append((str(path), dirs, files))
        results.sort()
        print(results)
        assert len(results) == 3
        assert results[0] == (wprefix2, ['subdir2a', 'subdir2b'], ['lorem1.txt'])
        if results[1][0].endswith('2a'):
            assert results[1] == (f'{wprefix2}/subdir2a', [], ['binary1.bin'])
            assert results[2] == (f'{wprefix2}/subdir2b', [], ['binary1.bin'])
        else:
            assert results[2] == (f'{wprefix2}/subdir2a', [], ['binary1.bin'])
            assert results[1] == (f'{wprefix2}/subdir2b', [], ['binary1.bin'])


@pytest.mark.parametrize('prefix', INDEXABLE_PREFIXES)
def test_walk_topdown(prefix):
    prefix = str(prefix)
    wprefix = prefix.replace('\\', '/')
    with FileCache(anonymous=True) as fc:
        pfx1 = fc.new_path(prefix, nthreads=23)
        results = []
        for path, dirs, files in pfx1.walk(top_down=False):
            assert path._nthreads == 23
            dirs.sort()
            files.sort()
            results.append((str(path), dirs, files))
        assert len(results) == 4
        assert results[3] == (wprefix, ['subdir1'], ['lorem1.txt'])
        assert results[2] == (f'{wprefix}/subdir1',
                              ['subdir2a', 'subdir2b'], ['lorem1.txt'])
        if results[1][0].endswith('2a'):
            assert results[1] == (f'{wprefix}/subdir1/subdir2a',
                                  [], ['binary1.bin'])
            assert results[0] == (f'{wprefix}/subdir1/subdir2b',
                                  [], ['binary1.bin'])
        else:
            assert results[0] == (f'{wprefix}/subdir1/subdir2a',
                                  [], ['binary1.bin'])
            assert results[1] == (f'{wprefix}/subdir1/subdir2b',
                                  [], ['binary1.bin'])

        prefix2 = f'{prefix}/subdir1'
        wprefix2 = prefix2.replace('\\', '/')
        pfx2 = fc.new_path(prefix2, nthreads=32)
        results = []
        for path, dirs, files in pfx2.walk(top_down=False):
            assert path._nthreads == 32
            dirs.sort()
            files.sort()
            results.append((str(path), dirs, files))
        assert len(results) == 3
        assert results[2] == (wprefix2, ['subdir2a', 'subdir2b'], ['lorem1.txt'])
        if results[1][0].endswith('2a'):
            assert results[1] == (f'{wprefix2}/subdir2a', [], ['binary1.bin'])
            assert results[0] == (f'{wprefix2}/subdir2b', [], ['binary1.bin'])
        else:
            assert results[0] == (f'{wprefix2}/subdir2a', [], ['binary1.bin'])
            assert results[1] == (f'{wprefix2}/subdir2b', [], ['binary1.bin'])


@pytest.mark.parametrize('prefix', INDEXABLE_PREFIXES)
@pytest.mark.parametrize('pattern', (
    (
        ('',
         '*',
         ['lorem1.txt', 'subdir1']),
        ('',
         '*i*',
         ['subdir1']),
        ('/subdir1',
         '*',
         ['lorem1.txt', 'subdir2a', 'subdir2b']),
        ('/subdir1',
         '**/*',  # Behavior of "**" changed in 3.13
         ['lorem1.txt',
          'subdir2a', 'subdir2a/binary1.bin',
          'subdir2b', 'subdir2b/binary1.bin']),
        ('/subdir1',
         'lorem1.txt',
         ['lorem1.txt']),
        ('/subdir1',
         'lo[mnopqrs]e[g-q]1.*',
         ['lorem1.txt']),
        ('/subdir1',
         'l*??.t??',
         ['lorem1.txt']),
        ('/subdir1',
         'l*??.t?',
         []),
        ('/subdir1',
         'l[a-c]rem1.txt',
         [])
    )
))
def test_glob(prefix, pattern):
    prefix = str(f'{prefix}{pattern[0]}')
    wprefix = prefix.replace('\\', '/')
    with FileCache(anonymous=True) as fc:
        pfx1 = fc.new_path(prefix)
        if prefix.startswith('gs://'):
            results = list(pfx1.glob(pattern[1]))
        else:
            results = list(pfx1.glob(FCPath(pattern[1])))
        results = sorted([x.path.replace(wprefix, '').lstrip('/') for x in results])
        assert results == pattern[2]


def test_glob_fail():
    list(FCPath(f'{GS_TEST_BUCKET_ROOT}/a/b').glob('a/b'))
    with pytest.raises(NotImplementedError):
        list(FCPath(f'{GS_TEST_BUCKET_ROOT}/a/b').glob('/a/b'))


@pytest.mark.parametrize('prefix', INDEXABLE_PREFIXES)
def test_rglob(prefix):
    prefix = str(prefix)
    wprefix = prefix.replace('\\', '/')
    with FileCache(anonymous=True) as fc:
        pfx = fc.new_path(prefix)
        if str(prefix).startswith('gs://'):
            results = list(pfx.rglob('binary1.bin'))
        else:
            results = list(pfx.rglob(FCPath('binary1.bin')))
        results = sorted([x.path.replace(str(wprefix), '').lstrip('/') for x in results])
        assert results == ['subdir1/subdir2a/binary1.bin',
                           'subdir1/subdir2b/binary1.bin']


def test_relative_to():
    assert FCPath('/a/b/c/d.txt').relative_to('/a/b/c') == FCPath('d.txt')
    assert FCPath('a/b/c/d.txt').relative_to('a/b/c') == FCPath('d.txt')
    with pytest.raises(ValueError):
        FCPath('/a/b/c/d.txt').relative_to('a/b/c')
    assert FCPath('C:/a/b/c/d.txt').relative_to('C:/a/b/c') == FCPath('d.txt')
    with pytest.raises(ValueError):
        FCPath('C:/a/b/c/d.txt').relative_to('/a/b/c')
    assert (FCPath(f'{GS_TEST_BUCKET_ROOT}/a/b/c/d.txt')
            .relative_to(f'{GS_TEST_BUCKET_ROOT}/a/b/c') == FCPath('d.txt'))
    with pytest.raises(ValueError):
        FCPath('/a/d/c/d.txt').relative_to('/a/b/c')
    with pytest.raises(ValueError):
        (FCPath(f'{GS_TEST_BUCKET_ROOT}/a/b/c/d.txt')
         .relative_to(f'{S3_TEST_BUCKET_ROOT}/a/b/c'))

    if sys.version_info >= (3, 12):
        assert FCPath('/a/b/c/d.txt').relative_to('/a/b/e', walk_up=True) == \
            FCPath('../c/d.txt')


def test_is_relative_to():
    assert FCPath('/a/b/c/d.txt').is_relative_to('/a/b/c')
    assert FCPath('a/b/c/d.txt').is_relative_to('a/b/c')
    assert not FCPath('/a/b/c/d.txt').is_relative_to('a/b/c')


def test_bad_threads():
    with pytest.raises(ValueError):
        FCPath('http://bucket/a/b/c', nthreads='a')
    with pytest.raises(ValueError):
        FCPath('http://bucket/a/b/c', nthreads=-1)


def test_relative_paths():
    f_cur_dir = FCPath.cwd()
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir).expanduser().resolve()
        try:
            os.chdir(str(temp_dir))
            with FileCache(cache_name=None) as fc:
                rp1 = 'file1.txt'
                rp2 = 'file2.txt'
                ap1 = temp_dir / rp1
                ap2 = temp_dir / rp2
                frp1 = fc.new_path(rp1)
                frp2 = fc.new_path(rp2)
                fap1 = fc.new_path(ap1)
                fap2 = fc.new_path(ap2)
                assert fap1.get_local_path() == ap1
                assert fap2.get_local_path() == ap2
                assert frp1.get_local_path() == ap1
                assert frp2.get_local_path() == ap2
                assert FCPath('').get_local_path([rp1, rp2]) == [ap1, ap2]

                frp2.touch()

                assert not fap1.exists()
                assert fap2.exists()
                assert not frp1.exists()
                assert frp2.exists()
                assert FCPath('').exists([rp1, rp2]) == [False, True]

                with pytest.raises(FileNotFoundError):
                    frp1.retrieve()
                assert frp2.retrieve() == ap2
                ret = FCPath('').retrieve([rp1, rp2], exception_on_fail=False)
                assert isinstance(ret[0], FileNotFoundError)
                assert ret[1] == ap2

                with pytest.raises(FileNotFoundError):
                    frp1.upload()
                assert frp2.upload() == ap2
                ret = FCPath('').upload([rp1, rp2], exception_on_fail=False)
                assert isinstance(ret[0], FileNotFoundError)
                assert ret[1] == ap2

                assert ap2.exists()
                frp2.unlink()
                assert not ap2.exists()

                frp1.touch()
                frp2.touch()
                assert ap1.exists()
                assert ap2.exists()
                FCPath('').unlink([rp1, rp2])
                assert not ap1.exists()
                assert not ap2.exists()

                frp1.touch()
                frp1.rename(frp2)
                assert not ap1.exists()
                assert ap2.exists()
                ap2.unlink()

                frp1.touch()
                frp1.rename(rp2)
                assert not ap1.exists()
                assert ap2.exists()
                ap2.unlink()

        finally:
            os.chdir(f_cur_dir.as_posix())

################################################################################
# tests/test_file_cache_path.py
################################################################################

from pathlib import Path

import pytest

from filecache import FileCache, FCPath


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
    assert FCPath._split_parts('c:') == ('c:', '', '')
    assert FCPath._split_parts('c:/') == ('c:', '/', '/')
    assert FCPath._split_parts('c:a/b') == ('c:', '', 'a/b')
    assert FCPath._split_parts('c:/a/b') == ('c:', '/', '/a/b')
    assert FCPath._split_parts(r'c:\a\b') == ('c:', '/', '/a/b')


def test_is_absolute():
    assert not FCPath._is_absolute('')
    assert not FCPath._is_absolute('a')
    assert not FCPath._is_absolute('a/b')
    assert not FCPath._is_absolute('c:')
    assert not FCPath._is_absolute('c:a')
    assert not FCPath._is_absolute('c:a/b')
    assert FCPath._is_absolute('/')
    assert FCPath._is_absolute('/a')
    assert FCPath._is_absolute('c:/')
    assert FCPath._is_absolute('c:/a')
    assert FCPath._is_absolute('gs://bucket')
    assert FCPath._is_absolute('gs://bucket/')
    assert FCPath._is_absolute('gs://bucket/a')
    assert FCPath._is_absolute('file:///a')

    assert not FCPath('').is_absolute()
    assert not FCPath('a').is_absolute()
    assert not FCPath('a/b').is_absolute()
    assert not FCPath('c:').is_absolute()
    assert not FCPath('c:a').is_absolute()
    assert not FCPath('c:a/b').is_absolute()
    assert FCPath('/').is_absolute()
    assert FCPath('/a').is_absolute()
    assert FCPath('c:/').is_absolute()
    assert FCPath('c:/a').is_absolute()
    assert FCPath('gs://bucket').is_absolute()
    assert FCPath('gs://bucket/').is_absolute()
    assert FCPath('gs://bucket/a').is_absolute()
    assert FCPath('file:///a').is_absolute()

def test__join():
    with pytest.raises(TypeError):
        FCPath._join(5)
    assert FCPath._join('') == ''
    assert FCPath._join('/') == '/'
    assert FCPath._join('c:/') == 'c:/'
    assert FCPath._join('a') == 'a'
    assert FCPath._join('a/') == 'a'
    assert FCPath._join('/a/b') == '/a/b'
    assert FCPath._join('/a/b/') == '/a/b'
    assert FCPath._join('', 'a') == 'a'
    assert FCPath._join('', '/a') == '/a'
    assert FCPath._join('a', 'b') == 'a/b'
    assert FCPath._join('/a', 'b') == '/a/b'
    assert FCPath._join('/', 'a', 'b') == '/a/b'
    assert FCPath._join('/a', '/b') == '/b'
    assert FCPath._join('/a', 'gs://bucket/a/b') == 'gs://bucket/a/b'
    assert FCPath._join('/a', 'c:/a/b') == 'c:/a/b'
    assert FCPath._join('/a', '/b/') == '/b'
    assert FCPath._join('/a', '') == '/a'
    assert FCPath._join('/a', Path('b', 'c'), FCPath('d/e')) == '/a/b/c/d/e'


def test__str():
    assert str(FCPath('a/b')) == 'a/b'
    assert str(FCPath(Path('a/b'))) == 'a/b'
    assert str(FCPath(r'\a\b')) == '/a/b'


def test_as_posix():
    assert FCPath('a/b').as_posix() == 'a/b'
    assert FCPath(Path('a/b')).as_posix() == 'a/b'
    assert FCPath(r'\a\b').as_posix() == '/a/b'


def test_drive():
    assert FCPath('/a/b').drive == ''
    assert FCPath('c:').drive == 'c:'
    assert FCPath('c:/').drive == 'c:'
    assert FCPath('gs://bucket/a/b').drive == 'gs://bucket'


def test_root():
    assert FCPath('').root == ''
    assert FCPath('a/b').root == ''
    assert FCPath('c:a/b').root == ''
    assert FCPath('/').root == '/'
    assert FCPath('/a/b').root == '/'
    assert FCPath('c:/a/b').root == '/'
    assert FCPath('gs://bucket/a/b').root == '/'


def test_anchor():
    assert FCPath('').anchor == ''
    assert FCPath('/').anchor == '/'
    assert FCPath('a/b').anchor == ''
    assert FCPath('/a/b').anchor == '/'
    assert FCPath('c:').anchor == 'c:'
    assert FCPath('c:a/b').anchor == 'c:'
    assert FCPath('c:/').anchor == 'c:/'
    assert FCPath('c:/a/b').anchor == 'c:/'
    assert FCPath('gs://bucket').anchor == 'gs://bucket/'
    assert FCPath('gs://bucket/').anchor == 'gs://bucket/'
    assert FCPath('gs://bucket/a/b').anchor == 'gs://bucket/'


def test__filename():
    assert FCPath._filename('') == ''
    assert FCPath._filename('a') == 'a'
    assert FCPath._filename('c:') == ''
    assert FCPath._filename('c:/') == ''
    assert FCPath._filename('/') == ''
    assert FCPath._filename('a/b') == 'b'
    assert FCPath._filename('/a/b') == 'b'
    assert FCPath._filename('gs://bucket') == ''
    assert FCPath._filename('gs://bucket/') == ''
    assert FCPath._filename('gs://bucket/a') == 'a'


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
        FCPath('a/b/c').with_name('c:')
    with pytest.raises(ValueError):
        FCPath('a/b/c').with_name('c:a')
    with pytest.raises(ValueError):
        FCPath('a/b/c').with_name('gs://bucket')
    with pytest.raises(ValueError):
        FCPath('a/b/c').with_name('gs://bucket/a')
    assert str(FCPath('').with_name('d')) == 'd'
    assert str(FCPath('/').with_name('d')) == '/d'
    assert str(FCPath('a/b/c').with_name('d')) == 'a/b/d'
    assert str(FCPath('a/b/c').with_name('c.txt')) == 'a/b/c.txt'
    assert str(FCPath('c:/a/b/c').with_name('d')) == 'c:/a/b/d'
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
        FCPath('a/b/c').with_stem('c:')
    with pytest.raises(ValueError):
        FCPath('a/b/c').with_stem('c:a')
    with pytest.raises(ValueError):
        FCPath('a/b/c').with_stem('gs://bucket')
    with pytest.raises(ValueError):
        FCPath('a/b/c').with_stem('gs://bucket/a')
    assert str(FCPath('').with_stem('d')) == 'd'
    assert str(FCPath('/').with_stem('d')) == '/d'
    assert str(FCPath('a/b/c').with_stem('d')) == 'a/b/d'
    assert str(FCPath('a/b/c.zip').with_stem('d')) == 'a/b/d.zip'
    assert str(FCPath('c:/a/b/c').with_stem('d')) == 'c:/a/b/d'
    assert str(FCPath('c:/a/b/c.zip').with_stem('d')) == 'c:/a/b/d.zip'
    assert str(FCPath('c:/a/b/c.txt.zip').with_stem('d')) == 'c:/a/b/d.zip'
    assert str(FCPath('c:/a/b/.zip').with_stem('d')) == 'c:/a/b/d'
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
        FCPath('a/b/c').with_suffix('c:')
    with pytest.raises(ValueError):
        FCPath('a/b/c').with_suffix('c:a')
    with pytest.raises(ValueError):
        FCPath('a/b/c').with_suffix('gs://bucket')
    with pytest.raises(ValueError):
        FCPath('a/b/c').with_suffix('gs://bucket/a')
    assert str(FCPath('a/b/c').with_suffix('')) == 'a/b/c'
    assert str(FCPath('a/b/c.txt').with_suffix('')) == 'a/b/c'
    assert str(FCPath('a/b/c').with_suffix('.txt')) == 'a/b/c.txt'
    assert str(FCPath('a/b/c.zip').with_suffix('.txt')) == 'a/b/c.txt'
    assert str(FCPath('c:/a/b/c').with_suffix('.txt')) == 'c:/a/b/c.txt'
    assert str(FCPath('c:/a/b/c.zip').with_suffix('.txt')) == 'c:/a/b/c.txt'
    assert str(FCPath('c:/a/b/c.txt.zip').with_suffix('.txt')) == 'c:/a/b/c.txt.txt'
    assert str(FCPath('c:/a/b/.zip').with_suffix('.txt')) == 'c:/a/b/.zip.txt'
    assert str(FCPath('gs://bucket/a/b/c.zip').with_suffix('.txt')) == 'gs://bucket/a/b/c.txt'

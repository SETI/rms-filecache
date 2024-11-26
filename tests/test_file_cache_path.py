################################################################################
# tests/test_file_cache_path.py
################################################################################

from pathlib import Path
import uuid

import pytest

from filecache import FileCache, FCPath


ROOT_DIR = Path(__file__).resolve().parent.parent
TEST_FILES_DIR = ROOT_DIR / 'test_files'
EXPECTED_DIR = TEST_FILES_DIR / 'expected'

EXPECTED_FILENAMES = ('lorem1.txt',
                      'subdir1/subdir2a/binary1.bin',
                      'subdir1/lorem1.txt',
                      'subdir1/subdir2b/binary1.bin')
LIMITED_FILENAMES = EXPECTED_FILENAMES[0:2]

HTTP_TEST_ROOT = 'https://storage.googleapis.com/rms-filecache-tests'
GS_WRITABLE_TEST_BUCKET_ROOT = 'gs://rms-filecache-tests-writable'


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


def test_default_filecache():
    with FileCache(delete_on_exit=True) as fc:
        p = FCPath(GS_WRITABLE_TEST_BUCKET_ROOT) / EXPECTED_FILENAMES[0]
        p2 = fc.new_path(GS_WRITABLE_TEST_BUCKET_ROOT) / EXPECTED_FILENAMES[0]
        assert p.get_local_path() == p2.get_local_path()
        p3 = FCPath(GS_WRITABLE_TEST_BUCKET_ROOT) / EXPECTED_FILENAMES[0]
        assert p2.get_local_path() == p3.get_local_path()


def test_operations_relative_paths():
    p = FCPath('b/c')
    with pytest.raises(ValueError):
        p.get_local_path()
    with pytest.raises(ValueError):
        p.get_local_path('d')
    with pytest.raises(ValueError):
        p.get_local_path(['d', 'e'])
    with pytest.raises(ValueError):
        p.retrieve()
    with pytest.raises(ValueError):
        p.retrieve('d')
    with pytest.raises(ValueError):
        p.retrieve(['d', 'e'])
    with pytest.raises(ValueError):
        p.exists()
    with pytest.raises(ValueError):
        p.exists('d')
    with pytest.raises(ValueError):
        p.exists(['d', 'e'])
    with pytest.raises(ValueError):
        p.upload()
    with pytest.raises(ValueError):
        p.upload('d')
    with pytest.raises(ValueError):
        p.upload(['d', 'e'])


def test_bad_threads():
    with pytest.raises(ValueError):
        FCPath('http://bucket/a/b/c', nthreads='a')
    with pytest.raises(ValueError):
        FCPath('http://bucket/a/b/c', nthreads=-1)


def test_unimplemented():
    with pytest.raises(NotImplementedError):
        FCPath('a').relative_to('b')
    with pytest.raises(NotImplementedError):
        FCPath('a').is_relative_to('b')
    with pytest.raises(NotImplementedError):
        FCPath('a').stat()
    with pytest.raises(NotImplementedError):
        FCPath('a').lstat()
    with pytest.raises(NotImplementedError):
        FCPath('a').is_dir()
    with pytest.raises(NotImplementedError):
        FCPath('a').is_mount()
    with pytest.raises(NotImplementedError):
        FCPath('a').is_symlink()
    with pytest.raises(NotImplementedError):
        FCPath('a').is_junction()
    with pytest.raises(NotImplementedError):
        FCPath('a').is_block_device()
    with pytest.raises(NotImplementedError):
        FCPath('a').is_char_device()
    with pytest.raises(NotImplementedError):
        FCPath('a').is_fifo()
    with pytest.raises(NotImplementedError):
        FCPath('a').is_socket()
    with pytest.raises(NotImplementedError):
        FCPath('a').samefile('a')
    with pytest.raises(NotImplementedError):
        FCPath('a').absolute()
    with pytest.raises(NotImplementedError):
        FCPath.cwd()
    with pytest.raises(NotImplementedError):
        FCPath('a').expanduser()
    with pytest.raises(NotImplementedError):
        FCPath.home()
    with pytest.raises(NotImplementedError):
        FCPath('a').readlink()
    with pytest.raises(NotImplementedError):
        FCPath('a').resolve()
    with pytest.raises(NotImplementedError):
        FCPath('a').symlink_to('a')
    with pytest.raises(NotImplementedError):
        FCPath('a').hardlink_to('a')
    with pytest.raises(NotImplementedError):
        FCPath('a').touch()
    with pytest.raises(NotImplementedError):
        FCPath('a').mkdir()
    with pytest.raises(NotImplementedError):
        FCPath('a').chmod(0)
    with pytest.raises(NotImplementedError):
        FCPath('a').lchmod(0)
    with pytest.raises(NotImplementedError):
        FCPath('a').unlink()
    with pytest.raises(NotImplementedError):
        FCPath('a').rmdir()
    with pytest.raises(NotImplementedError):
        FCPath('a').owner()
    with pytest.raises(NotImplementedError):
        FCPath('a').group()
    with pytest.raises(NotImplementedError):
        FCPath.from_uri('a')
    with pytest.raises(NotImplementedError):
        FCPath('a').as_uri()

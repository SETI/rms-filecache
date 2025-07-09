################################################################################
# tests/test_file_cache_source.py
################################################################################

import pathlib
from pathlib import Path
import pytest
import shutil
import os

from filecache import (FileCacheSource,
                       FileCacheSourceFile,
                       FileCacheSourceHTTP,
                       FileCacheSourceGS,
                       FileCacheSourceS3,
                       FileCacheSourceFake)

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

    with pytest.raises(ValueError):
        FileCacheSourceFake('not-fake', 'test-bucket')
    with pytest.raises(ValueError):
        FileCacheSourceFake('fake', 'hi/hi')
    with pytest.raises(ValueError):
        FileCacheSourceFake('fake', '')


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


def test_fake_source_init():
    """Test FileCacheSourceFake initialization."""
    FileCacheSourceFake.delete_default_storage_dir()
    try:
        # Test valid initialization
        fake = FileCacheSourceFake('fake', 'test-bucket')
        assert fake.schemes() == ('fake',)
        assert not fake.uses_anonymous()

        # Test invalid scheme
        with pytest.raises(ValueError, match='Scheme must be "fake"'):
            FileCacheSourceFake('not-fake', 'test-bucket')

        # Test empty remote
        with pytest.raises(ValueError, match='remote parameter must have a value'):
            FileCacheSourceFake('fake', '')
    finally:
        FileCacheSourceFake.delete_default_storage_dir()


def test_fake_source_default_storage_dir(tmp_path: Path):
    """Test the default storage directory management."""
    FileCacheSourceFake.delete_default_storage_dir()
    try:
        # Test getting default directory
        default_dir = FileCacheSourceFake.get_default_storage_dir()
        assert '.filecache_fake_remote' in str(default_dir)

        # Test setting default directory
        FileCacheSourceFake.set_default_storage_dir(tmp_path)
        assert FileCacheSourceFake.get_default_storage_dir() == tmp_path

        # Test creating a source uses the default directory
        fake = FileCacheSourceFake('fake', 'test-bucket')
        assert fake._storage_base == tmp_path

        # Test deleting default directory
        path = tmp_path / 'test.txt'
        path.write_text('test')
        assert path.exists()
        FileCacheSourceFake.delete_default_storage_dir()
        assert not path.exists()
    finally:
        FileCacheSourceFake.delete_default_storage_dir()


def test_fake_source_file_operations(tmp_path: Path):
    """Test basic file operations with FileCacheSourceFake."""
    FileCacheSourceFake.delete_default_storage_dir()
    try:
        fake = FileCacheSourceFake('fake', 'test-bucket', storage_dir=tmp_path)

        # Test file that doesn't exist
        assert not fake.exists('test.txt')

        # Test upload
        source_file = tmp_path / 'source.txt'
        source_file.write_text('test content')

        fake.upload('test.txt', source_file)
        assert fake.exists('test.txt')

        # Test retrieve
        dest_file = tmp_path / 'dest.txt'
        fake.retrieve('test.txt', dest_file)
        assert dest_file.read_text() == 'test content'

        # Test unlink
        fake.unlink('test.txt')
        assert not fake.exists('test.txt')

        # Test unlink missing file
        with pytest.raises(FileNotFoundError):
            fake.unlink('missing.txt')

        # Test unlink missing file with missing_ok=True
        fake.unlink('missing.txt', missing_ok=True)
    finally:
        FileCacheSourceFake.delete_default_storage_dir()


def test_fake_source_directory_operations(tmp_path: Path):
    """Test directory operations with FileCacheSourceFake."""
    FileCacheSourceFake.delete_default_storage_dir()
    try:
        fake = FileCacheSourceFake('fake', 'test-bucket', storage_dir=tmp_path)

        # Create some test files and directories
        (tmp_path / 'test-bucket/dir1').mkdir(parents=True)
        (tmp_path / 'test-bucket/dir1/file1.txt').write_text('content1')
        (tmp_path / 'test-bucket/dir2').mkdir(parents=True)
        (tmp_path / 'test-bucket/dir2/file2.txt').write_text('content2')
        (tmp_path / 'test-bucket/file3.txt').write_text('content3')

        # Test iterdir_type
        entries = list(fake.iterdir_type(''))
        assert len(entries) == 3

        # Sort entries for consistent testing
        entries.sort()

        # Test full paths and types
        assert entries[0] == ('fake://test-bucket/dir1', True)
        assert entries[1] == ('fake://test-bucket/dir2', True)
        assert entries[2] == ('fake://test-bucket/file3.txt', False)

        # Test subdirectory listing
        entries = list(fake.iterdir_type('dir1'))
        assert len(entries) == 1
        assert entries[0] == ('fake://test-bucket/dir1/file1.txt', False)
    finally:
        FileCacheSourceFake.delete_default_storage_dir()


def test_fake_source_error_cases(tmp_path: Path):
    """Test error cases with FileCacheSourceFake."""
    FileCacheSourceFake.delete_default_storage_dir()
    try:
        fake = FileCacheSourceFake('fake', 'test-bucket', storage_dir=tmp_path)

        # Test retrieve non-existent file
        with pytest.raises(FileNotFoundError):
            fake.retrieve('missing.txt', tmp_path / 'dest.txt')

        # Test upload non-existent file
        with pytest.raises(FileNotFoundError):
            fake.upload('dest.txt', tmp_path / 'missing.txt')

        # Test iterdir_type on non-existent directory
        assert list(fake.iterdir_type('missing-dir')) == []

        # Create a test file
        source_path = tmp_path / 'test-bucket/test.txt'
        source_path.parent.mkdir(parents=True, exist_ok=True)
        source_path.write_text('test content')

        # Create a destination directory that exists but is not writable
        dest_dir = tmp_path / 'dest'
        dest_dir.mkdir()
        dest_path = dest_dir / 'test.txt'

        # Mock shutil.copy2 to raise an error after creating the temp file
        original_copy2 = shutil.copy2

        def mock_copy2(src, dst):
            # Create the temp file
            with open(dst, 'w') as f:
                f.write('partial content')
            # Then raise an error
            raise OSError('Mock copy error')

        shutil.copy2 = mock_copy2
        try:
            with pytest.raises(OSError, match="Mock copy error"):
                fake.retrieve('test.txt', dest_path)
        finally:
            shutil.copy2 = original_copy2
    finally:
        FileCacheSourceFake.delete_default_storage_dir()


def test_fake_source_multi_operations(tmp_path: Path):
    """Test multi-file operations with FileCacheSourceFake."""
    FileCacheSourceFake.delete_default_storage_dir()
    try:
        fake = FileCacheSourceFake('fake', 'test-bucket', storage_dir=tmp_path)

        # Create test files
        files = []
        for i in range(3):
            path = tmp_path / f'source{i}.txt'
            path.write_text(f'content{i}')
            files.append(path)

        # Test exists_multi
        results = fake.exists_multi(['test1.txt', 'test2.txt', 'test3.txt'])
        assert results == [False, False, False]

        # Test upload_multi
        sub_paths = [f'test{i}.txt' for i in range(3)]
        results = fake.upload_multi(sub_paths[:2], files[:2])
        assert all(isinstance(r, Path) for r in results)

        # Test exists_multi after upload
        results = fake.exists_multi(sub_paths)
        assert results == [True, True, False]

        # Test retrieve_multi
        dest_files = [tmp_path / f'dest{i}.txt' for i in range(3)]
        results = fake.retrieve_multi(sub_paths, dest_files)
        assert isinstance(results[0], Path)
        assert isinstance(results[1], Path)
        assert isinstance(results[2], BaseException)
        assert dest_files[0].exists()
        assert dest_files[1].exists()
        assert not dest_files[2].exists()

        # Test unlink_multi
        results = fake.unlink_multi(sub_paths)
        assert isinstance(results[0], str)
        assert isinstance(results[1], str)
        assert isinstance(results[2], BaseException)
        assert not any(fake.exists(p) for p in sub_paths)
    finally:
        FileCacheSourceFake.delete_default_storage_dir()


def test_fake_source_atomic_operations(tmp_path: Path):
    """Test that uploads and downloads are atomic operations."""
    FileCacheSourceFake.delete_default_storage_dir()
    try:
        fake = FileCacheSourceFake('fake', 'test-bucket', storage_dir=tmp_path)

        # Test atomic upload
        source_file = tmp_path / 'source.txt'
        source_file.write_text('test content')

        # Verify upload creates temp file and then renames
        def mock_rename(src, dst):
            # Verify temp file exists and destination doesn't
            assert src.exists()
            assert not dst.exists()
            # Just verify it's a temp file with a UUID-like pattern
            assert '.txt_' in str(src)
            os.rename(str(src), str(dst))  # Use os.rename to avoid recursion

        original_rename = pathlib.Path.rename
        pathlib.Path.rename = mock_rename
        try:
            fake.upload('test.txt', source_file)
        finally:
            pathlib.Path.rename = original_rename

        # Test atomic download
        dest_file = tmp_path / 'dest.txt'

        # Verify download creates temp file and then renames
        def mock_rename2(src, dst):
            # Verify temp file exists and destination doesn't
            assert src.exists()
            assert not dst.exists()
            # Just verify it's a temp file with a UUID-like pattern
            assert '.txt_' in str(src)
            os.rename(str(src), str(dst))  # Use os.rename to avoid recursion

        pathlib.Path.rename = mock_rename2
        try:
            fake.retrieve('test.txt', dest_file)
        finally:
            pathlib.Path.rename = original_rename
    finally:
        FileCacheSourceFake.delete_default_storage_dir()

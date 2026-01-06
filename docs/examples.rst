Examples
========

FileCache Examples
******************

Here are some examples of how to use :class:`FileCache` in practice.

Example 1: Ephemeral Cache - Automatically Deleted on Exit
----------------------------------------------------------

This creates a unique, temporary cache that is automatically deleted when the
context manager exits. Useful for one-time operations where you don't want
to leave files behind.

.. code-block:: python

    from filecache import FileCache

    with FileCache(cache_name=None) as fc:
        # Retrieve a file from Google Cloud Storage
        local_path = fc.retrieve('gs://my-bucket/data/file.txt')
        with open(local_path, 'r') as f:
            content = f.read()
        print(f'Read {len(content)} bytes')
    # Cache is automatically deleted here


Example 2: Shared Persistent Cache (Default 'global')
-----------------------------------------------------

This uses the default shared cache named 'global' that persists after
program exit. Multiple processes can share the same downloaded files.

.. code-block:: python

    from filecache import FileCache

    fc = FileCache()  # Uses default cache_name='global'
    # First process downloads the file
    path1 = fc.retrieve('https://example.com/data.txt')
    # Second process (or same process later) reuses the cached file
    path2 = fc.retrieve('https://example.com/data.txt')
    assert path1 == path2  # Same cached file location
    # Cache persists after program exits


Example 3: Named Shared Cache with Custom Location
--------------------------------------------------

Creates a named cache in a specific directory. Useful for organizing
different types of cached data separately.

.. code-block:: python

    from filecache import FileCache
    from pathlib import Path

    # Create a cache named 'myproject' in a custom location
    fc = FileCache(cache_name='myproject', cache_root=Path.home() / 'my_caches')
    local_path = fc.retrieve('s3://my-bucket/project_data/file.dat')
    print(f'Cached at: {local_path}')
    # Cache persists and can be shared by other processes using the same name


Example 4: Time-Sensitive Cache with Metadata Caching
-----------------------------------------------------

Preserves file modification times and caches metadata for efficiency.
Useful when you need to track when files were last modified.

.. code-block:: python

    from filecache import FileCache

    fc = FileCache(time_sensitive=True, cache_metadata=True)
    # First call retrieves modification time from server
    mtime1 = fc.modification_time('gs://my-bucket/data.txt')
    # Second call uses cached value
    mtime2 = fc.modification_time('gs://my-bucket/data.txt')
    assert mtime1 == mtime2  # From cache, no network call

    # Retrieval downloads and sets modification time
    path1 = fc.retrieve('gs://my-bucket/data.txt')
    # Local file's modification time should match the server's
    assert path1.stat().st_mtime == mtime1


Example 5: Parallel File Operations
-----------------------------------

Downloads multiple files simultaneously using multiple threads.
This is significantly faster when retrieving many files from the same source.
Download errors are returned as Exceptions in the return list.

.. code-block:: python

    from filecache import FileCache

    fc = FileCache(nthreads=4)  # Use 4 threads for parallel operations
    urls = [
        'gs://my-bucket/file1.txt',
        'gs://my-bucket/file2.txt',
        'gs://my-bucket/file3.txt',
        'gs://my-bucket/file4.txt'
    ]
    # All files downloaded in parallel
    paths = fc.retrieve(urls, exception_on_fail=False)
    for path in paths:
        if isinstance(path, Exception):
            print(f'Download failed: {path}')
            continue
        print(f'Downloaded: {path}')


Example 6: Upload Operations
-----------------------------

Writes files to remote storage. Files are written locally first, then uploaded.

.. code-block:: python

    from filecache import FileCache

    with FileCache(cache_name=None) as fc:
        # Write a file to cloud storage
        with fc.open('gs://my-bucket/output.txt', 'w') as f:
            f.write('Hello, World!')
        # File is automatically uploaded when the context manager for the file handle exits
    # Ephemeral cache is automatically deleted here

    # Verify it was uploaded by reading it back using a difference cache
    with FileCache(cache_name=None) as fc:
        with fc.open('gs://my-bucket/output.txt', 'r') as f:
            content = f.read()
        print(f'File contents: {content}')
    # Ephemeral cache is automatically deleted here


.. _example_url_to_url:

Example 7: URL to URL Translation
---------------------------------

Translates URLs from one source to another, allowing code to work with
different data layouts without modification.

For example, assume the data is laid out on a webserver as:

.. code-block:: text

    /data/file11.txt
    /data/file12.txt
    /data/file21.txt
    /data/file22.txt

and the data is available in a cloud storage bucket as:

.. code-block:: text

    gs://my-bucket/data/dir1/file11.txt
    gs://my-bucket/data/dir1/file12.txt
    gs://my-bucket/data/dir2/file21.txt
    gs://my-bucket/data/dir2/file22.txt

The code was written using the layout of the webserver. Later, the Google Cloud Storage
bucket was made available. Rather than rewriting the code (and thus making it incompatible
with the original webserver layout), a mapping function can be used to translate the URLs:

.. code-block:: python

    def url_to_url(scheme, remote, path):
        if scheme == "https" and remote == "data.com" and path.startswith("data/"):
            dir_num_match = re.match(r"data/file(\d+)\.txt", path)
            if dir_num_match:
                dir_num = dir_num_match.group(1)
                return f"gs://my-bucket/data/dir{dir_num[0]}/file{dir_num}.txt"
        return None

This code will now work both with the original webserver layout and the new Google Cloud Storage
layout:

.. code-block:: python

    fc = FileCache()
    # This will download from the webserver
    fc.retrieve("https://data.com/data/file11.txt")

    fc = FileCache(url_to_url=url_to_url)
    # This will download from the Google Cloud Storage bucket
    fc.retrieve("https://data.com/data/file11.txt")


.. _example_url_to_path:

Example 8: URL to Path Translation
----------------------------------

Translates URLs into local paths, allowing code to store files in the local cache in a
different hierarchy than the remote source. Assume the data is available in a cloud
storage bucket as:

.. code-block:: text

    gs://my-bucket/data/dir1/file11.txt
    gs://my-bucket/data/dir1/file12.txt
    gs://my-bucket/data/dir2/file21.txt
    gs://my-bucket/data/dir2/file22.txt

To store the above Google Cloud Storage data in a flat hierarchy like this:

.. code-block:: text

    /data/file11.txt
    /data/file12.txt
    /data/file21.txt
    /data/file22.txt

.. code-block:: python

    def url_to_path(scheme, remote, path, cache_dir, cache_subdir):
        if scheme == "gs" and remote == "my-bucket" and path.startswith("data/dir"):
            path_split = path.split("/", 2)
            if len(path_split) > 2:
                new_path = path_split[2]
                ret = f"{cache_dir}/{cache_subdir}/data/{new_path}"
                print(ret)
                return ret
        return None

    fc = FileCache()
    # This file will be stored in <cache_root>/_filecache_global/gs_my-bucket/data/dir1/file11.txt
    fc.retrieve("gs://my-bucket/data/dir1/file11.txt")

    fc = FileCache(url_to_path=url_to_path)
    # This file will be stored in <cache_root>/_filecache_global/gs_my-bucket/data/file11.txt
    fc.retrieve("gs://my-bucket/data/dir1/file11.txt")


Example 9: Local File Access
------------------------------

:class:`FileCache` can also work with local files, providing a unified interface
regardless of file location.

.. code-block:: python

    from filecache import FileCache
    from pathlib import Path

    fc = FileCache()
    # Access a local file (no download needed, accessed in-place)
    local_file = Path.home() / 'myfile.txt'
    local_file.write_text('Local content')

    # FileCache handles local files transparently
    path = fc.retrieve(str(local_file))
    with open(path, 'r') as f:
        content = f.read()
    print(f'Read local file: {content}')


Example 10: Manual Cache Management (Non-Context Manager)
---------------------------------------------------------

When using a shared cache, you must manually manage cache deletion.
Useful when you need more control over cache lifetime.

.. code-block:: python

    from filecache import FileCache

    # Create a permanent cache (would not auto-delete on program exit)
    fc = FileCache(cache_name='mycache')

    # Use the cache
    path1 = fc.retrieve('gs://my-bucket/file1.txt')
    path2 = fc.retrieve('gs://my-bucket/file2.txt')

    # Manually delete cache when done
    # Be careful that another process is not using the cache at the same time!
    fc.delete_cache()
    print('Cache manually deleted')


FCPath Examples
***************

Here are examples showcasing the unique features of :class:`FCPath`, which provides a
Path-like interface for working with remote files.


Example 11: Using FCPath for Simpler Syntax
-------------------------------------------

Here are examples of creating :class:`FCPath` instances and using them to access files.

.. code-block:: python

    from filecache import FileCache, FCPath

    with FileCache(cache_name=None) as fc:
        # Create an FCPath that encapsulates the cache settings
        base_path = fc.new_path('gs://my-bucket/data')

        # Use Path-like operations
        file1 = base_path / 'subdir' / 'file1.txt'
        file2 = base_path / 'subdir' / 'file2.txt'

        # Read files using simple Path methods
        content1 = file1.read_text()
        content2 = file2.read_text()
        print(f'Read {len(content1)} and {len(content2)} bytes')

The same code but using the :class:`FCPath` constructor directly:

.. code-block:: python

    from filecache import FileCache, FCPath

    fc = FileCache(cache_name=None)
    base_path = FCPath('gs://my-bucket/data', filecache=fc)

    file1 = base_path / 'subdir' / 'file1.txt'
    file2 = base_path / 'subdir' / 'file2.txt'

    content1 = file1.read_text()
    content2 = file2.read_text()
    print(f'Read {len(content1)} and {len(content2)} bytes')


Example 12: Path Joining with the / Operator
--------------------------------------------

:class:`FCPath` supports the ``/`` operator for joining paths, just like `pathlib.Path`.
The new :class:`FCPath` inherits all settings (FileCache, time_sensitive, etc.) from the
left-hand side.

.. code-block:: python

    from filecache import FileCache, FCPath

    with FileCache(cache_name=None, time_sensitive=True) as fc:
        # Create a base FCPath with specific settings
        base = fc.new_path('gs://my-bucket/data')

        # Join paths using the / operator
        file1 = base / 'subdir' / 'file1.txt'
        file2 = base / 'subdir' / 'file2.txt'

        # All resulting FCPath objects inherit the FileCache and time_sensitive setting
        content1 = file1.read_text()  # Uses same cache and time_sensitive=True
        content2 = file2.read_text()  # Uses same cache and time_sensitive=True


Example 13: Creating FCPath from Different Types
-------------------------------------------------

:class:`FCPath` can be created from strings, `Path` objects, or other `FCPath` objects.
When created from an existing :class:`FCPath`, it inherits all settings.

.. code-block:: python

    from filecache import FileCache, FCPath
    from pathlib import Path

    with FileCache(cache_name='myproject', time_sensitive=True) as fc:
        # Create from string
        path1 = FCPath('gs://my-bucket/data/file.txt', filecache=fc)

        # Create from Path object
        local_path = Path('/local/path/file.txt')
        path2 = FCPath(local_path, filecache=fc)

        # Create from another FCPath (inherits all settings)
        path3 = FCPath(path1)  # Inherits filecache, time_sensitive, etc. from path1
        path4 = path1 / 'subdir' / 'file2.txt'  # Also inherits via / operator

        # All can be used the same way
        content1 = path1.read_text()
        content2 = path2.read_text()
        content3 = path3.read_text()


Example 14: Inheriting FileCache Settings
------------------------------------------

When creating new :class:`FCPath` objects from existing ones, all FileCache-related
settings are automatically inherited, making it easy to work with paths that share
the same configuration.

.. code-block:: python

    from filecache import FileCache, FCPath

    # Create a FileCache with specific settings
    fc = FileCache(cache_name='project', time_sensitive=True, nthreads=4)

    # Create base FCPath with a different lock timeout
    base = fc.new_path('gs://my-bucket/data', lock_timeout=30)

    # All child paths inherit: filecache, lock_timeout=30
    file1 = base / 'dir1' / 'file1.txt'
    file2 = base / 'dir2' / 'file2.txt'
    file3 = FCPath(base) / 'dir3' / 'file3.txt'

    # All operations use the same settings
    paths = [file1, file2, file3]
    contents = [f.read_text() for f in paths]  # All use time_sensitive=True, lock_timeout=30


Example 15: Using glob() for Pattern Matching
---------------------------------------------

:class:`FCPath` supports `glob()` for pattern matching on remote directories, a feature
not available directly in :class:`FileCache`. This works on both local and remote paths.

.. code-block:: python

    from filecache import FileCache, FCPath

    with FileCache(cache_name=None) as fc:
        # Create base path to a directory
        base_dir = fc.new_path('gs://my-bucket/data')

        # Find all .txt files in the directory
        txt_files = list(base_dir.glob('*.txt'))
        for txt_file in txt_files:
            print(f'Found: {txt_file.path}')
            content = txt_file.read_text()

        # Find files in subdirectories
        all_txt = list(base_dir.glob('**/*.txt'))  # Recursive
        for file in all_txt:
            print(f'Recursive match: {file.path}')


Example 16: Using rglob() for Recursive Pattern Matching
--------------------------------------------------------

:meth:`FCPath.rglob()` is a convenience method that automatically adds ``**/`` to the pattern,
making recursive searches easier.

.. code-block:: python

    from filecache import FileCache, FCPath

    with FileCache() as fc:
        base = fc.new_path('gs://my-bucket/project')

        # Find all Python files recursively
        # rglob('*.py') is equivalent to glob('**/*.py')
        python_files = list(base.rglob('*.py'))

        for py_file in python_files:
            print(f'Python file: {py_file.path}')
            # Process each file
            content = py_file.read_text()


Example 17: Directory Traversal with iterdir()
----------------------------------------------

:class:`FCPath` provides :meth:`FCPath.iterdir()` for iterating over directory contents, returning
:class:`FCPath` objects that inherit settings from the parent.

.. code-block:: python

    from filecache import FileCache, FCPath

    with FileCache(cache_name=None) as fc:
        base_dir = fc.new_path('gs://my-bucket/data')

        # Iterate over directory contents
        for item in base_dir.iterdir():
            if item.is_dir():
                print(f'Directory: {item.path}')
                # Recursively process subdirectories
                for subitem in item.iterdir():
                    print(f'  Subitem: {subitem.path}')
            else:
                print(f'File: {item.path}')
                # Read file content
                content = item.read_text()


Example 18: Directory Walking with walk()
-----------------------------------------

The `walk()` method provides a convenient way to traverse directory trees, similar to
`os.walk()` but returning :class:`FCPath` objects.

.. code-block:: python

    from filecache import FileCache, FCPath

    with FileCache() as fc:
        root = fc.new_path('gs://my-bucket/project')

        # Walk the directory tree
        for dirpath, dirnames, filenames in root.walk():
            print(f'Directory: {dirpath.path}')
            print(f'  Subdirectories: {dirnames}')
            print(f'  Files: {filenames}')

            # Process files in this directory
            for filename in filenames:
                file_path = dirpath / filename
                content = file_path.read_text()


Example 19: Combining Path Operations
-------------------------------------

:class:`FCPath` supports chaining of path operations, making complex path manipulations
easy and readable.

.. code-block:: python

    from filecache import FileCache, FCPath

    with FileCache(cache_name='analysis') as fc:
        # Start with a base URL
        base = fc.new_path('gs://data-bucket')

        # Build complex paths using chaining
        data_file = base / 'year' / '2024' / 'month' / '01' / 'data.csv'

        # Read both files
        data = data_file.read_text()
        config = (base / 'config' / 'settings.json').read_text()

        # Create output path in same structure
        output_dir = base / 'output' / '2024' / '01'

        # Write to output
        (output_dir / 'results.txt').write_text('Analysis results')


Example 20: Working with Metadata via iterdir_metadata()
--------------------------------------------------------

The `iterdir_metadata()` method provides directory contents along with metadata
(is_dir, mtime, size), useful for filtering or sorting files.

.. code-block:: python

    from filecache import FileCache, FCPath

    with FileCache() as fc:
        base_dir = fc.new_path('gs://my-bucket/data')

        # Get directory contents with metadata
        files_by_size = []
        for item, metadata in base_dir.iterdir_metadata():
            if metadata and not metadata['is_dir']:
                files_by_size.append((item, metadata['size']))

        # Sort by size (largest first)
        files_by_size.sort(key=lambda x: x[1] or 0, reverse=True)

        # Process largest files first
        for item, size in files_by_size:
            print(f'Processing {item.path} ({size} bytes)')
            content = item.read_text()


Example 21: Creating FCPath Without Explicit FileCache
-------------------------------------------------------

When an :class:`FCPath` is created without specifying a `filecache`, it uses the default
global :class:`FileCache` when an operation is performed. This allows for simpler
syntax when the default cache is sufficient.

.. code-block:: python

    from filecache import FCPath

    # Create FCPath without explicit FileCache
    # Will use default global cache when operations are performed
    base = FCPath('gs://my-bucket/data')

    # Operations automatically use the default FileCache
    file1 = base / 'file1.txt'
    file2 = base / 'file2.txt'

    # Read files (uses default global cache)
    content1 = file1.read_text()
    content2 = file2.read_text()

    # All paths share the same default cache
    assert file1.filecache == file2.filecache

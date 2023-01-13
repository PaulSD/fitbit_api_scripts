# Module to provide atomicity, durability, and/or isolation for file operations.
# (A, D, and I in the "ACID" transaction properties of most database management systems.)
#
# Naive file handling (in any programming language) DOES NOT provide atomicity, durability, or
# isolation.  Programs that require these properties must follow specific procedures to support
# them, and must accept the trade-offs (performance impacts, additional disk space consumption,
# additional complexity, etc) associated with those procedures.
#
# For example, consider the following naive code:
# ```
#   with open(file_name, 'r+') as f:
#     contents = f.read()
#     new_contents = contents + 1
#     f.seek(0)
#     f.truncate(0)
#     f.write(new_contents)
# ```
# If the process crashes during the `f.write()` call, then the file may be left empty or partially
# written but contiguous.  (The write is not atomic.)
# If the system crashes during the `f.write()` call, then (depending on underlying IO/caching
# behavior) the file may be left empty, partially written but contiguous, partially written but
# discontiguous, complete but containing a mixture of original and new data, etc.  (The write is
# not atomic.)
# If the system crashes after the `with` block returns, then the behavior may be the same as if it
# crashes during the `f.write()` call.  (The write is not durable.)
# If the same code runs concurrently in two separate processes, then `f.read()` in one process may
# return incomplete data or no data (depending on when it is called relative to `f.truncate()` and
# `f.write()` in the other process), and the concurrent writes may cause a mixture of the two writes
# to be written.  (The operations are not isolated.)
#
# This module provides a general implementation for achieving all or some (as requested by the
# caller) of these properties.
# Other implementations (that follow different procedures or use slightly different implementation
# options for the same procedures) are possible.  However, the implementation options are limited,
# and the alternative procedure options have additional trade-offs that must be considered.
#
# This module's atomicity and durability implementations are portable, however the isolation
# implementation currently only supports Unix systems.

import os
import shutil
import stat
import signal
import errno
import threading
try:
  import fcntl
  have_fcntl = True
except ImportError:
  have_fcntl = False
from contextlib import contextmanager


# Default prefix/suffix used for the lock file and new file created during atomic file writes.
DEFAULT_ATOMIC_LOCK_FILE_PREFIX = '.'
DEFAULT_ATOMIC_LOCK_FILE_SUFFIX = '.lock'
DEFAULT_ATOMIC_FILE_PREFIX = ''
DEFAULT_ATOMIC_FILE_SUFFIX = '.new'

DEFAULT_LOCK_TIMEOUT = 30

# Implementation Notes:
#
# The only portable way to provide atomicity at the file level is to write into a new file, flush
# the write buffers, fsync or fdatasync the new file, then rename the file to replace the existing
# file with the new one.  The flush and fsync are required to ensure that the new file's data blocks
# are written to disk before the filesystem metadata changes (made by the rename operation) are
# written, as blocks may normally be written to disk in a different order than they were written by
# processes.  fsync/fdatasync is not needed if the O_SYNC or O_DSYNC flag was passed to open() on a
# Unix system.
# Some filesystems support other mechanisms to provide atomicity, but those are not portable.
# There are other ways to provide atomicity on top of files without losing portability, but they all
# require creating another layer on top of files; For example, by implementing a
# log/journal/copy-on-write/etc scheme that is independent of the underlying filesystem, as many
# database management systems do.  This means that the files cannot be used directly without this
# additional layer.  (Which implies that this atomicity is not implemented at the file level, and is
# instead implemented at a different level.)
#
# To provide durability only, flush and fsync the file after writing.
# To provide atomicity+durability, fsync the directory containing the file after renaming the file
# as described above for atomicity.
#
# Isolation may be implemented using locks/mutexes/semaphores between processes.
# On Unix systems, flock (BSD API) or lockf (POSIX API) are convenient and portable lock options.
# However, note that these are both advisory locking mechanisms, and the two APIs are handled
# independently by the OS.  To ensure isolation, the same locking API must be explicitly used by all
# processes that use the relevant files; Isolation is not provided if different processes use
# different APIs or if a naive process does not use any lock API.
# This module uses flock to permit coordinating isolation with shell scripts.  (Shell scripts have
# easy access to the flock API via the `flock` command, but do not have easy access to the lockf
# API.)
# To provide isolation only or isolation+durability, flock may be used on the file itself to
# synchronize both reads and writes across processes.
# To provide isolation+atomicity or isolation+atomicity+durability, flock may be used on a separate
# file or directory to synchronize writes only.  (The atomic behavior eliminates the need to
# synchronize reads except where a write depends on a read.  However, due to the file replacement
# behavior described above for atomicity, flock cannot be used on the file being written and must be
# used on a separate file or directory.)
#
# References:
# https://lwn.net/Articles/457667/
#   Explanation of the atomic and durable write processes.
# https://www.slideshare.net/nan1nan1/eat-my-data
#   https://stackoverflow.com/questions/7433057/is-rename-without-fsync-safe
#   Explanation of the atomic write process and some common mistakes.
# https://bugs.launchpad.net/ubuntu/+source/linux/+bug/317781/comments/45
#   List of work-arounds implemented in ext4 to attempt to handle applications that need atomic
#   atomic writes but don't actually implement them.
# https://lwn.net/Articles/789600/
#   https://ostconf.com/system/attachments/files/000/001/370/original/Christoph_Hellwig.pdf?1509644424
#   Discussions about adding additional atomic write support to some Linux filesystems.
# https://pypi.org/project/atomicwrites/
#   Proper implementation of atomic and durable write.  However, does not preserve file permissions/
#   attributes, and not sure why it uses os.path.normpath() everywhere.
# https://stackoverflow.com/questions/2333872/how-to-make-file-creation-an-atomic-operation
#   Proper implementation of atomic write.  However, note that it doesn't fsync the directory for
#   durability.
#
# Bad examples/documentation:
# https://alexwlchan.net/2019/03/atomic-cross-filesystem-moves-in-python/
#   Improper implementation of atomic write.  Does not fsync after writing and before renaming.
# http://stupidpythonideas.blogspot.com/2014/07/getting-atomic-writes-right.html
#   Improper implementation of atomic write.  Does not flush/fsync the file after writing and before
#   renaming.  tempfile writes the file to a temporary path by default, which may be on a different
#   filesystem and therefore may make the rename non-atomic.  tempfile may not create a
#   re-readable/rename-able file on all platforms.
# https://github.com/msiemens/tinydb/blob/master/tinydb/storages.py
#   Database implementation that provides durability but not atomicity.
# https://github.com/fredysomy/pysonDB/blob/master/pysondb/db.py
#   Database implementation that uses naive writes.
# https://github.com/gunthercox/jsondb/tree/master/jsondb
#   Database implementation that uses naive writes.

# Open a file for an atomic, isolated, and durable write or read+write.
# (Ensure that the file on disk will contain either the original state or the new state even if the
# system crashes in the middle of the write, that concurrent reads will see either the original or
# the new state, that concurrent writes or read+writes cannot interfere with each other, and that
# the file cannot revert to the original state after this function returns.)
#
# Internally, this uses a lock on a separate file or directory to isolate the write/read+write, then
# it creates a new file for writing into and provides an open file handle for the new file to the
# `with` block.  When the block exits, file permissions and xattrs are copied to the new file, the
# original file is atomically replaced by the new file, and `fsync()` is called on the directory
# containing the file to flush the rename to disk.
# By default, a separate lock file is created for every file that is written, however a shared file
# or directory may be used instead to avoid cluttering the filesystem with lock files.  However,
# note that using a shared lock will serialize write operations across all files that use the same
# lock file/directory.
# The new file will initially be empty.  The caller may open the original file as a separate file
# handle to read the original data while writing.
#
# Primary trade-offs of this implementation:
# Two full copies of the file will exist on the disk at the same time.  Creating the second full
# copy may have a significant impact on both performance and disk space consumption, particularly
# for large files.
# Write/read+write operations are delayed while other write/read+write operations are being
# performed.
# Immediately writing data to disk may impact performance (by eliminating write
# optimizations/merging and forcing this process to wait for the writes to complete).
#
# Usage:
# ```
#   o = filesystem.open_for_atomic_isolated_durable_read_write
#   with o(file) as f:
#     f.write('...')
# ```
# This function accepts the same arguments as the Python built-in `open()` function, except that the
# 'r' and 'a' `mode` characters are not permitted, if `mode` is not specified then 'w' will be used
# by default instead of the `open()` default of 'r', and if `perms` is specified then `opener`
# cannot be specified.
# This function also accepts additional arguments:
# If `lock_file` is specified then it will be used as the lock file/directory.  The same
# file/directory may be used for write operations for multiple files (to synchronize write
# operations on all of those files).
# If `lock_file` is not specified and `lock_dir` is `True` then the directory containing the
# specified `file_name` will be used as the lock directory.
# If `lock_file` and `lock_dir` are not specified then `lock_prefix` and `lock_suffix` will be used
# to derive a lock file name from `file_name`.
# The `timeout` argument specifies a time limit in seconds to wait for other processes to finish
# writing before this process can enter the `with` block.  If this time limit is reached then an
# exception will be thrown.  A `timeout` of '0' disables the time limit.  The time limit
# implementation only works in the main thread; The time limit is ignored when this is used in a
# different thread.
# `prefix` and `suffix` may be used to adjust the temporary name of the new file.
# `perms` may be used to make the temporary initial permissions of the new file more restrictive
# than the umask.
# `remove_new_on_exception` determines whether the new file will be removed (`True`) or left in
# place (`False`) if any exceptions are thrown.  If it is left in place then it will be overwritten
# by a subsequent call to this function unless the 'x' `mode` character is specified.  Default is
# `True`.
#
# To ensure proper isolation, all processes and operations that write the relevant file must use
# `open_for_atomic_isolated_durable_read_write()`, `open_for_atomic_isolated_read_write()`, or
# `flock()` with `LOCK_EX` with the same lock file.  However, operations that read the relevant file
# do not need to be isolated/synchronized except where a write depends on a read (in which case the
# read should occur within the write lock).
#
@contextmanager
def open_for_atomic_isolated_durable_read_write(
  file_name,
  lock_file=None, lock_dir=False,
  lock_prefix=DEFAULT_ATOMIC_LOCK_FILE_PREFIX, lock_suffix=DEFAULT_ATOMIC_LOCK_FILE_SUFFIX,
  timeout=DEFAULT_LOCK_TIMEOUT,
  perms=None,
  *args, **kwargs,
):
  with _open_with_lock(
    lock_file=lock_file, base_file=file_name, lock_dir=lock_dir,
    prefix=lock_prefix, suffix=lock_suffix,
    perms=perms, lock_type='write', timeout=timeout,
  ):
    with open_for_atomic_durable_write(file_name, perms=perms, *args, **kwargs) as f:
      yield f

# Open a file for an atomic and isolated write or read+write, without durability.
# (Ensure that the file on disk will contain either the original state or the new state even if the
# system crashes in the middle of the write, that concurrent reads will see either the original or
# the new state, and that concurrent writes or read+writes cannot interfere with each other.
# However, if the system crashes after the write then the system may or may not revert to the
# original state.)
#
# Internally, this uses a lock on a separate file or directory to isolate the write/read+write, then
# it creates a new file for writing into and provides an open file handle for the new file to the
# `with` block.  When the block exits, file permissions and xattrs are copied to the new file, then
# the original file is atomically replaced by the new file.
# By default, a separate lock file is created for every file that is written, however a shared file
# or directory may be used instead to avoid cluttering the filesystem with lock files.  However,
# note that using a shared lock will serialize write operations across all files that use the same
# lock file/directory.
# The new file will initially be empty.  The caller may open the original file as a separate file
# handle to read the original data while writing.
#
# Primary trade-offs of this implementation:
# Two full copies of the file will exist on the disk at the same time.  Creating the second full
# copy may have a significant impact on both performance and disk space consumption, particularly
# for large files.
# Write/read+write operations are delayed while other write/read+write operations are being
# performed.
# Immediately writing data to disk may impact performance (by eliminating write
# optimizations/merging and forcing this process to wait for the writes to complete).
#
# The additional performance impact of adding durability to this implementation (by calling
# `fsync()` on the directory containing the file to flush the rename to disk) is minimal, so you may
# want to consider using `open_for_atomic_isolated_durable_read_write()` instead of this, even if
# you don't strictly need durability.
#
# Usage:
# ```
#   o = filesystem.open_for_atomic_isolated_read_write
#   with o(file) as f:
#     f.write('...')
# ```
# This function accepts the same arguments as the Python built-in `open()` function, except that the
# 'r' and 'a' `mode` characters are not permitted, if `mode` is not specified then 'w' will be used
# by default instead of the `open()` default of 'r', and if `perms` is specified then `opener`
# cannot be specified.
# This function also accepts additional arguments:
# If `lock_file` is specified then it will be used as the lock file/directory.  The same
# file/directory may be used for write operations for multiple files (to synchronize write
# operations on all of those files).
# If `lock_file` is not specified and `lock_dir` is `True` then the directory containing the
# specified `file_name` will be used as the lock directory.
# If `lock_file` and `lock_dir` are not specified then `lock_prefix` and `lock_suffix` will be used
# to derive a lock file name from `file_name`.
# The `timeout` argument specifies a time limit in seconds to wait for other processes to finish
# writing before this process can enter the `with` block.  If this time limit is reached then an
# exception will be thrown.  A `timeout` of '0' disables the time limit.  The time limit
# implementation only works in the main thread; The time limit is ignored when this is used in a
# different thread.
# `prefix` and `suffix` may be used to adjust the temporary name of the new file.
# `perms` may be used to make the temporary initial permissions of the new file more restrictive
# than the umask.
# `remove_new_on_exception` determines whether the new file will be removed (`True`) or left in
# place (`False`) if any exceptions are thrown.  If it is left in place then it will be overwritten
# by a subsequent call to this function unless the 'x' `mode` character is specified.  Default is
# `True`.
#
# To ensure proper isolation, all processes and operations that write the relevant file must use
# `open_for_atomic_isolated_durable_read_write()`, `open_for_atomic_isolated_read_write()`, or
# `flock()` with `LOCK_EX` with the same lock file.  However, operations that read the relevant file
# do not need to be isolated/synchronized except where a write depends on a read (in which case the
# read should occur within the write lock).
#
@contextmanager
def open_for_atomic_isolated_read_write(
  file_name,
  lock_file=None, lock_dir=False,
  lock_prefix=DEFAULT_ATOMIC_LOCK_FILE_PREFIX, lock_suffix=DEFAULT_ATOMIC_LOCK_FILE_SUFFIX,
  timeout=DEFAULT_LOCK_TIMEOUT,
  perms=None,
  *args, **kwargs,
):
  with _open_with_lock(
    lock_file=lock_file, base_file=file_name, lock_dir=lock_dir,
    prefix=lock_prefix, suffix=lock_suffix,
    perms=perms, lock_type='write', timeout=timeout,
  ):
    with open_for_atomic_write(file_name, perms=perms, *args, **kwargs) as f:
      yield f

# Open a file for an atomic and durable write, without write isolation.
# (Ensure that the file on disk will contain either the original state or the new state even if the
# system crashes in the middle of the write, and ensure that the file cannot revert to the original
# state after this function returns.  This also inherently provides isolation for read only
# operations, since a concurrent process that reads the file is guaranteed to see either the
# original state or the new state.  However, concurrent writes may cause lost or mixed writes, and
# concurrent read+writes may cause interspersed read and write operations.)
#
# Internally, this creates a new file for writing into and provides an open file handle for the new
# file to the `with` block.  When the block exits, file permissions and xattrs are copied to the new
# file, the original file is atomically replaced by the new file, and `fsync()` is called on the
# directory containing the file to flush the rename to disk.
# The new file will initially be empty.  The caller may open the original file as a separate file
# handle to read the original data while writing.
#
# Primary trade-offs of this implementation:
# Two full copies of the file will exist on the disk at the same time.  Creating the second full
# copy may have a significant impact on both performance and disk space consumption, particularly
# for large files.
#
# Usage:
# ```
#   o = filesystem.open_for_atomic_durable_write
#   with o(file) as f:
#     f.write('...')
# ```
# This function accepts the same arguments as the Python built-in `open()` function, except that the
# 'r' and 'a' `mode` characters are not permitted, if `mode` is not specified then 'w' will be used
# by default instead of the `open()` default of 'r', and if `perms` is specified then `opener`
# cannot be specified.
# This function also accepts additional arguments:
# `prefix` and `suffix` may be used to adjust the temporary name of the new file.
# `perms` may be used to make the temporary initial permissions of the new file more restrictive
# than the umask.
# `remove_new_on_exception` determines whether the new file will be removed (`True`) or left in
# place (`False`) if any exceptions are thrown.  If it is left in place then it will be overwritten
# by a subsequent call to this function unless the 'x' `mode` character is specified.  Default is
# `True`.
#
@contextmanager
def open_for_atomic_durable_write(file_name, *args, **kwargs):
  with open_for_atomic_write(file_name, *args, **kwargs) as f:
    yield f
  f = os.open((os.path.dirname(file_name) or '.'), os.O_RDONLY|os.O_DIRECTORY)
  try:
    _fsync(f)
  finally:
    os.close(f)

# Open a file for an atomic write, without durability or write isolation.
# (Ensure that the file on disk will contain either the original state or the new state even if the
# system crashes in the middle of the write.  This also inherently provides isolation for read only
# operations, since a concurrent process that reads the file is guaranteed to see either the
# original state or the new state.  However, if the system crashes after the write then the system
# may or may not revert to the original state, concurrent writes may cause lost or mixed writes, and
# concurrent read+writes may cause interspersed read and write operations.)
#
# Internally, this creates a new file for writing into and provides an open file handle for the new
# file to the `with` block.  When the block exits, file permissions and xattrs are copied to the new
# file, then the original file is atomically replaced by the new file.
# The new file will initially be empty.  The caller may open the original file as a separate file
# handle to read the original data while writing.
#
# Primary trade-offs of this implementation:
# Two full copies of the file will exist on the disk at the same time.  Creating the second full
# copy may have a significant impact on both performance and disk space consumption, particularly
# for large files.
#
# The additional performance impact of adding durability to this implementation (by calling
# `fsync()` on the directory containing the file to flush the rename to disk) is minimal, so you may
# want to consider using `open_for_atomic_durable_write()` instead of this, even if you don't
# strictly need durability.
#
# Usage:
# ```
#   o = filesystem.open_for_atomic_write
#   with o(file) as f:
#     f.write('...')
# ```
# This function accepts the same arguments as the Python built-in `open()` function, except that the
# 'r' and 'a' `mode` characters are not permitted, if `mode` is not specified then 'w' will be used
# by default instead of the `open()` default of 'r', and if `perms` is specified then `opener`
# cannot be specified.
# This function also accepts additional arguments:
# `prefix` and `suffix` may be used to adjust the temporary name of the new file.
# `perms` may be used to make the temporary initial permissions of the new file more restrictive
# than the umask.
# `remove_new_on_exception` determines whether the new file will be removed (`True`) or left in
# place (`False`) if any exceptions are thrown.  If it is left in place then it will be overwritten
# by a subsequent call to this function unless the 'x' `mode` character is specified.  Default is
# `True`.
#
@contextmanager
def open_for_atomic_write(
  file_name, mode='w',
  prefix=DEFAULT_ATOMIC_FILE_PREFIX, suffix=DEFAULT_ATOMIC_FILE_SUFFIX,
  perms=None, opener=None, remove_new_on_exception=True,
  *args, **kwargs,
):
  if 'r' in mode or 'a' in mode:
    raise ValueError("atomic_write() does not support the 'r' or 'a' mode flags")
  if not ('w' in mode or 'x' in mode):
    raise ValueError("atomic_write() requires one of the 'w' or 'x' mode flags")
  if perms and opener:
    raise ValueError('Only one of perms or opener may be specified')
  new_file_name = os.path.join(os.path.dirname(file_name), prefix + os.path.basename(file_name) + suffix)
  if perms:
    opener = _get_perms_opener(perms)
  try:
    with open(new_file_name, mode=mode, opener=opener, *args, **kwargs) as f:
      created = True
      yield f
      # flush and fsync are required to ensure that the new file's data blocks are written to disk
      # before the filesystem metadata changes (made by the following operations) are written, as
      # blocks may normally be written to disk in a different order than they were written by
      # processes, and writing the filesystem metadata changes before the file data could break
      # atomicity.
      _flush_and_fsync(f)
    if os.path.exists(file_name):
      shutil.copystat(file_name, new_file_name)  # Copy permissions and xattrs but not UID/GID
      os.utime(new_file_name)  # shutil.copystat() also copies atime/mtime, but we want current time
      if os.geteuid() == 0:
        st = os.stat(file_name)
        os.chown(new_file_name, st[stat.ST_UID], st[stat.ST_GID])  # If root then also copy UID/GID
    os.replace(new_file_name, file_name)
  except BaseException as e:
    if created and remove_new_on_exception:
      os.remove(new_file_name)
    raise e

# Ensure that all previous writes to the specified file handle are durable, without atomicity or
# isolation.
# (Ensure that the writes cannot be lost if the system crashes after this function returns.
# However, the file may be empty, incomplete, or corrupted if the system crashes before then, and
# concurrent reads/writes may produce to empty, incomplete, or corrupted data.)
#
# Primary trade-offs of this implementation:
# Immediately writing data to disk may impact performance (by eliminating write
# optimizations/merging and forcing this process to wait for the writes to complete).
#
# Usage:
# ```
#   with open(file) as f:
#     f.write('...')
#     filesystem.durable_flush(f)
# ```
#
def durable_flush(file_handle):
  _flush_and_fsync(file_handle)

# Isolate a file for write or read+write and ensure that the write is durable, without atomicity.
# (Ensure that concurrent writes to the file will not cause lost or mixed writes, that concurrent
# reads will not cause incomplete or mixed reads, that concurrent read+writes will not cause
# interspersed read and write operations, and that the writes cannot be lost if the system crashes
# after this function returns.  However, the file may be empty, incomplete, or corrupted if the
# system crashes before this function returns.)
#
# Primary trade-offs of this implementation:
# Write/read+write operations are delayed while other write/read+write or read only operations are
# being performed.
# Read only operations are delayed while write/read+write operations are being performed.
# Immediately writing data to disk may impact performance (by eliminating write
# optimizations/merging and forcing this process to wait for the writes to complete).
#
# Usage:
# ```
#   with open(file) as f:
#     with filesystem.isolated_durable_read_write(f):
#       f.write('...')
# ```
# The `timeout` argument specifies a time limit in seconds to wait for other processes to finish
# writing before this process can enter the `with` block.  If this time limit is reached then an
# exception will be thrown.  A `timeout` of '0' disables the time limit.  The time limit
# implementation only works in the main thread; The time limit is ignored when this is used in a
# different thread.
#
# To ensure proper isolation, all processes and operations that write the relevant file must use
# `isolated_durable_read_write()`, `isolated_read_write()`, or `flock()` with `LOCK_EX` on the file,
# and all processes and operations that read the relevant file must use `isolated_read()` or
# `flock()` with `LOCK_SH` on the file.
#
@contextmanager
def isolated_durable_read_write(file_handle, timeout=DEFAULT_LOCK_TIMEOUT):
  with _lock(file_handle, lock_type='write', timeout=timeout):
    yield
    _flush_and_fsync(file_handle)

# Isolate a file for write or read+write, without atomicity or durability.
# (Ensure that concurrent writes to the file will not cause lost or mixed writes, that concurrent
# reads will not cause incomplete or mixed reads, and that concurrent read+writes will not cause
# interspersed read and write operations.  However, the file may be empty, incomplete, or corrupted
# if the system crashes during or after this operation.)
#
# Primary trade-offs of this implementation:
# Write/read+write operations are delayed while other write/read+write or read only operations are
# being performed.
# Read only operations are delayed while write/read+write operations are being performed.
#
# Usage:
# ```
#   with open(file) as f:
#     with filesystem.isolated_read_write(f):
#       f.write('...')
# ```
# The `timeout` argument specifies a time limit in seconds to wait for other processes to finish
# writing before this process can enter the `with` block.  If this time limit is reached then an
# exception will be thrown.  A `timeout` of '0' disables the time limit.  The time limit
# implementation only works in the main thread; The time limit is ignored when this is used in a
# different thread.
#
# To ensure proper isolation, all processes and operations that write the relevant file must use
# `isolated_durable_read_write()`, `isolated_read_write()`, or `flock()` with `LOCK_EX` on the file,
# and all processes and operations that read the relevant file must use `isolated_read()` or
# `flock()` with `LOCK_SH` on the file.
#
@contextmanager
def isolated_read_write(file_handle, timeout=DEFAULT_LOCK_TIMEOUT):
  with _lock(file_handle, lock_type='write', timeout=timeout):
    yield

# Isolate a file for read only (for a read that will not affect a subsequent write).
# (Ensure that concurrent writes to the file will not cause incomplete or mixed reads.)
#
# Primary trade-offs of this implementation:
# Read only operations are delayed while write/read+write operations are being performed.
# Write/read+write operations are delayed while read only operations are being performed.
#
# Usage:
# ```
#   with open(file) as f:
#     with filesystem.isolated_read(f):
#       data = f.read()
# ```
# The `timeout` argument specifies a time limit in seconds to wait for other processes to finish
# writing before this process can enter the `with` block.  If this time limit is reached then an
# exception will be thrown.  A `timeout` of '0' disables the time limit.  The time limit
# implementation only works in the main thread; The time limit is ignored when this is used in a
# different thread.
#
# `isolated_read()` may be used with either `isolated_read_write()` or
# `isolated_durable_read_write()`.
#
@contextmanager
def isolated_read(file_handle, timeout=DEFAULT_LOCK_TIMEOUT):
  with _lock(file_handle, lock_type='read', timeout=timeout):
    yield

# Generate an "opener" for passing to the Python `open()` builtin, to allow making the default file
# permissions (if a new file is created) more restrictive than the umask.
def _get_perms_opener(perms):
  def opener(path, flags):
    return os.open(path, flags, mode=perms)
  return opener

def _fsync(file_handle):
  if have_fcntl and hasattr(fcntl, 'F_FULLFSYNC'):
    # Needed on MacOS:
    # https://lists.apple.com/archives/darwin-dev/2005/Feb/msg00072.html
    # https://developer.apple.com/library/mac/documentation/Darwin/Reference/ManPages/man2/fsync.2.html
    fcntl.fcntl(file_handle, fcntl.F_FULLSYNC)
  else:
    if hasattr(file_handle, 'fileno'):
      file_handle = file_handle.fileno()
    if hasattr(os, 'fdatasync'):
      os.fdatasync(file_handle)
    else:
      os.fsync(file_handle)

def _fsync_if_needed(file_handle):
  # fsync/fdatasync is not needed if the O_SYNC or O_DSYNC flag was passed to open() on a Unix
  # system.
  if have_fcntl:
    flags = fcntl.fcntl(file_handle, fcntl.F_GETFD)
    if flags & os.O_SYNC or flags & os.O_DSYNC:
      return
  _fsync(file_handle)

def _flush_and_fsync(file_handle):
  file_handle.flush()
  _fsync_if_needed(file_handle)

# Lock a file or directory with `flock()`.
#
# Usage:
# ```
#   with open(file) as f:
#     with _lock(f):
#       ...
# ```
# Or:
# ```
#   f = os.open(dir, os.O_RDONLY|os.O_DIRECTORY)
#   try:
#     with _lock(f):
#       ...
#   finally:
#     os.close(f)
# ```
# The file_handle argument may be a `file` object returned by `open()` or a file descriptor returned
# by `os.open()`.  Directory file descriptors opened using `os.open()` are supported.  Locking a
# directory only locks the directory itself; It does not inherently lock the contents of the
# directory (eg. the files within the directory).
# The `lock_type` argument may be 'write' for an exclusive lock, or 'read' for a shared lock.
# Default is 'write'.
# The `timeout` argument specifies a time limit in seconds to wait for the lock to be obtained.  If
# this time limit is reached then an exception will be thrown.  A `timeout` of '0' disables the time
# limit.  The time limit implementation only works in the main thread; The time limit is ignored
# when this is used in a different thread.
#
signal.signal(signal.SIGALRM, signal.SIG_IGN)
@contextmanager
def _lock(file_handle, lock_type='write', timeout=DEFAULT_LOCK_TIMEOUT):
  if lock_type not in ['write', 'read']:
    raise ValueError(f"Invalid lock_type '{lock_type}'")
  if threading.current_thread() is threading.main_thread() and timeout:
    try:
      signal.alarm(timeout)  # Send SIGALRM after timeout
      fcntl.flock(file_handle, (fcntl.LOCK_EX if lock_type == 'write' else fcntl.LOCK_SH))
      yield
    except IOError as e:
      if e.errno == errno.EINTR:  # If interrupted by SIGALRM
        raise Exception('Timed out waiting for lock')
      raise e
    finally:
      signal.alarm(0)  # Cancel SIGALRM timer
  else:
    fcntl.flock(file_handle, (fcntl.LOCK_EX if lock_type == 'write' else fcntl.LOCK_SH))
    yield

# Convenience function for opening and locking a file or directory with `flock()`.
#
# Usage:
# ```
#   with _open_with_lock(lock_file='...'):
#     ...
# ```
# If `lock_file` is specified then it will be used as the lock file/directory.  The same
# file/directory may be used for write operations for multiple files (to synchronize write
# operations on all of those files).
# If `lock_file` is not specified and `lock_dir` is `True` then the directory containing the
# specified `base_file` will be used as the lock directory.
# If `lock_file` and `lock_dir` are not specified then `prefix` and `suffix` will be used to derive
# a lock file name from `base_file`.
# If necessary, the lock file is created (as a file, not as a directory).
# Locking a directory only locks the directory itself; It does not inherently lock the contents of
# the directory (eg. the files within the directory).
# The `lock_type` argument may be 'write' for an exclusive lock, or 'read' for a shared lock.
# Default is 'write'.
# The `timeout` argument specifies a time limit in seconds to wait for the lock to be obtained.  If
# this time limit is reached then an exception will be thrown.  A `timeout` of '0' disables the time
# limit.  The time limit implementation only works in the main thread; The time limit is ignored
# when this is used in a different thread.
#
@contextmanager
def _open_with_lock(
  lock_file=None, base_file=None, lock_dir=False,
  prefix=DEFAULT_ATOMIC_LOCK_FILE_PREFIX, suffix=DEFAULT_ATOMIC_LOCK_FILE_SUFFIX,
  perms=None, lock_type='write', timeout=DEFAULT_LOCK_TIMEOUT,
):
  if not lock_file:
    lock_path = os.path.dirname(base_file)
    if lock_dir:
      lock_file = (lock_path or '.')
    else:
      lock_file = os.path.join(lock_path, prefix + os.path.basename(base_file) + suffix)
  # If using a lock directory:
  # * The directory must be opened with `O_RDONLY`.
  #   `os.open()` will throw an exception if `O_RDWR` is specified.
  # * `O_DIRECTORY` is not required if the directory already exists and `O_CREAT` is not specified.
  #   If `O_CREAT` is specified then `O_DIRECTORY` must also be specified, otherwise `os.open()`
  #   will throw an exception if the directory already exists, or a file will be created instead of
  #   a directory of the path does not already exist.
  # If using a lock file:
  # * NFS4 requires the file to be opened with `O_RDWR` in order to use `flock()` with `LOCK_EX`.
  #   NFS4 permits `flock()` with `LOCK_SH` on a file opened with either `O_RDONLY` or `O_RDWR`.
  #   EXT4 permits `flock()` with either `LOCK_EX` or `LOCK_SH` on a file opened with either
  #   `O_RDONLY` or `O_RDWR`.
  # * `O_CREAT` may be specified even if the file already exists.
  if os.path.isdir(lock_file):
    lock_open_flags = os.O_RDONLY
  else:
    lock_open_flags = (os.O_RDWR if lock_type == 'write' else os.O_RDONLY)|os.O_CREAT
  f = os.open(lock_file, lock_open_flags, mode=(perms if perms else 0o777))
  try:
    with _lock(f, lock_type=lock_type, timeout=timeout):
      yield
  finally:
    os.close(f)

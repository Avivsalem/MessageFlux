import os
import shutil

import errno
import time

from messageflux.utils import KwargsException

MAX_LOCKFILE_AGE = 60  # max lockfile age in seconds


class AtomicMoveException(KwargsException):
    """
    an exception which is raised when atomic move fails
    """
    pass


def atomic_move(src: str, dest: str, lock_filename: str) -> bool:
    """
    moves a file atomically (hopefully) from src to dest, using lock_filename as lockfile

    :param src: the source file path
    :param dest: the destination file path
    :param lock_filename: the lockfile path
    :return: True if succeeded, False otherwise
    """
    fd = None
    filenames = [lock_filename]
    try:
        try:
            while True:
                if os.path.exists(lock_filename):
                    file_age = time.time() - os.stat(lock_filename).st_mtime
                    if file_age >= MAX_LOCKFILE_AGE:
                        lock_filename += '.new'
                        filenames.append(lock_filename)
                    else:
                        return False
                else:
                    break
            fd = os.open(lock_filename, os.O_RDWR | os.O_CREAT | os.O_EXCL)
        except (OSError, IOError):
            return False

        try:
            if not os.path.exists(src):
                return False
            shutil.move(src, dest)
            return True
        except (OSError, IOError) as e:
            if os.path.exists(dest):
                os.remove(dest)
            if e.errno == errno.ENOENT:  # no such file or directory
                return False
            raise AtomicMoveException(f"Error Moving file {src} to {dest}") from e
    finally:
        try:
            if fd is not None:
                os.close(fd)
        except Exception:
            pass
        for lock_filename in reversed(filenames):
            try:
                os.remove(lock_filename)
            except Exception:
                pass


def recursive_chmod(dir_name: str):
    """
    chmods to 0o777 the directory and its directory tree

    :param dir_name: the dir to chmod
    """
    os.chmod(dir_name, 0o777)
    for root, dirs, files in os.walk(dir_name):
        for d in dirs:
            try:
                os.chmod(os.path.join(root, d), 0o777)
            except OSError:
                pass
        for f in files:
            try:
                os.chmod(os.path.join(root, f), 0o777)
            except OSError:
                pass

#!/usr/bin/env python3
# PYTHON_ARGCOMPLETE_OK
"""
Simple backup script based on position, timestamp and size comparison. Backup folder is effectively synchronized
with source folder. New and updated files are copied to backup and deleted files or folders are removed from
backup as well.
"""

import argparse
import os
from filecmp import dircmp
import itertools
import shutil
from datetime import datetime
import traceback
from functools import wraps, reduce
import stat
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures._base import TimeoutError as FutureTimeoutError
import threading
from queue import Queue
from typing import Iterator, List, NamedTuple
import logging
import argcomplete


class DirCmpShallowOnly(dircmp):
    """
    Compare directories but involve shallow compare only - NEVER read content in order to compare.
    """

    def __init__(self, a, b, ignore=None, hide=None):
        super().__init__(a, b, ignore, hide)
        self.methodmap.update(dict(
            subdirs=DirCmpShallowOnly.phase4,
            same_files=DirCmpShallowOnly.phase3,
            diff_files=DirCmpShallowOnly.phase3,
            funny_files=DirCmpShallowOnly.phase3,
        ))

    def phase3(self):  # override
        # Find out differences between common files

        # print('phase3: {0} vs. {1}'.format(self.left, self.right))
        composite_cmp_result = DirCmpShallowOnly.cmpfiles(self.left, self.right, self.common_files)
        self.same_files, self.diff_files, self.funny_files = composite_cmp_result

    def phase4(self):  # override
        # Find out differences between common subdirectories
        # A new dircmp object is created for each common subdirectory,
        # these are stored in a dictionary indexed by filename.
        # The hide and ignore properties are inherited from the parent

        self.subdirs = {}
        for common_dir in self.common_dirs:
            a_x = os.path.join(self.left, common_dir)
            b_x = os.path.join(self.right, common_dir)
            self.subdirs[common_dir] = DirCmpShallowOnly(a_x, b_x, self.ignore, self.hide)

    @staticmethod
    def cmpfiles(dir_a, dir_b, common_files):
        """
        Compare common files in two directories.

        a, b -- directory names
        common -- list of file names found in both directories
        shallow -- if true, do comparison based solely on stat() information

        Returns a tuple of three lists:
          files that compare equal
          files that are different
          filenames that aren't regular files.
        """
        res = ([], [], [])
        for common_file in common_files:
            file_a = os.path.join(dir_a, common_file)
            file_b = os.path.join(dir_b, common_file)
            cmp_result = DirCmpShallowOnly._cmp(file_a, file_b)
            # print('cmpfiles: {1}:{0} vs. {2}:{0} --> {3}\n'.format(common_file, a, b, abs(cmp_result)))
            res[cmp_result].append(common_file)
        # print(res)
        return res

    @staticmethod
    def _cmp(file_a, file_b):
        try:
            return not abs(DirCmpShallowOnly.cmp(file_a, file_b))
        except OSError:
            return 2

    @staticmethod
    def cmp(file_a, file_b):
        """Compare two files.

        Arguments:

        f1 -- First file name

        f2 -- Second file name

        shallow -- Just check stat signature (do not read the files).
                   defaults to True.

        Return value:

        True if the files are the same (based on shallow compare), False otherwise.
        """

        signature_a = DirCmpShallowOnly._sig(os.stat(file_a))
        signature_b = DirCmpShallowOnly._sig(os.stat(file_b))
        outcome = False
        if signature_a[0] != stat.S_IFREG or signature_b[0] != stat.S_IFREG:
            outcome = False
        if signature_a == signature_b:
            outcome = True

        # print('#cmp {0}: {1}'.format(f1, signature_a))
        # print('#cmp {0}: {1}'.format(f2, signature_b))

        return outcome

    @staticmethod
    def _sig(full_stat):
        return (stat.S_IFMT(full_stat.st_mode),
                full_stat.st_size,
                full_stat.st_mtime)


class BackupShallowDiff:
    """
    Compare recursively source anf target folders and detect what got updated.
    """

    # this type hinting is supported by python-3.6+
    TaskContext = NamedTuple('TaskContext', [
        ('all_diff_files', List[Iterator]),
        ('pool', ThreadPoolExecutor),
        ('pool_lock', threading.Lock),
        ('async_result_lot', Queue)
    ])

    def __init__(self, source_folder: str, target_folder: str):
        self._root_diff = DirCmpShallowOnly(source_folder, target_folder)
        self.pool = ThreadPoolExecutor(10)
        self.pool_lock = threading.Lock()
        # self._diff.report_full_closure()
        # for subdir, subdiff in self._diff.subdirs.items():
        #     print('subdir: {0} -> {1}'.format(subdir, subdiff))

    def collect_updates(self):
        """
        :return: lazy iterable of tuples containing full source and target paths of changed files or folders
        """
        all_diff_files = []
        async_results = Queue()  # type~ Queue[Tuple[str, Future]]

        task_context = BackupShallowDiff.TaskContext(all_diff_files, self.pool, self.pool_lock, async_results)
        BackupShallowDiff.process_diff(self._root_diff, task_context)

        result_lot = {}
        while not async_results.empty():
            path, result = async_results.get()
            try:
                result_lot[path] = result.result(30)
            except FutureTimeoutError:
                logging.warning('diff task timed out for: %s', path)

        self.pool.shutdown(False)
        logging.debug('# amount of tasks:   %d', len(result_lot))
        logging.debug('# amount of subdirs: %d', reduce(lambda x, y: x+y, result_lot.values(), 0))

        return itertools.chain(*all_diff_files)

    @staticmethod
    def process_diff(diff_item: dircmp, task: TaskContext):
        """
        Asynchronous task body, purpose: walk through local files and subfolders

        :param diff_item: current cmpdir object
        :param task: common task context
        :return: amount of subfolders (fired asynchronous visiting - thread per folder)
        """
        logging.debug('PROCESSING diff: %s', diff_item.left)
        file_chain = itertools.chain(diff_item.left_only, diff_item.diff_files)
        task.pool_lock.acquire()
        task.all_diff_files.append(map(ParentPairJoiner(diff_item.left, diff_item.right).join, file_chain))
        task.pool_lock.release()
        if diff_item.subdirs:
            logging.debug('entering subdirs in: %s [%s]', diff_item.left, threading.current_thread())
            task.pool_lock.acquire()
            for next_diff_item in diff_item.subdirs.values():
                logging.debug('entering subdir: %s', next_diff_item.left)
                result = task.pool.submit(BackupShallowDiff.process_diff, next_diff_item, task)
                logging.debug('future: %s', result)
                task.async_result_lot.put((next_diff_item.left, result))
            task.pool_lock.release()
            logging.debug('leaving subdirs in: %s .. done', diff_item.left)
        return len(diff_item.subdirs.values())

    def collect_removals(self):
        """
        :return: lazy iterable of files or folders to be removed (right side means backup side)
        """
        all_removed_files = []
        diff_stack = [self._root_diff]
        while diff_stack:
            diff_cursor = diff_stack.pop()
            all_removed_files.append(map(ParentJoiner(diff_cursor.right).join, diff_cursor.right_only))
            if diff_cursor.subdirs:
                diff_stack.extend(diff_cursor.subdirs.values())
        return itertools.chain(*all_removed_files)


class ParentJoiner:  # pylint: disable=too-few-public-methods
    """
    Provide path + child joining - suitable for lazy operations.
    """

    def __init__(self, parent_path):
        self._parent_path = parent_path

    def join(self, child_path):
        """
        :param child_path: child file or folder name
        :return: parent path joined with child (plain file or folder name)
        """
        return os.path.join(self._parent_path, child_path)


class ParentPairJoiner:  # pylint: disable=too-few-public-methods
    """
    Provide path +child joining in pairs (source, target) - suitable for lazy operations.
    """

    def __init__(self, parent_left_path, parent_right_path):
        self._parent_left_path_joiner = ParentJoiner(parent_left_path)
        self._parent_right_path_joiner = ParentJoiner(parent_right_path)

    def join(self, child_path):
        """
        :param child_path: child file or folder name
        :return: tuple with (left, right) parent path joined with child (plain file or folder name)
        """
        return self._parent_left_path_joiner.join(child_path), self._parent_right_path_joiner.join(child_path)


def check_and_create_folder(target: str, dry_run=False):
    """
    Check if target folder exists - if not then try to create it

    :param dry_run: simulate if true
    :param target: folder to check
    """
    if not os.path.isdir(target):
        if dry_run:
            logging.warning('.. need to create target folder: %s', _TARGET_PATH)
        else:
            logging.warning('.. creating target folder: %s', _TARGET_PATH)
            os.mkdir(_TARGET_PATH)


def safe_action(message):
    """
    Expose message value to inner decorator

    :param message: message seed in case of exception
    :return: try-except-log decorator
    """
    def decorator(func):
        """
        Wraps risky method into wrapper

        :param func: original callable
        :return: try-except-log wrapper
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            """
            Surround func with try-except-log blocks

            :param args:
            :param kwargs:
            :return: wrapped callable
            """
            try:
                func(*args, **kwargs)
            except Exception:
                print('! [{0}] '.format(func.__name__) + message.format(*args))
                print(traceback.format_exc())

        return wrapper
    return decorator


@safe_action('failed to copy {0} -> {1}')
def do_copy(file_source, file_target):
    """
    Copy file or folder from source to target

    :param file_source: source file or folder
    :param file_target: target file or folder
    """
    if os.path.isdir(file_source):
        shutil.copytree(file_source, file_target)
    else:
        shutil.copy2(file_source, file_target)


@safe_action('failed to remove {0}')
def do_remove(file_target):
    """
    Remove file or folder (recursively)

    :param file_target: source file or folder
    """
    if os.path.isdir(file_target):
        shutil.rmtree(file_target)
    else:
        os.remove(file_target)


if __name__ == '__main__':
    _PARSER = argparse.ArgumentParser(description='One-shot backup of projects using shallow file compare.')
    _PARSER.add_argument('--source', type=str, required=True,
                         help='source folder (with living data inside)')
    _PARSER.add_argument('--backup-root', type=str, required=True,
                         help='target folder (with backups)')
    _PARSER.add_argument('--dry-run', action='store_true',
                         help='just show what would be copied - no change on file system')
    _PARSER.add_argument('--verbose', action='store_true',
                         help='show verbose output')

    argcomplete.autocomplete(_PARSER)
    _ARGS = _PARSER.parse_args()
    _DATE_TIME_FORM = '%Y-%m-%dT%H:%M:%S'
    print(' {0} '.format(datetime.strftime(datetime.now(), _DATE_TIME_FORM)).center(35, 'v'))
    if _ARGS.verbose:
        _LEVEL = logging.DEBUG
    else:
        _LEVEL = logging.INFO
    logging.basicConfig(level=_LEVEL)

    if _ARGS.verbose:
        logging.info('#args: %s', _ARGS)

    _SOURCE_PATH = os.path.abspath(_ARGS.source)
    _TARGET_PATH = os.path.abspath(_ARGS.backup_root)
    logging.info('#source_path: %s', _SOURCE_PATH)
    logging.info('#target_path: %s', _TARGET_PATH)

    check_and_create_folder(_TARGET_PATH, dry_run=_ARGS.dry_run)

    _DIFF = BackupShallowDiff(_SOURCE_PATH, _TARGET_PATH)
    # copy+update
    for file_left, file_right in _DIFF.collect_updates():
        if _ARGS.dry_run:
            print('backup [dry-run]: {0}\n               -> {1}'.format(file_left, file_right))
        else:
            print('backup: {0}\n     -> {1}'.format(file_left, file_right))
            do_copy(file_left, file_right)
    # remove
    for doomed_file_right in _DIFF.collect_removals():
        if _ARGS.dry_run:
            print('REMOVE [dry-run]: {0}'.format(doomed_file_right))
        else:
            print('REMOVE: {0}'.format(doomed_file_right))
            do_remove(doomed_file_right)

    print(' {0} '.format(datetime.strftime(datetime.now(), _DATE_TIME_FORM)).center(35, '^'))

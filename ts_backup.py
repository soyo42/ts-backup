#!/usr/bin/env python3
# PYTHON_ARGCOMPLETE_OK
"""
Simple backup script based on position, timestamp and size comparison. Backup folder is effectively synchronized
with source folder. New and updated files are copied to backup and deleted files or folders are removed from
backup as well.
"""

import argparse
import sys
import os
from filecmp import dircmp
import itertools
import shutil
from datetime import datetime
import traceback
import argcomplete


class BackupShallowDiff:
    """
    Compare recursively source anf target folders and detect what got updated.
    """

    def __init__(self, source_folder, target_folder):
        self._diff = dircmp(source_folder, target_folder)
        # self._diff.report_full_closure()
        # for subdir, subdiff in self._diff.subdirs.items():
        #     print('subdir: {0} -> {1}'.format(subdir, subdiff))

    def collect_updates(self):
        """
        :return: lazy iterable of tuples containing full source and target paths of changed files or folders
        """
        all_diff_files = []
        diff_stack = [self._diff]
        while diff_stack:
            diff_cursor = diff_stack.pop()
            file_chain = itertools.chain(diff_cursor.left_only, diff_cursor.diff_files)
            all_diff_files.append(map(ParentPairJoiner(diff_cursor.left, diff_cursor.right).join, file_chain))
            if diff_cursor.subdirs:
                diff_stack.extend(diff_cursor.subdirs.values())
        return itertools.chain(*all_diff_files)

    def collect_removals(self):
        """
        :return: lazy iterable of files or folders to be removed (right side means backup side)
        """
        all_removed_files = []
        diff_stack = [self._diff]
        while diff_stack:
            diff_cursor = diff_stack.pop()
            all_removed_files.append(map(ParentJoiner(diff_cursor.right).join, diff_cursor.right_only))
            if diff_cursor.subdirs:
                diff_stack.extend(diff_cursor.subdirs.values())
        return itertools.chain(*all_removed_files)


class ParentJoiner:
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


class ParentPairJoiner:
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
            sys.stderr.write('.. need to create target folder: {0}\n'.format(_TARGET_PATH))
        else:
            sys.stderr.write('.. creating target folder: {0}\n'.format(_TARGET_PATH))
            os.mkdir(_TARGET_PATH)


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


def do_remove(file_target):
    """
    Remove file or folder (recursively)
    :param file_target: source file or folder
    """
    if os.path.isdir(file_target):
        shutil.rmtree(file_target)
    else:
        os.remove(file_target)


def safe_wrapper(action_callable, message, *action_input):
    """
    Wraps risky method into try-except block
    :param action_callable: risky action
    :param message: message seed in case of exception
    :param action_input: input parameters for action callable and message
    """
    try:
        action_callable(*action_input)
    except Exception:
        print('! ' + message.format(*action_input))
        print(traceback.format_exc())


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
        sys.stderr.write('#args: {0}\n'.format(_ARGS))

    _SOURCE_PATH = os.path.abspath(_ARGS.source)
    _TARGET_PATH = os.path.abspath(_ARGS.backup_root)
    if _ARGS.verbose:
        sys.stderr.write('#source_path: {0}\n'.format(_SOURCE_PATH))
        sys.stderr.write('#target_path: {0}\n'.format(_TARGET_PATH))

    check_and_create_folder(_TARGET_PATH, dry_run=_ARGS.dry_run)

    _DIFF = BackupShallowDiff(_SOURCE_PATH, _TARGET_PATH)
    # copy+update
    for file_left, file_right in _DIFF.collect_updates():
        if _ARGS.dry_run:
            print('backup [dry-run]: {0}\n               -> {1}'.format(file_left, file_right))
        else:
            print('backup: {0}\n     -> {1}'.format(file_left, file_right))
            safe_wrapper(do_copy, 'failed to copy {0} -> {1}', file_left, file_right)
    # remove
    for doomed_file_right in _DIFF.collect_removals():
        if _ARGS.dry_run:
            print('REMOVE [dry-run]: {0}'.format(doomed_file_right))
        else:
            print('REMOVE: {0}'.format(doomed_file_right))
            safe_wrapper(do_remove, 'failed to remove {0}', doomed_file_right)

    print(' {0} '.format(datetime.strftime(datetime.now(), _DATE_TIME_FORM)).center(35, '^'))

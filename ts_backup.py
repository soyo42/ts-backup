#!/usr/bin/env python3
# PYTHON_ARGCOMPLETE_OK

import argcomplete
import argparse
import sys
import os
from filecmp import dircmp
import itertools
import shutil
from datetime import datetime
import traceback


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
            sys.stderr.write('.. need to create target folder\n'.format(target_path))
        else:
            sys.stderr.write('.. creating target folder: {0}\n'.format(target_path))
            os.mkdir(target_path)


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


def safe_wrapper(action_callable, message, *args):
    try:
        action_callable(*args)
    except Exception as e:
        print('! '+message.format(*args))
        print(traceback.format_exc())


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='One-shot backup of projects using shallow file compare.')
    parser.add_argument('--source', type=str, required=True,
                        help='source folder (with living data inside)')
    parser.add_argument('--backup-root', type=str, required=True,
                        help='target folder (with backups)')
    parser.add_argument('--dry-run', action='store_true',
                        help='just show what would be copied - no change on file system')
    parser.add_argument('--verbose', action='store_true',
                        help='show verbose output')

    argcomplete.autocomplete(parser)
    args = parser.parse_args()
    DATE_TIME_FORM = '%Y-%m-%dT%H:%M:%S'
    print(' {0} '.format(datetime.strftime(datetime.now(), DATE_TIME_FORM)).center(35, 'v'))

    if args.verbose:
        sys.stderr.write('#args: {0}\n'.format(args))

    source_path = os.path.abspath(args.source)
    target_path = os.path.abspath(args.backup_root)
    if args.verbose:
        sys.stderr.write('#source_path: {0}\n'.format(source_path))
        sys.stderr.write('#target_path: {0}\n'.format(target_path))

    check_and_create_folder(target_path, dry_run=args.dry_run)

    diff = BackupShallowDiff(source_path, target_path)
    # copy+update
    for file_left, file_right in diff.collect_updates():
        if args.dry_run:
            print('backup [dry-run]: {0}\n               -> {1}'.format(file_left, file_right))
        else:
            print('backup: {0}\n     -> {1}'.format(file_left, file_right))
            safe_wrapper(do_copy, 'failed to copy {0} -> {1}', file_left, file_right)
    # remove
    for doomed_file_right in diff.collect_removals():
        if args.dry_run:
            print('REMOVE [dry-run]: {0}'.format(doomed_file_right))
        else:
            print('REMOVE: {0}'.format(doomed_file_right))
            safe_wrapper(do_remove, 'failed to remove {0}', doomed_file_right)

    print(' {0} '.format(datetime.strftime(datetime.now(), DATE_TIME_FORM)).center(35, '^'))

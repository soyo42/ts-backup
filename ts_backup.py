#!/usr/bin/env python3
# PYTHON_ARGCOMPLETE_OK

import argcomplete
import argparse
import sys
import os
from filecmp import dircmp
import itertools


class BackupShallowDiff:
    """
    Compare recursively source anf target folders and detect what got updated.
    """

    def __init__(self, source_folder, target_folder):
        self._diff = dircmp(source_folder, target_folder)
        self._diff.report_full_closure()
        for subdir, subdiff in self._diff.subdirs.items():
            print('subdir: {0} -> {1}'.format(subdir, subdiff))

    def get_updates(self):
        return itertools.chain(self._diff.left_only, self._diff.diff_files)


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

    if args.verbose:
        sys.stderr.write('args: {0}\n'.format(args))

    source_path = os.path.abspath(args.source)
    project_folder = os.path.basename(source_path)
    target_path = os.path.join(os.path.abspath(args.backup_root), project_folder)
    if args.verbose:
        sys.stderr.write('project folder: {0}\n'.format(project_folder))
        sys.stderr.write('target_path: {0}\n'.format(target_path))

    check_and_create_folder(target_path, dry_run=args.dry_run)

    diff = BackupShallowDiff(source_path, target_path)
    for file in diff.get_updates():
        if args.dry_run:
            print('backup [tbd]: {0}'.format(os.path.join(args.source, file)))
        else:
            pass


#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import argparse

from setup_compare import compare
from pr_review import PR


def main():
    parser = argparse.ArgumentParser(
        description='GitHub reviewer',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument('owner', metavar='owner',
                        help='Github repository owner')

    parser.add_argument('repo', metavar='repo', help='Github repository')

    parser.add_argument('pr', metavar='pr-number', type=int, help='PR number')

    parser.add_argument(
        'token', metavar='token',
        help='Github token with permission to read/write repos'
    )

    parser.add_argument("--verbose", action="store_true",
                        help="Output more info")

    parser.add_argument(
        "--status-update", action="store_true",
        help="Set success status for PR head commit before merge"
    )

    merge_group = parser.add_argument_group(
        "merge", "options for merge action"
    )

    merge_group.add_argument(
        '--message', metavar='message', default=None,
        help='Extra detail to append to automatic commit message'
    )

    review_group = parser.add_argument_group(
        "review", "options for review action"
    )

    review_group.add_argument(
        '--body', metavar='comment', default=None,
        help='The body text of the pull request review'
    )

    status_group = parser.add_argument_group(
        "status",
        "options for commit status update "
        "[https://developer.github.com/v3/repos/statuses/#create-a-status]"
    )

    status_group.add_argument(
        '--status-state',
        metavar='status',
        choices=["pending", "success", "error", "failure"],
        default="success",
        help='Action to perform on PR'
    )

    status_group.add_argument(
        '--status-url', metavar='URL', default=None,
        help="The target URL to associate with this status. "
             "This URL will be linked from the GitHub UI to allow users "
             "to easily see the 'source' of the Status'"
    )

    status_group.add_argument(
        '--status-descr', metavar='descr', default=None,
        help='A short description of the status. Must be less than 1024 bytes'
    )

    status_group.add_argument(
        '--status-context', metavar='context', default="default",
        help="A string label to differentiate this status "
             "from the status of other systems"
    )

    args = parser.parse_args()

    pr = PR(args.owner, args.repo, args.pr, args.token)

    pr_base_sha = pr.base_sha()
    pr_head_sha = pr.head_sha()

    diff_files = pr.files()

    if diff_files == ['setup.py']:
        if args.verbose:
            print("=== setup.py patch ====\n"
                  "{}\n"
                  "=======================".format(
                      pr.get_patches()["setup.py"]))

        if compare(
                pr.content("setup.py", pr_base_sha).decode("utf-8"),
                pr.content("setup.py", pr_head_sha).decode("utf-8")) == 0:

            pr.review("APPROVE", args.body)
            print("AUTOMERGE: approved")

            if args.status_update:
                pr.create_status(
                    pr_head_sha,
                    args.status_state,
                    args.status_url,
                    args.status_descr,
                    args.status_context
                )
                print("AUTOMERGE: created commit status")

            pr.merge(args.message)
            print("AUTOMERGE: merged")

            return 0
        else:
            print("AUTOMERGE: setup.py(s) from {} and {} have changes out "
                  "of the versioning scope, "
                  "cancel automerge".format(pr_base_sha, pr_head_sha))
    else:
        print("AUTOMERGE: Modified files: {}, "
              "cancel automerge".format(diff_files))

    return 1


if __name__ == '__main__':
    sys.exit(main())

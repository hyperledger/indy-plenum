#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import argparse
from datetime import datetime, timedelta

from github import Github
from github import GithubObject


def main():
    parser = argparse.ArgumentParser(
        description='GitHub PR matcher',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument('owner', metavar='owner',
                        help='Github repository owner')

    parser.add_argument('repo', metavar='repo', help='Github repository')

    parser.add_argument('sha', metavar='sha', help='Current commit SHA')

    parser.add_argument(
        'token', metavar='token',
        help='Github token with permission to read/write repos'
    )

    parser.add_argument('--context', metavar='context', action='append',
                        help="Required context with successful status. "
                        "Could be used multiple times. If not defined all "
                        "contexts (there should be at least one) are checked.")

    parser.add_argument(
        '--branch', metavar='branch', default=None,
        help='Filter pull requests by base branch name'
    )

    parser.add_argument(
        '--updated', metavar='days', type=int, default=7,
        help="Filter pull requests updated within specified number "
        "of days from the current date"
    )

    parser.add_argument("--verbose", action="store_true",
                        help="Output more info")

    args = parser.parse_args()

    repo = Github(args.token).get_user(args.owner).get_repo(args.repo)

    prs = repo.get_pulls(
        state='closed',
        base=(GithubObject.NotSet if args.branch is None else args.branch),
        sort='updated',
        direction='desc'
    )

    pr = None
    update_deadline = datetime.now() - timedelta(days=args.updated)
    for _pr in prs:
        if _pr.updated_at < update_deadline:  # skip older PRs
            break
        elif _pr.merge_commit_sha == args.sha:
            pr = _pr
            break

    if pr is None:
        print("PR not found in '{}'. Criterias: state='closed', branch='{}', "
              "merge_commit_sha='{}', updated: within {} days.".format(
                repo.full_name, args.branch, args.sha, args.updated),
              file=sys.stderr)
        return 0

    if args.verbose:
        print("Found closed PR #{} with merged_commit_sha='{}' and head_sha='{}'. title: '{}', url: '{}'"
              "".format(pr.number, args.sha, pr.head.sha, pr.title, pr.html_url),
              file=sys.stderr)

    assert pr.is_merged()

    ci = repo.get_commit(pr.head.sha)

    # details:
    #  - https://developer.github.com/v3/repos/statuses/#get-the-combined-status-for-a-specific-ref
    cst = ci.get_combined_status()

    ctxs = {}
    for st in cst.statuses:
        if args.verbose:
            print("Found status for '{}': {}".format(pr.head.sha, st),
                  file=sys.stderr)
        if (args.context is None or st.context in args.context):
            if (st.state != 'success'):
                print("Context '{}' for commit '{}' has not passed status. "
                      "id: '{}', status source: '{}', url: '{}'".format(
                        st.context, ci.sha, st.id, st.target_url, cst.url),
                      file=sys.stderr)
                return 0
            else:
                ctxs[st.context] = True

    if args.context is not None:
        not_passed_ctx = set(args.context) - set(ctxs.keys())
        if len(not_passed_ctx) > 0:
            print("The following required contexts for commit '{}' are not "
                  "passed: {}. url: '{}'".format(
                      ci.sha, list(not_passed_ctx), cst.url),
                  file=sys.stderr)
            return 0
    elif len(ctxs) == 0:
        print("Not found statuses for required contexts for commit '{}'. "
              "url: '{}'".format(ci.sha, cst.url),
              file=sys.stderr)
        return 0

    print("{}".format(pr.html_url))
    return 0


if __name__ == '__main__':
    sys.exit(main())

#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import argparse
import requests
import base64
from github import Github
from github import GithubObject


class PR:
    def __init__(self, owner, repo, pr_number, token):
        self.owner = owner
        self.repo = repo
        self.pr_number = pr_number
        self.token = token
        self.repo = Github(token).get_user(owner).get_repo(repo)
        self.pr = self.repo.get_pull(pr_number)

        self.merge_url = (
            "https://api.github.com/repos/{}/{}/pulls/{}/reviews".format(
             owner, repo, pr_number)
        )

        self.merge_headers = {
            'Authorization': 'token {}'.format(token),
            'Accept': 'application/vnd.github.black-cat-preview+json'
        }

    def content(self, fpath, ref=None):
        if ref is None:
            ref = GithubObject.NotSet

        content = self.repo.get_contents(fpath, ref)
        assert(content.encoding == "base64")

        return base64.b64decode(content.content)

    def create_status(self, commit_sha, state,
                      target_url=None, description=None, context=None):

        if target_url is None:
            target_url = GithubObject.NotSet
        if description is None:
            description = GithubObject.NotSet
        if context is None:
            context = GithubObject.NotSet
        self.repo.get_commit(commit_sha).create_status(
                state, target_url, description, context)

    def base_sha(self):
        return self.pr.base.sha

    def head_sha(self):
        return self.pr.head.sha

    def files(self):
        return [f.filename for f in self.pr.get_files()]

    def get_patches(self):
        return {f.filename: f.patch for f in self.pr.get_files()}

    def merge(self, commit_message=None):
        if commit_message is None:
            commit_message = GithubObject.NotSet
        self.pr.merge(commit_message)
        return 0

    def review(self, event, body=None):
        if body is None:
            body = GithubObject.NotSet

        data = {'event': event, 'body': body}

        r = requests.post(self.merge_url,
                          json=data,
                          headers=self.merge_headers)

        return 0 if r.status_code == requests.codes.ok else 1


def main():
    parser = argparse.ArgumentParser(
        description='GitHub PR reviewer',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument('action', metavar='action',
                        choices=['merge', 'review'],
                        help='Action to perform on PR')

    parser.add_argument('owner', metavar='owner',
                        help='Github repository owner')

    parser.add_argument('repo', metavar='repo', help='Github repository')

    parser.add_argument('pr', metavar='pr-number', type=int, help='PR number')

    parser.add_argument(
        'token', metavar='token',
        help='Github token with permission to read/write repos'
    )

    merge_group = parser.add_argument_group(
        "merge", "options for merge action"
    )

    merge_group.add_argument(
        '--message', metavar='message', default='',
        help='Extra detail to append to automatic commit message'
    )

    review_group = parser.add_argument_group(
        "review", "options for review action"
    )

    review_group.add_argument(
        '--event', metavar='event',
        choices=['APPROVE', 'REQUEST_CHANGES', 'COMMENT'],
        default='APPROVE',
        help='The review action (event) to perform'
    )
    review_group.add_argument(
        '--body', metavar='comment', default='',
        help='The body text of the pull request review'
    )

    args = parser.parse_args()
    pr = PR(args.owner, args.repo, args.pr, args.token)

    if args.action == 'merge':
        return pr.merge(args.message)
    else:
        return pr.review(args.event, args.body)

if __name__ == '__main__':
    sys.exit(main())

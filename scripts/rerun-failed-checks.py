#!/usr/bin/env python3
# Usage: python3 rerun-failed-checks.py -t <github token> -r <repo> -b <branch>
#
# Description: This script will fetch the latest commit from a branch, and check the status of all check runs of the commit.
# If any check run is not successful, it will trigger a rerun of the failed jobs.
#
# Default branch is master, default repo is emqx/emqx
#
# Limitation: only works for upstream repo, not for forked.
import requests
import http.client
import json
import os
import sys
import time
import math
from optparse import OptionParser

job_black_list = [
    'windows',
    'publish_artifacts',
    'stale'
]

def fetch_latest_commit(token: str, repo: str, branch: str):
    url = f'https://api.github.com/repos/{repo}/commits/{branch}'
    headers = {'Accept': 'application/vnd.github+json',
               'Authorization': f'Bearer {token}',
               'X-GitHub-Api-Version': '2022-11-28',
               'User-Agent': 'python3'
               }
    r = requests.get(url, headers=headers)
    if r.status_code == 200:
        res = r.json()
        return res
    else:
        print(
            f'Failed to fetch latest commit from {branch} branch, code: {r.status_code}')
        sys.exit(1)


'''
fetch check runs of a commit.
@note, only works for public repos
'''
def fetch_check_runs(token: str, repo: str, ref: str):
    all_checks = []
    page = 1
    total_pages = 1
    per_page = 100
    failed_checks = []
    while page <= total_pages:
        print(f'Fetching check runs for page {page} of {total_pages} pages')
        url = f'https://api.github.com/repos/{repo}/commits/{ref}/check-runs?per_page={per_page}&page={page}'
        headers = {'Accept': 'application/vnd.github.v3+json',
                    'Authorization': f'Bearer {token}'
                  }
        r = requests.get(url, headers=headers)
        if r.status_code == 200:
            resp = r.json()
            all_checks.extend(resp['check_runs'])

            page += 1
            if 'total_count' in resp and resp['total_count'] > per_page:
                total_pages = math.ceil(resp['total_count'] / per_page)
        else:
            print(f'Failed to fetch check runs {r.status_code}')
            sys.exit(1)


    for crun in all_checks:
        if crun['status'] == 'completed' and crun['conclusion'] != 'success':
            print('Failed check: ', crun['name'])
            failed_checks.append(
                {'id': crun['id'], 'name': crun['name'], 'url': crun['url']})
        else:
            # pretty print crun
            # print(json.dumps(crun, indent=4))
            print('successed:', crun['id'], crun['name'],
                    crun['status'], crun['conclusion'])

    return failed_checks

'''
rerquest a check-run
'''
def trigger_build(failed_checks: list, repo: str, token: str):
    reruns = []
    for crun in failed_checks:
        if crun['name'].strip() in job_black_list:
            print(f'Skip black listed job {crun["name"]}')
            continue

        r = requests.get(crun['url'], headers={'Accept': 'application/vnd.github.v3+json',
                                               'User-Agent': 'python3',
                                               'Authorization': f'Bearer {token}'}
                         )
        if r.status_code == 200:
            # url example: https://github.com/qzhuyan/emqx/actions/runs/4469557961/jobs/7852858687
            run_id = r.json()['details_url'].split('/')[-3]
            reruns.append(run_id)
        else:
            print(f'failed to fetch check run {crun["name"]}')

    # remove duplicates
    for run_id in set(reruns):
        url = f'https://api.github.com/repos/{repo}/actions/runs/{run_id}/rerun-failed-jobs'

        r = requests.post(url, headers={'Accept': 'application/vnd.github.v3+json',
                                        'User-Agent': 'python3',
                                        'Authorization': f'Bearer {token}'}
                          )
        if r.status_code == 201:
            print(f'Successfully triggered build for {crun["name"]}')

        else:
            # Only complain but not exit.
            print(
                f'Failed to trigger rerun for {run_id}, {crun["name"]}: {r.status_code} : {r.text}')


def main():
    parser = OptionParser()
    parser.add_option("-r", "--repo", dest="repo",
                      help="github repo", default="emqx/emqx")
    parser.add_option("-t", "--token", dest="gh_token",
                      help="github API token")
    parser.add_option("-b", "--branch", dest="branch", default='master',
                      help="Branch that workflow runs on")
    (options, args) = parser.parse_args()

    # Get gh token from env var GITHUB_TOKEN if provided, else use the one from command line
    token = os.environ['GITHUB_TOKEN'] if 'GITHUB_TOKEN' in os.environ else options.gh_token

    target_commit = fetch_latest_commit(token, options.repo, options.branch)

    failed_checks = fetch_check_runs(token, options.repo, target_commit['sha'])

    trigger_build(failed_checks, options.repo, token)


if __name__ == '__main__':
    main()

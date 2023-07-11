#!/usr/bin/env python3
# Usage: python3 rerun-apps-version-check.py -t <github token> -r <repo>
#
# Default repo is emqx/emqx
#
import requests
import http.client
import json
import os
import sys
import time
import math
import inspect
from optparse import OptionParser
from urllib.parse import urlparse, parse_qs
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

user_agent = sys.argv[0].split('/')[-1]

def query(owner, repo):
    return """
query {
  repository(owner: "%s", name: "%s") {
    pullRequests(first: 25, states: OPEN, orderBy: {field:CREATED_AT, direction:DESC}) {
      nodes {
        url
        commits(last: 1) {
          nodes {
            commit {
              checkSuites(first: 25) {
                nodes {
                  url
                  checkRuns(first: 1, filterBy: {checkName: "check_apps_version"}) {
                    nodes {
                      url
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
    """ % (owner, repo)


def get_headers(token: str):
    return {'Accept': 'application/vnd.github+json',
            'Authorization': f'Bearer {token}',
            'X-GitHub-Api-Version': '2022-11-28',
            'User-Agent': f'{user_agent}'
            }

def get_session():
    session = requests.Session()

    retries = Retry(total=10,
                    backoff_factor=1,  # 1s
                    allowed_methods=None,
                    status_forcelist=[ 429, 500, 502, 503, 504 ])  # Retry on these status codes

    session.mount('https://', HTTPAdapter(max_retries=retries))

    return session

def get_check_suite_ids(token: str, repo: str):
    session = get_session()
    url = f'https://api.github.com/graphql'
    [repo_owner, repo_repo] = repo.split('/')
    r = session.post(url, headers=get_headers(token), json={'query': query(repo_owner, repo_repo)})
    if r.status_code == 200:
        resp = r.json()
        if not 'data' in resp:
            print(f'Failed to fetch check runs: {r.status_code}\n{r.json()}')
            sys.exit(1)
        result = []
        for pr in resp['data']['repository']['pullRequests']['nodes']:
            if not pr['commits']['nodes']:
                continue
            if not pr['commits']['nodes'][0]['commit']['checkSuites']['nodes']:
                continue
            for node in pr['commits']['nodes'][0]['commit']['checkSuites']['nodes']:
                if node['checkRuns']['nodes']:
                    url_parsed = urlparse(node['url'])
                    params = parse_qs(url_parsed.query)
                    check_suite_id = params['check_suite_id'][0] 
                    result.extend([(check_suite_id, pr['url'], node['checkRuns']['nodes'][0]['url'])])
        return result
    else:
        print(f'Failed to fetch check runs: {r.status_code}\n{r.text}')
        sys.exit(1)

def rerequest_check_suite(token: str, repo: str, check_suite_id: str):
    session = get_session()
    url = f'https://api.github.com/repos/{repo}/check-suites/{check_suite_id}/rerequest'
    r = session.post(url, headers=get_headers(token))
    if r.status_code == 201:
        print(f'Successfully triggered {url}')
    else:
        print(f'Failed to trigger {url}: {r.status_code}\n{r.text}')

def main():
    parser = OptionParser()
    parser.add_option("-r", "--repo", dest="repo",
                      help="github repo", default="emqx/emqx")
    parser.add_option("-t", "--token", dest="gh_token",
                      help="github API token")
    (options, args) = parser.parse_args()

    # Get github token from env var if provided, else use the one from command line.
    # The token must be exported in the env from ${{ secrets.GITHUB_TOKEN }} in the workflow.
    token = os.environ['GITHUB_TOKEN'] if 'GITHUB_TOKEN' in os.environ else options.gh_token
    for id, pr_url, check_run_url in get_check_suite_ids(token, options.repo):
        print(f'Attempting to re-request {check_run_url} for {pr_url}')
        rerequest_check_suite(token, options.repo, id)

if __name__ == '__main__':
    main()

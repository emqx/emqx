#!/usr/bin/env python3

import os
import sys
import json
import logging
import requests
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
from collections import defaultdict
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
import re

logger = logging.getLogger(__name__)

CT_JOB_PREFIX = 'run_test_cases / apps/'
ENTRYPOINT_WORKFLOW_NAMES = ['_push-entrypoint.yaml', '_pr_entrypoint.yaml']
REPO = 'emqx/emqx'
PLOT_GRID_COLS = 5
DPI = 150
FIG_WIDTH = 3
FIG_HEIGHT = 2.5

ACCEPTED_BASE_BRANCH_PATTERNS = [r'^release-', r'^master$']

def make_headers():
    token = os.environ.get('GH_TOKEN')
    if not token:
        raise ValueError("GH_TOKEN must be set")
    return {
        'Authorization': f'token {token}',
        'Accept': 'application/vnd.github.v3+json',
    }

def fetch_workflow_runs(workflow_info, since_date, per_page=100):
    workflow_id = workflow_info['id']
    workflow_name = workflow_info['name']
    runs_url = f"https://api.github.com/repos/{REPO}/actions/workflows/{workflow_id}/runs"
    headers = make_headers()
    page = 1
    workflow_runs = []

    while True:
        params = {
            'per_page': per_page,
            'page': page,
            'status': 'completed'
        }
        logger.debug(f"Fetching page {page}...")
        runs_response = requests.get(runs_url, headers=headers, params=params)
        runs_response.raise_for_status()
        runs_data = runs_response.json()

        if not runs_data['workflow_runs']:
            logger.debug("No more runs available")
            break

        page_runs = 0
        reached_date_limit = False
        for run in runs_data['workflow_runs']:
            run_date = to_datetime(run['created_at'])
            if run_date < since_date.astimezone():
                reached_date_limit = True
                break
            workflow_runs.append(run)
            page_runs += 1

        logger.debug(f"Page {page}: got {page_runs} runs")

        if reached_date_limit or len(runs_data['workflow_runs']) < per_page:
            break

        page += 1

    return workflow_runs


def fetch_runs(workflow_names, days=30):
    headers = make_headers()
    logger.info("Fetching workflows from GitHub")

    workflows_url = f"https://api.github.com/repos/{REPO}/actions/workflows"
    logger.debug(f"Workflows URL: {workflows_url}")
    workflows_response = requests.get(workflows_url, headers=headers)
    workflows_response.raise_for_status()

    all_workflows = workflows_response.json()['workflows']
    logger.debug(f"Repository has {len(all_workflows)} total workflows")

    workflow_ids = []
    for wf in all_workflows:
        for workflow_name in workflow_names:
            if wf['path'].endswith(workflow_name):
                workflow_ids.append({
                    'id': wf['id'],
                    'name': workflow_name,
                    'path': wf['path']
                })
                break

    if not workflow_ids:
        logger.error(f"No workflows found matching: {workflow_names}")
        paths = [wf['path'] for wf in all_workflows]
        logger.error(f"Available workflows: {paths.join(', ')}")
        sys.exit(1)

    since_date = datetime.now() - timedelta(days=days)
    logger.info(f"Fetching runs since: {since_date.strftime('%Y-%m-%d %H:%M:%S')}")

    all_runs = []

    for workflow_info in workflow_ids:
        workflow_name = workflow_info['name']

        logger.info(f"Fetching runs for '{workflow_name}'...")
        workflow_runs = fetch_workflow_runs(
            workflow_info, since_date
        )
        all_runs.extend(workflow_runs)
        logger.info(f"Collected {len(workflow_runs)} runs for '{workflow_name}'")

    logger.info(f"Total workflow runs collected: {len(all_runs)}")
    return all_runs


def get_pr_number_from_run(run):
    referenced = run.get('referenced_workflows', [])
    for ref in referenced:
        ref_str = ref.get('ref', '')
        # Look for refs/pull/12345/merge pattern
        match = re.match(r'refs/pull/(\d+)/merge', ref_str)
        if match:
            return int(match.group(1))
    return None


def fetch_pr_base_branch(pr_number):
    headers = make_headers()
    pr_url = f"https://api.github.com/repos/{REPO}/pulls/{pr_number}"

    try:
        response = requests.get(pr_url, headers=headers)
        response.raise_for_status()
        pr_data = response.json()
        return pr_data.get('base', {}).get('ref')
    except Exception as e:
        logger.debug(f"Failed to fetch PR {pr_number}: {e}")
        return None


def get_base_branch(run):
    run_id = run['id']
    head_branch = run.get('head_branch', 'unknown')
    event = run.get('event', 'unknown')

    base_branch = None

    if event == 'pull_request':
        pr_data = run.get('pull_requests', [])
        if pr_data:
            base_branch = pr_data[0].get('base', {}).get('ref')

        if not base_branch:
            pr_number = get_pr_number_from_run(run)
            if pr_number:
                logger.debug(f"Run {run_id}: fetching base branch for PR #{pr_number}")
                base_branch = fetch_pr_base_branch(pr_number)
            else:
                logger.debug(f"Run {run_id}: cannot determine PR number from run data")

        if not base_branch:
            logger.debug(f"Run {run_id}: cannot determine base branch for PR event")
    elif event == 'push':
        base_branch = head_branch
    else:
        logger.debug(f"Run {run_id}: event type '{event}' has no clear base branch")

    if base_branch:
        matches = any(re.match(pattern, base_branch) for pattern in ACCEPTED_BASE_BRANCH_PATTERNS)
        if not matches:
            logger.debug(f"Run {run_id}: skipping base branch '{base_branch}' (does not match accepted patterns)")
            base_branch = None

    return base_branch, head_branch, event


def process_single_run(run):
    headers = make_headers()
    run_id = run['id']
    run_date = to_datetime(run['created_at'])
    workflow_name = run.get('name', 'unknown')

    base_branch, head_branch, event = get_base_branch(run)

    if base_branch is None:
        logger.debug(f"Skipping run {run_id}: cannot determine base branch")
        return {
            'ct_data_points': [],
            'total_jobs': 0,
            'ct_jobs': 0,
            'skipped': 0,
            'skipped_no_base': 1
        }

    logger.debug(f"Processing run {run_id} ({workflow_name}) from {run_date.strftime('%Y-%m-%d %H:%M:%S')}, event={event}, head={head_branch}, base={base_branch}")

    jobs_url = f"https://api.github.com/repos/{REPO}/actions/runs/{run_id}/jobs"
    jobs_response = requests.get(jobs_url, headers=headers)
    jobs_response.raise_for_status()

    jobs_data = jobs_response.json()['jobs']
    logger.debug(f"Found {len(jobs_data)} jobs in run {run_id}")

    run_results = {
        'ct_data_points': [],
        'total_jobs': 0,
        'ct_jobs': 0,
        'skipped': 0,
        'skipped_no_base': 0
    }

    for job in jobs_data:
        job_name = job['name']
        job_conclusion = job.get('conclusion', 'unknown')
        run_results['total_jobs'] += 1

        if job_conclusion != 'success':
            logger.debug(f"Skipping unsuccessful job ({job_conclusion}): {job_name}")
            run_results['skipped'] += 1
            continue

        if not job_name.startswith(CT_JOB_PREFIX):
            continue
        job_name = job_name.replace(CT_JOB_PREFIX, '')
        job_name = job_name.split(' ')[0]

        steps = job.get('steps', [])
        ct_step = None
        for step in steps:
            step_name = step.get('name', '').lower()
            if 'run common test' in step_name:
                ct_step = step
                break

        if not ct_step or not ct_step.get('started_at') or not ct_step.get('completed_at'):
            continue

        start = to_datetime(ct_step['started_at'])
        end = to_datetime(ct_step['completed_at'])
        duration = (end - start).total_seconds()
        ct_step["duration"] = duration

        data_point = {
            'date': run_date,
            'duration': duration,
            'job_name': job_name,
            'run_id': run_id,
            'head_branch': head_branch,
            'base_branch': base_branch,
            'event': event
        }

        run_results['ct_data_points'].append(data_point)
        run_results['ct_jobs'] += 1
        logger.debug(f"Collected CT job: {job_name} ({duration:.1f}s)")

    return run_results


def collect_job_durations(runs):
    ct_durations = defaultdict(list)
    total_jobs_processed = 0
    ct_jobs_collected = 0
    skipped_unsuccessful = 0
    skipped_no_base = 0

    logger.info("Collecting job durations from workflow runs")

    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(tqdm(
            executor.map(process_single_run, runs),
            total=len(runs),
            desc="Processing runs",
            unit="run"
        ))

    for result in results:
        total_jobs_processed += result['total_jobs']
        ct_jobs_collected += result['ct_jobs']
        skipped_unsuccessful += result['skipped']
        skipped_no_base += result.get('skipped_no_base', 0)

        for data_point in result['ct_data_points']:
            job_name = data_point['job_name']
            ct_durations[job_name].append(data_point)

    logger.info(f"Job collection complete: {ct_jobs_collected} CT jobs collected from {total_jobs_processed} total jobs ({skipped_unsuccessful} unsuccessful skipped, {skipped_no_base} runs skipped due to unknown base branch), {len(ct_durations)} unique job names")

    if ct_durations:
        logger.info("CT Jobs found:")
        for job_name, data_points in sorted(ct_durations.items()):
            logger.info(f"  - {job_name}: {len(data_points)} runs")

    return ct_durations


def generate_plot(ct_durations, days, output_file='test_durations.png'):
    logger.info("Generating duration plot")

    if not ct_durations:
        logger.warning("No CT jobs to plot")
        return None

    all_dates = []
    all_durations = []
    for data_points in ct_durations.values():
        for dp in data_points:
            all_dates.append(dp['date'])
            all_durations.append(dp['duration'])

    if not all_dates or not all_durations:
        logger.warning("No data points to plot")
        return None

    x_min, x_max = min(all_dates), max(all_dates)
    y_min, y_max = min(all_durations), max(all_durations)

    y_padding = (y_max - y_min) * 0.05
    y_min = max(0, y_min - y_padding)
    y_max = y_max + y_padding

    branch_colors = {}
    all_branches = set()
    for data_points in ct_durations.values():
        for dp in data_points:
            all_branches.add(dp.get('base_branch', 'unknown'))

    cmap = plt.get_cmap('tab10')
    for i, branch in enumerate(sorted(all_branches)):
        branch_colors[branch] = cmap(i % 10)

    sorted_jobs = sorted(ct_durations.items())
    num_jobs = len(sorted_jobs)

    num_cols = PLOT_GRID_COLS
    num_rows = (num_jobs + num_cols - 1) // num_cols

    logger.info(f"Plotting {num_jobs} jobs in {num_rows}x{num_cols} grid")

    fig_width = num_cols * FIG_WIDTH
    fig_height = num_rows * FIG_HEIGHT
    fig, axes = plt.subplots(num_rows, num_cols, figsize=(fig_width, fig_height))
    fig.suptitle(f'Test Duration Analysis - Last {days} Days', fontsize=11, fontweight='bold')

    if num_rows == 1 and num_cols == 1:
        axes = [axes]
    else:
        axes = axes.flatten()

    for idx, (job_name, data_points) in enumerate(sorted_jobs):
        ax = axes[idx]

        if len(data_points) > 0:
            title = job_name.split(' ')[0]

            branch_groups = defaultdict(list)
            for dp in data_points:
                branch = dp.get('base_branch', 'unknown')
                branch_groups[branch].append(dp)

            for branch in sorted(branch_groups.keys()):
                branch_data = branch_groups[branch]
                df = pd.DataFrame(branch_data)
                df = df.sort_values('date')
                color = branch_colors[branch]
                ax.plot(df['date'], df['duration'], marker='o', linewidth=1, markersize=2,
                       color=color, label=branch)

            ax.set_title(title, fontsize=10, fontweight='bold')
            ax.grid(True, alpha=0.3)
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%m.%d'))
            ax.tick_params(axis='x', rotation=45, labelsize=7)
            ax.tick_params(axis='y', labelsize=7)

            ax.set_xlim(x_min, x_max)
            ax.set_ylim(y_min, y_max)

            if len(branch_groups) > 1:
                ax.legend(fontsize=6, loc='best')

            logger.debug(f"Plotted {job_name}: {len(data_points)} data points across {len(branch_groups)} branches")

    for idx in range(num_jobs, len(axes)):
        axes[idx].set_visible(False)

    logger.info(f"Plotted {num_jobs} job series in {num_rows}x{num_cols} grid")

    plt.tight_layout(rect=[0, 0, 1, 0.97])

    plt.savefig(output_file, dpi=DPI, bbox_inches='tight')
    plt.close(fig)

    image_size_kb = os.path.getsize(output_file) / 1024
    logger.info(f"Image size: {image_size_kb:.1f} KB")
    logger.info(f"Graph saved to {output_file}")

    logger.info(f"Ranges - X: {x_min.strftime('%Y-%m-%d')} to {x_max.strftime('%Y-%m-%d')}, Y: {y_min:.1f}s to {y_max:.1f}s")
    return output_file


def to_datetime(datetime_str):
    return datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))

def load_cached_durations(durations_file='durations.json'):
    logger.info(f"Loading cached durations from {durations_file}")

    if not os.path.exists(durations_file):
        logger.error(f"Cached durations file not found: {durations_file}")
        sys.exit(1)

    with open(durations_file, 'r') as f:
        durations_output = json.load(f)

    ct_durations = defaultdict(list)
    for job_name, data_points in durations_output.items():
        for dp in data_points:
            ct_durations[job_name].append({
                'date': to_datetime(dp['date']),
                'duration': dp['duration'],
                'run_id': dp['run_id'],
                'head_branch': dp['head_branch'],
                'base_branch': dp['base_branch'],
                'event': dp['event']
            })

    logger.info(f"Loaded {len(ct_durations)} jobs from cache")
    return ct_durations


def save_raw_data(runs, ct_durations):
    logger.info("Saving runs data to runs.json")
    with open('runs.json', 'w') as f:
        json.dump(runs, f, indent=2, default=str)
    logger.info("Runs data saved to runs.json")

    logger.info("Saving durations data to durations.json")
    durations_output = {}
    for job_name, data_points in ct_durations.items():
        durations_output[job_name] = [
            {
                'date': dp['date'].isoformat(),
                'duration': dp['duration'],
                'run_id': dp['run_id'],
                'head_branch': dp['head_branch'],
                'base_branch': dp['base_branch'],
                'event': dp['event']
            }
            for dp in data_points
        ]
    with open('durations.json', 'w') as f:
        json.dump(durations_output, f, indent=2)
    logger.info("Durations data saved to durations.json")


def save_outputs(ct_durations, days):
    graph_file = generate_plot(ct_durations, days)

    total_jobs = len(ct_durations)
    total_data_points = sum(len(dps) for dps in ct_durations.values())
    logger.info(f"Analysis complete! Plotted {total_jobs} jobs with {total_data_points} total runs")
    logger.info(f"Output file: {graph_file}")

def main():
    log_level = os.environ.get('LOG_LEVEL', 'INFO').upper()
    logging.basicConfig(
        level=log_level,
        format='%(levelname)s: %(message)s',
        stream=sys.stdout
    )
    days = int(os.environ.get('DAYS', '30'))
    use_cached_data = os.environ.get('USE_CACHED_DATA', '').lower() in ('1', 'true', 'yes')

    if use_cached_data:
        logger.info("Using cached data from durations.json")
        ct_durations = load_cached_durations()
    else:
        runs = fetch_runs(ENTRYPOINT_WORKFLOW_NAMES, days)

        if not runs:
            logger.warning("No workflow runs found. Nothing to analyze.")
            sys.exit(0)

        ct_durations = collect_job_durations(runs)

        if not ct_durations:
            logger.warning("No CT jobs found in the collected runs.")
            sys.exit(0)

        save_raw_data(runs, ct_durations)

    if not ct_durations:
        logger.warning("No CT jobs to process.")
        sys.exit(0)

    save_outputs(ct_durations, days)


if __name__ == '__main__':
    main()

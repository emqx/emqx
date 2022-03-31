#!/usr/bin/env python3

# format_app.py

import argparse
import os
import re
import subprocess
import sys


def handle_cli_args():
  parser = argparse.ArgumentParser(description='A script for formatting a given app within emqx')
  parser.add_argument("-a", "--application", required=True, help="The relative path to the application which is to be formatted.")
  parser.add_argument('-b', "--branch", help="The git branch to be switched to before formatting the code. Required unless the -f option is passed in which case the value will be ignored even if provided.")
  parser.add_argument('-f', "--format_in_place", default=False, action="store_true", help="Pass the -f option to format on the current git branch, otherwise a branch name must be provided by passing the -b option.")
  args = parser.parse_args()

  if not args.format_in_place and not args.branch:
    sys.exit("A new git branch name must be provided with the -b option unless the -f option is given to format the code in place.")

  return args


def get_app_path(application):
  root_path = os.path.dirname(os.path.realpath(__file__))
  full_path = f"{root_path}/../{application}"
  if os.path.isdir(full_path):
    return full_path
  sys.exit(f"The application provided ({application}) does not appear at the expected location: {full_path}")


def maybe_switch_git_branch(format_in_place, branch):
  if not format_in_place:
    PIPE = subprocess.PIPE
    process = subprocess.Popen(["git", "status"], stdout=PIPE, text=True)
    stdoutput, stderroutput = process.communicate()

    if f"On branch {branch}" in stdoutput:
      return
    elif "working tree clean" in stdoutput:
      subprocess.call(["git", "checkout", "-b", branch])
    else:
      sys.exit("Cannot switch git branches while there are changes waiting to be committed.")


def maybe_add_rebar_plugin():
  with open("rebar.config", "r+") as f:
    text = f.read()
    erlfmt_pattern = "\{project_plugins.+?erlfmt.+\}"
    plugin_pattern = "\{project_plugins.+?\}"

    if not re.search(erlfmt_pattern, text) and not re.search(plugin_pattern, text):
      f.write("\n{project_plugins, [erlfmt]}.\n")
    elif not re.search(erlfmt_pattern, text) and re.search(plugin_pattern, text):
      sys.exit("Could not find the erlfmt plugin but the 'project_plugins' declaration already exists. Please add 'erlfmt' to the plugins list manually and rerun the script.")


def execute_formatting():
  subprocess.call(["rebar3", "fmt", "-w", "{src,include,test}/**/*.{hrl,erl,app.src}"])
  subprocess.call(["rebar3", "fmt", "-w", "rebar.config"])


def main():
  args = handle_cli_args()
  app_path = get_app_path(args.application)
  os.chdir(app_path)
  maybe_switch_git_branch(args.format_in_place, args.branch)
  maybe_add_rebar_plugin()
  execute_formatting()


if __name__ == "__main__":
  main()

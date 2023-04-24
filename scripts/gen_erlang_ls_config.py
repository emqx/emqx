#!/usr/bin/python3

import os
import argparse
from jinja2 import Template

DEFAULT_TEMPLATE_FILE = './scripts/gen_erlang_ls_config.tpl'
DEPS_DIR = '_build/default/lib/'
APPS_DIR = 'apps/'

def get_dir_list(parent_dir):
  return [dir for dir in os.listdir(parent_dir) if os.path.isdir(os.path.join(parent_dir, dir)) and not dir.startswith('.')]

parser = argparse.ArgumentParser(description='Reads dirs in `_build/default/lib`, removes those found in `apps` from the list and provides the result as `default_deps_dir` to a jinja2 template.')
parser.add_argument('-f', '--file', dest='input_file', default=DEFAULT_TEMPLATE_FILE,
                    help=f'input template file (default: {DEFAULT_TEMPLATE_FILE})')

args = parser.parse_args()

with open(args.input_file, 'r') as file:
    template_str = file.read()

template = Template(template_str)

deps_list = get_dir_list(DEPS_DIR)
apps_list = get_dir_list(APPS_DIR)

deps_dirs = [DEPS_DIR  + dir for dir in deps_list if dir not in apps_list]

rendered_str = template.render(default_deps_dirs=deps_dirs)

print(rendered_str)

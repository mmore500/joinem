#!/bin/bash

set -e # exit with error if any of this fails

script_dir="$(dirname "$(readlink -f "$0")")"

python3 -m pytest "${script_dir}" "$@"

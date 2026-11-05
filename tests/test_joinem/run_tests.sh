#!/bin/bash

# adapted from https://github.com/mmore500/hstrat/blob/e664e7ab3e8ea7aed0bbdd9ac7c6190bd6c83015/tests/test_hstrat/run_tests.sh

set -e # exit with error if any of this fails

script_dir="$(dirname "$(readlink -f "$0")")"

python3 -m pytest "${script_dir}" "$@"

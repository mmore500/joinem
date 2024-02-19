#!/usr/bin/bash

set -e
set -u

cd "$(dirname "$0")"

python3.10 -m piptools compile "pyproject.toml" --extra "dev"

#!/usr/bin/bash

set -e
set -u

cd "$(dirname "$0")"

python3 -m uv pip compile "pyproject.toml" \
    --extra "dev" --python-version "3.10" \
    | tee requirements.txt

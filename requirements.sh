#!/usr/bin/bash

set -e
set -u

cd "$(dirname "$0")"

python3 -m uv pip compile "pyproject.toml" \
    --extra "dev" --extra "testing" --python-version "3.10" --universal \
    | tee requirements.txt

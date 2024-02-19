#!/bin/bash

set -e
set -u

cd "$(dirname "$0")"
python3 -m black .
python3 -m isort .
python3 -m ruff .

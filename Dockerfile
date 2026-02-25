FROM python:3.10-slim

WORKDIR /usr/src/app

COPY . .

RUN python3 -m pip install --upgrade pip setuptools
RUN python3 -m pip install .
RUN joinem --version

# Block default entrypoint; use 'singularity exec python3 -m joinem' instead
ENTRYPOINT echo "Error:  no default entrypoint; use 'singularity exec python3 -m joinem' instead of 'singularity run'." >&2; exit 1

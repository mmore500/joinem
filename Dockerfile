FROM python:3.10-slim

WORKDIR /usr/src/app

COPY . .

RUN python3 -m pip install .
RUN joinem --version

# Run joinem when the container launches
ENTRYPOINT ["joinem"]

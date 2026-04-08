FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends r-base \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir pandas matplotlib numpy

# Keep container alive so ECPaaS can run it as a long-lived service if desired.
CMD ["bash", "-lc", "sleep infinity"]

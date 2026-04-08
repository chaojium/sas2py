## Runner Image

This image is intended for ECPaaS-hosted code execution workloads.

Build:

```bash
docker build -t sas2py-runner:latest -f runner/Dockerfile .
```

Push to your ECPaaS registry, then set:

- `CODE_RUNNER_DOCKER_PYTHON_IMAGE=<your-registry>/sas2py-runner:latest`
- `CODE_RUNNER_DOCKER_R_IMAGE=<your-registry>/sas2py-runner:latest`

The app will execute:

- Python via `python -u -`
- R via `Rscript -`

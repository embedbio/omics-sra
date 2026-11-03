# Lambda container image for the weekly WGS-run batch job.
# AWS has no Python 3.15 Lambda base image, so this uses the official Python image
# plus awslambdaric (the Lambda runtime interface client).
# Build for arm64 with provenance off (Lambda rejects multi-manifest images):
#   docker build --platform linux/arm64 --provenance=false -t omics-sra .
FROM python:3.15.2-slim

COPY --from=ghcr.io/astral-sh/uv:0.7.13 /uv /bin/uv

ENV LAMBDA_TASK_ROOT=/var/task \
    HOME=/tmp \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
WORKDIR ${LAMBDA_TASK_ROOT}

# Install exactly the locked runtime dependencies (no dev tools) plus awslambdaric.
COPY pyproject.toml uv.lock ./
RUN uv export --frozen --no-dev --group lambda --no-emit-project -o requirements.txt \
    && uv pip install --system --no-cache -r requirements.txt \
    && rm requirements.txt

COPY wgs_runs.py batches.py lambda_handler.py config.yaml ./

ENTRYPOINT ["python", "-m", "awslambdaric"]
CMD ["lambda_handler.handler"]

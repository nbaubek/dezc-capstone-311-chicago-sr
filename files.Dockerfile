# ============================================
# Dockerfile for running Prefect flows with uv
# ============================================
FROM python:3.12-slim

# Copy uv binary from the official uv image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Set working directory
WORKDIR /app

# Set PATH to include the virtual environment
ENV PATH="/app/.venv/bin:$PATH"

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PREFECT_HOME=/home/prefectuser/.prefect

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency files
COPY .python-version pyproject.toml uv.lock ./

# Install dependencies using uv
# --frozen: Use exact versions from uv.lock
RUN uv sync --frozen --no-dev

# Copy application code
COPY flows/ ./flows/
COPY .pyiceberg.yaml ./

# Create a non-root user for running flows
RUN useradd -m -u 1000 prefectuser && \
    chown -R prefectuser:prefectuser /app

# Switch to non-root user
USER prefectuser

# Default command: Prefect worker
# This can be overridden at runtime
CMD ["prefect", "worker", "start", "--pool", "local-pool"]

# Health check to verify the container is running
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

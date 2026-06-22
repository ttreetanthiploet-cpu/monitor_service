FROM python:3.12-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Copy dependency files first (layer cache)
COPY pyproject.toml uv.lock ./

# Install dependencies from lock file (no network resolution needed)
RUN uv sync --frozen --no-dev

# Copy source
COPY src/ ./src/

# Run the monitor using uv
CMD ["uv", "run", "python", "-m", "src.monitor"]

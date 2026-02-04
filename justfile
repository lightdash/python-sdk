# List available commands
default:
    @just --list

# Install development dependencies
install:
    uv venv --quiet || true
    uv pip install -e ".[dev]"

# Run acceptance tests
test:
    #!/usr/bin/env bash
    if [ -f .env ]; then
        source .env
    else
        echo "Warning: .env file not found. Make sure environment variables are set manually."
    fi
    uv run pytest tests/test_acceptance.py -v

# Clean build artifacts
clean:
    rm -rf dist/
    rm -rf *.egg-info/

# Build package distributions
build: clean
    uv pip install build
    python -m build

# Build and publish to PyPI
publish: build
    uv pip install twine
    twine check dist/*
    twine upload dist/*

# Build and publish to TestPyPI
publish-test: build
    uv pip install twine
    twine check dist/*
    twine upload --repository testpypi --config-file ~/.pypirc dist/*

# Launch Jupyter notebook for interactive development
notebook:
    uv run jupyter notebook
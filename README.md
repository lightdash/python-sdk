# Lightdash Python Client

A Python client for interacting with the Lightdash API.

## Installation

```bash
pip install lightdash
```

## Usage

See the [example notebook](examples/getting_started.ipynb) for a tutorial of how to use the client.

```python
from lightdash import Client

client = Client(
    instance_url="https://your-instance.lightdash.com",
    access_token="your-access-token",
    project_uuid="your-project-uuid"
)
```

## Development

### Prerequisites

- Python 3.9 or higher
- [uv](https://github.com/astral-sh/uv) for dependency management
- [just](https://github.com/casey/just) for running commands

### Setting up the development environment

1. Clone the repository:
```bash
git clone https://github.com/yourusername/pylightdash.git
cd pylightdash
```

2. Create and activate a virtual environment:
```bash
uv venv
```

3. Set up your environment variables by copying the example file:
```bash
cp .env.example .env
```

4. Edit `.env` with your Lightdash credentials:
```bash
LIGHTDASH_INSTANCE_URL="https://your-instance.lightdash.com"
LIGHTDASH_ACCESS_TOKEN="your-access-token"
LIGHTDASH_PROJECT_UUID="your-project-uuid"
```

5. Install development dependencies:
```bash
just install
```

### Available Commands

View all available commands:
```bash
just
```

Common commands:
- `just install` - Install development dependencies
- `just test` - Run acceptance tests
- `just build` - Build package distributions
- `just clean` - Remove build artifacts

## Publishing

### Setting up PyPI credentials

Create a `~/.pypirc` file with your PyPI API tokens:

```ini
[pypi]
username = __token__
password = your-pypi-token-here

[testpypi]
username = __token__
password = your-testpypi-token-here
```

Make sure to:
1. Use API tokens instead of your actual username/password
2. Keep the file secure (`chmod 600 ~/.pypirc`)
3. Never commit this file to version control
4. Use different tokens for TestPyPI and PyPI
5. Generate tokens with minimal required permissions

### Publishing to PyPI

First, test your package on TestPyPI:
```bash
just publish-test
```

If everything looks good on TestPyPI, publish to PyPI:
```bash
just publish
```

Note: The package version in `pyproject.toml` must be incremented for each new release.

## License

MIT

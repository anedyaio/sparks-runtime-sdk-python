[<img alt="PyPI" src="https://img.shields.io/pypi/v/anedya-sparks-runtime?style=for-the-badge">](https://pypi.org/project/anedya-sparks-runtime/)&nbsp;&nbsp;[<img alt="Anedya Documentation" src="https://img.shields.io/badge/Anedya-Documentation-blue?style=for-the-badge">](https://docs.anedya.io?utm_source=github&utm_medium=link&utm_campaign=github-sparks-sdk&utm_content=python)


 <!---<div style="width:20%; margin:0 auto;margin-bottom:50px;margin-top:50px;">-->
<p align="center">
    <img src="https://cdn.anedya.io/anedya_black_banner.png" alt="Logo">
</p>
<!--</div>-->

# Anedya Sparks Runtime SDK

Build Python functions for Anedya Sparks with this SDK. You write a handler for each invocation; the SDK connects to the Sparks runtime, delivers events, and returns your handler's result.

## Project structure

Create a project with `packages/` for the function's installed dependencies and `src/` for your function. Use a separate local virtual environment for development. Sparks runs `src/main.py` as the entrypoint.

```text
my-spark-function/
├── packages/        # Function dependencies included in the asset
├── src/
│   └── main.py      # Required entrypoint
└── .venv/           # Local development environment; do not upload
```

Set up the environment from your project directory:

Use Python 3.13 or newer.

```bash
python -m venv .venv
source .venv/bin/activate
mkdir -p packages src
python -m pip install --target packages anedya-sparks-runtime
```

On Windows, activate the environment with `.venv\Scripts\activate`.

## Write a function

Put this in `src/main.py`:

```python
import json

import anedya.sparks_runtime as sparks


@sparks.handler
def handle(event: sparks.Event) -> str:
    data = event.json_payload()
    result = {"message": f"Hello, {data.get('name', 'Sparks')}!"}
    return json.dumps(result)


sparks.run()
```

The handler receives one `sparks.Event`. Use `event.json_payload()` for a JSON payload, or inspect `event.request_id`, `event.trigger_type`, and other event fields. Return `bytes`, `str`, or `None`; strings are encoded by the SDK. Keep `sparks.run()` at the end of `main.py`: it starts the invocation loop and blocks while the function serves requests.

## Package for upload

The setup command installs the SDK and its dependencies into `packages/`. Install additional function dependencies there with `python -m pip install --target packages <package>`. From the project directory, zip `packages/` and `src/`; the local `.venv/` is only for development and does not need to be included.

```bash
zip -r function.zip packages src
```

Upload `function.zip` as the function asset.

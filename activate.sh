#!/bin/bash
#
#
#       Script to check and install required Python packages, 
#       add directories to PATH, and activate a virtual environment.
#
# ---------------------------------------------------------------------------------------
#
set -e

PACKAGES=""
if ! command -v python &> /dev/null; then PACKAGES="$PACKAGES python3"; fi
if ! command -v pip &> /dev/null; then PACKAGES="$PACKAGES python3-pip"; fi
if [ ! -z "$PACKAGES" ]; then
    sudo apt-get update 2>&1 > /dev/null
    sudo DEBIAN_FRONTEND=noninteractive apt-get install -y $PACKAGES 2>&1 > /dev/null
fi

if ! command -v uv &> /dev/null; then pip install --break-system-packages uv; fi
if ! command -v ruff &> /dev/null; then pip install --break-system-packages ruff; fi

[[ ":$PATH:" != *":$HOME/.local/bin:"* ]] && export PATH="$PATH:$HOME/.local/bin"

uv sync --python 3.11
[ -f .venv/bin/activate ] && source .venv/bin/activate

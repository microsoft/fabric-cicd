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
if ! command -v python &> /dev/null; then PACKAGES="python3"; fi
if ! command -v pip &> /dev/null; then PACKAGES="${PACKAGES:+$PACKAGES }python3-pip"; fi
if ! command -v ruff &> /dev/null; then PACKAGES="${PACKAGES:+$PACKAGES }ruff"; fi
if [ -n "$PACKAGES" ]; then
    sudo apt-get update > /dev/null 2>&1
    sudo DEBIAN_FRONTEND=noninteractive apt-get install -y $PACKAGES > /dev/null 2>&1
fi
command -v uv &> /dev/null || curl -LsSf https://astral.sh/uv/install.sh | sh

[[ ":$PATH:" != *":$HOME/.local/bin:"* ]] && export PATH="$PATH:$HOME/.local/bin"
[[ ":$PATH:" != *":$HOME/.cargo/bin:"* ]] && export PATH="$PATH:$HOME/.cargo/bin"

uv sync --python 3.11
[ -f .venv/bin/activate ] && source .venv/bin/activate

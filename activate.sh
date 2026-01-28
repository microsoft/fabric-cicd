#!/bin/bash
#
#
#       Script to check and install required Python packages, Node.js tools,
#       add directories to PATH, and activate a virtual environment.
#
# ---------------------------------------------------------------------------------------
#
set -e

PACKAGES=""
if ! command -v python3.11 &> /dev/null; then PACKAGES="python3.11"; fi
if ! command -v pip &> /dev/null; then PACKAGES="${PACKAGES:+$PACKAGES }python3-pip"; fi
if ! command -v node &> /dev/null; then PACKAGES="${PACKAGES:+$PACKAGES }nodejs"; fi
if ! command -v npm &> /dev/null; then PACKAGES="${PACKAGES:+$PACKAGES }npm"; fi
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    if [ -n "$PACKAGES" ]; then
        echo "Installing required packages for Linux: $PACKAGES"
        sudo apt-get update > /dev/null 2>&1
        if sudo DEBIAN_FRONTEND=noninteractive apt-get install -y $PACKAGES > /dev/null 2>&1; then
            echo "Packages installed successfully."
        else
            echo "Failed to install packages."
            exit 1
        fi
    fi
elif [[ "$OSTYPE" == "darwin"* ]]; then
    if ! command -v brew &> /dev/null; then
        echo "Homebrew not found. Please install Homebrew first."
        exit 1
    fi
    
    BREW_PACKAGES=""
    if ! command -v python3.11 &> /dev/null; then BREW_PACKAGES="python@3.11"; fi
    if ! command -v node &> /dev/null; then BREW_PACKAGES="${BREW_PACKAGES:+$BREW_PACKAGES }node"; fi

    if [ -n "$BREW_PACKAGES" ]; then
        echo "Installing required packages for macOS: $BREW_PACKAGES"
        if brew install $BREW_PACKAGES; then
            echo "Packages installed successfully."
        else
            echo "Failed to install packages."
            exit 1
        fi
    fi
fi

# Install uv if not present
if ! command -v uv &> /dev/null; then
    echo "Installing uv..."
    if curl -LsSf https://astral.sh/uv/install.sh | sh; then
        echo "uv installed successfully."
    else
        echo "Failed to install uv."
        exit 1
    fi
else
    echo "uv is already installed."
fi

# Install changie globally via npm if not present
if ! command -v changie &> /dev/null; then
    echo "Installing changie globally via npm..."
    if npm install -g changie; then
        echo "changie installed successfully."
    else
        echo "Failed to install changie."
        exit 1
    fi
else
    echo "changie is already installed."
fi

# Install VS Code Python extension if VS Code is available
if command -v code &> /dev/null; then
    echo "Installing VS Code Python extension..."
    if code --install-extension ms-python.python --force > /dev/null 2>&1; then
        echo "VS Code Python extension installed successfully."
    else
        echo "Failed to install VS Code Python extension."
    fi
else
    echo "VS Code not found, skipping extension installation."
fi

# Add required directories to PATH
if [[ ":$PATH:" != *":$HOME/.local/bin:"* ]]; then
    export PATH="$PATH:$HOME/.local/bin"
    echo "Added $HOME/.local/bin to PATH."
fi
if [[ ":$PATH:" != *":$HOME/.cargo/bin:"* ]]; then
    export PATH="$PATH:$HOME/.cargo/bin"
    echo "Added $HOME/.cargo/bin to PATH."
fi

# Sync Python environment and activate
echo "Syncing Python environment with uv..."
if uv sync --python 3.11; then
    echo "Python environment synced successfully."
else
    echo "Failed to sync Python environment."
    exit 1
fi

if [ -f .venv/bin/activate ]; then
    source .venv/bin/activate
    echo "Virtual environment activated."
else
    echo "Virtual environment not found."
fi

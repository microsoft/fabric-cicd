# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

<#
.SYNOPSIS
Script to check and install required Python packages, Node.js tools, add directories to PATH, and activate a virtual environment.

.DESCRIPTION
This script performs the following tasks:
1. Checks if Python is installed.
2. Checks if pip is installed.
3. Checks if Node.js and npm are installed.
4. Checks and installs specified Python packages if they are not already installed.
5. Installs changie globally via npm if not already installed.
6. Adds a specified directory to the system PATH if it is not already included.
7. Ensures the 'uv' command is available in the PATH.
8. Activates a virtual environment using 'uv'.

.NOTES
Make sure that Python, pip, Node.js, and npm are installed and available in the system PATH.
#>

# Function to check if a dependency is available
function Test-Dependancy {
    param (
        [string]$commandName
    )

    if (-not (Get-Command $commandName -ErrorAction SilentlyContinue)) {
        Write-Host " $commandName is not installed or not in PATH. Please install $commandName and make sure it's available in the PATH."
        exit 1
    }
    else {
        $commandPath = (Get-Command $commandName).Path
        $commandDirectory = [System.IO.Path]::GetDirectoryName($commandPath)
        Write-Host "$commandName is installed in $commandPath" 
        Add-DirectoryToPath -directory $commandDirectory
    }
}

# Function to install required packages if not already installed
function Test-And-Install-Python-Package {
    param (
        [string]$packageName
    )

    pip show $packageName -q
    if ($LASTEXITCODE -ne 0) {
        Write-Host "$packageName is not installed. Installing $packageName..."
        try {
            pip install $packageName
            Write-Host "$packageName installed successfully."
        }
        catch {
            Write-Host "Failed to install $packageName. Please check your pip installation."
            exit 1
        }
    }
    else {
        Write-Host "$packageName is already installed."
    }
}

# Function to install changie globally via npm if not already installed
function Test-And-Install-Changie {
    if (-not (Get-Command changie -ErrorAction SilentlyContinue)) {
        Write-Host "changie not found, installing globally via npm..."
        try {
            npm install -g changie --registry https://registry.npmjs.org/
            
            # Add npm global bin to PATH if needed
            $npmGlobalPath = npm config get prefix
            if ($npmGlobalPath) {
                Add-DirectoryToPath -directory $npmGlobalPath
            }
            
            # Refresh PATH for the current session
            $env:Path = [System.Environment]::GetEnvironmentVariable("Path","Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path","User")
            
            Test-Dependancy -commandName "changie"
        }
        catch {
            Write-Host "Failed to install changie via npm. Please check your npm installation and connection."
            exit 1
        }
    }
    else {
        Write-Host "changie is already installed."
    }
}

# Function to add a directory to PATH
function Add-DirectoryToPath {
    param (
        [string]$directory
    )

    if (-not ($env:Path -split ';' | ForEach-Object { $_.Trim() } | Where-Object { $_ -eq $directory })) {
        $env:Path += ";$directory"
        Write-Host "Added $directory to PATH."
    }
}

# Check if dependencies are installed and add directory to PATH
Test-Dependancy -commandName "python"
Test-Dependancy -commandName "pip"
Test-Dependancy -commandName "node"
Test-Dependancy -commandName "npm"
# Check and install required packages
Test-And-Install-Python-Package -packageName "uv"
Test-And-Install-Python-Package -packageName "ruff"
Test-And-Install-Changie


# uv fallback to default path if unavailable in python directory
if (-not (Get-Command uv -ErrorAction SilentlyContinue)) {
    Write-Host "uv is not recognized. Attempting to add uv to PATH..."
    $localBinPath = [System.IO.Path]::Combine($env:USERPROFILE, '.local', 'bin')
    Add-DirectoryToPath -directory $localBinPath
    Test-Dependancy -commandName "uv"
}

# Activate the environment
uv sync --python 3.11
$venvPath = ".venv\Scripts\activate.ps1"

if (Test-Path $venvPath) {
    & $venvPath
    Write-Host "venv activated"
}
else {
    Write-Host "venv not found"
}

Write-Host ""
Write-Host "To deactivate the environment, run " -NoNewline
Write-Host "deactivate" -ForegroundColor Green

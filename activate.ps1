# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

<#
.SYNOPSIS
Script to check and install required Python packages, add directories to PATH, and activate a virtual environment.

.DESCRIPTION
This script performs the following tasks:
1. Checks if Python is installed.
2. Checks if pip is installed.
3. Checks and installs specified Python packages if they are not already installed.
4. Adds a specified directory to the system PATH if it is not already included.
5. Ensures the 'uv' command is available in the PATH.
6. Activates a virtual environment using 'uv'.

.NOTES
Make sure that Python and pip are installed and available in the system PATH.
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
function Test-And-Install-Package {
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

# Check and install required packages
Test-And-Install-Package -packageName "uv"
Test-And-Install-Package -packageName "ruff"

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

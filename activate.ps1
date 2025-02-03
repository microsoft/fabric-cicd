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

# Check if Python is installed
if (Get-Command python -ErrorAction SilentlyContinue) {
    Write-Host "Python is installed in " -NoNewline
    Write-Host (Get-Command python).Path
    
    # Check if pip is installed
    if (Get-Command pip -ErrorAction SilentlyContinue) {
        Write-Host "Pip is installed in " -NoNewline
        Write-Host (Get-Command pip).Path
        
        # Check and install required packages
        Test-And-Install-Package -packageName "uv"
        Test-And-Install-Package -packageName "ruff"
    }
    else {
        Write-Host "pip is not installed or not in PATH. Please install pip and make sure it's available in the PATH."
        exit 1
    }
}
else {
    Write-Host "Python is not installed. Please install Python and make sure it's available in the PATH."
    exit 1
}

# Ensure uv is in PATH
if (-not (Get-Command uv -ErrorAction SilentlyContinue)) {
    Write-Host "uv is not recognized. Attempting to add uv to PATH..."
    $localBinPath = [System.IO.Path]::Combine($env:USERPROFILE, '.local', 'bin')
    Add-DirectoryToPath -directory $localBinPath
}

# Activate the environment
if (Get-Command uv -ErrorAction SilentlyContinue) {
    uv sync  --python 3.11
    $venvPath = ".venv\Scripts\activate.ps1"
    
    if (Test-Path $venvPath) {
        & $venvPath
        Write-Host "venv activated"
    }
    else {
        Write-Host "venv not found"
    }
}
else {
    Write-Host "uv is not installed or not in PATH. Please install uv and make sure it's available in the PATH."
    exit 1
}


Write-Host ""
Write-Host "To deactivate the environment, run " -NoNewline
Write-Host "deactivate" -ForegroundColor Green

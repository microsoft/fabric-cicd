<#
.SYNOPSIS
Checks if a Python package is installed and installs it if it is not.

.DESCRIPTION
The Test-And-Install-Package function checks if a specified Python package is installed using pip. 
If the package is not installed, it attempts to install the package. If the installation fails, 
an error message is displayed and the script exits with an error code.

.PARAMETER packageName
The name of the Python package to check and install if necessary.

.EXAMPLE
Test-And-Install-Package -packageName "uv"
This example checks if the "uv" package is installed. If it is not installed, the function 
will attempt to install it.

.NOTES
Make sure that pip is installed and available in the system PATH.
#>
function Test-And-Install-Package {
    param (
        [string]$packageName
    )

    $packageInfo = pip show $packageName -q
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

# Check if pip is installed
if (Get-Command pip -ErrorAction SilentlyContinue) {
    # Check and install required packages
    Test-And-Install-Package -packageName "uv"
    Test-And-Install-Package -packageName "ruff"
}
else {
    Write-Host "pip is not installed. Please install python first."
}

# Activate the environment
uv sync  --python 3.11
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

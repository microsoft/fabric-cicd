function global:env.activate() {
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

function global:env.deactivate() {
    if (Get-Command -Name deactivate -CommandType Function -ErrorAction SilentlyContinue) {
        deactivate
        Write-Host "venv deactivated"
    }
    else {
        Write-Host "venv not activated"
    }
}

# Check if pip is installed
if (Get-Command pip -ErrorAction SilentlyContinue) {
    # Install ruff and uv with error handling
    try {
        pip install uv
        pip install ruff
    }
    catch {
        Write-Host "Failed to install packages. Please check your pip installation."
    }
}
else {
    Write-Host "pip is not installed. Please install python first."
}

# Activate the environment
env.activate

Write-Host "To activate the environment, run " -NoNewline
Write-Host "env.activate" -ForegroundColor Green
Write-Host "To deactivate the environment, run " -NoNewline
Write-Host "env.deactivate" -ForegroundColor Green

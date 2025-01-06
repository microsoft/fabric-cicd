$ErrorActionPreference = "Stop"

# Define the paths relative to the script location
$rootDir = (Resolve-Path "$PSScriptRoot\..\..\").Path
$buildDir = "$rootDir\.readthedocs\_build"
$docsDir = "$rootDir\.readthedocs\docs"
$confDir = "$rootDir\.readthedocs\config"
$requirements = "$confDir\requirements.txt"
$packageRequirements = "$rootDir\requirements.txt"

# Remove the contents of the build directory if it exists
if (Test-Path -Path $buildDir) {
    Remove-Item -Recurse -Force $buildDir/*
}

# Install the requirements
pip install -r $requirements

# Install the package requirements
pip install -r $packageRequirements

# Run the Sphinx build
sphinx-build -b html $docsDir $buildDir -c $confDir

# Serve the built documentation
Start-Process -NoNewWindow -FilePath "python" -ArgumentList "-m http.server --directory `"$buildDir`""

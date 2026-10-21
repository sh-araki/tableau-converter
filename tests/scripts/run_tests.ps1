$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent (Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path))
$pythonExe = Join-Path $projectRoot 'python\python.exe'

$env:PYTHONDONTWRITEBYTECODE = '1'
& $pythonExe -B -m unittest discover -s (Join-Path $projectRoot 'tests') -v
exit $LASTEXITCODE

$ErrorActionPreference = 'Stop'

$paths = @(
    'C:\Program Files\TigerVNC\vncviewer.exe',
    'C:\Program Files (x86)\TigerVNC\vncviewer.exe',
    "$env:LOCALAPPDATA\Programs\TigerVNC\vncviewer.exe"
)

$exe = $null
foreach ($p in $paths) {
    if (Test-Path -LiteralPath $p) {
        $exe = $p
        break
    }
}

if (-not $exe) {
    $cmd = Get-Command vncviewer.exe -ErrorAction SilentlyContinue
    if ($cmd) { $exe = $cmd.Source }
}

if (-not $exe) {
    Write-Output 'VIEWER_NOT_FOUND'
    exit 1
}

Write-Output ('VIEWER=' + $exe)
Write-Output 'Launching TigerVNC viewer to localhost:5901 ...'
Start-Process -FilePath $exe -ArgumentList 'localhost:5901'
Write-Output 'OK'

$ErrorActionPreference = 'SilentlyContinue'

Write-Output '=== port 5901 ==='
$ports = Get-NetTCPConnection -LocalPort 5901 -State Listen
if ($ports) {
    Write-Output 'PORT_5901_ALREADY_BOUND'
    $ports | Format-Table -AutoSize | Out-String | Write-Output
} else {
    Write-Output 'PORT_5901_FREE'
}

Write-Output ''
Write-Output '=== VNC viewer search ==='
$found = @()

$candidates = @('vncviewer.exe','vncviewer64.exe','tvnviewer.exe','tvnviewer64.exe')
foreach ($c in $candidates) {
    $cmd = Get-Command $c
    if ($cmd) {
        $found += $cmd.Source
        Write-Output ('FOUND_IN_PATH: ' + $cmd.Source)
    }
}

$dirs = @(
    'C:\Program Files\TigerVNC',
    'C:\Program Files (x86)\TigerVNC',
    'C:\Program Files\RealVNC\VNC Viewer',
    'C:\Program Files (x86)\RealVNC\VNC Viewer',
    'C:\Program Files\TightVNC',
    'C:\Program Files (x86)\TightVNC',
    'C:\Program Files\uvnc bvba',
    "$env:LOCALAPPDATA\Programs\TigerVNC",
    "$env:LOCALAPPDATA\Programs\RealVNC"
)
foreach ($d in $dirs) {
    if (Test-Path $d) {
        Write-Output ('DIR_FOUND: ' + $d)
        Get-ChildItem -Path $d -Filter '*.exe' -Recurse -ErrorAction SilentlyContinue |
            Where-Object { $_.Name -match 'vncviewer|tvnviewer' } |
            Select-Object -First 5 |
            ForEach-Object {
                $found += $_.FullName
                Write-Output ('  EXE: ' + $_.FullName)
            }
    }
}

Write-Output ''
Write-Output '=== summary ==='
if ($found.Count -gt 0) {
    Write-Output ('VIEWER_FOUND=' + $found[0])
} else {
    Write-Output 'VIEWER_NOT_FOUND'
}

param([int]$ParentId, [string]$Target, [string]$Staged)
$ErrorActionPreference = 'Stop'
$backup = "$Staged.backup"
$log = "$Target.update.log"
$task = 'agent-transcript watch'
$restart = $false
try {
    # Stop the scheduled watcher before replacing a binary it may be using.
    & schtasks.exe /Query /TN $task *> $null
    if ($LASTEXITCODE -eq 0) {
        & schtasks.exe /End /TN $task *> $null
        $restart = ($LASTEXITCODE -eq 0)
    }
    try { Wait-Process -Id $ParentId -Timeout 60 -ErrorAction SilentlyContinue } catch {}
    $done = $false
    for ($i = 0; $i -lt 30 -and -not $done; $i++) {
        try {
            [System.IO.File]::Move($Target, $backup)
            try {
                [System.IO.File]::Move($Staged, $Target)
                $done = $true
            } catch {
                # If rollback fails, do not retry and overwrite the backup.
                try { [System.IO.File]::Move($backup, $Target) }
                catch { throw "rollback failed; original executable is at $backup : $_" }
                throw
            }
        } catch {
            if ($i -eq 29) { throw }
            Start-Sleep -Seconds 1
        }
    }
    Remove-Item -LiteralPath $backup -Force
    Remove-Item -LiteralPath $log -ErrorAction SilentlyContinue
} catch {
    # A failed swap keeps the original executable; leave the staged file for diagnosis.
    $_ | Out-String | Set-Content -LiteralPath $log
} finally {
    if ($restart) { & schtasks.exe /Run /TN $task *> $null }
    Remove-Item -LiteralPath $PSCommandPath -Force -ErrorAction SilentlyContinue
}

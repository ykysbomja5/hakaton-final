param(
  [string]$EnvFile = ".env",
  [string]$PgDsn = "",
  [string]$PgReadOnlyDsn = "",
  [string]$LlmProvider = "",
  [switch]$StopExisting
)

$root = Split-Path -Parent $PSScriptRoot
$resolvedEnvFile = Join-Path $root $EnvFile
$binDir = Join-Path $root ".bin"
$hostArch = if ($env:PROCESSOR_ARCHITECTURE -eq "ARM64") { "arm64" } else { "amd64" }

function Read-DotEnv([string]$Path) {
  $result = @{}
  if (-not (Test-Path -LiteralPath $Path)) {
    return $result
  }

  foreach ($line in Get-Content -LiteralPath $Path) {
    $trimmed = $line.Trim()
    if ([string]::IsNullOrWhiteSpace($trimmed) -or $trimmed.StartsWith("#")) {
      continue
    }

    if ($trimmed.StartsWith("export ")) {
      $trimmed = $trimmed.Substring(7).Trim()
    }

    $parts = $trimmed -split "=", 2
    if ($parts.Count -ne 2) {
      continue
    }

    $key = $parts[0].Trim()
    $value = $parts[1].Trim().Trim("'`"")
    if (-not [string]::IsNullOrWhiteSpace($key)) {
      $result[$key] = $value
    }
  }

  return $result
}

function Escape-SingleQuotes([string]$Value) {
  return $Value.Replace("'", "''")
}

function Get-PortOwner([int]$Port) {
  $connection = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
  if ($null -eq $connection) {
    return $null
  }

  $processName = ""
  try {
    $process = Get-Process -Id $connection.OwningProcess -ErrorAction Stop
    $processName = $process.ProcessName
  } catch {
    $processName = "PID $($connection.OwningProcess)"
  }

  return @{
    Port = $Port
    Process = $processName
    Pid = $connection.OwningProcess
  }
}

function Get-FreePort([int]$PreferredPort, [int[]]$ReservedPorts) {
  $candidate = $PreferredPort
  while ($candidate -lt 65535) {
    if ($ReservedPorts -contains $candidate) {
      $candidate++
      continue
    }

    $owner = Get-PortOwner $candidate
    if ($null -eq $owner) {
      return $candidate
    }

    $candidate++
  }

  throw "Failed to find a free TCP port starting from $PreferredPort"
}

function Get-ServicePort([array]$Services, [string]$Name) {
  $match = $Services | Where-Object { $_.Name -eq $Name } | Select-Object -First 1
  if ($null -eq $match -or $null -eq $match.Port) {
    throw "Failed to resolve port for service '$Name'"
  }

  return [int]$match.Port
}

$envMap = Read-DotEnv $resolvedEnvFile

if (-not [string]::IsNullOrWhiteSpace($PgDsn)) {
  $envMap["PG_DSN"] = $PgDsn
}
if (-not [string]::IsNullOrWhiteSpace($PgReadOnlyDsn)) {
  $envMap["PG_READONLY_DSN"] = $PgReadOnlyDsn
}
if (-not [string]::IsNullOrWhiteSpace($LlmProvider)) {
  $envMap["LLM_PROVIDER"] = $LlmProvider
}

if (-not $envMap.ContainsKey("PG_DSN")) {
  $envMap["PG_DSN"] = "postgres://postgres:postgres@localhost:5432/drivee_analytics?sslmode=disable"
}
if (-not $envMap.ContainsKey("PG_READONLY_DSN")) {
  $envMap["PG_READONLY_DSN"] = "postgres://analytics_readonly:analytics_demo@localhost:5432/drivee_analytics?sslmode=disable"
}
if (-not $envMap.ContainsKey("LLM_PROVIDER")) {
  $envMap["LLM_PROVIDER"] = "gigachat"
}
if (-not $envMap.ContainsKey("LLM_ALLOW_RULE_BASED_FALLBACK")) {
  $envMap["LLM_ALLOW_RULE_BASED_FALLBACK"] = "false"
}

$services = @(
  @{ Name = "meta";    DefaultPort = 8084; Package = "./cmd/meta" },
  @{ Name = "llm";     DefaultPort = 8082; Package = "./cmd/llm" },
  @{ Name = "query";   DefaultPort = 8081; Package = "./cmd/query" },
  @{ Name = "reports"; DefaultPort = 8083; Package = "./cmd/reports" },
  @{ Name = "gateway"; DefaultPort = 8080; Package = "./cmd/gateway" }
)

if ($StopExisting) {
  $servicePorts = $services | ForEach-Object { [int]$_.DefaultPort }
  $connections = foreach ($port in $servicePorts) {
    Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
  }

  $pidsToStop = $connections |
    Where-Object { $_ -ne $null } |
    Select-Object -ExpandProperty OwningProcess -Unique

  foreach ($processId in $pidsToStop) {
    try {
      $proc = Get-Process -Id $processId -ErrorAction Stop
      Write-Host "Stopping $($proc.ProcessName) (PID $processId)..."
      Stop-Process -Id $processId -Force -ErrorAction Stop
    } catch {
      Write-Warning "Failed to stop PID ${processId}: $($_.Exception.Message)"
    }
  }

  Start-Sleep -Milliseconds 500
}

$busyPorts = @()
$reservedPorts = @()
foreach ($service in $services) {
  $preferredPort = [int]$service.DefaultPort
  $owner = Get-PortOwner $preferredPort
  if ($null -eq $owner) {
    $service["Port"] = $preferredPort
    $reservedPorts += $preferredPort
    continue
  }

  $resolvedPort = Get-FreePort -PreferredPort ($preferredPort + 1) -ReservedPorts $reservedPorts
  $service["Port"] = $resolvedPort
  $reservedPorts += $resolvedPort
  $busyPorts += @{
    Name = $service.Name
    PreferredPort = $preferredPort
    ChosenPort = $resolvedPort
    Process = $owner.Process
    Pid = $owner.Pid
  }
}

if ($busyPorts.Count -gt 0) {
  Write-Warning "Some default service ports are busy. Alternate free ports will be used for this run:"
  foreach ($entry in $busyPorts) {
    Write-Warning ("  {0}: {1} is busy by {2} (PID {3}); using {4} instead." -f $entry.Name, $entry.PreferredPort, $entry.Process, $entry.Pid, $entry.ChosenPort)
  }
}

New-Item -ItemType Directory -Force -Path $binDir | Out-Null

foreach ($service in $services) {
  $outputPath = Join-Path $binDir ($service.Name + ".exe")
  Write-Host "Building $($service.Name)..."
  $buildEnv = @{
    GOOS   = "windows"
    GOARCH = $hostArch
    CGO_ENABLED = "0"
  }
  $previousValues = @{}
  foreach ($entry in $buildEnv.GetEnumerator()) {
    $previousValues[$entry.Key] = [Environment]::GetEnvironmentVariable($entry.Key, "Process")
    [Environment]::SetEnvironmentVariable($entry.Key, $entry.Value, "Process")
  }

  try {
    & go build -o $outputPath $service.Package
  } finally {
    foreach ($entry in $buildEnv.GetEnumerator()) {
      [Environment]::SetEnvironmentVariable($entry.Key, $previousValues[$entry.Key], "Process")
    }
  }

  if ($LASTEXITCODE -ne 0) {
    throw "Failed to build $($service.Name)"
  }
  $service["Executable"] = $outputPath
}

$serviceUrls = @{
  query = 'http://localhost:{0}' -f (Get-ServicePort -Services $services -Name "query")
  llm = 'http://localhost:{0}' -f (Get-ServicePort -Services $services -Name "llm")
  reports = 'http://localhost:{0}' -f (Get-ServicePort -Services $services -Name "reports")
  meta = 'http://localhost:{0}' -f (Get-ServicePort -Services $services -Name "meta")
}

foreach ($service in $services) {
  $envAssignments = foreach ($entry in $envMap.GetEnumerator()) {
    '$env:{0} = ''{1}''' -f $entry.Key, (Escape-SingleQuotes $entry.Value)
  }

  $scriptLines = @(
    "Set-Location '$root'"
    $envAssignments
    '$env:QUERY_SERVICE_URL = ''{0}''' -f $serviceUrls.query
    '$env:LLM_SERVICE_URL = ''{0}''' -f $serviceUrls.llm
    '$env:REPORTS_SERVICE_URL = ''{0}''' -f $serviceUrls.reports
    '$env:META_SERVICE_URL = ''{0}''' -f $serviceUrls.meta
    '$env:PORT = ''{0}''' -f $service.Port
    '& ''{0}''' -f (Escape-SingleQuotes $service.Executable)
  )

  $script = [string]::Join([Environment]::NewLine, $scriptLines)
  Start-Process powershell -ArgumentList "-NoExit", "-Command", $script | Out-Null
}

$gatewayPort = Get-ServicePort -Services $services -Name "gateway"

Write-Host "Drivee Analytics services are starting in separate PowerShell windows."
Write-Host "Environment file: $resolvedEnvFile"
Write-Host "Open http://localhost:$gatewayPort after the services finish booting."

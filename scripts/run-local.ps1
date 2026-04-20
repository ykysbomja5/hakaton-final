param(
  [string]$PgDsn = "postgres://postgres:postgres@localhost:5432/drivee_analytics?sslmode=disable",
  [string]$LlmProvider = "gigachat"
)

$root = Split-Path -Parent $PSScriptRoot

$services = @(
  @{ Name = "meta";    Port = "8084"; Command = "go run ./cmd/meta" },
  @{ Name = "llm";     Port = "8082"; Command = "go run ./cmd/llm" },
  @{ Name = "query";   Port = "8081"; Command = "go run ./cmd/query" },
  @{ Name = "reports"; Port = "8083"; Command = "go run ./cmd/reports" },
  @{ Name = "gateway"; Port = "8080"; Command = "go run ./cmd/gateway" }
)

foreach ($service in $services) {
  $script = @"
Set-Location '$root'
\$env:PG_DSN = '$PgDsn'
\$env:LLM_PROVIDER = '$LlmProvider'
\$env:QUERY_SERVICE_URL = 'http://localhost:8081'
\$env:LLM_SERVICE_URL = 'http://localhost:8082'
\$env:REPORTS_SERVICE_URL = 'http://localhost:8083'
\$env:META_SERVICE_URL = 'http://localhost:8084'
\$env:PORT = '$($service.Port)'
$($service.Command)
"@

  Start-Process powershell -ArgumentList "-NoExit", "-Command", $script | Out-Null
}

Write-Host "Drivee Analytics services are starting in separate PowerShell windows."
Write-Host "Open http://localhost:8080 after the services finish booting."

#!/bin/bash
set -euo pipefail

# Ver: https://stackoverflow.com/a/5947802/32697703
readonly BOLD='\033[1m'
readonly RED='\033[0;31m'
readonly GREEN='\033[0;32m'
readonly YELLOW='\033[0;33m'
readonly BLUE='\033[0;34m'
readonly NO_BOLD='\033[22m'
readonly NC='\033[0m'


log() {
  local level="$1"
  local color="$2"
  shift 2


  printf "%b%b[%s]%b %s%b\n" "$color" "$BOLD" "$level" "$NO_BOLD" "$*" "$NC"
}


info()  { log "INFO"  "$BLUE" "$@"; }
ok()    { log "OK"    "$GREEN" "$@"; }
warn()  { log "WARN"  "$YELLOW" "$@"; }
error() { log "ERROR" "$RED" "$@" >&2; }

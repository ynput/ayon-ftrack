#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

ADDON_VERSION="$(python -c "import os;import sys;content={};f=open(r'$($SCRIPT_DIR)/../package.py');exec(f.read(),content);f.close();print(content['version'])")"

print_help() {
  cat <<'EOF'

*************************
AYON ftrack services tool
   Run ftrack services
*************************

Run service processes from terminal. It is recommended to use docker images for production.

Usage: ./manage.sh [target] [options]

Optional arguments for service targets:
  --variant <variant>    Define settings variant (default in app config is usually production)

Runtime targets:
  install      Install requirements
  leecher      Start leecher of ftrack events
  processor    Main processing logic
  transmitter  AYON to ftrack sync
  ftrack2ayon  Services related to ftrack to AYON sync
  ayon2ftrack  Alias to transmitter
  services     Start all services (experimental)

EOF
}

load_env() {
  local env_path="${SCRIPT_DIR}/.env"
  [[ -f "${env_path}" ]] || return 0

  while IFS= read -r line || [[ -n "${line}" ]]; do
    line="${line%%$'\r'}"
    [[ -z "${line}" || "${line}" =~ ^[[:space:]]*# ]] && continue
    [[ "${line}" == *"="* ]] || continue

    local key="${line%%=*}"
    local value="${line#*=}"

    # Trim surrounding whitespace from key.
    key="$(echo "${key}" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
    [[ -z "${key}" ]] && continue

    export "${key}=${value}"
  done < "${env_path}"
}

install_requirements() {
  uv sync
}

run_service() {
  local service_name="$1"
  shift || true
  uv run "${SCRIPT_DIR}/main.py" --service "${service_name}" "$@"
}

main() {
  local function_name="${1:-}"
  if [[ -z "${function_name}" ]]; then
    print_help
    return 0
  fi

  shift || true

  export AYON_ADDON_NAME="ftrack"
  export AYON_ADDON_VERSION="${ADDON_VERSION}"
  load_env

  case "${function_name}" in
    install)
      install_requirements
      ;;
    leecher)
      run_service "leecher" "$@"
      ;;
    processor)
      run_service "processor" "$@"
      ;;
    transmitter|ayon2ftrack)
      run_service "transmitter" "$@"
      ;;
    ftrack2ayon)
      run_service "ftrack2ayon" "$@"
      ;;
    services)
      run_service "all" "$@"
      ;;
    help|-h|--help)
      print_help
      ;;
    *)
      echo "Unknown function '${function_name}'"
      print_help
      return 1
      ;;
  esac
}

main "$@"


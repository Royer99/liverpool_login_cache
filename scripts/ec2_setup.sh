#!/usr/bin/env bash
# Prepare an EC2 load-generator host (Amazon Linux 2023 or Ubuntu 22.04+).
#
# Prerequisites this script cannot do for you:
#   * Launch the instance in the SAME AWS REGION as the Atlas cluster.
#     Cross-region RTT alone can exceed the 10 ms read budget.
#   * Add the instance's public IP (or its VPC via peering / private endpoint)
#     to the Atlas IP access list.
#   * scp/git the repo onto the host, then create .env from .env.example.
set -euo pipefail

cd "$(dirname "$0")/.."

if command -v dnf >/dev/null; then
  sudo dnf install -y python3.11 python3.11-pip git
  PY=python3.11
elif command -v apt-get >/dev/null; then
  sudo apt-get update -y
  sudo apt-get install -y python3.11 python3.11-venv git || sudo apt-get install -y python3 python3-venv git
  PY=$(command -v python3.11 || command -v python3)
else
  echo "unsupported distro: need dnf or apt-get" >&2; exit 1
fi

$PY -m venv .venv
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -r requirements.txt

# Hundreds of Locust users + a large connection pool exceed the default 1024
# open-files limit. Raise it persistently and for this shell.
sudo tee /etc/security/limits.d/99-locust.conf >/dev/null <<'EOF'
* soft nofile 65535
* hard nofile 65535
EOF
ulimit -n 65535 || true

echo
echo "Setup done. Next:"
echo "  1. cp .env.example .env   # then fill in MONGODB_URI"
echo "  2. .venv/bin/python src/verify.py --preflight   # RTT + cache sizing"
echo "  3. .venv/bin/python src/load.py                 # generate + load (resumable)"
echo "  4. .venv/bin/python src/verify.py               # prove the access paths"
echo "  5. scripts/run_headless.sh                      # warm-up + measured run"
echo "Re-login (or 'ulimit -n 65535') so the nofile limit applies to your session."

#!/usr/bin/env bash
# Install explicitly configured units. Preview never syncs, writes, or starts.
set -euo pipefail
exec python3 - "$@" <<'PY'
import argparse
import json
import getpass
from pathlib import Path
import subprocess

root = Path.cwd()
# This wrapper is checkout-only; invoke it from the repository root.
p = argparse.ArgumentParser(description="Render gateway units; SSH forwarding is optional")
p.add_argument('--env-file', type=Path, required=True)
p.add_argument('--user', action='store_true')
p.add_argument('--enable', action='store_true')
p.add_argument('--dry-run', action='store_true')
p.add_argument('--no-sync', action='store_true')
p.add_argument('--tunnel-host')
p.add_argument('--ssh-key', type=Path)
p.add_argument('--ssh-port', type=int, default=22)
p.add_argument('--local-port', type=int, default=8766)
p.add_argument('--remote-port', type=int, default=8766)
a = p.parse_args()
if not (root / 'systemd/mattermost-acp-gateway.service.example').is_file():
    p.error('run from the gateway checkout root')
if not a.env_file.expanduser().is_file():
    p.error('selected environment file is missing')
if a.tunnel_host and (not a.ssh_key or not a.ssh_key.expanduser().is_file()):
    p.error('--tunnel-host requires an existing --ssh-key')
if any(not 1 <= port <= 65535 for port in (a.ssh_port, a.local_port, a.remote_port)):
    p.error('ports must be between 1 and 65535')

def quote(value):
    value = str(value)
    if any(char in value for char in '\n\r\0'):
        p.error('unit values must be single-line text')
    return json.dumps(value.replace('%', '%%'))

values = {
    '__USER__': quote(getpass.getuser()),
    '__HOME__': quote(Path.home()),
    '__INSTALL_DIR__': quote(root),
    '__ENV_FILE__': quote(a.env_file.expanduser().absolute()),
    '__EXECUTABLE__': quote(root / '.venv/bin/mattermost-acp-gateway'),
    '__SSH_KEY__': quote(a.ssh_key.expanduser().absolute()) if a.ssh_key else '',
    '__SSH_HOST__': quote(a.tunnel_host or ''),
    '__SSH_PORT__': str(a.ssh_port),
    '__LOCAL_PORT__': str(a.local_port),
    '__REMOTE_PORT__': str(a.remote_port),
}
names = ['mattermost-acp-gateway.service']
if a.tunnel_host:
    names.append('mattermost-acp-tunnel.service')
rendered = {}
for name in names:
    text = (root / 'systemd' / (name + '.example')).read_text()
    for key, value in values.items():
        text = text.replace(key, value)
    if a.user:
        text = '\n'.join(line for line in text.splitlines() if not line.startswith('User=')) + '\n'
        text = text.replace('WantedBy=multi-user.target', 'WantedBy=default.target')
    rendered[name] = text
if a.dry_run:
    for name, text in rendered.items():
        print('# ' + name + '\n' + text)
    raise SystemExit(0)
if not a.no_sync:
    subprocess.run(['uv', 'sync', '--locked'], cwd=root, check=True)
for name, text in rendered.items():
    out = root / 'systemd' / name
    out.write_text(text)
    print('Rendered ' + str(out))
if a.enable:
    target = Path.home() / '.config/systemd/user' if a.user else Path('/etc/systemd/system')
    command = ['systemctl', '--user'] if a.user else ['sudo', 'systemctl']
    if a.user:
        target.mkdir(parents=True, exist_ok=True)
    for name, text in rendered.items():
        if a.user:
            (target / name).write_text(text)
        else:
            subprocess.run(['sudo', 'cp', str(root / 'systemd' / name), str(target / name)], check=True)
    subprocess.run(command + ['daemon-reload'], check=True)
    subprocess.run(command + ['enable', '--now', *names], check=True)
PY

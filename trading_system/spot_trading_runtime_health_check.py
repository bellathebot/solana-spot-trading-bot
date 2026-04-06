#!/usr/bin/env python3
import argparse
import json
import subprocess
from pathlib import Path

from trading_system.runtime_config import BRIDGE_DIR, JUP_BIN, build_path_env

DEFAULT_STATE = BRIDGE_DIR / 'spot_runtime_health_state.json'


def run(cmd: str) -> tuple[int, str, str]:
    res = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=120)
    return res.returncode, (res.stdout or '').strip(), (res.stderr or '').strip()


def load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text())
    except Exception:
        return default


def save_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2))


def main():
    parser = argparse.ArgumentParser(description='Spot runtime health check for Jupiter key/portfolio access')
    parser.add_argument('--state-file', default=str(DEFAULT_STATE))
    args = parser.parse_args()

    env_prefix = f'export PATH="{build_path_env()}" && '
    key_cmd = env_prefix + f'{JUP_BIN} -f json keys list'
    portfolio_cmd = env_prefix + f'{JUP_BIN} -f json spot portfolio --key trading'

    key_rc, key_out, key_err = run(key_cmd)
    port_rc, port_out, port_err = run(portfolio_cmd)

    ok = key_rc == 0 and '"name": "trading"' in key_out and port_rc == 0 and '"totalValue"' in port_out
    status = 'healthy' if ok else 'degraded'
    payload = {
        'status': status,
        'key_check_exit_code': key_rc,
        'portfolio_check_exit_code': port_rc,
        'key_check_error': key_err,
        'portfolio_check_error': port_err,
    }
    save_json(Path(args.state_file), payload)
    print(json.dumps(payload, indent=2))


if __name__ == '__main__':
    main()

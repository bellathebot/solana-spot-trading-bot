#!/usr/bin/env python3
import argparse
from pathlib import Path

from trading_system.runtime_config import DATA_DIR, DB_PATH
from trading_system.trading_db import sync_from_files


def main() -> None:
    parser = argparse.ArgumentParser(description='Sync JSON/log trading data into SQLite.')
    parser.add_argument('--db', default=str(DB_PATH), help='Path to SQLite database')
    parser.add_argument('--data-dir', default=str(DATA_DIR), help='Directory containing trading JSON/log files')
    args = parser.parse_args()

    counts = sync_from_files(Path(args.db), Path(args.data_dir))
    print('Trading DB sync complete:')
    for key, value in counts.items():
        print(f'- {key}: {value}')


if __name__ == '__main__':
    main()

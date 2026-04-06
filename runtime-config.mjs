import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
export const REPO_ROOT = path.dirname(__filename);
export const BOT_HOME = process.env.HERMES_SPOT_BOT_HOME || process.env.HOME || REPO_ROOT;
export const DATA_DIR = process.env.AUTO_TRADER_DATA_DIR || path.join(BOT_HOME, '.trading-data');
export const DB_PATH = process.env.AUTO_TRADER_DB_PATH || path.join(DATA_DIR, 'trading.db');
export const TRADING_MD = process.env.AUTO_TRADER_TRADING_MD || path.join(REPO_ROOT, 'trading.md');
export const DB_CLI = process.env.AUTO_TRADER_DB_CLI || path.join(REPO_ROOT, 'trading_system', 'trading_db_cli.py');
export const JUP_BIN = process.env.JUP_BIN || 'jup';
export const HELIUS_BIN = process.env.HELIUS_BIN || 'helius';

const inferredBinDirs = [
  process.env.JUP_BIN_DIR,
  process.env.HELIUS_BIN_DIR,
  process.env.HOME ? path.join(process.env.HOME, '.hermes', 'node', 'bin') : '',
  process.env.HOME ? path.join(process.env.HOME, '.cargo', 'bin') : '',
].filter(Boolean);

export const PATH_ENV = inferredBinDirs.length ? `${inferredBinDirs.join(':')}:${process.env.PATH || ''}` : (process.env.PATH || '');

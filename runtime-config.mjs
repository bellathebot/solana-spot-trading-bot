import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
export const REPO_ROOT = path.dirname(__filename);
const CONFIG_FILE = process.env.SPOT_BOT_CONFIG_FILE || path.join(REPO_ROOT, 'config', 'local.json');

function readConfig() {
  try {
    if (!fs.existsSync(CONFIG_FILE)) return {};
    const raw = JSON.parse(fs.readFileSync(CONFIG_FILE, 'utf-8'));
    return raw && typeof raw === 'object' ? raw : {};
  } catch {
    return {};
  }
}

const CONFIG = readConfig();
const cfg = (key, fallback) => process.env[key] || CONFIG[key] || fallback;

export const BOT_HOME = cfg('HERMES_SPOT_BOT_HOME', process.env.HOME || REPO_ROOT);
export const DATA_DIR = cfg('AUTO_TRADER_DATA_DIR', path.join(BOT_HOME, '.trading-data'));
export const DB_PATH = cfg('AUTO_TRADER_DB_PATH', path.join(DATA_DIR, 'trading.db'));
export const TRADING_MD = cfg('AUTO_TRADER_TRADING_MD', path.join(REPO_ROOT, 'trading.md'));
export const DB_CLI = cfg('AUTO_TRADER_DB_CLI', path.join(REPO_ROOT, 'trading_system', 'trading_db_cli.py'));
export const JUP_BIN = cfg('JUP_BIN', 'jup');
export const HELIUS_BIN = cfg('HELIUS_BIN', 'helius');

const inferredBinDirs = [
  process.env.JUP_BIN_DIR || CONFIG.JUP_BIN_DIR,
  process.env.HELIUS_BIN_DIR || CONFIG.HELIUS_BIN_DIR,
  process.env.HOME ? path.join(process.env.HOME, '.hermes', 'node', 'bin') : '',
  process.env.HOME ? path.join(process.env.HOME, '.cargo', 'bin') : '',
].filter(Boolean);

export const PATH_ENV = inferredBinDirs.length ? `${inferredBinDirs.join(':')}:${process.env.PATH || ''}` : (process.env.PATH || '');
export const SPOT_BOT_CONFIG_FILE = CONFIG_FILE;

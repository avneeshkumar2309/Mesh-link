import fs from 'fs';
import path from 'path';
import os from 'os';
import { v4 as uuidv4 } from 'uuid';

// Parse command line arguments
function parseArgs() {
  const args = process.argv.slice(2);
  const config = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i].startsWith('--')) {
      const key = args[i].slice(2);
      const val = args[i + 1] && !args[i + 1].startsWith('--') ? args[++i] : true;
      config[key] = val;
    }
  }
  return config;
}

const args = parseArgs();

export const PORT = parseInt(process.env.PORT || args.port || 4000, 10);
export const CLIENT_PORT = parseInt(args['client-port'] || 5173, 10);
export const DISCOVERY_PORT = parseInt(process.env.DISCOVERY_PORT || args.discoveryPort || 43891, 10);
export const DATA_DIR = path.resolve(process.cwd(), args.dataDir || 'data');
export const DOWNLOADS_DIR = path.join(DATA_DIR, 'downloads');
export const TEMP_DIR = path.join(DATA_DIR, 'temp');

// Ensure directories exist
[DATA_DIR, DOWNLOADS_DIR, TEMP_DIR].forEach((dir) => {
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
});

const IDENTITY_FILE = path.join(DATA_DIR, 'identity.json');

const AVATAR_COLORS = [
  '#3b82f6', // blue
  '#8b5cf6', // purple
  '#ec4899', // pink
  '#10b981', // emerald
  '#f59e0b', // amber
  '#06b6d4', // cyan
  '#6366f1', // indigo
];

// Load or generate device identity
function loadIdentity() {
  if (fs.existsSync(IDENTITY_FILE)) {
    try {
      const raw = fs.readFileSync(IDENTITY_FILE, 'utf8');
      const saved = JSON.parse(raw);
      if (args.name) saved.deviceName = args.name;
      return saved;
    } catch {
      // fallback
    }
  }

  const identity = {
    deviceId: uuidv4(),
    deviceName: args.name || `${os.userInfo().username || 'User'}'s ${os.platform() === 'win32' ? 'PC' : 'Device'}`,
    deviceType: os.platform() === 'win32' ? 'desktop' : 'laptop',
    avatarColor: AVATAR_COLORS[Math.floor(Math.random() * AVATAR_COLORS.length)],
    created: Date.now(),
  };

  fs.writeFileSync(IDENTITY_FILE, JSON.stringify(identity, null, 2), 'utf8');
  return identity;
}

export const deviceIdentity = loadIdentity();

export function updateIdentity(updates) {
  Object.assign(deviceIdentity, updates);
  fs.writeFileSync(IDENTITY_FILE, JSON.stringify(deviceIdentity, null, 2), 'utf8');
  return deviceIdentity;
}

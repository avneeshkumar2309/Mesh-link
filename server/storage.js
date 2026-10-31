import fs from 'fs';
import path from 'path';
import { DATA_DIR } from './config.js';

const STORAGE_FILE = path.join(DATA_DIR, 'storage.json');
const TMP_STORAGE_FILE = path.join(DATA_DIR, 'storage.json.tmp');

class Storage {
  constructor() {
    this.data = {
      peers: {}, // { [deviceId]: peerData }
      messages: {}, // { [peerId]: [message, ...] }
      transfers: {}, // { [transferId]: transferData }
      settings: {
        autoAcceptTransfers: false,
        soundEnabled: true,
        notificationEnabled: true,
      },
    };
    this.load();
  }

  load() {
    try {
      if (fs.existsSync(STORAGE_FILE)) {
        const raw = fs.readFileSync(STORAGE_FILE, 'utf8');
        const parsed = JSON.parse(raw);
        this.data = {
          peers: parsed.peers || {},
          messages: parsed.messages || {},
          transfers: parsed.transfers || {},
          settings: { ...this.data.settings, ...(parsed.settings || {}) },
        };
      }
    } catch (err) {
      console.error('Error loading storage, resetting to defaults:', err);
    }
  }

  persist() {
    try {
      fs.writeFileSync(TMP_STORAGE_FILE, JSON.stringify(this.data, null, 2), 'utf8');
      fs.renameSync(TMP_STORAGE_FILE, STORAGE_FILE);
    } catch (err) {
      console.error('Error writing storage:', err);
    }
  }

  // Peer methods
  getPeers() {
    return Object.values(this.data.peers);
  }

  getPeer(deviceId) {
    return this.data.peers[deviceId] || null;
  }

  savePeer(peer) {
    if (!peer || !peer.deviceId) return;
    this.data.peers[peer.deviceId] = {
      ...(this.data.peers[peer.deviceId] || {}),
      ...peer,
      updatedAt: Date.now(),
    };
    this.persist();
    return this.data.peers[peer.deviceId];
  }

  removePeer(deviceId) {
    delete this.data.peers[deviceId];
    this.persist();
  }

  // Message methods
  getMessages(peerId) {
    return this.data.messages[peerId] || [];
  }

  addMessage(peerId, message) {
    if (!this.data.messages[peerId]) {
      this.data.messages[peerId] = [];
    }
    this.data.messages[peerId].push(message);
    this.persist();
    return message;
  }

  updateMessageStatus(peerId, messageId, status) {
    const list = this.data.messages[peerId];
    if (list) {
      const msg = list.find((m) => m.id === messageId);
      if (msg) {
        msg.status = status;
        this.persist();
        return msg;
      }
    }
    return null;
  }

  markAllAsRead(peerId) {
    const list = this.data.messages[peerId];
    if (list) {
      let changed = false;
      for (const m of list) {
        if (m.senderId === peerId && m.status !== 'read') {
          m.status = 'read';
          changed = true;
        }
      }
      if (changed) this.persist();
    }
  }

  clearMessages(peerId) {
    this.data.messages[peerId] = [];
    this.persist();
  }

  // Transfer methods
  getTransfers() {
    return Object.values(this.data.transfers);
  }

  getTransfer(transferId) {
    return this.data.transfers[transferId] || null;
  }

  saveTransfer(transfer) {
    if (!transfer || !transfer.transferId) return;
    this.data.transfers[transfer.transferId] = {
      ...(this.data.transfers[transfer.transferId] || {}),
      ...transfer,
      updatedAt: Date.now(),
    };
    this.persist();
    return this.data.transfers[transfer.transferId];
  }

  // Settings methods
  getSettings() {
    return this.data.settings;
  }

  updateSettings(updates) {
    this.data.settings = { ...this.data.settings, ...updates };
    this.persist();
    return this.data.settings;
  }
}

export const storage = new Storage();

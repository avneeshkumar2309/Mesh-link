import express from 'express';
import http from 'http';
import { WebSocketServer } from 'ws';
import cors from 'cors';
import path from 'path';
import fs from 'fs';
import multer from 'multer';
import mime from 'mime-types';
import { PORT, CLIENT_PORT, DATA_DIR, DOWNLOADS_DIR, TEMP_DIR, deviceIdentity, updateIdentity } from './config.js';
import { getLocalNetworkDetails } from './networkUtils.js';
import { storage } from './storage.js';
import { DiscoveryService } from './discovery.js';
import { PeerManager } from './peerManager.js';
import { fileTransferManager } from './fileTransfer.js';

const app = express();
const server = http.createServer(app);

// Enable CORS and JSON parsing
app.use(cors());
app.use(express.json());

// Set up file upload with multer to temp folder
const uploadStorage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, TEMP_DIR),
  filename: (req, file, cb) => {
    const uniqueSuffix = `${Date.now()}-${Math.round(Math.random() * 1e9)}`;
    cb(null, `${uniqueSuffix}-${file.originalname}`);
  },
});
const upload = multer({ storage: uploadStorage });

// Initialize Discovery and Peer Manager
const discovery = new DiscoveryService();
const peerManager = new PeerManager(discovery);

// Forward discovery events to UI
discovery.on('peer_updated', (peer) => {
  peerManager.broadcastToUI({ type: 'PEER_DISCOVERED', peer });
});
discovery.on('peers_changed', (peers) => {
  peerManager.broadcastToUI({ type: 'DISCOVERY_UPDATE', peers });
});

// Start Discovery
discovery.start();

// --- REST Endpoints ---

// Get local node profile and network interfaces
app.get('/api/info', (req, res) => {
  const netDetails = getLocalNetworkDetails();
  res.json({
    ...deviceIdentity,
    port: PORT,
    clientPort: CLIENT_PORT,
    addresses: netDetails.allIps,
    primaryIp: netDetails.primaryIp,
    interfaces: netDetails.interfaces,
    downloadsDir: DOWNLOADS_DIR,
    settings: storage.getSettings(),
  });
});

// Update device profile or settings
app.post('/api/settings', (req, res) => {
  const { deviceName, avatarColor, ...otherSettings } = req.body;
  if (deviceName || avatarColor) {
    updateIdentity({
      ...(deviceName ? { deviceName } : {}),
      ...(avatarColor ? { avatarColor } : {}),
    });
    // Broadcast refreshed beacon
    discovery.broadcastBeacon();
  }
  if (Object.keys(otherSettings).length > 0) {
    storage.updateSettings(otherSettings);
  }
  peerManager.broadcastToUI({
    type: 'SETTINGS_UPDATED',
    identity: deviceIdentity,
    settings: storage.getSettings(),
  });
  res.json({ success: true, identity: deviceIdentity, settings: storage.getSettings() });
});

// List peers (both discovered and saved history)
app.get('/api/peers', (req, res) => {
  res.json({
    discovered: discovery.getDiscoveredPeers(),
    saved: storage.getPeers(),
  });
});

// Manual IP/port connect
app.post('/api/peers/manual-connect', async (req, res) => {
  const { ip, port } = req.body;
  if (!ip || !port) {
    return res.status(400).json({ success: false, error: 'IP and Port are required' });
  }

  const result = await discovery.manualConnect(ip, port);
  res.json(result);
});

// Get chat messages for peer
app.get('/api/messages/:peerId', (req, res) => {
  const messages = storage.getMessages(req.params.peerId);
  res.json({ messages });
});

// Upload and send file to peer
app.post('/api/files/upload', upload.single('file'), (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: 'No file provided' });
  }

  const { peerId } = req.body;
  if (!peerId) {
    return res.status(400).json({ error: 'Target peerId is required' });
  }

  const detectedMime = mime.lookup(req.file.originalname) || req.file.mimetype || 'application/octet-stream';

  const transfer = peerManager.offerFileTransfer(peerId, {
    fileName: req.file.originalname,
    fileSize: req.file.size,
    mimeType: detectedMime,
    localFilePath: req.file.path,
  });

  res.json({ success: true, transfer });
});

// Download a completed file
app.get('/api/files/:transferId/download', (req, res) => {
  const transfer = fileTransferManager.getTransfer(req.params.transferId);
  if (!transfer) {
    return res.status(404).send('Transfer not found');
  }

  const filePath = transfer.savedFilePath || transfer.localFilePath;
  if (!filePath || !fs.existsSync(filePath)) {
    return res.status(404).send('File not found on disk');
  }

  res.download(filePath, transfer.savedFileName || transfer.fileName);
});

// Inline preview for images and documents
app.get('/api/files/:transferId/preview', (req, res) => {
  const transfer = fileTransferManager.getTransfer(req.params.transferId);
  if (!transfer) {
    return res.status(404).send('Transfer not found');
  }

  const filePath = transfer.savedFilePath || transfer.localFilePath;
  if (!filePath || !fs.existsSync(filePath)) {
    return res.status(404).send('File not found on disk');
  }

  const contentType = transfer.mimeType || mime.lookup(filePath) || 'application/octet-stream';
  res.setHeader('Content-Type', contentType);
  fs.createReadStream(filePath).pipe(res);
});

// Serve frontend build if exists
const clientDist = path.resolve(process.cwd(), 'client', 'dist');
if (fs.existsSync(clientDist)) {
  app.use(express.static(clientDist));
  app.use((req, res) => {
    res.sendFile(path.join(clientDist, 'index.html'));
  });
}

// --- WebSocket Routing ---
const wssUI = new WebSocketServer({ noServer: true });
const wssPeer = new WebSocketServer({ noServer: true });

wssUI.on('connection', (ws) => {
  peerManager.registerUIClient(ws);
});

wssPeer.on('connection', (ws, req) => {
  peerManager.handleIncomingPeerSocket(ws, req);
});

server.on('upgrade', (request, socket, head) => {
  const pathname = new URL(request.url, `http://${request.headers.host}`).pathname;

  if (pathname === '/ws' || pathname === '/ws/ui') {
    wssUI.handleUpgrade(request, socket, head, (ws) => {
      wssUI.emit('connection', ws, request);
    });
  } else if (pathname === '/ws/peer') {
    wssPeer.handleUpgrade(request, socket, head, (ws) => {
      wssPeer.emit('connection', ws, request);
    });
  } else {
    socket.destroy();
  }
});

// Start listening
server.listen(PORT, '0.0.0.0', () => {
  const netDetails = getLocalNetworkDetails();
  console.log('====================================================');
  console.log(`🚀 MeshLink Node running!`);
  console.log(`   Device Name: "${deviceIdentity.deviceName}"`);
  console.log(`   Device ID:   ${deviceIdentity.deviceId}`);
  console.log(`   Local Web:   http://localhost:${PORT}`);
  console.log(`   LAN Web:     http://${netDetails.primaryIp}:${PORT}`);
  console.log(`   UDP Beacon:  Port ${discovery.socket?.address()?.port || 43891}`);
  console.log('====================================================');
});

// Handle graceful shutdown
process.on('SIGINT', () => {
  console.log('\nShutting down MeshLink node...');
  discovery.stop();
  process.exit(0);
});

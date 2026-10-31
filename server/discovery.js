import dgram from 'dgram';
import { EventEmitter } from 'events';
import { PORT, DISCOVERY_PORT, deviceIdentity } from './config.js';
import { getLocalNetworkDetails } from './networkUtils.js';

export class DiscoveryService extends EventEmitter {
  constructor() {
    super();
    this.socket = null;
    this.peers = new Map(); // deviceId -> peer info
    this.beaconTimer = null;
    this.cleanupTimer = null;
    this.multicastAddress = '239.255.255.250';
  }

  start() {
    this.socket = dgram.createSocket({ type: 'udp4', reuseAddr: true });

    this.socket.on('error', (err) => {
      console.warn('[Discovery] Socket warning/error:', err.message);
    });

    this.socket.on('message', (msg, rinfo) => {
      this.handleMessage(msg, rinfo);
    });

    this.socket.on('listening', () => {
      try {
        this.socket.setBroadcast(true);
        try {
          this.socket.addMembership(this.multicastAddress);
        } catch {
          // Multicast membership might not be supported on all interfaces
        }
      } catch (e) {
        console.warn('[Discovery] Could not set broadcast flag:', e.message);
      }
      const address = this.socket.address();
      console.log(`[Discovery] Listening on UDP ${address.address}:${address.port}`);

      // Send initial probe to discover already-running peers immediately
      this.sendProbe();
    });

    // Try binding to DISCOVERY_PORT, fallback to random port if busy
    try {
      this.socket.bind(DISCOVERY_PORT, '0.0.0.0');
    } catch {
      try {
        this.socket.bind(0, '0.0.0.0');
      } catch (err) {
        console.error('[Discovery] Failed to bind UDP socket:', err);
      }
    }

    // Periodic beacon every 2.5 seconds
    this.beaconTimer = setInterval(() => {
      this.broadcastBeacon();
    }, 2500);

    // Peer liveness check every 3 seconds
    this.cleanupTimer = setInterval(() => {
      this.prunePeers();
    }, 3000);

    // Initial broadcast
    this.broadcastBeacon();
  }

  handleMessage(msg, rinfo) {
    try {
      const data = JSON.parse(msg.toString('utf8'));
      if (data.magic !== 'MESHLINK') return;

      // Ignore beacons from self
      if (data.deviceId === deviceIdentity.deviceId) return;

      if (data.type === 'BEACON') {
        const remoteIp = rinfo.address === '127.0.0.1' || rinfo.address === '::ffff:127.0.0.1'
          ? '127.0.0.1'
          : rinfo.address;

        const peerInfo = {
          deviceId: data.deviceId,
          deviceName: data.deviceName,
          deviceType: data.deviceType || 'desktop',
          avatarColor: data.avatarColor || '#3b82f6',
          port: data.port,
          ip: remoteIp,
          addresses: data.addresses || [remoteIp],
          lastSeen: Date.now(),
          online: true,
        };

        const existing = this.peers.get(data.deviceId);
        const isNew = !existing;
        this.peers.set(data.deviceId, { ...existing, ...peerInfo });

        if (isNew || existing?.online === false || existing?.deviceName !== data.deviceName) {
          this.emit('peer_updated', this.peers.get(data.deviceId));
        }
      } else if (data.type === 'PROBE') {
        // Direct reply with our beacon
        this.sendBeaconTo(rinfo.address, data.port || DISCOVERY_PORT);
      }
    } catch {
      // Invalid packet, ignore
    }
  }

  broadcastBeacon() {
    if (!this.socket) return;
    const netDetails = getLocalNetworkDetails();

    const beacon = Buffer.from(
      JSON.stringify({
        magic: 'MESHLINK',
        type: 'BEACON',
        deviceId: deviceIdentity.deviceId,
        deviceName: deviceIdentity.deviceName,
        deviceType: deviceIdentity.deviceType,
        avatarColor: deviceIdentity.avatarColor,
        port: PORT,
        addresses: netDetails.allIps,
        timestamp: Date.now(),
      })
    );

    // Targets: general broadcast, specific subnet broadcasts, multicast, and localhost
    const targets = new Set([
      '255.255.255.255',
      '127.0.0.1',
      this.multicastAddress,
      ...netDetails.broadcastAddresses,
    ]);

    for (const target of targets) {
      try {
        this.socket.send(beacon, 0, beacon.length, DISCOVERY_PORT, target, () => {});
      } catch {
        // Ignore broadcast route errors for inactive interfaces
      }
    }
  }

  sendProbe() {
    if (!this.socket) return;
    const netDetails = getLocalNetworkDetails();
    const probe = Buffer.from(
      JSON.stringify({
        magic: 'MESHLINK',
        type: 'PROBE',
        deviceId: deviceIdentity.deviceId,
        port: PORT,
      })
    );

    const targets = ['255.255.255.255', '127.0.0.1', ...netDetails.broadcastAddresses];
    for (const target of targets) {
      try {
        this.socket.send(probe, 0, probe.length, DISCOVERY_PORT, target, () => {});
      } catch {}
    }
  }

  sendBeaconTo(ip, port) {
    if (!this.socket) return;
    const netDetails = getLocalNetworkDetails();
    const beacon = Buffer.from(
      JSON.stringify({
        magic: 'MESHLINK',
        type: 'BEACON',
        deviceId: deviceIdentity.deviceId,
        deviceName: deviceIdentity.deviceName,
        deviceType: deviceIdentity.deviceType,
        avatarColor: deviceIdentity.avatarColor,
        port: PORT,
        addresses: netDetails.allIps,
        timestamp: Date.now(),
      })
    );

    try {
      this.socket.send(beacon, 0, beacon.length, port, ip, () => {});
    } catch {}
  }

  prunePeers() {
    const now = Date.now();
    let hasChanges = false;

    for (const [deviceId, peer] of this.peers.entries()) {
      if (peer.online && now - peer.lastSeen > 8000) {
        peer.online = false;
        hasChanges = true;
        this.emit('peer_updated', peer);
      }
    }

    if (hasChanges) {
      this.emit('peers_changed', this.getDiscoveredPeers());
    }
  }

  getDiscoveredPeers() {
    return Array.from(this.peers.values());
  }

  async manualConnect(targetIp, targetPort) {
    const port = parseInt(targetPort, 10);
    // Send direct probe via UDP
    this.sendBeaconTo(targetIp, DISCOVERY_PORT);
    if (port !== DISCOVERY_PORT) {
      this.sendBeaconTo(targetIp, port);
    }

    // Also attempt HTTP handshake probe
    try {
      const response = await fetch(`http://${targetIp}:${port}/api/info`, {
        signal: AbortSignal.timeout(3000),
      });
      if (response.ok) {
        const info = await response.json();
        if (info.deviceId && info.deviceId !== deviceIdentity.deviceId) {
          const peerInfo = {
            deviceId: info.deviceId,
            deviceName: info.deviceName,
            deviceType: info.deviceType || 'desktop',
            avatarColor: info.avatarColor || '#3b82f6',
            port: port,
            ip: targetIp,
            addresses: [targetIp],
            lastSeen: Date.now(),
            online: true,
          };
          this.peers.set(info.deviceId, peerInfo);
          this.emit('peer_updated', peerInfo);
          return { success: true, peer: peerInfo };
        }
      }
    } catch (err) {
      return { success: false, error: err.message };
    }

    return { success: false, error: 'Could not reach target device' };
  }

  stop() {
    clearInterval(this.beaconTimer);
    clearInterval(this.cleanupTimer);
    if (this.socket) {
      try {
        this.socket.close();
      } catch {}
    }
  }
}

import WebSocket from 'ws';
import fs from 'fs';
import { EventEmitter } from 'events';
import { v4 as uuidv4 } from 'uuid';
import { deviceIdentity, PORT } from './config.js';
import { storage } from './storage.js';
import { getLocalNetworkDetails } from './networkUtils.js';
import { fileTransferManager, CHUNK_SIZE } from './fileTransfer.js';

export class PeerManager extends EventEmitter {
  constructor(discoveryService) {
    super();
    this.discovery = discoveryService;
    this.peerSockets = new Map(); // peerDeviceId -> WebSocket connection
    this.uiSockets = new Set(); // Browser UI WebSocket connections
    this.pendingPairings = new Map(); // peerDeviceId -> { pin, timestamp, isIncoming, ws, info }
    this.activeStreaming = new Map(); // transferId -> boolean (is streaming active)

    // Wire file transfer manager events to UI
    fileTransferManager.on('progress', (transfer) => {
      this.broadcastToUI({ type: 'TRANSFER_PROGRESS', transfer });
    });
    fileTransferManager.on('completed', (transfer) => {
      this.broadcastToUI({ type: 'TRANSFER_COMPLETED', transfer });
      // Record completed file message in chat
      this.recordFileMessage(transfer);
    });
    fileTransferManager.on('cancelled', (transfer) => {
      this.broadcastToUI({ type: 'TRANSFER_CANCELLED', transfer });
    });
    fileTransferManager.on('failed', (transfer) => {
      this.broadcastToUI({ type: 'TRANSFER_FAILED', transfer });
    });
  }

  // Register a browser UI WebSocket client
  registerUIClient(ws) {
    this.uiSockets.add(ws);

    // Send initial bootstrap state
    const netDetails = getLocalNetworkDetails();
    ws.send(
      JSON.stringify({
        type: 'INIT_STATE',
        identity: deviceIdentity,
        network: netDetails,
        discoveredPeers: this.discovery.getDiscoveredPeers(),
        savedPeers: storage.getPeers(),
        connectedPeerIds: Array.from(this.peerSockets.keys()),
        pendingPairings: Array.from(this.pendingPairings.entries()).map(([peerId, p]) => ({
          peerId,
          ...p.info,
          pin: p.pin,
          isIncoming: p.isIncoming,
        })),
        settings: storage.getSettings(),
        transfers: storage.getTransfers(),
      })
    );

    ws.on('close', () => {
      this.uiSockets.delete(ws);
    });

    ws.on('message', (raw) => {
      try {
        const data = JSON.parse(raw.toString('utf8'));
        this.handleUIMessage(data, ws);
      } catch (err) {
        console.error('Error handling UI message:', err);
      }
    });
  }

  broadcastToUI(payload) {
    const raw = JSON.stringify(payload);
    for (const ws of this.uiSockets) {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(raw);
      }
    }
  }

  // Handle message from browser frontend UI
  async handleUIMessage(msg, uiWs) {
    switch (msg.type) {
      case 'REQUEST_PAIR': {
        const { targetPeer } = msg;
        await this.initiatePairing(targetPeer);
        break;
      }

      case 'ACCEPT_PAIR': {
        const { peerId } = msg;
        await this.acceptPairing(peerId);
        break;
      }

      case 'REJECT_PAIR': {
        const { peerId } = msg;
        this.rejectPairing(peerId);
        break;
      }

      case 'DISCONNECT_PEER': {
        const { peerId } = msg;
        this.disconnectPeer(peerId);
        break;
      }

      case 'SEND_MESSAGE': {
        const { peerId, content } = msg;
        await this.sendChatMessage(peerId, content);
        break;
      }

      case 'TYPING_INDICATOR': {
        const { peerId, isTyping } = msg;
        this.sendToPeer(peerId, {
          type: 'TYPING',
          fromDeviceId: deviceIdentity.deviceId,
          isTyping,
        });
        break;
      }

      case 'MARK_READ': {
        const { peerId } = msg;
        storage.markAllAsRead(peerId);
        this.sendToPeer(peerId, {
          type: 'MSG_READ',
          fromDeviceId: deviceIdentity.deviceId,
        });
        this.broadcastToUI({
          type: 'MESSAGES_UPDATED',
          peerId,
          messages: storage.getMessages(peerId),
        });
        break;
      }

      case 'ACCEPT_FILE_TRANSFER': {
        const { transferId } = msg;
        this.acceptIncomingFileTransfer(transferId);
        break;
      }

      case 'REJECT_FILE_TRANSFER': {
        const { transferId } = msg;
        this.rejectIncomingFileTransfer(transferId);
        break;
      }

      case 'CANCEL_FILE_TRANSFER': {
        const { transferId } = msg;
        this.cancelFileTransfer(transferId);
        break;
      }

      case 'RETRY_FILE_TRANSFER': {
        const { transferId } = msg;
        this.retryFileTransfer(transferId);
        break;
      }

      case 'GET_MESSAGES': {
        const { peerId } = msg;
        uiWs.send(
          JSON.stringify({
            type: 'MESSAGES_UPDATED',
            peerId,
            messages: storage.getMessages(peerId),
          })
        );
        break;
      }

      case 'CLEAR_CHAT': {
        const { peerId } = msg;
        storage.clearMessages(peerId);
        this.broadcastToUI({
          type: 'MESSAGES_UPDATED',
          peerId,
          messages: [],
        });
        break;
      }

      default:
        break;
    }
  }

  // Handle incoming peer WebSocket connection to `/ws/peer`
  handleIncomingPeerSocket(ws, req) {
    let authenticatedPeerId = null;

    ws.on('message', async (raw) => {
      try {
        const msg = JSON.parse(raw.toString('utf8'));
        const resultPeerId = await this.processPeerMessage(msg, ws);
        if (resultPeerId) authenticatedPeerId = resultPeerId;
      } catch (err) {
        console.error('Error handling peer message:', err);
      }
    });

    ws.on('close', () => {
      if (authenticatedPeerId) {
        this.handlePeerDisconnected(authenticatedPeerId);
      }
    });

    ws.on('error', (err) => {
      console.warn('Peer socket error:', err.message);
    });
  }

  // Process message from remote peer
  async processPeerMessage(msg, ws) {
    switch (msg.type) {
      case 'PAIR_REQUEST': {
        const { from, pin } = msg;
        const peerId = from.deviceId;

        this.pendingPairings.set(peerId, {
          pin,
          timestamp: Date.now(),
          isIncoming: true,
          ws,
          info: from,
        });

        // Notify local UI of incoming pairing request
        this.broadcastToUI({
          type: 'INCOMING_PAIR_REQUEST',
          peer: from,
          pin,
        });
        return peerId;
      }

      case 'PAIR_ACCEPT': {
        const { from, pin } = msg;
        const peerId = from.deviceId;
        const pending = this.pendingPairings.get(peerId);

        if (pending) {
          this.pendingPairings.delete(peerId);
          this.peerSockets.set(peerId, ws);

          const savedPeer = storage.savePeer({
            ...from,
            status: 'connected',
            paired: true,
            lastConnected: Date.now(),
          });

          this.broadcastToUI({
            type: 'PAIR_SUCCESS',
            peer: savedPeer,
            message: `Connected with ${from.deviceName}`,
          });
        }
        return peerId;
      }

      case 'PAIR_REJECT': {
        const { from, reason } = msg;
        const peerId = from.deviceId;
        this.pendingPairings.delete(peerId);

        this.broadcastToUI({
          type: 'PAIR_REJECTED',
          peerId,
          reason: reason || 'Connection request was declined',
        });
        ws.close();
        return null;
      }

      case 'CHAT_MESSAGE': {
        const { message } = msg;
        const peerId = message.senderId;

        // Save to local storage
        const receivedMsg = {
          ...message,
          status: 'delivered',
        };
        storage.addMessage(peerId, receivedMsg);

        // Acknowledge delivery
        this.sendToPeer(peerId, {
          type: 'MSG_DELIVERED',
          messageId: message.id,
        });

        this.broadcastToUI({
          type: 'NEW_CHAT_MESSAGE',
          peerId,
          message: receivedMsg,
        });
        return peerId;
      }

      case 'MSG_DELIVERED': {
        const { messageId, fromDeviceId } = msg;
        const updated = storage.updateMessageStatus(fromDeviceId, messageId, 'delivered');
        if (updated) {
          this.broadcastToUI({
            type: 'MESSAGE_STATUS_CHANGED',
            peerId: fromDeviceId,
            messageId,
            status: 'delivered',
          });
        }
        return fromDeviceId;
      }

      case 'MSG_READ': {
        const { fromDeviceId } = msg;
        storage.markAllAsRead(fromDeviceId);
        this.broadcastToUI({
          type: 'MESSAGES_UPDATED',
          peerId: fromDeviceId,
          messages: storage.getMessages(fromDeviceId),
        });
        return fromDeviceId;
      }

      case 'TYPING': {
        const { fromDeviceId, isTyping } = msg;
        this.broadcastToUI({
          type: 'PEER_TYPING',
          peerId: fromDeviceId,
          isTyping,
        });
        return fromDeviceId;
      }

      case 'FILE_OFFER': {
        const { transfer } = msg;
        const incoming = fileTransferManager.createIncomingTransfer({
          transferId: transfer.transferId,
          peerId: transfer.peerId,
          fileName: transfer.fileName,
          fileSize: transfer.fileSize,
          mimeType: transfer.mimeType,
          totalChunks: transfer.totalChunks,
        });

        const settings = storage.getSettings();
        if (settings.autoAcceptTransfers) {
          this.acceptIncomingFileTransfer(transfer.transferId);
        } else {
          this.broadcastToUI({
            type: 'INCOMING_FILE_OFFER',
            transfer: incoming,
          });
        }
        return transfer.peerId;
      }

      case 'FILE_ACCEPT': {
        const { transferId } = msg;
        this.startStreamingChunks(transferId);
        return null;
      }

      case 'FILE_REJECT': {
        const { transferId, reason } = msg;
        fileTransferManager.cancelTransfer(transferId);
        this.broadcastToUI({
          type: 'TRANSFER_REJECTED',
          transferId,
          reason: reason || 'Transfer declined by recipient',
        });
        return null;
      }

      case 'FILE_CHUNK': {
        const { transferId, chunkIndex, totalChunks, data } = msg;
        await fileTransferManager.handleIncomingChunk({
          transferId,
          chunkIndex,
          totalChunks,
          data,
        });
        return null;
      }

      case 'FILE_CANCEL': {
        const { transferId } = msg;
        fileTransferManager.cancelTransfer(transferId);
        return null;
      }

      case 'FILE_COMPLETE': {
        const { transferId } = msg;
        const transfer = fileTransferManager.getTransfer(transferId);
        if (transfer) {
          transfer.status = 'completed';
          transfer.progress = 100;
          storage.saveTransfer(transfer);
          this.broadcastToUI({ type: 'TRANSFER_COMPLETED', transfer });
          this.recordFileMessage(transfer);
        }
        return null;
      }

      case 'DISCONNECT': {
        const { fromDeviceId } = msg;
        this.handlePeerDisconnected(fromDeviceId);
        return null;
      }

      default:
        return null;
    }
  }

  // Record completed file transfer as a chat message item
  recordFileMessage(transfer) {
    const isOutgoing = transfer.direction === 'outgoing';
    const msg = {
      id: uuidv4(),
      senderId: isOutgoing ? deviceIdentity.deviceId : transfer.peerId,
      recipientId: isOutgoing ? transfer.peerId : deviceIdentity.deviceId,
      type: 'file',
      content: transfer.fileName,
      fileMeta: {
        transferId: transfer.transferId,
        fileName: transfer.fileName,
        savedFileName: transfer.savedFileName || transfer.fileName,
        fileSize: transfer.fileSize,
        mimeType: transfer.mimeType,
        isImage: transfer.mimeType?.startsWith('image/'),
      },
      status: 'delivered',
      timestamp: Date.now(),
    };

    storage.addMessage(transfer.peerId, msg);
    this.broadcastToUI({
      type: 'NEW_CHAT_MESSAGE',
      peerId: transfer.peerId,
      message: msg,
    });
  }

  // Initiate outgoing pairing to a discovered peer
  async initiatePairing(targetPeer) {
    const netDetails = getLocalNetworkDetails();
    const pin = Math.floor(100000 + Math.random() * 900000).toString();

    const targetUrl = `ws://${targetPeer.ip}:${targetPeer.port}/ws/peer`;

    try {
      const ws = new WebSocket(targetUrl);

      this.pendingPairings.set(targetPeer.deviceId, {
        pin,
        timestamp: Date.now(),
        isIncoming: false,
        ws,
        info: targetPeer,
      });

      this.broadcastToUI({
        type: 'OUTGOING_PAIR_PENDING',
        peer: targetPeer,
        pin,
      });

      ws.on('open', () => {
        ws.send(
          JSON.stringify({
            type: 'PAIR_REQUEST',
            from: {
              deviceId: deviceIdentity.deviceId,
              deviceName: deviceIdentity.deviceName,
              deviceType: deviceIdentity.deviceType,
              avatarColor: deviceIdentity.avatarColor,
              ip: netDetails.primaryIp,
              port: PORT,
            },
            pin,
          })
        );
      });

      ws.on('message', async (raw) => {
        try {
          const msg = JSON.parse(raw.toString('utf8'));
          await this.processPeerMessage(msg, ws);
        } catch (err) {
          console.error('Error in outgoing ws message:', err);
        }
      });

      ws.on('close', () => {
        this.handlePeerDisconnected(targetPeer.deviceId);
      });

      ws.on('error', (err) => {
        console.warn(`Could not connect to peer ${targetPeer.deviceName}:`, err.message);
        this.pendingPairings.delete(targetPeer.deviceId);
        this.broadcastToUI({
          type: 'PAIR_ERROR',
          peerId: targetPeer.deviceId,
          error: `Connection to ${targetPeer.deviceName} failed (${err.message})`,
        });
      });
    } catch (err) {
      this.pendingPairings.delete(targetPeer.deviceId);
      this.broadcastToUI({
        type: 'PAIR_ERROR',
        peerId: targetPeer.deviceId,
        error: err.message,
      });
    }
  }

  // Accept incoming pairing
  async acceptPairing(peerId) {
    const pending = this.pendingPairings.get(peerId);
    if (!pending || !pending.ws) return;

    const netDetails = getLocalNetworkDetails();
    pending.ws.send(
      JSON.stringify({
        type: 'PAIR_ACCEPT',
        from: {
          deviceId: deviceIdentity.deviceId,
          deviceName: deviceIdentity.deviceName,
          deviceType: deviceIdentity.deviceType,
          avatarColor: deviceIdentity.avatarColor,
          ip: netDetails.primaryIp,
          port: PORT,
        },
        pin: pending.pin,
      })
    );

    this.peerSockets.set(peerId, pending.ws);
    this.pendingPairings.delete(peerId);

    const saved = storage.savePeer({
      ...pending.info,
      status: 'connected',
      paired: true,
      lastConnected: Date.now(),
    });

    this.broadcastToUI({
      type: 'PAIR_SUCCESS',
      peer: saved,
      message: `Connected with ${pending.info.deviceName}`,
    });
  }

  // Reject incoming pairing
  rejectPairing(peerId) {
    const pending = this.pendingPairings.get(peerId);
    if (!pending) return;

    if (pending.ws && pending.ws.readyState === WebSocket.OPEN) {
      pending.ws.send(
        JSON.stringify({
          type: 'PAIR_REJECT',
          from: { deviceId: deviceIdentity.deviceId },
          reason: 'Pairing request declined',
        })
      );
      pending.ws.close();
    }

    this.pendingPairings.delete(peerId);
    this.broadcastToUI({ type: 'PAIR_CANCELLED', peerId });
  }

  // Disconnect an active peer
  disconnectPeer(peerId) {
    const ws = this.peerSockets.get(peerId);
    if (ws) {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(
          JSON.stringify({
            type: 'DISCONNECT',
            fromDeviceId: deviceIdentity.deviceId,
          })
        );
      }
      ws.close();
      this.peerSockets.delete(peerId);
    }
    this.handlePeerDisconnected(peerId);
  }

  handlePeerDisconnected(peerId) {
    this.peerSockets.delete(peerId);
    this.pendingPairings.delete(peerId);
    storage.savePeer({ deviceId: peerId, status: 'disconnected' });
    this.broadcastToUI({
      type: 'PEER_DISCONNECTED',
      peerId,
    });
  }

  sendToPeer(peerId, payload) {
    const ws = this.peerSockets.get(peerId);
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(payload));
      return true;
    }
    return false;
  }

  // Send a text chat message
  async sendChatMessage(peerId, content) {
    const message = {
      id: uuidv4(),
      senderId: deviceIdentity.deviceId,
      recipientId: peerId,
      type: 'text',
      content,
      status: 'sending',
      timestamp: Date.now(),
    };

    storage.addMessage(peerId, message);
    this.broadcastToUI({
      type: 'NEW_CHAT_MESSAGE',
      peerId,
      message,
    });

    const sent = this.sendToPeer(peerId, {
      type: 'CHAT_MESSAGE',
      message,
    });

    if (sent) {
      message.status = 'sent';
      storage.updateMessageStatus(peerId, message.id, 'sent');
      this.broadcastToUI({
        type: 'MESSAGE_STATUS_CHANGED',
        peerId,
        messageId: message.id,
        status: 'sent',
      });
    } else {
      message.status = 'failed';
      storage.updateMessageStatus(peerId, message.id, 'failed');
      this.broadcastToUI({
        type: 'MESSAGE_STATUS_CHANGED',
        peerId,
        messageId: message.id,
        status: 'failed',
      });
    }

    return message;
  }

  // Send a file offer to a peer
  offerFileTransfer(peerId, { fileName, fileSize, mimeType, localFilePath }) {
    const transfer = fileTransferManager.createOutgoingTransfer({
      peerId,
      fileName,
      fileSize,
      mimeType,
      localFilePath,
    });

    this.broadcastToUI({
      type: 'TRANSFER_OFFERED',
      transfer,
    });

    this.sendToPeer(peerId, {
      type: 'FILE_OFFER',
      transfer: {
        transferId: transfer.transferId,
        peerId: deviceIdentity.deviceId,
        fileName: transfer.fileName,
        fileSize: transfer.fileSize,
        mimeType: transfer.mimeType,
        totalChunks: transfer.totalChunks,
      },
    });

    return transfer;
  }

  acceptIncomingFileTransfer(transferId) {
    const transfer = fileTransferManager.getTransfer(transferId);
    if (!transfer) return;

    this.sendToPeer(transfer.peerId, {
      type: 'FILE_ACCEPT',
      transferId,
    });
  }

  rejectIncomingFileTransfer(transferId) {
    const transfer = fileTransferManager.getTransfer(transferId);
    if (!transfer) return;

    this.sendToPeer(transfer.peerId, {
      type: 'FILE_REJECT',
      transferId,
      reason: 'Rejected by recipient',
    });
    fileTransferManager.cancelTransfer(transferId);
  }

  cancelFileTransfer(transferId) {
    const transfer = fileTransferManager.getTransfer(transferId);
    if (!transfer) return;

    fileTransferManager.cancelTransfer(transferId);
    this.sendToPeer(transfer.peerId, {
      type: 'FILE_CANCEL',
      transferId,
    });
  }

  retryFileTransfer(transferId) {
    const transfer = fileTransferManager.resetForRetry(transferId);
    if (!transfer) return;

    if (transfer.direction === 'outgoing') {
      this.sendToPeer(transfer.peerId, {
        type: 'FILE_OFFER',
        transfer: {
          transferId: transfer.transferId,
          peerId: deviceIdentity.deviceId,
          fileName: transfer.fileName,
          fileSize: transfer.fileSize,
          mimeType: transfer.mimeType,
          totalChunks: transfer.totalChunks,
        },
      });
    }
  }

  // Stream chunks to recipient over WebSocket
  async startStreamingChunks(transferId) {
    const transfer = fileTransferManager.getTransfer(transferId);
    if (!transfer || !transfer.localFilePath) return;

    transfer.status = 'transferring';
    transfer.startTime = Date.now();
    transfer.lastUpdateTime = Date.now();
    storage.saveTransfer(transfer);
    this.broadcastToUI({ type: 'TRANSFER_PROGRESS', transfer });

    let fd = null;
    try {
      fd = await fs.promises.open(transfer.localFilePath, 'r');
      const buffer = Buffer.alloc(CHUNK_SIZE);

      for (let chunkIndex = 0; chunkIndex < transfer.totalChunks; chunkIndex++) {
        // Check if cancelled
        if (transfer.status === 'cancelled') {
          break;
        }

        const offset = chunkIndex * CHUNK_SIZE;
        const bytesToRead = Math.min(CHUNK_SIZE, transfer.fileSize - offset);
        const { bytesRead } = await fd.read(buffer, 0, bytesToRead, offset);

        const chunkData = buffer.subarray(0, bytesRead).toString('base64');

        const sent = this.sendToPeer(transfer.peerId, {
          type: 'FILE_CHUNK',
          transferId,
          chunkIndex,
          totalChunks: transfer.totalChunks,
          data: chunkData,
        });

        if (!sent) {
          throw new Error('Peer connection lost during transfer');
        }

        transfer.bytesTransferred += bytesRead;
        transfer.currentChunk = chunkIndex + 1;
        transfer.progress = Math.min(100, Math.round((transfer.bytesTransferred / transfer.fileSize) * 100));

        const now = Date.now();
        const timeDelta = (now - transfer.lastUpdateTime) / 1000;
        if (timeDelta >= 0.5 || transfer.currentChunk >= transfer.totalChunks) {
          const bytesDelta = transfer.bytesTransferred - transfer.lastBytesTransferred;
          transfer.speed = Math.round(bytesDelta / timeDelta);
          const remainingBytes = transfer.fileSize - transfer.bytesTransferred;
          transfer.eta = transfer.speed > 0 ? Math.ceil(remainingBytes / transfer.speed) : 0;
          transfer.lastUpdateTime = now;
          transfer.lastBytesTransferred = transfer.bytesTransferred;
          this.broadcastToUI({ type: 'TRANSFER_PROGRESS', transfer });
        }

        // Slight breathing space between chunks to prevent buffer starvation on LAN
        if (chunkIndex % 4 === 0) {
          await new Promise((resolve) => setImmediate(resolve));
        }
      }

      await fd.close();
      fd = null;

      if (transfer.status !== 'cancelled') {
        transfer.status = 'completed';
        transfer.progress = 100;
        transfer.speed = 0;
        transfer.eta = 0;
        storage.saveTransfer(transfer);

        this.sendToPeer(transfer.peerId, {
          type: 'FILE_COMPLETE',
          transferId,
        });

        this.broadcastToUI({ type: 'TRANSFER_COMPLETED', transfer });
        this.recordFileMessage(transfer);
      }
    } catch (err) {
      console.error('Error during file streaming:', err);
      if (fd) {
        try {
          await fd.close();
        } catch {}
      }
      transfer.status = 'failed';
      transfer.error = err.message;
      storage.saveTransfer(transfer);
      this.broadcastToUI({ type: 'TRANSFER_FAILED', transfer });
    }
  }
}

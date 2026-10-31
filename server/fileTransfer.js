import fs from 'fs';
import path from 'path';
import { EventEmitter } from 'events';
import { v4 as uuidv4 } from 'uuid';
import { DOWNLOADS_DIR, TEMP_DIR } from './config.js';
import { storage } from './storage.js';

export const CHUNK_SIZE = 64 * 1024; // 64 KB chunk size

export class FileTransferManager extends EventEmitter {
  constructor() {
    super();
    this.activeTransfers = new Map(); // transferId -> transfer object
    this.sendStreams = new Map(); // transferId -> readStream/abortController
  }

  // Prepares an outgoing file transfer from uploaded file or local path
  createOutgoingTransfer({ peerId, fileName, fileSize, mimeType, localFilePath }) {
    const transferId = uuidv4();
    const totalChunks = Math.ceil(fileSize / CHUNK_SIZE);

    const transfer = {
      transferId,
      peerId,
      direction: 'outgoing',
      fileName,
      fileSize,
      mimeType: mimeType || 'application/octet-stream',
      totalChunks,
      chunkSize: CHUNK_SIZE,
      bytesTransferred: 0,
      currentChunk: 0,
      progress: 0,
      speed: 0,
      eta: 0,
      status: 'pending', // pending, transferring, completed, cancelled, failed
      localFilePath,
      startTime: Date.now(),
      lastUpdateTime: Date.now(),
      lastBytesTransferred: 0,
      createdAt: Date.now(),
    };

    this.activeTransfers.set(transferId, transfer);
    storage.saveTransfer(transfer);
    return transfer;
  }

  // Prepares an incoming file transfer upon receiving FILE_OFFER
  createIncomingTransfer({ transferId, peerId, fileName, fileSize, mimeType, totalChunks }) {
    const tempFilePath = path.join(TEMP_DIR, `${transferId}.part`);

    const transfer = {
      transferId,
      peerId,
      direction: 'incoming',
      fileName,
      fileSize,
      mimeType: mimeType || 'application/octet-stream',
      totalChunks,
      chunkSize: CHUNK_SIZE,
      bytesTransferred: 0,
      currentChunk: 0,
      progress: 0,
      speed: 0,
      eta: 0,
      status: 'pending',
      tempFilePath,
      startTime: Date.now(),
      lastUpdateTime: Date.now(),
      lastBytesTransferred: 0,
      createdAt: Date.now(),
    };

    this.activeTransfers.set(transferId, transfer);
    storage.saveTransfer(transfer);
    return transfer;
  }

  getTransfer(transferId) {
    return this.activeTransfers.get(transferId) || storage.getTransfer(transferId);
  }

  // Process received chunk
  async handleIncomingChunk({ transferId, chunkIndex, totalChunks, data }) {
    const transfer = this.activeTransfers.get(transferId);
    if (!transfer || transfer.status === 'cancelled') return null;

    if (transfer.status === 'pending') {
      transfer.status = 'transferring';
      transfer.startTime = Date.now();
      transfer.lastUpdateTime = Date.now();
    }

    const chunkBuffer = Buffer.from(data, 'base64');
    await fs.promises.appendFile(transfer.tempFilePath, chunkBuffer);

    transfer.bytesTransferred += chunkBuffer.length;
    transfer.currentChunk = chunkIndex + 1;
    transfer.progress = Math.min(100, Math.round((transfer.bytesTransferred / transfer.fileSize) * 100));

    // Calculate speed & ETA
    const now = Date.now();
    const timeDelta = (now - transfer.lastUpdateTime) / 1000;
    if (timeDelta >= 0.5) {
      const bytesDelta = transfer.bytesTransferred - transfer.lastBytesTransferred;
      transfer.speed = Math.round(bytesDelta / timeDelta); // bytes per second
      const remainingBytes = transfer.fileSize - transfer.bytesTransferred;
      transfer.eta = transfer.speed > 0 ? Math.ceil(remainingBytes / transfer.speed) : 0;
      transfer.lastUpdateTime = now;
      transfer.lastBytesTransferred = transfer.bytesTransferred;
    }

    this.emit('progress', transfer);

    // Complete check
    if (transfer.currentChunk >= totalChunks || transfer.bytesTransferred >= transfer.fileSize) {
      return await this.completeIncomingTransfer(transferId);
    }

    return transfer;
  }

  async completeIncomingTransfer(transferId) {
    const transfer = this.activeTransfers.get(transferId);
    if (!transfer) return null;

    // Pick unique target filename in DOWNLOADS_DIR
    let destName = transfer.fileName;
    let destPath = path.join(DOWNLOADS_DIR, destName);
    let counter = 1;

    const parsed = path.parse(transfer.fileName);
    while (fs.existsSync(destPath)) {
      destName = `${parsed.name} (${counter})${parsed.ext}`;
      destPath = path.join(DOWNLOADS_DIR, destName);
      counter++;
    }

    try {
      if (transfer.tempFilePath && fs.existsSync(transfer.tempFilePath)) {
        await fs.promises.rename(transfer.tempFilePath, destPath);
      }
      transfer.status = 'completed';
      transfer.progress = 100;
      transfer.savedFileName = destName;
      transfer.savedFilePath = destPath;
      transfer.completedAt = Date.now();
      transfer.speed = 0;
      transfer.eta = 0;

      storage.saveTransfer(transfer);
      this.emit('completed', transfer);
      return transfer;
    } catch (err) {
      console.error('Failed to finalize file transfer:', err);
      transfer.status = 'failed';
      transfer.error = err.message;
      storage.saveTransfer(transfer);
      this.emit('failed', transfer);
      return transfer;
    }
  }

  // Cancel in-flight transfer
  cancelTransfer(transferId) {
    const transfer = this.activeTransfers.get(transferId) || storage.getTransfer(transferId);
    if (!transfer) return null;

    transfer.status = 'cancelled';
    transfer.speed = 0;
    transfer.eta = 0;

    // Abort read stream if outgoing
    const abortCtrl = this.sendStreams.get(transferId);
    if (abortCtrl) {
      abortCtrl.abort();
      this.sendStreams.delete(transferId);
    }

    // Clean up partial incoming file if exists
    if (transfer.direction === 'incoming' && transfer.tempFilePath && fs.existsSync(transfer.tempFilePath)) {
      try {
        fs.unlinkSync(transfer.tempFilePath);
      } catch {}
    }

    this.activeTransfers.set(transferId, transfer);
    storage.saveTransfer(transfer);
    this.emit('cancelled', transfer);
    return transfer;
  }

  // Clean retry of a cancelled or failed transfer
  resetForRetry(transferId) {
    const transfer = this.activeTransfers.get(transferId) || storage.getTransfer(transferId);
    if (!transfer) return null;

    transfer.status = 'pending';
    transfer.bytesTransferred = 0;
    transfer.currentChunk = 0;
    transfer.progress = 0;
    transfer.speed = 0;
    transfer.eta = 0;
    transfer.startTime = Date.now();
    transfer.lastUpdateTime = Date.now();
    transfer.lastBytesTransferred = 0;

    if (transfer.direction === 'incoming' && transfer.tempFilePath && fs.existsSync(transfer.tempFilePath)) {
      try {
        fs.unlinkSync(transfer.tempFilePath);
      } catch {}
    }

    this.activeTransfers.set(transferId, transfer);
    storage.saveTransfer(transfer);
    return transfer;
  }
}

export const fileTransferManager = new FileTransferManager();

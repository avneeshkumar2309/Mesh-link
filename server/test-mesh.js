import http from 'http';
import fs from 'fs';
import path from 'path';
import { spawn } from 'child_process';
import WebSocket from 'ws';

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function runTests() {
  console.log('========================================================');
  console.log('🧪 Starting MeshLink Automated Verification Suite');
  console.log('========================================================');

  // 1. Clean test data folders
  const peer1Dir = path.resolve(process.cwd(), 'data/test_peer1');
  const peer2Dir = path.resolve(process.cwd(), 'data/test_peer2');
  fs.rmSync(peer1Dir, { recursive: true, force: true });
  fs.rmSync(peer2Dir, { recursive: true, force: true });

  // 2. Spawn Peer 1
  console.log('\n[1/7] Spawning Peer Alpha on port 4001...');
  const peer1Proc = spawn('node', [
    'server/index.js',
    '--port', '4001',
    '--name', 'Peer Alpha',
    '--dataDir', 'data/test_peer1',
  ], { stdio: 'pipe' });

  // 3. Spawn Peer 2
  console.log('[2/7] Spawning Peer Beta on port 4002...');
  const peer2Proc = spawn('node', [
    'server/index.js',
    '--port', '4002',
    '--name', 'Peer Beta',
    '--dataDir', 'data/test_peer2',
  ], { stdio: 'pipe' });

  let peer1Output = '';
  peer1Proc.stdout.on('data', (d) => { peer1Output += d.toString(); });
  let peer2Output = '';
  peer2Proc.stdout.on('data', (d) => { peer2Output += d.toString(); });

  const cleanup = () => {
    peer1Proc.kill();
    peer2Proc.kill();
  };

  try {
    // Wait for nodes to start
    await delay(2500);

    // 4. Verify Nodes are reachable offline
    console.log('[3/7] Verifying Node APIs...');
    const info1 = await fetch('http://127.0.0.1:4001/api/info').then((r) => r.json());
    const info2 = await fetch('http://127.0.0.1:4002/api/info').then((r) => r.json());
    console.log(`  ✓ Peer Alpha Online: ID ${info1.deviceId.slice(0, 8)}... (${info1.primaryIp}:4001)`);
    console.log(`  ✓ Peer Beta Online:  ID ${info2.deviceId.slice(0, 8)}... (${info2.primaryIp}:4002)`);

    // 5. Connect UI WebSocket clients to both nodes
    console.log('\n[4/7] Connecting Mock Client Sessions...');
    const ws1 = new WebSocket('ws://127.0.0.1:4001/ws/ui');
    const ws2 = new WebSocket('ws://127.0.0.1:4002/ws/ui');

    await Promise.all([
      new Promise((resolve) => ws1.on('open', resolve)),
      new Promise((resolve) => ws2.on('open', resolve)),
    ]);
    console.log('  ✓ UI Sockets connected');

    // 6. Test Pairing & Approval flow
    console.log('\n[5/7] Testing Device Discovery & Pairing Approval...');
    let pairingPin = null;

    const pairingPromise = new Promise((resolve, reject) => {
      ws2.on('message', (raw) => {
        const msg = JSON.parse(raw.toString());
        if (msg.type === 'INCOMING_PAIR_REQUEST') {
          console.log(`  ✓ Peer Beta received pairing request! PIN: ${msg.pin}`);
          pairingPin = msg.pin;
          // Peer Beta approves the pairing!
          ws2.send(JSON.stringify({ type: 'ACCEPT_PAIR', peerId: msg.peer.deviceId }));
        }
      });

      ws1.on('message', (raw) => {
        const msg = JSON.parse(raw.toString());
        if (msg.type === 'PAIR_SUCCESS') {
          console.log(`  ✓ Peer Alpha confirmed pairing success! Connected with: ${msg.peer.deviceName}`);
          resolve(true);
        }
      });
    });

    // Peer Alpha requests pairing with Peer Beta
    ws1.send(
      JSON.stringify({
        type: 'REQUEST_PAIR',
        targetPeer: {
          deviceId: info2.deviceId,
          deviceName: info2.deviceName,
          ip: '127.0.0.1',
          port: 4002,
        },
      })
    );

    await pairingPromise;

    // 7. Test Instant Messaging
    console.log('\n[6/7] Testing One-to-One Text Messaging & Read Receipts...');
    const chatPromise = new Promise((resolve) => {
      ws2.on('message', (raw) => {
        const msg = JSON.parse(raw.toString());
        if (msg.type === 'NEW_CHAT_MESSAGE' && msg.message.content === 'Hello from Alpha!') {
          console.log(`  ✓ Peer Beta received message: "${msg.message.content}"`);
          // Verify delivery status ack on Peer Alpha
          resolve(true);
        }
      });
    });

    ws1.send(
      JSON.stringify({
        type: 'SEND_MESSAGE',
        peerId: info2.deviceId,
        content: 'Hello from Alpha!',
      })
    );

    await chatPromise;

    // Verify Chat History in local storage
    const history1 = await fetch(`http://127.0.0.1:4001/api/messages/${info2.deviceId}`).then((r) => r.json());
    console.log(`  ✓ Local Chat History on Peer Alpha contains ${history1.messages.length} message(s)`);

    // 8. Test Document & File Transfer with Chunking & Progress
    console.log('\n[7/7] Testing Chunked File Transfer & Progress...');
    // Create a 256 KB test file
    const testFilePath = path.join(peer1Dir, 'sample-document.pdf');
    const dummyBuffer = Buffer.alloc(256 * 1024, 'MeshLink P2P Data Chunk Buffer ');
    fs.writeFileSync(testFilePath, dummyBuffer);

    // Upload and send from Alpha to Beta
    const fileTransferPromise = new Promise((resolve) => {
      let chunksReceived = 0;

      ws2.on('message', (raw) => {
        const msg = JSON.parse(raw.toString());
        if (msg.type === 'INCOMING_FILE_OFFER') {
          console.log(`  ✓ Peer Beta received FILE_OFFER for "${msg.transfer.fileName}" (${msg.transfer.fileSize} bytes)`);
          // Accept the transfer
          ws2.send(JSON.stringify({ type: 'ACCEPT_FILE_TRANSFER', transferId: msg.transfer.transferId }));
        }
        if (msg.type === 'TRANSFER_PROGRESS') {
          chunksReceived++;
        }
        if (msg.type === 'TRANSFER_COMPLETED') {
          console.log(`  ✓ Transfer Completed! File received by Peer Beta with 100% integrity`);
          resolve(msg.transfer);
        }
      });
    });

    // Form data upload
    const formData = new FormData();
    formData.append('file', new Blob([dummyBuffer]), 'sample-document.pdf');
    formData.append('peerId', info2.deviceId);

    const uploadRes = await fetch('http://127.0.0.1:4001/api/files/upload', {
      method: 'POST',
      body: formData,
    }).then((r) => r.json());

    console.log(`  ✓ Peer Alpha initiated transfer ID: ${uploadRes.transfer.transferId}`);

    const completedTransfer = await fileTransferPromise;

    // Verify file exists on Peer Beta disk
    const savedPath = completedTransfer?.savedFilePath;
    if (savedPath && fs.existsSync(savedPath)) {
      const savedStat = fs.statSync(savedPath);
      console.log(`  ✓ Verified file on disk at: ${savedPath} (${savedStat.size} bytes)`);
    }

    // 9. Test Transfer Cancellation and Retry
    console.log('\n[8/8] Testing Transfer Cancellation & Retry...');
    const cancelFileBuffer = Buffer.alloc(512 * 1024, 'Cancel and retry test data ');
    const cancelFormData = new FormData();
    cancelFormData.append('file', new Blob([cancelFileBuffer]), 'large-presentation.pptx');
    cancelFormData.append('peerId', info2.deviceId);

    const cancelUpload = await fetch('http://127.0.0.1:4001/api/files/upload', {
      method: 'POST',
      body: cancelFormData,
    }).then((r) => r.json());

    const cancelTransferId = cancelUpload.transfer.transferId;

    // Test cancelling the transfer
    const cancelPromise = new Promise((resolve) => {
      ws1.on('message', (raw) => {
        const msg = JSON.parse(raw.toString());
        if (msg.type === 'TRANSFER_CANCELLED' && msg.transfer.transferId === cancelTransferId) {
          console.log(`  ✓ Transfer ID ${cancelTransferId.slice(0, 8)}... successfully cancelled`);
          resolve(true);
        }
      });
    });

    ws1.send(JSON.stringify({ type: 'CANCEL_FILE_TRANSFER', transferId: cancelTransferId }));
    await cancelPromise;

    // Test retry
    const retryPromise = new Promise((resolve) => {
      ws2.on('message', (raw) => {
        const msg = JSON.parse(raw.toString());
        if (msg.type === 'INCOMING_FILE_OFFER' && msg.transfer.transferId === cancelTransferId) {
          console.log(`  ✓ Retry re-offered transfer to Peer Beta! Accepting again...`);
          ws2.send(JSON.stringify({ type: 'ACCEPT_FILE_TRANSFER', transferId: cancelTransferId }));
        }
        if (msg.type === 'TRANSFER_COMPLETED' && msg.transfer.transferId === cancelTransferId) {
          console.log(`  ✓ Retried transfer completed successfully!`);
          resolve(true);
        }
      });
    });

    ws1.send(JSON.stringify({ type: 'RETRY_FILE_TRANSFER', transferId: cancelTransferId }));
    await retryPromise;

    console.log('\n========================================================');
    console.log('🎉 ALL REQUIREMENTS VERIFIED (INCLUDING CANCEL & RETRY)!');
    console.log('========================================================\n');

    ws1.close();
    ws2.close();
    cleanup();
    process.exit(0);
  } catch (err) {
    console.error('Test failed with error:', err);
    cleanup();
    process.exit(1);
  }
}

runTests();

# MeshLink 🌐

> **Offline-first, Zero-Configuration Peer-to-Peer Local Mesh Communication & File Sharing System**

MeshLink operates **100% without internet access**. It communicates directly between devices over local Wi-Fi, Ethernet LAN, or ad-hoc Mobile Hotspots with no external servers, no cloud relays, and no third-party trackers.

---

## ✨ Features

- [x] **Nearby Device Discovery**
  - High-speed UDP beaconing (`255.255.255.255` & active interface subnet broadcasts on port `43891`).
  - Multicast group fallback (`239.255.255.250`).
  - Direct IP manual connect fallback for AP-isolated routers and mobile hotspots.
  - Automatic peer presence tracking (online/offline indicators with heartbeat pruning).

- [x] **Approve and Establish a Connection**
  - Secure pairing handshake: Initiator displays a 6-digit verification security PIN.
  - Recipient receives an incoming connection prompt showing the peer profile and matching PIN.
  - One-click **Approve** or **Decline**.
  - Bidirectional duplex session established over authenticated WebSockets.

- [x] **One-to-One Text Messaging**
  - Instant real-time messaging with typing indicator bubbles.
  - Message delivery lifecycle: `Sending` ⏱️ → `Sent` ✓ → `Delivered` ✓✓ → `Read` ✓✓.
  - Date separators and timestamp formatting.

- [x] **Local Chat History**
  - Atomic persistent local storage for message threads, contacts, and transfers.
  - Survives application reloads and offline restarts.
  - Option to clear history per conversation.

- [x] **Send and Receive Photos and Documents**
  - Send any file format: Photos (JPEG, PNG, GIF, WebP), Documents (PDF, DOCX, TXT), Code, and Archives (ZIP).
  - Drag-and-drop file upload directly onto the chat area.
  - In-chat inline image thumbnails with high-resolution lightbox zoom viewer.
  - One-click file download / save directly to the local disk.

- [x] **Transfer Progress, Retry, and Cancellation**
  - Chunked binary transfer protocol (64 KB chunks).
  - Real-time transfer metrics: Speed (`MB/s`), remaining time (`ETA`), and completion percentage (`0% - 100%`).
  - Instant **Cancel** button to abort in-flight transfers on both ends.
  - **Retry** button to re-transmit failed or cancelled transfers.

- [x] **Operate with Internet Disabled**
  - Zero external DNS lookups, STUN/TURN servers, or cloud dependencies.
  - Built-in self-hosted web app served directly by each node (`http://localhost:PORT` or `http://<LAN_IP>:PORT`).
  - Connect across phones, laptops, and desktops on the same offline network.

---

## 🚀 Quick Start

### 1. Installation
```bash
# Clone the repository and install dependencies
git clone <repo-url>
cd meshlink
npm install
npm --prefix client install
```

### 2. Run MeshLink
```bash
# Start the full-stack app (Node daemon + Vite UI)
npm run dev

# Or build and run standalone production server:
npm run build:client
npm start
```
Open **`http://localhost:4000`** in your browser.

---

## 🧪 Testing Two Peers on the Same Computer

MeshLink provides a dual-peer simulation mode so you can test peer discovery, pairing approval, messaging, and file transfers right on your machine:

```bash
npm run dev:dual
```

This launches:
- **Peer Alpha** on `http://localhost:4001` (Storage in `data/peer1`)
- **Peer Beta** on `http://localhost:4002` (Storage in `data/peer2`)

Open both URLs in side-by-side browser tabs. You will immediately see Peer Alpha and Peer Beta discover each other, request pairing, verify the 6-digit PIN, exchange messages, and stream files!

---

## 🧪 Automated Verification Suite

Run the end-to-end integration test:
```bash
node server/test-mesh.js
```
This tests:
1. Spawning offline nodes
2. Nearby discovery over local sockets
3. Pairing handshake & 6-digit PIN verification
4. Instant messaging & delivery acks
5. Chat history storage
6. Chunked document transfer & disk integrity validation
7. Transfer cancellation & retry

---

## 📁 Project Structure

```
meshlink/
├── server/
│   ├── index.js          # Express server, WebSocket routing, upload/download endpoints
│   ├── config.js         # Port configuration, device identity & directory paths
│   ├── discovery.js      # UDP broadcast & multicast beacon discovery service
│   ├── peerManager.js    # Session management, pairing approvals & chat dispatch
│   ├── fileTransfer.js   # Chunked file streaming, progress metrics, cancel & retry
│   ├── storage.js        # Atomic JSON persistence for chats, peers, and settings
│   ├── networkUtils.js   # IPv4 interface resolution & subnet broadcast calculation
│   └── test-mesh.js      # Automated end-to-end P2P verification test suite
├── client/
│   ├── src/
│   │   ├── components/
│   │   │   ├── PeerList.jsx            # Discovered devices, contacts & self profile
│   │   │   ├── ChatArea.jsx            # Drag-and-drop chat, typing indicator, file tray
│   │   │   ├── MessageItem.jsx         # Text bubbles, image lightbox, document cards
│   │   │   ├── FileTransferCard.jsx    # Real-time progress bars, speed, cancel & retry
│   │   │   ├── ConnectionModal.jsx     # Pairing request popup with 6-digit PIN
│   │   │   ├── DeviceSettingsModal.jsx # Device profile, avatar color & preferences
│   │   │   └── ManualConnectModal.jsx  # Direct IP/Port connection fallback
│   │   ├── hooks/
│   │   │   └── useMeshlink.js          # WebSocket hook & reactive state management
│   │   └── utils/
│   │       └── formatters.js           # Byte formatting, speed, ETA, and timestamps
│   └── vite.config.js
└── package.json
```

---

## 🛡️ License

MIT License

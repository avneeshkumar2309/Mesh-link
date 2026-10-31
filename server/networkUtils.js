import os from 'os';

/**
 * Calculates the IPv4 broadcast address from an IP and netmask
 */
function calculateBroadcastAddress(ip, netmask) {
  try {
    const ipParts = ip.split('.').map(Number);
    const maskParts = netmask.split('.').map(Number);
    if (ipParts.length !== 4 || maskParts.length !== 4) return '255.255.255.255';

    const broadcastParts = [];
    for (let i = 0; i < 4; i++) {
      broadcastParts.push((ipParts[i] | (~maskParts[i] & 255)));
    }
    return broadcastParts.join('.');
  } catch {
    return '255.255.255.255';
  }
}

/**
 * Retrieves all active non-internal IPv4 network interfaces and their broadcast addresses
 */
export function getLocalNetworkDetails() {
  const interfaces = os.networkInterfaces();
  const addresses = [];
  const broadcastAddresses = new Set(['255.255.255.255']);

  for (const [name, netList] of Object.entries(interfaces)) {
    if (!netList) continue;
    for (const net of netList) {
      // Look for IPv4 non-internal addresses
      if (net.family === 'IPv4' && !net.internal) {
        addresses.push({
          interface: name,
          ip: net.address,
          netmask: net.netmask,
          mac: net.mac,
        });

        if (net.netmask) {
          const bcast = calculateBroadcastAddress(net.address, net.netmask);
          if (bcast) broadcastAddresses.add(bcast);
        }
      }
    }
  }

  // Fallback to localhost if no active network
  if (addresses.length === 0) {
    addresses.push({
      interface: 'loopback',
      ip: '127.0.0.1',
      netmask: '255.0.0.0',
      mac: '00:00:00:00:00:00',
    });
  }

  return {
    primaryIp: addresses[0]?.ip || '127.0.0.1',
    allIps: addresses.map(a => a.ip),
    interfaces: addresses,
    broadcastAddresses: Array.from(broadcastAddresses),
  };
}

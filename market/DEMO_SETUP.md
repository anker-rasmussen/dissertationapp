# Demo Setup Guide

## Running Multiple Market Instances for Demo

This guide explains how to run 3 market app instances simultaneously for your dissertation demo.

### Prerequisites

1. **Devnet must be running** (20 nodes: 1 bootstrap + 19 regular):
   ```bash
   cd /home/broadcom/Repos/Dissertation/Repos/veilid/.devcontainer/compose
   docker compose -f docker-compose.dev.yml up -d
   # Wait for all nodes to be healthy
   docker compose -f docker-compose.dev.yml ps
   ```

2. **Verify devnet is healthy** - all 20 nodes should show "healthy" status

### Running 3 Market Instances

Open **3 separate terminals** and run one instance in each:

#### Terminal 1 - Bidder 1 (Node 20)
```bash
cd /home/broadcom/Repos/Dissertation/Repos/dissertationapp/market
./run-demo-instance.sh 20
```
- Port: 5180
- IP: 1.2.3.21
- Data: ~/.local/share/smpc-auction-node-20

#### Terminal 2 - Bidder 2 (Node 21)
```bash
cd /home/broadcom/Repos/Dissertation/Repos/dissertationapp/market
./run-demo-instance.sh 21
```
- Port: 5181
- IP: 1.2.3.22
- Data: ~/.local/share/smpc-auction-node-21

#### Terminal 3 - Auctioneer (Node 22)
```bash
cd /home/broadcom/Repos/Dissertation/Repos/dissertationapp/market
./run-demo-instance.sh 22
```
- Port: 5182
- IP: 1.2.3.23
- Data: ~/.local/share/smpc-auction-node-22

### Cluster Mode (Recommended)

Run all 3 nodes in a single terminal with interleaved output:

```bash
cd /home/broadcom/Repos/Dissertation/Repos/dissertationapp/market
./run-demo-instance.sh cluster
```

### Manual Run (Alternative)

If you prefer to run manually with more control:

```bash
# Instance 1
MARKET_NODE_OFFSET=20 LD_PRELOAD=/home/broadcom/Repos/Dissertation/Repos/veilid/.devcontainer/scripts/libipspoof.so RUST_LOG=info cargo run

# Instance 2
MARKET_NODE_OFFSET=21 LD_PRELOAD=/home/broadcom/Repos/Dissertation/Repos/veilid/.devcontainer/scripts/libipspoof.so RUST_LOG=info cargo run

# Instance 3
MARKET_NODE_OFFSET=22 LD_PRELOAD=/home/broadcom/Repos/Dissertation/Repos/veilid/.devcontainer/scripts/libipspoof.so RUST_LOG=info cargo run
```

### Verification

Each instance should show:
- **Status**: "Connected" (FullyAttached state)
- **Peers**: Non-zero count (should see other market instances + devnet nodes)
- **Node ID**: Different VLD0: identifier for each instance
- **Warning**: "This node has no valid public dial info" is EXPECTED - you're using LocalNetwork routing domain

### Connecting to Public Veilid Network

To connect to the real public Veilid network (for testing or production):

```bash
./run-public.sh
```

This will:
- Connect to public bootstrap nodes on the internet
- Make your node discoverable on the public network
- Use data directory: `~/.local/share/smpc-auction-public`
- **NOT require LD_PRELOAD or devnet**

### Network Topology

```
Devnet Nodes (Docker, offsets 0-19):
├── Bootstrap: 1.2.3.1:5160 (offset 0)
├── Node 1:    1.2.3.2:5161 (offset 1)
├── ...
├── Node 18:   1.2.3.19:5178 (offset 18)
└── Node 19:   1.2.3.20:5179 (offset 19)

Market Instances (Local, offsets 20+):
├── Party 0:   1.2.3.21:5180 (offset 20)
├── Party 1:   1.2.3.22:5181 (offset 21)
├── Party 2:   1.2.3.23:5182 (offset 22)
└── Party n:   1.2.3.(n+1):(5160+n) — max ~20 market nodes (LD_PRELOAD translates 40 IPs, devnet uses 20)
```

All nodes use the same network key: `development-network-2025`

### Troubleshooting

1. **"Connecting..." stuck**:
   - Check devnet is running: `docker ps`
   - Verify capabilities match (SGNL, RLAY, DIAL disabled)
   - Look for "no routing domain" errors in devnet logs

2. **Port already in use**:
   - Kill existing instances
   - Check with: `lsof -i :5169` (or 5170, 5171)

3. **Zero peers**:
   - Bootstrap is usually fast with warm data dirs; if data was cleaned after a devnet restart, the first attachment may take longer
   - Check LD_PRELOAD is set correctly
   - Verify devnet nodes are healthy

4. **Data conflicts / stale data**:
   - Each instance uses separate data directory
   - Clear data: `rm -rf ~/.local/share/smpc-auction-node-*`
   - **Important**: Always clean data dirs after restarting the devnet (new node identities)

### Stopping Everything

```bash
# Stop market instances: Ctrl+C in each terminal

# Stop devnet:
cd /home/broadcom/Repos/Dissertation/Repos/veilid/.devcontainer/compose
docker compose -f docker-compose.dev.yml down
```

### Architecture Notes

- **LD_PRELOAD**: Translates fake global IPs (1.2.3.x) to localhost (127.0.0.1)
- **Port-based routing**: Each node identified by its port number
- **Capabilities**: Market nodes disable SGNL and DIAL; RELAY and TUNNEL left enabled for route construction
- **Bootstrap**: All nodes connect to 1.2.3.1:5160 for initial peer discovery
- **Protocol**: Uses UDP for BOOT protocol (TCP requires VL framing)
- **20-node devnet**: Provides sufficient relay diversity for private route allocation (required for broadcast messaging and MPC routes)

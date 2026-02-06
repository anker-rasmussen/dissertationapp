# Demo Setup Guide

## Running Multiple Market Instances for Demo

This guide explains how to run 3 market app instances simultaneously for your dissertation demo.

### Prerequisites

1. **Devnet must be running** (5 nodes: bootstrap + nodes 1-4):
   ```bash
   cd /home/broadcom/Repos/Dissertation/Repos/veilid/.devcontainer/compose
   docker compose -f docker-compose.dev.yml up -d
   # Wait for all nodes to be healthy
   docker compose -f docker-compose.dev.yml ps
   ```

2. **Verify devnet is healthy** - all 5 nodes should show "healthy" status

### Running 3 Market Instances

Open **3 separate terminals** and run one instance in each:

#### Terminal 1 - Bidder 1 (Node 6)
```bash
cd /home/broadcom/Repos/Dissertation/Repos/dissertationapp/market
./run-demo-instance.sh 5
```
- Port: 5165
- IP: 1.2.3.6
- Data: ~/.local/share/smpc-auction-node-5

#### Terminal 2 - Bidder 2 (Node 7)
```bash
cd /home/broadcom/Repos/Dissertation/Repos/dissertationapp/market
./run-demo-instance.sh 6
```
- Port: 5166
- IP: 1.2.3.7
- Data: ~/.local/share/smpc-auction-node-6

#### Terminal 3 - Auctioneer (Node 8)
```bash
cd /home/broadcom/Repos/Dissertation/Repos/dissertationapp/market
./run-demo-instance.sh 7
```
- Port: 5167
- IP: 1.2.3.8
- Data: ~/.local/share/smpc-auction-node-8

### Manual Run (Alternative)

If you prefer to run manually with more control:

```bash
# Instance 1
MARKET_NODE_OFFSET=5 LD_PRELOAD=/home/broadcom/Repos/Dissertation/Repos/veilid/.devcontainer/scripts/libipspoof.so RUST_LOG=info cargo run

# Instance 2
MARKET_NODE_OFFSET=6 LD_PRELOAD=/home/broadcom/Repos/Dissertation/Repos/veilid/.devcontainer/scripts/libipspoof.so RUST_LOG=info cargo run

# Instance 3
MARKET_NODE_OFFSET=7 LD_PRELOAD=/home/broadcom/Repos/Dissertation/Repos/veilid/.devcontainer/scripts/libipspoof.so RUST_LOG=info cargo run
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
Devnet Nodes (Docker):
├── Bootstrap: 1.2.3.1:5160 (port 5160)
├── Node 1:    1.2.3.2:5161 (port 5161)
├── Node 2:    1.2.3.3:5162 (port 5162)
├── Node 3:    1.2.3.4:5163 (port 5163)
└── Node 4:    1.2.3.5:5164 (port 5164)

Market Instances (Local):
├── Party 0:     1.2.3.6:5165 (port 5165)
├── Party 1:     1.2.3.7:5166 (port 5166)
├── Party 2:     1.2.3.8:5167 (port 5167)
└── Party n:     1.2.3.(6+n):(5165+n) (port 5165+n) - note. cap of 35 nodes (LD_PRELOAD only translates 40 IPs.)
``` 

All nodes use the same network key: `development-network-2025`

### Troubleshooting

1. **"Connecting..." stuck**:
   - Check devnet is running: `docker ps`
   - Verify capabilities match (SGNL, RLAY, DIAL disabled)
   - Look for "no routing domain" errors in devnet logs

2. **Port already in use**:
   - Kill existing instances
   - Check with: `lsof -i :5165` (or 5166, 5167)

3. **Zero peers**:
   - Wait 10-15 seconds for bootstrap to complete
   - Check LD_PRELOAD is set correctly
   - Verify devnet nodes are healthy

4. **Data conflicts**:
   - Each instance uses separate data directory
   - Clear data: `rm -rf ~/.local/share/smpc-auction-node-*`

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
- **Capabilities**: All nodes have TUNL, SGNL, RLAY, DIAL disabled (devnet mode)
- **Bootstrap**: All nodes connect to 1.2.3.1:5160 for initial peer discovery
- **Protocol**: Uses UDP for BOOT protocol (TCP requires VL framing)

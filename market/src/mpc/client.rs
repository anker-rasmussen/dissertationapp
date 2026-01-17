use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::net::SocketAddr;
use tracing::{info, error, debug};
use anyhow::{Result, Context};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

/// MP-SPDZ External IO Client Implementation
pub struct MpcClient {
    client_id: u32,
    party_addrs: Vec<SocketAddr>,
    sockets: Vec<TcpStream>,
}

impl MpcClient {
    pub fn new(client_id: u32, party_addrs: Vec<SocketAddr>) -> Self {
        Self {
            client_id,
            party_addrs,
            sockets: Vec::new(),
        }
    }

    /// Connect to all MPC parties
    pub async fn connect(&mut self) -> Result<()> {
        info!("Connecting to {} MPC parties...", self.party_addrs.len());
        
        for &addr in &self.party_addrs {
            let mut socket = TcpStream::connect(addr).await
                .context(format!("Failed to connect to party at {}", addr))?;
            
            // Handshake Phase 1: Send client ID
            // MP-SPDZ expects a string representation of the client ID (e.g. "0", "1")
            // wrapped in an octetStream (len + bytes).
            let id_str = self.client_id.to_string();
            let id_bytes = id_str.as_bytes();
            
            socket.write_u32_le(id_bytes.len() as u32).await?;
            socket.write_all(id_bytes).await?;
            
            self.sockets.push(socket);
        }
        
        Ok(())
    }

    /// Submit a private input (the bid) to all parties

    /// Submit a private input (the bid) to all parties
    pub async fn submit_input(&mut self, value: u64) -> Result<()> {
        if self.sockets.is_empty() {
            anyhow::bail!("Not connected to MPC parties");
        }

        info!("Submitting private input: {}", value);

        // Phase 3: Receive triples (random shares used to mask input)
        // For each input, we receive 1 share of a random 'a' from each party.
        // We sum them up to get 'a', then send 'x + a' back.
        
        let mut sum_a: i128 = 0;
        // In this simple version, we assume Fp (prime field) and i64 size for shares
        // Real MP-SPDZ uses arbitrary precision integers (bigints)
        
        for i in 0..self.sockets.len() {
            // Receive length of share data
            let len = self.sockets[i].read_u32_le().await?;
            let mut share_data = vec![0u8; len as usize];
            self.sockets[i].read_exact(&mut share_data).await?;
            
            // For 'semi' protocol, it's just the share value
            // Format: [sign byte (0/1)] [4-byte length] [big-endian bytes]
            let share = parse_mp_spdz_bigint(&share_data)?;
            sum_a += share;
        }

        // Mask value: x + a
        // Note: Real implementation needs to handle modular arithmetic with the field prime
        let masked_value = (value as i128) + sum_a;
        
        // Phase 4: Send masked value back to all parties
        let masked_data = encode_mp_spdz_bigint(masked_value);
        for i in 0..self.sockets.len() {
            self.sockets[i].write_u32_le(masked_data.len() as u32).await?;
            self.sockets[i].write_all(&masked_data).await?;
        }

        Ok(())
    }

    /// Receive output from all parties (e.g. winner ID)
    pub async fn receive_output(&mut self) -> Result<u64> {
        // Similar to receive_triples, but we get shares of the result
        let mut sum_out: i128 = 0;
        for i in 0..self.sockets.len() {
            let len = self.sockets[i].read_u32_le().await?;
            let mut out_data = vec![0u8; len as usize];
            self.sockets[i].read_exact(&mut out_data).await?;
            let out_share = parse_mp_spdz_bigint(&out_data)?;
            sum_out += out_share;
        }
        
        Ok(sum_out as u64)
    }
}

/// Parse MP-SPDZ bigint format: [sign] [4-byte len] [big-endian bytes]
fn parse_mp_spdz_bigint(data: &[u8]) -> Result<i128> {
    if data.is_empty() { return Ok(0); }
    let sign = data[0];
    let len = u32::from_le_bytes(data[1..5].try_into()?) as usize;
    let mut val: i128 = 0;
    for i in 0..len {
        val = (val << 8) | (data[5 + i] as i128);
    }
    if sign == 1 { val = -val; }
    Ok(val)
}

/// Encode to MP-SPDZ bigint format
fn encode_mp_spdz_bigint(val: i128) -> Vec<u8> {
    let sign = if val < 0 { 1u8 } else { 0u8 };
    let abs_val = val.abs();
    let mut bytes = Vec::new();
    let mut temp = abs_val;
    while temp > 0 {
        bytes.push((temp & 0xFF) as u8);
        temp >>= 8;
    }
    bytes.reverse();
    if bytes.is_empty() { bytes.push(0); }
    
    let mut res = Vec::new();
    res.push(sign);
    res.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    res.extend_from_slice(&bytes);
    res
}

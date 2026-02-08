use aes_gcm::{
    aead::{Aead, KeyInit, OsRng},
    Aes256Gcm, Nonce,
};
use anyhow::{Context, Result};

/// AES-256-GCM key (32 bytes)
pub type ContentKey = [u8; 32];

/// AES-256-GCM nonce (12 bytes)
pub type ContentNonce = [u8; 12];

/// Generate a random AES-256 key
pub fn generate_key() -> ContentKey {
    Aes256Gcm::generate_key(&mut OsRng).into()
}

/// Encrypt text content with AES-256-GCM
/// Returns (ciphertext, nonce) tuple
/// The nonce must be stored alongside the ciphertext for decryption
pub fn encrypt_content(plaintext: &str, key: &ContentKey) -> Result<(Vec<u8>, ContentNonce)> {
    let cipher = Aes256Gcm::new(key.into());

    // Generate random nonce (12 bytes for GCM)
    let nonce_bytes: ContentNonce = rand::random();
    let nonce = Nonce::from_slice(&nonce_bytes);

    // Encrypt the plaintext
    let ciphertext = cipher
        .encrypt(nonce, plaintext.as_bytes())
        .map_err(|e| anyhow::anyhow!("Encryption failed: {e}"))?;

    Ok((ciphertext, nonce_bytes))
}

/// Decrypt content with AES-256-GCM
/// Returns the plaintext as UTF-8 string
pub fn decrypt_content(
    ciphertext: &[u8],
    key: &ContentKey,
    nonce: &ContentNonce,
) -> Result<String> {
    let cipher = Aes256Gcm::new(key.into());
    let nonce = Nonce::from_slice(nonce);

    // Decrypt the ciphertext
    let plaintext_bytes = cipher
        .decrypt(nonce, ciphertext)
        .map_err(|e| anyhow::anyhow!("Decryption failed: {e}"))?;

    // Convert to UTF-8 string
    String::from_utf8(plaintext_bytes).context("Decrypted content is not valid UTF-8")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encrypt_decrypt_roundtrip() {
        let key = generate_key();
        let plaintext = "Secret auction item details!";

        // Encrypt
        let (ciphertext, nonce) = encrypt_content(plaintext, &key).unwrap();

        // Decrypt
        let decrypted = decrypt_content(&ciphertext, &key, &nonce).unwrap();

        assert_eq!(plaintext, decrypted);
    }

    #[test]
    fn test_wrong_key_fails() {
        let key1 = generate_key();
        let key2 = generate_key();
        let plaintext = "Secret message";

        let (ciphertext, nonce) = encrypt_content(plaintext, &key1).unwrap();

        // Should fail with wrong key
        let result = decrypt_content(&ciphertext, &key2, &nonce);
        assert!(result.is_err());
    }

    #[test]
    fn test_wrong_nonce_fails() {
        let key = generate_key();
        let plaintext = "Secret message";

        let (ciphertext, _) = encrypt_content(plaintext, &key).unwrap();
        let wrong_nonce: ContentNonce = rand::random();

        // Should fail with wrong nonce
        let result = decrypt_content(&ciphertext, &key, &wrong_nonce);
        assert!(result.is_err());
    }
}

use chacha20poly1305::{
    aead::{Aead, KeyInit},
    ChaCha20Poly1305, Key, Nonce,
};
use argon2::{
    password_hash::{SaltString},
    Argon2, PasswordHasher,
};
use rand::{rngs::OsRng, RngCore};
use crate::error::{FuseError, Result};
use crate::types::LOGICAL_BLOCK_SIZE;

/// Size of the ChaCha20 Nonce in bytes.
pub const NONCE_SIZE: usize = 12;

/// Size of the Chacha20-Poly1305 MAC tag.
pub const MAC_SIZE: usize = 16;

/// Derives a 32-byte master key from a password.
/// In a real system, the salt should be stored/loaded. Here we use a hardcoded
/// salt for simplicity, but ideally it should be saved alongside the database.
pub fn derive_key(password: &str, salt_str: &str) -> Result<[u8; 32]> {
    let salt = SaltString::from_b64(salt_str).map_err(|_| FuseError::Crypto("Invalid salt format".into()))?;
    let argon2 = Argon2::default();
    
    let password_hash = argon2
        .hash_password(password.as_bytes(), &salt)
        .map_err(|e| FuseError::Crypto(format!("Key derivation failed: {}", e)))?;
    
    let hash = password_hash.hash.ok_or_else(|| FuseError::Crypto("Failed to get hash".into()))?;
    
    let mut key = [0u8; 32];
    let hash_bytes = hash.as_bytes();
    let len = hash_bytes.len().min(32);
    key[..len].copy_from_slice(&hash_bytes[..len]);
    
    Ok(key)
}

/// Encrypts a block of plaintext (max size: 16777188 bytes) to a 16MB ciphertext.
pub fn encrypt_block(key: &[u8; 32], plaintext: &[u8]) -> Result<Vec<u8>> {
    if plaintext.len() > LOGICAL_BLOCK_SIZE {
        return Err(FuseError::Crypto(format!(
            "Plaintext size {} exceeds logical block size {}",
            plaintext.len(),
            LOGICAL_BLOCK_SIZE
        )));
    }

    let cipher = ChaCha20Poly1305::new(Key::from_slice(key));
    let mut nonce_bytes = [0u8; NONCE_SIZE];
    OsRng.fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);

    let ciphertext = cipher.encrypt(nonce, plaintext)
        .map_err(|e| FuseError::Crypto(format!("Encryption failed: {}", e)))?;

    let mut result = Vec::with_capacity(NONCE_SIZE + ciphertext.len());
    result.extend_from_slice(&nonce_bytes);
    result.extend_from_slice(&ciphertext);

    Ok(result)
}

/// Decrypts a ciphertext block, separating the nonce and verifying the MAC.
pub fn decrypt_block(key: &[u8; 32], ciphertext: &[u8]) -> Result<Vec<u8>> {
    if ciphertext.len() < NONCE_SIZE + MAC_SIZE {
        return Err(FuseError::Crypto("Ciphertext too short".into()));
    }

    let nonce_bytes = &ciphertext[..NONCE_SIZE];
    let actual_ciphertext = &ciphertext[NONCE_SIZE..];

    let cipher = ChaCha20Poly1305::new(Key::from_slice(key));
    let nonce = Nonce::from_slice(nonce_bytes);

    let plaintext = cipher.decrypt(nonce, actual_ciphertext)
        .map_err(|e| FuseError::Crypto(format!("Decryption failed: {}", e)))?;

    Ok(plaintext)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{BlockId, REMOTE_BLOCK_SIZE};

    #[test]
    fn test_encryption_size() {
        let key = [0x42; 32];
        let mut plaintext = vec![0u8; LOGICAL_BLOCK_SIZE];
        OsRng.fill_bytes(&mut plaintext);

        let ciphertext = encrypt_block(&key, &plaintext).unwrap();
        // Ciphertext should be exactly 16777216 bytes
        assert_eq!(ciphertext.len(), REMOTE_BLOCK_SIZE);

        let decrypted = decrypt_block(&key, &ciphertext).unwrap();
        assert_eq!(plaintext, decrypted);
    }

    #[test]
    fn test_block_math() {
        // Test exactly at the boundary of a block
        let offset = LOGICAL_BLOCK_SIZE as u64;
        let (block_id, internal_offset) = BlockId::from_offset(1, offset);
        assert_eq!(block_id.ino, 1);
        assert_eq!(block_id.index, 1);
        assert_eq!(internal_offset, 0);

        let offset = LOGICAL_BLOCK_SIZE as u64 - 1;
        let (block_id, internal_offset) = BlockId::from_offset(1, offset);
        assert_eq!(block_id.ino, 1);
        assert_eq!(block_id.index, 0);
        assert_eq!(internal_offset, LOGICAL_BLOCK_SIZE - 1);
    }
}

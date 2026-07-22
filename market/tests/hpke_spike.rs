//! Phase 5c HPKE design spike (investigate-only, see ROADMAP.md).
//!
//! Proves the 0.5.7 VLD0 signing→KEM bridge round-trips with the market's
//! ed25519-dalek app-level keys (the ones in `BidRecord.signing_pubkey`),
//! i.e. a seller could `hpke_seal` the AES-256-GCM content key to the
//! winner's *existing* published identity with no extra key distribution.
//!
//! Not part of any suite; run manually:
//! `cargo test --test hpke_spike -- --ignored`

use veilid_core::bytes::Bytes;
use veilid_core::{
    api_startup, BareKeyPair, BarePublicKey, BareSecretKey, KeyPair, PublicKey, SecretKey,
    VeilidConfig, VeilidConfigProtectedStore, VeilidConfigTableStore, CRYPTO_KIND_VLD0,
};

#[tokio::test]
#[ignore = "design spike: proves the API bridge, not part of any suite"]
async fn hpke_bridge_roundtrips_with_ed25519_dalek_keys() {
    // Isolated stores; crypto needs startup but not attach.
    let dir = std::env::temp_dir().join(format!("hpke-spike-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    let config = VeilidConfig {
        program_name: "market".into(),
        namespace: "hpke-spike".into(),
        protected_store: VeilidConfigProtectedStore {
            always_use_insecure_storage: true,
            directory: dir.join("protected").to_string_lossy().into_owned(),
            ..Default::default()
        },
        table_store: VeilidConfigTableStore {
            directory: dir.join("table").to_string_lossy().into_owned(),
            ..Default::default()
        },
        ..Default::default()
    };

    let api = api_startup(std::sync::Arc::new(|_| {}), config)
        .await
        .expect("veilid api startup");

    // The winner's identity exactly as the market creates it: an
    // ed25519-dalek signing key whose verifying-key bytes are published
    // in BidRecord.signing_pubkey.
    let winner_signing = ed25519_dalek::SigningKey::from_bytes(&rand::random::<[u8; 32]>());
    let winner_pub_bytes = winner_signing.verifying_key().to_bytes();

    let crypto = api.crypto().expect("crypto");
    let vld0 = crypto.get_async(CRYPTO_KIND_VLD0).expect("vld0 system");

    // Seller side: derive the KEM encapsulation key from the winner's
    // published signing pubkey and seal the content key to it.
    let winner_public = PublicKey::new(CRYPTO_KIND_VLD0, BarePublicKey::new(&winner_pub_bytes));
    let encap_key = vld0
        .encapsulation_key_from_signing_key(&winner_public)
        .await
        .expect("ed25519→x25519 encapsulation bridge");

    let content_key: [u8; 32] = rand::random();
    let aad = Bytes::from_static(b"listing:spike-test");
    let sealed = vld0
        .hpke_seal(
            &encap_key,
            aad.clone(),
            Bytes::copy_from_slice(&content_key),
        )
        .await
        .expect("hpke_seal");

    // Winner side: derive the decapsulation key from its signing secret
    // and open the sealed blob.
    let winner_secret = SecretKey::new(
        CRYPTO_KIND_VLD0,
        BareSecretKey::new(&winner_signing.to_bytes()),
    );
    let decap_key = vld0
        .decapsulation_key_from_signing_secret(&winner_secret)
        .await
        .expect("ed25519→x25519 decapsulation bridge");

    let opened = vld0
        .hpke_open(&decap_key, aad.clone(), sealed.clone())
        .await
        .expect("hpke_open");
    assert_eq!(&opened[..], &content_key[..], "content key round-trips");

    // Only the recipient can open: a different identity must fail.
    let other_signing = ed25519_dalek::SigningKey::from_bytes(&rand::random::<[u8; 32]>());
    let other_secret = SecretKey::new(
        CRYPTO_KIND_VLD0,
        BareSecretKey::new(&other_signing.to_bytes()),
    );
    let other_decap = vld0
        .decapsulation_key_from_signing_secret(&other_secret)
        .await
        .expect("bridge for non-recipient");
    assert!(
        vld0.hpke_open(&other_decap, aad.clone(), sealed.clone())
            .await
            .is_err(),
        "non-recipient must not open the sealed content key"
    );

    // Mismatched AAD must fail (binds the seal to the listing).
    assert!(
        vld0.hpke_open(&decap_key, Bytes::from_static(b"listing:other"), sealed)
            .await
            .is_err(),
        "mismatched AAD must not open"
    );

    // Sanity: the bridged keypair also verifies as a KEM pair.
    let _ = KeyPair::new(
        CRYPTO_KIND_VLD0,
        BareKeyPair::new(
            BarePublicKey::new(&winner_pub_bytes),
            BareSecretKey::new(&winner_signing.to_bytes()),
        ),
    );

    api.shutdown().await;
    std::fs::remove_dir_all(&dir).ok();
}

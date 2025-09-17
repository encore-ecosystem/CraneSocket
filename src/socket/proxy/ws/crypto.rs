use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use log::debug;
use snow::{Builder, TransportState};
use tokio::{net::TcpStream, time::timeout};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::{
    server::{
        constant::READ_TIMEOUT_MS,
        message::{ClientMessage, ServerMessage, Tags},
    },
    socket::crypto::{
        CryptoError,
        constant::{ALGO, MAX_BUFFER_SIZE},
    },
};

pub async fn establish_encryption(
    stream: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    is_initiator: bool,
) -> Result<TransportState, CryptoError> {
    let mut buffer = vec![0u8; MAX_BUFFER_SIZE];
    let timeout_duration = Duration::from_secs(READ_TIMEOUT_MS);
    match is_initiator {
        true => {
            let keypair = Builder::new(ALGO.parse().map_err(|_| CryptoError::FailedHandShake)?)
                .generate_keypair()
                .map_err(|_| CryptoError::InvalidState)?;
            let mut noise = Builder::new(ALGO.parse().map_err(|_| CryptoError::FailedHandShake)?)
                .local_private_key(&keypair.private)
                .unwrap()
                .build_initiator()
                .map_err(|_| CryptoError::FailedHandShake)?;

            // -> e
            let len = noise
                .write_message(&[], &mut buffer[..])
                .map_err(|_| CryptoError::Write)?;
            stream
                .send(ClientMessage::binary(buffer[..len].to_vec()).into())
                .await
                .map_err(|_| CryptoError::Send)?;
            debug!("Sent 1st msg");

            // <- e, ee, s, es
            let msg = timeout(timeout_duration, stream.next())
                .await
                .map_err(|_| CryptoError::Timeout)?
                .ok_or(CryptoError::FailedHandShake)?;
            let msg: ServerMessage = msg.map_err(|_| CryptoError::InvalidMessageType)?.into();
            if let ServerMessage::Binary(data) = msg {
                noise
                    .read_message(&data, &mut buffer[..])
                    .map_err(|_| CryptoError::Read)?;
            } else {
                return Err(CryptoError::InvalidMessageType);
            }
            debug!("Received 2nd msg");

            // -> s, se
            let len = noise
                .write_message(&[], &mut buffer[..])
                .map_err(|_| CryptoError::Write)?;
            stream
                .send(ClientMessage::binary(buffer[..len].to_vec()).into())
                .await
                .map_err(|_| CryptoError::Send)?;
            debug!("Sent 3rd msg");

            let noise = noise
                .into_transport_mode()
                .map_err(|_| CryptoError::FailedHandShake)?;

            Ok(noise)
        }
        false => {
            let keypair = Builder::new(ALGO.parse().map_err(|_| CryptoError::FailedHandShake)?)
                .generate_keypair()
                .map_err(|_| CryptoError::InvalidState)?;
            let mut noise = Builder::new(ALGO.parse().map_err(|_| CryptoError::FailedHandShake)?)
                .local_private_key(&keypair.private)
                .unwrap()
                .build_responder()
                .map_err(|_| CryptoError::FailedHandShake)?;

            // -> e
            let msg = timeout(timeout_duration, stream.next())
                .await
                .map_err(|_| CryptoError::Timeout)?
                .ok_or(CryptoError::FailedHandShake)?;
            let msg: ServerMessage = msg.map_err(|_| CryptoError::InvalidMessageType)?.into();
            if let ServerMessage::Binary(data) = msg {
                noise
                    .read_message(&data, &mut buffer[..])
                    .map_err(|_| CryptoError::Read)?;
            } else {
                return Err(CryptoError::InvalidMessageType);
            }
            debug!("Received 1st msg");

            // <- e, ee, s, es
            let len = noise
                .write_message(&[], &mut buffer[..])
                .map_err(|_| CryptoError::Write)?;
            stream
                .send(ClientMessage::binary(buffer[..len].to_vec()).into())
                .await
                .map_err(|_| CryptoError::Send)?;
            debug!("Sent 2nd msg");

            // -> s, se
            let msg = timeout(timeout_duration, stream.next())
                .await
                .map_err(|_| CryptoError::Timeout)?
                .ok_or(CryptoError::FailedHandShake)?;
            let msg: ServerMessage = msg.map_err(|_| CryptoError::InvalidMessageType)?.into();
            if let ServerMessage::Binary(data) = msg {
                noise
                    .read_message(&data, &mut buffer[..])
                    .map_err(|_| CryptoError::Read)?;
            } else {
                return Err(CryptoError::InvalidMessageType);
            }
            debug!("Received 3rd msg");

            let noise = noise
                .into_transport_mode()
                .map_err(|_| CryptoError::FailedHandShake)?;

            Ok(noise)
        }
    }
}

pub fn encrypt_message(
    msg: ClientMessage,
    noise: &mut TransportState,
    buffer: &mut [u8],
) -> Result<ClientMessage, CryptoError> {
    match msg {
        ClientMessage::Text(text) => {
            let mut data = vec![Tags::Text as u8];
            data.extend_from_slice(text.as_bytes());
            let len = noise
                .write_message(&data, &mut buffer[..])
                .map_err(|_| CryptoError::Write)?;
            Ok(ClientMessage::binary(buffer[..len].to_vec()))
        }
        ClientMessage::Binary(raw_data) => {
            let mut data = vec![Tags::Binary as u8];
            data.extend_from_slice(&raw_data);
            let len = noise
                .write_message(&data, &mut buffer[..])
                .map_err(|_| CryptoError::Write)?;
            Ok(ClientMessage::binary(buffer[..len].to_vec()))
        }
        _ => Ok(msg),
    }
}

pub fn decrypt_message(
    msg: ServerMessage,
    noise: &mut TransportState,
    buffer: &mut [u8],
) -> Result<ServerMessage, CryptoError> {
    let msg = match msg {
        ServerMessage::Binary(data) => {
            let len = noise
                .read_message(&data, &mut buffer[..])
                .map_err(|_| CryptoError::Read)?;
            let decrypted = &buffer[..len];
            parse_decrypted_message(decrypted)?
        }
        _ => msg,
    };

    Ok(msg)
}

fn parse_decrypted_message(decrypted: &[u8]) -> Result<ServerMessage, CryptoError> {
    if decrypted.is_empty() {
        return Err(CryptoError::Decrypt);
    }
    match decrypted[0] {
        x if x == Tags::Text as u8 => Ok(ServerMessage::text(
            String::from_utf8(decrypted[1..].to_vec()).map_err(|_| CryptoError::Decrypt)?,
        )),
        x if x == Tags::Binary as u8 => Ok(ServerMessage::binary(decrypted[1..].to_vec())),
        _ => Err(CryptoError::Decrypt),
    }
}

#[allow(dead_code, unused_imports)]
mod tests {
    use std::io::Bytes;

    use tokio_tungstenite::tungstenite::Message;

    use crate::server::message::{ClientMessage, ServerMessage};
    use crate::socket::crypto::constant::{ALGO, MAX_BUFFER_SIZE};

    use super::*;

    fn setup_test_states() -> (TransportState, TransportState) {
        let mut initiator = snow::Builder::new(ALGO.parse().unwrap())
            .local_private_key(&[0u8; 32])
            .unwrap()
            .build_initiator()
            .unwrap();
        let mut responder = snow::Builder::new(ALGO.parse().unwrap())
            .local_private_key(&[1u8; 32])
            .unwrap()
            .build_responder()
            .unwrap();

        // Perform handshake
        let mut init_buffer = [0u8; 1024];
        let mut resp_buffer = [0u8; 1024];

        // -> e
        let len = initiator.write_message(&[], &mut init_buffer).unwrap();
        responder
            .read_message(&init_buffer[..len], &mut [])
            .unwrap();

        // <- e, ee, s, es
        let len = responder.write_message(&[], &mut resp_buffer).unwrap();
        initiator
            .read_message(&resp_buffer[..len], &mut [])
            .unwrap();

        // -> s, se
        let len = initiator.write_message(&[], &mut init_buffer).unwrap();
        responder
            .read_message(&init_buffer[..len], &mut [])
            .unwrap();

        (
            initiator.into_transport_mode().unwrap(),
            responder.into_transport_mode().unwrap(),
        )
    }

    #[test]
    fn test_encrypt_text_message() {
        let (mut noise_init, _) = setup_test_states();
        let mut buffer = vec![0u8; MAX_BUFFER_SIZE];
        let text = "Hello, World!";
        let message = ClientMessage::text(text.to_string());

        let encrypted = encrypt_message(message, &mut noise_init, &mut buffer).unwrap();
        assert!(matches!(encrypted, ClientMessage::Binary(_)));
        if let Message::Binary(data) = encrypted.into() {
            assert!(!data.is_empty());
        }
    }

    #[test]
    fn test_encrypt_binary_message() {
        let (mut noise_init, _) = setup_test_states();
        let mut buffer = vec![0u8; MAX_BUFFER_SIZE];
        let data = vec![1, 2, 3, 4, 5];
        let message = ClientMessage::binary(data);

        let encrypted = encrypt_message(message, &mut noise_init, &mut buffer).unwrap();
        assert!(matches!(encrypted, ClientMessage::Binary(_)));
        if let Message::Binary(data) = encrypted.into() {
            assert!(!data.is_empty());
        }
    }

    #[test]
    fn test_decrypt_invalid_message() {
        let (mut noise_init, _) = setup_test_states();
        let mut buffer = vec![0u8; MAX_BUFFER_SIZE];
        let invalid_message = ServerMessage::text("Invalid message".to_string());

        let result = decrypt_message(invalid_message, &mut noise_init, &mut buffer);
        assert!(matches!(result.unwrap(), ServerMessage::Text(_)));
    }

    #[test]
    fn test_decrypt_empty_message() {
        let (mut noise_init, _) = setup_test_states();
        let mut buffer = vec![0u8; MAX_BUFFER_SIZE];
        let empty_message = ServerMessage::binary(vec![]);

        let result = decrypt_message(empty_message, &mut noise_init, &mut buffer);
        assert!(result.is_err());
    }

    #[test]
    fn test_round_trip_text_message() {
        let (mut noise_init, mut noise_resp) = setup_test_states();
        let mut buffer_encrypt = vec![0u8; MAX_BUFFER_SIZE];
        let mut buffer_decrypt = vec![0u8; MAX_BUFFER_SIZE];

        let text = "Hello, World!";
        let original = ClientMessage::text(text.to_string());

        let encrypted =
            encrypt_message(original.clone(), &mut noise_init, &mut buffer_encrypt).unwrap();

        let msg = Message::from(encrypted);

        let decrypted = decrypt_message(
            ServerMessage::from(msg),
            &mut noise_resp,
            &mut buffer_decrypt,
        )
        .unwrap();

        assert!(matches!(decrypted, ServerMessage::Text(_)));
        if let ServerMessage::Text(decrypted_text) = decrypted {
            assert_eq!(decrypted_text, text);
        }
    }

    #[test]
    fn test_round_trip_binary_message() {
        let (mut noise_init, mut noise_resp) = setup_test_states();
        let mut buffer_encrypt = vec![0u8; MAX_BUFFER_SIZE];
        let mut buffer_decrypt = vec![0u8; MAX_BUFFER_SIZE];

        let data = vec![1, 2, 3, 4, 5];
        let original = ClientMessage::binary(data.clone());

        let encrypted =
            encrypt_message(original.clone(), &mut noise_init, &mut buffer_encrypt).unwrap();

        let msg = Message::from(encrypted);

        let decrypted = decrypt_message(
            ServerMessage::from(msg),
            &mut noise_resp,
            &mut buffer_decrypt,
        )
        .unwrap();

        assert!(matches!(decrypted, ServerMessage::Binary(_)));
        if let ServerMessage::Binary(decrypted_data) = decrypted {
            assert_eq!(decrypted_data, data);
        }
    }

    #[test]
    fn test_message_size_limits() {
        let (mut noise_init, _) = setup_test_states();
        let mut buffer = vec![0u8; MAX_BUFFER_SIZE];

        // Test with message approaching MAX_BUFFER_SIZE
        let large_data = vec![0u8; MAX_BUFFER_SIZE - 100]; // Leave some room for encryption overhead
        let message = ClientMessage::binary(large_data);

        let result = encrypt_message(message, &mut noise_init, &mut buffer);
        assert!(result.is_ok());
    }

    #[test]
    fn test_decrypt_non_binary_messages() {
        let (mut noise_init, _) = setup_test_states();
        let mut buffer = vec![0u8; MAX_BUFFER_SIZE];

        // Test text message pass through
        let text_msg = ServerMessage::text("Hello".to_string());
        let result = decrypt_message(text_msg.clone(), &mut noise_init, &mut buffer).unwrap();
        assert_eq!(text_msg, result);
    }
}

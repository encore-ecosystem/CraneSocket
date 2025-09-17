use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use log::debug;
use snow::{Builder, TransportState};
use tokio::{net::TcpStream, time::timeout};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream,
    tungstenite::{
        Bytes, Message,
        protocol::frame::{Frame, FrameHeader},
    },
};

use crate::{
    server::constant::READ_TIMEOUT_MS,
    socket::crypto::{
        CryptoError,
        constant::{ALGO, MAX_BUFFER_SIZE},
        utils::Tags,
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
                .send(Message::binary(buffer[..len].to_vec()))
                .await
                .map_err(|_| CryptoError::Send)?;
            debug!("Sent 1st msg");

            // <- e, ee, s, es
            let msg = timeout(timeout_duration, stream.next())
                .await
                .map_err(|_| CryptoError::Timeout)?
                .ok_or(CryptoError::FailedHandShake)?;
            if let Ok(Message::Binary(data)) = msg {
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
                .send(Message::binary(buffer[..len].to_vec()))
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
            if let Ok(Message::Binary(data)) = msg {
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
                .send(Message::binary(buffer[..len].to_vec()))
                .await
                .map_err(|_| CryptoError::Send)?;
            debug!("Sent 2nd msg");

            // -> s, se
            let msg = timeout(timeout_duration, stream.next())
                .await
                .map_err(|_| CryptoError::Timeout)?
                .ok_or(CryptoError::FailedHandShake)?;
            if let Ok(Message::Binary(data)) = msg {
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
    msg: Message,
    noise: &mut TransportState,
    buffer: &mut [u8],
) -> Result<Message, CryptoError> {
    let data = match msg {
        Message::Text(text) => {
            let mut data = vec![Tags::Text as u8];
            data.extend_from_slice(text.as_bytes());
            data
        }
        Message::Binary(data) => {
            let mut new_data = vec![Tags::Binary as u8];
            new_data.extend_from_slice(&data);
            new_data
        }
        Message::Ping(data) => {
            let mut new_data = vec![Tags::Ping as u8];
            new_data.extend_from_slice(&data);
            new_data
        }
        Message::Pong(data) => {
            let mut new_data = vec![Tags::Pong as u8];
            new_data.extend_from_slice(&data);
            new_data
        }
        Message::Close(_) => {
            let data = vec![Tags::Close as u8];
            data
        }
        Message::Frame(_) => {
            let data = vec![Tags::Frame as u8];
            data
        }
    };

    let len = noise
        .write_message(&data, &mut buffer[..])
        .map_err(|_| CryptoError::Write)?;

    Ok(Message::binary(buffer[..len].to_vec()))
}

pub fn decrypt_message(
    msg: Message,
    noise: &mut TransportState,
    buffer: &mut [u8],
) -> Result<Message, CryptoError> {
    match msg {
        Message::Binary(data) => {
            let len = noise
                .read_message(&data, &mut buffer[..])
                .map_err(|_| CryptoError::Read)?;
            let decrypted = &buffer[..len];
            parse_decrypted_message(decrypted)
        }
        _ => Err(CryptoError::Decrypt),
    }
}

fn parse_decrypted_message(decrypted: &[u8]) -> Result<Message, CryptoError> {
    if decrypted.is_empty() {
        return Err(CryptoError::Decrypt);
    }
    match decrypted[0] {
        x if x == Tags::Text as u8 => Ok(Message::text(
            String::from_utf8(decrypted[1..].to_vec()).map_err(|_| CryptoError::Decrypt)?,
        )),
        x if x == Tags::Binary as u8 => Ok(Message::binary(decrypted[1..].to_vec())),
        x if x == Tags::Ping as u8 => Ok(Message::Ping(decrypted[1..].to_vec().into())),
        x if x == Tags::Pong as u8 => Ok(Message::Pong(decrypted[1..].to_vec().into())),
        x if x == Tags::Close as u8 => Ok(Message::Close(None)),
        x if x == Tags::Frame as u8 => Ok(Message::Frame(Frame::from_payload(
            FrameHeader::default(),
            Bytes::new(),
        ))),
        _ => Err(CryptoError::Decrypt),
    }
}

#[allow(dead_code, unused_imports)]
mod tests {
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
        let message = Message::text(text.to_string());

        let encrypted = encrypt_message(message, &mut noise_init, &mut buffer).unwrap();

        assert!(matches!(encrypted, Message::Binary(_)));
        if let Message::Binary(data) = encrypted {
            assert!(!data.is_empty());
        }
    }

    #[test]
    fn test_encrypt_binary_message() {
        let (mut noise_init, _) = setup_test_states();
        let mut buffer = vec![0u8; MAX_BUFFER_SIZE];
        let data = vec![1, 2, 3, 4, 5];
        let message = Message::binary(data);

        let encrypted = encrypt_message(message, &mut noise_init, &mut buffer).unwrap();

        assert!(matches!(encrypted, Message::Binary(_)));
        if let Message::Binary(data) = encrypted {
            assert!(!data.is_empty());
        }
    }

    #[test]
    fn test_encrypt_ping_message() {
        let (mut noise_init, _) = setup_test_states();
        let mut buffer = vec![0u8; MAX_BUFFER_SIZE];
        let data = vec![1, 2, 3];
        let message = Message::Ping(data.into());

        let encrypted = encrypt_message(message, &mut noise_init, &mut buffer).unwrap();

        assert!(matches!(encrypted, Message::Binary(_)));
        if let Message::Binary(data) = encrypted {
            assert!(!data.is_empty());
        }
    }

    #[test]
    fn test_encrypt_pong_message() {
        let (mut noise_init, _) = setup_test_states();
        let mut buffer = vec![0u8; MAX_BUFFER_SIZE];
        let data = vec![1, 2, 3];
        let message = Message::Pong(data.into());

        let encrypted = encrypt_message(message, &mut noise_init, &mut buffer).unwrap();

        assert!(matches!(encrypted, Message::Binary(_)));
        if let Message::Binary(data) = encrypted {
            assert!(!data.is_empty());
        }
    }

    #[test]
    fn test_encrypt_close_message() {
        let (mut noise_init, _) = setup_test_states();
        let mut buffer = vec![0u8; MAX_BUFFER_SIZE];
        let message = Message::Close(None);

        let encrypted = encrypt_message(message, &mut noise_init, &mut buffer).unwrap();

        assert!(matches!(encrypted, Message::Binary(_)));
        if let Message::Binary(data) = encrypted {
            assert!(!data.is_empty());
        }
    }

    #[test]
    fn test_encrypt_frame_message() {
        let (mut noise_init, _) = setup_test_states();
        let mut buffer = vec![0u8; MAX_BUFFER_SIZE];
        let message = Message::Frame(Frame::from_payload(FrameHeader::default(), Bytes::new()));

        let encrypted = encrypt_message(message, &mut noise_init, &mut buffer).unwrap();

        assert!(matches!(encrypted, Message::Binary(_)));
        if let Message::Binary(data) = encrypted {
            assert!(!data.is_empty());
        }
    }

    #[test]
    fn test_round_trip_text_message() {
        let (mut noise_init, mut noise_resp) = setup_test_states();
        let mut buffer_encrypt = vec![0u8; MAX_BUFFER_SIZE];
        let mut buffer_decrypt = vec![0u8; MAX_BUFFER_SIZE];

        let text = "Hello, World!";
        let original = Message::text(text.to_string());

        let encrypted =
            encrypt_message(original.clone(), &mut noise_init, &mut buffer_encrypt).unwrap();
        let decrypted = decrypt_message(encrypted, &mut noise_resp, &mut buffer_decrypt).unwrap();

        assert!(matches!(decrypted, Message::Text(_)));
        if let Message::Text(decrypted_text) = decrypted {
            assert_eq!(decrypted_text, text);
        }
    }

    #[test]
    fn test_decrypt_invalid_message() {
        let (mut noise_init, _) = setup_test_states();
        let mut buffer = vec![0u8; MAX_BUFFER_SIZE];
        let invalid_message = Message::text("Invalid message".to_string());

        let result = decrypt_message(invalid_message, &mut noise_init, &mut buffer);
        assert!(result.is_err());
    }

    #[test]
    fn test_decrypt_empty_message() {
        let (mut noise_init, _) = setup_test_states();
        let mut buffer = vec![0u8; MAX_BUFFER_SIZE];
        let empty_message = Message::binary(vec![]);

        let result = decrypt_message(empty_message, &mut noise_init, &mut buffer);
        assert!(result.is_err());
    }

    #[test]
    fn test_decrypt_message_with_invalid_tag() {
        let (mut noise_init, _) = setup_test_states();
        let mut buffer = vec![0u8; MAX_BUFFER_SIZE];
        let invalid_tag = 255u8; // Invalid tag value
        let message = Message::binary(vec![invalid_tag]);

        let result = decrypt_message(message, &mut noise_init, &mut buffer);
        assert!(result.is_err());
    }

    #[test]
    fn test_round_trip_all_message_types() {
        let (mut noise_init, mut noise_resp) = setup_test_states();
        let mut buffer_encrypt = vec![0u8; MAX_BUFFER_SIZE];
        let mut buffer_decrypt = vec![0u8; MAX_BUFFER_SIZE];

        let test_cases = vec![
            Message::text("Hello".to_string()),
            Message::binary(vec![1, 2, 3]),
            Message::Ping(vec![1].into()),
            Message::Pong(vec![2].into()),
            Message::Close(None),
            Message::Frame(Frame::from_payload(FrameHeader::default(), Bytes::new())),
        ];

        for original in test_cases {
            let encrypted =
                encrypt_message(original.clone(), &mut noise_init, &mut buffer_encrypt).unwrap();
            let decrypted =
                decrypt_message(encrypted, &mut noise_resp, &mut buffer_decrypt).unwrap();

            match (original, decrypted) {
                (Message::Text(t1), Message::Text(t2)) => assert_eq!(t1, t2),
                (Message::Binary(b1), Message::Binary(b2)) => assert_eq!(b1, b2),
                (Message::Ping(p1), Message::Ping(p2)) => assert_eq!(p1, p2),
                (Message::Pong(p1), Message::Pong(p2)) => assert_eq!(p1, p2),
                (Message::Close(_), Message::Close(_)) => (),
                (Message::Frame(_), Message::Frame(_)) => (),
                _ => panic!("Message types don't match after round trip"),
            }
        }
    }
}

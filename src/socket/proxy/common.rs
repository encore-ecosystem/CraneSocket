use crate::server::message::Tags;

pub fn is_tag_only_message(tag: u8) -> bool {
    matches!(
        tag,
        x if x == Tags::CreateRoom as u8
            || x == Tags::LeaveRoom as u8
            || x == Tags::JoinedSuccessfully as u8
            || x == Tags::ClientJoined as u8
            || x == Tags::ClientLeft as u8
            || x == Tags::ServerClosed as u8
            || x == Tags::Close as u8
            || x == Tags::Frame as u8
    )
}

pub fn validate_tag_with_payload(tag: u8) -> Result<(), String> {
    if !matches!(
        tag,
        x if x == Tags::Text as u8
            || x == Tags::Binary as u8
            || x == Tags::Ping as u8
            || x == Tags::Pong as u8
            || x == Tags::RoomCreated as u8
            || x == Tags::JoinRoom as u8
            || x == Tags::Error as u8
    ) {
        return Err("Received a message with an unexpected tag".into());
    }
    Ok(())
}

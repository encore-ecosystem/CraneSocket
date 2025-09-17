use crate::server::Rooms;
use crate::server::message::ServerMessage;

pub async fn close_server(rooms: Rooms) {
    let rooms_lock = rooms.read().await;
    for room in rooms_lock.values() {
        for (client_addr, sender) in room.iter() {
            sender
                .send((ServerMessage::ServerClosed, *client_addr))
                .await
                .unwrap()
        }
    }
}

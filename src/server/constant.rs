// timeouts
pub const SERVER_CONNECTION_TIMEOUT_SEC: u64 = 10;
pub const CLIENT_CONNECTION_WAIT_TIMEOUT_SEC: u64 = 120;
pub const READ_TIMEOUT_MS: u64 = 1000;

// limits
pub const MAX_ERRORS_ALLOWED: usize = 1;
pub const MAX_MESSAGE_SIZE: usize = 1024 * 512; // 512KB
pub const MAX_NUM_CLIENTS_IN_ROOM: usize = 2;
pub const MAX_NUM_ROOMS: usize = 10;
pub const MPSC_CHANNEL_CAPACITY: usize = 100;
pub const SHUTDOWN_CHANNEL_CAPACITY: usize = 1;

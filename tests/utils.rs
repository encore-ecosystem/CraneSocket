pub const TEST_SLEEP_TIME_MS: u64 = 100;
pub const TEST_TIMEOUT_TIME_MS: u64 = 2000;
pub const MPSC_CHANNEL_CAPACITY: usize = 10;
pub const FILE_SIZE: usize = 10 * 1024 * 1024; // 10 MB
pub const CHUNK_SIZE: usize = 16 * 1024; // 8 KB

pub async fn timed<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(
        tokio::time::Duration::from_millis(TEST_TIMEOUT_TIME_MS),
        future,
    )
    .await
    .expect("Timed out")
}

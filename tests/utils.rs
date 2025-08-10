pub const TEST_SLEEP_TIME_MS: u64 = 100;
pub const TEST_TIMEOUT_TIME_MS: u64 = 1000;

pub async fn timed<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(
        tokio::time::Duration::from_millis(TEST_TIMEOUT_TIME_MS),
        future,
    )
    .await
    .expect("Timed out")
}

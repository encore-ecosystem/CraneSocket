use crate::utils::TEST_TIMEOUT_TIME_MS;

pub async fn timed<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(
        tokio::time::Duration::from_millis(TEST_TIMEOUT_TIME_MS),
        future,
    )
    .await
    .expect("Timed out")
}

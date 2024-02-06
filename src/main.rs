#[tokio::main]
async fn main() {
    broadcast_rs::start_server()
        .await
        .expect("Broadcast server failed");
}

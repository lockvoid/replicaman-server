//! Development process lifecycle, separate from the synchronization protocol.
//! A supervisor holds stdin open when REPLICAMAN_SUPERVISED=1. Reading it
//! on a detached OS thread avoids keeping Tokio's blocking pool alive on TERM.
pub async fn signal() {
    let interrupt = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut stream) => {
                stream.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    let lifeline = async {
        if std::env::var("REPLICAMAN_SUPERVISED").as_deref() != Ok("1") {
            return std::future::pending::<()>().await;
        }
        let (sender, receiver) = tokio::sync::oneshot::channel();
        std::thread::spawn(move || {
            use std::io::{ErrorKind, Read};
            let mut buffer = [0; 64];
            loop {
                match std::io::stdin().read(&mut buffer) {
                    Ok(0) => break,
                    Ok(_) => (),
                    Err(error) if error.kind() == ErrorKind::Interrupted => (),
                    Err(_) => break,
                }
            }
            let _ = sender.send(());
        });
        let _ = receiver.await;
    };
    tokio::select! {
        _ = interrupt => (),
        _ = terminate => (),
        _ = lifeline => (),
    }
}

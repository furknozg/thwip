use proxy_common::{read_config, AsyncRuntimeConfig, Config};
use std::{
    fs,
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config_path = Path::new("rginx.toml");

    let config = if config_path.exists() {
        println!("Ayar dosyası bulundu, yükleniyor...");
        read_config(config_path)?
    } else {
        println!("Ayar dosyası bulunamadı. Varsayılan config.toml oluşturuluyor.");
        let default_config = Config::default();
        let toml_string = default_config.to_toml()?;
        fs::write(config_path, toml_string)?;
        default_config
    };

    // Eğer config'de verilmişse onu al, verilmemişse sistem CPU sayısını kullan
    let total_workers = config.worker_count;
    for server in &config.http.servers {
        println!(
            "Proxy başlatılıyor: {} (server_name: {})",
            server.listen,
            server.server_name.as_deref().unwrap_or("_")
        );
    }
    println!("Toplam Worker Süreç Sayısı: {}", total_workers);
    match &config.runtime {
        AsyncRuntimeConfig::Auto { .. } => {
            println!("Runtime: auto (each worker will report the selected backend)");
        }
        AsyncRuntimeConfig::Epoll { max_events } => {
            println!("Runtime: epoll (max events: {})", max_events);
        }
        AsyncRuntimeConfig::Kqueue { max_events } => {
            println!("Runtime: kqueue (max events: {})", max_events);
        }
        AsyncRuntimeConfig::IoUring {
            sq_entries,
            cq_entries,
            ..
        } => {
            println!(
                "Runtime: io_uring (SQ depth: {}, CQ depth: {})",
                sq_entries, cq_entries
            );
        }
    }

    let shutdown_requested = Arc::new(AtomicBool::new(false));
    for signal in [
        signal_hook::consts::signal::SIGINT,
        signal_hook::consts::signal::SIGTERM,
    ] {
        signal_hook::flag::register(signal, Arc::clone(&shutdown_requested))?;
    }

    //  (Fork) Yönetimi
    let mut workers = Vec::with_capacity(total_workers);
    for cpu_id in 0..total_workers {
        // config'i child process'e güvenli şekilde klonlayarak taşıyoruz
        let worker_config = config.clone();

        match unsafe { nix::unistd::fork() } {
            Ok(nix::unistd::ForkResult::Parent { child }) => {
                println!("[Parent] Worker #{} (PID: {}) fork edildi.", cpu_id, child);
                workers.push(child);
            }
            Ok(nix::unistd::ForkResult::Child) => {
                // Okuduğumuz 'worker_config' ve 'cpu_id'yi işçi fonksiyona paslıyoruz.
                run_child_worker(cpu_id, worker_config);
                std::process::exit(0);
            }
            Err(err) => panic!("Fork başarısız oldu: {}", err),
        }
    }

    supervise_workers(workers, shutdown_requested)
}

fn supervise_workers(
    workers: Vec<nix::unistd::Pid>,
    shutdown_requested: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut remaining = workers.len();
    let mut stopping = false;

    while remaining > 0 {
        if shutdown_requested.load(Ordering::Acquire) && !stopping {
            stopping = true;
            println!("[Parent] Shutdown requested; draining workers...");
            for worker in &workers {
                if let Err(error) =
                    nix::sys::signal::kill(*worker, nix::sys::signal::Signal::SIGTERM)
                {
                    if error != nix::errno::Errno::ESRCH {
                        return Err(error.into());
                    }
                }
            }
        }

        match nix::sys::wait::waitpid(None, Some(nix::sys::wait::WaitPidFlag::WNOHANG)) {
            Ok(nix::sys::wait::WaitStatus::StillAlive) => {
                std::thread::sleep(Duration::from_millis(50));
            }
            Ok(status) => {
                remaining = remaining.saturating_sub(1);
                println!("[Parent] Worker exited: {:?}", status);
            }
            Err(nix::errno::Errno::ECHILD) => break,
            Err(error) => return Err(error.into()),
        }
    }

    Ok(())
}

fn run_child_worker(cpu_id: usize, config: Config) {
    if let Err(error) = slave::start_worker(cpu_id, &config) {
        eprintln!("[Worker {}] failed to start: {}", cpu_id, error);
    }
}

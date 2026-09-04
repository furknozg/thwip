use proxy_common::{read_config, AsyncRuntimeConfig, Config};
use std::{
    fs,
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

const RESTART_BASE_DELAY: Duration = Duration::from_millis(100);
const RESTART_MAX_DELAY: Duration = Duration::from_secs(10);
const RESTART_STABLE_WINDOW: Duration = Duration::from_secs(30);

struct WorkerSlot {
    cpu_id: usize,
    pid: Option<nix::unistd::Pid>,
    failures: u32,
    started_at: Instant,
    restart_at: Option<Instant>,
}

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
        let pid = spawn_worker(cpu_id, &config)?;
        workers.push(WorkerSlot {
            cpu_id,
            pid: Some(pid),
            failures: 0,
            started_at: Instant::now(),
            restart_at: None,
        });
    }

    supervise_workers(workers, shutdown_requested, &config)
}

fn supervise_workers(
    mut workers: Vec<WorkerSlot>,
    shutdown_requested: Arc<AtomicBool>,
    config: &Config,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut stopping = false;

    loop {
        if shutdown_requested.load(Ordering::Acquire) && !stopping {
            stopping = true;
            println!("[Parent] Shutdown requested; draining workers...");
            for pid in workers.iter().filter_map(|worker| worker.pid) {
                if let Err(error) = nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGTERM) {
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
                println!("[Parent] Worker exited: {:?}", status);
                let Some(pid) = status_pid(status) else {
                    continue;
                };
                let Some(worker) = workers.iter_mut().find(|worker| worker.pid == Some(pid)) else {
                    continue;
                };
                worker.pid = None;
                if !stopping && status_failed(status) {
                    if worker.started_at.elapsed() >= RESTART_STABLE_WINDOW {
                        worker.failures = 0;
                    }
                    worker.failures = worker.failures.saturating_add(1);
                    let delay = restart_delay(worker.failures);
                    worker.restart_at = Some(Instant::now() + delay);
                    eprintln!(
                        "[Parent] Worker #{} crashed; restarting in {} ms (failure #{})",
                        worker.cpu_id,
                        delay.as_millis(),
                        worker.failures
                    );
                }
            }
            Err(nix::errno::Errno::ECHILD) => {}
            Err(error) => return Err(error.into()),
        }

        if stopping && workers.iter().all(|worker| worker.pid.is_none()) {
            return Ok(());
        }

        if !stopping {
            for worker in workers.iter_mut().filter(|worker| {
                worker.pid.is_none()
                    && worker
                        .restart_at
                        .is_some_and(|restart_at| Instant::now() >= restart_at)
            }) {
                let pid = spawn_worker(worker.cpu_id, config)?;
                worker.pid = Some(pid);
                worker.started_at = Instant::now();
                worker.restart_at = None;
            }
            if workers
                .iter()
                .all(|worker| worker.pid.is_none() && worker.restart_at.is_none())
            {
                return Ok(());
            }
        }
    }
}

fn spawn_worker(
    cpu_id: usize,
    config: &Config,
) -> Result<nix::unistd::Pid, Box<dyn std::error::Error>> {
    let worker_config = config.clone();
    match unsafe { nix::unistd::fork() }? {
        nix::unistd::ForkResult::Parent { child } => {
            println!("[Parent] Worker #{} started (PID: {})", cpu_id, child);
            Ok(child)
        }
        nix::unistd::ForkResult::Child => run_child_worker(cpu_id, worker_config),
    }
}

fn run_child_worker(cpu_id: usize, config: Config) -> ! {
    if let Err(error) = slave::start_worker(cpu_id, &config) {
        eprintln!("[Worker {}] failed to start: {}", cpu_id, error);
        std::process::exit(1);
    }
    std::process::exit(0);
}

fn status_pid(status: nix::sys::wait::WaitStatus) -> Option<nix::unistd::Pid> {
    use nix::sys::wait::WaitStatus;
    match status {
        WaitStatus::Exited(pid, _) | WaitStatus::Signaled(pid, _, _) => Some(pid),
        _ => None,
    }
}

fn status_failed(status: nix::sys::wait::WaitStatus) -> bool {
    !matches!(status, nix::sys::wait::WaitStatus::Exited(_, 0))
}

fn restart_delay(failures: u32) -> Duration {
    let exponent = failures.saturating_sub(1).min(16);
    RESTART_BASE_DELAY
        .saturating_mul(1_u32 << exponent)
        .min(RESTART_MAX_DELAY)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restart_backoff_grows_and_is_bounded() {
        assert_eq!(restart_delay(1), Duration::from_millis(100));
        assert_eq!(restart_delay(2), Duration::from_millis(200));
        assert_eq!(restart_delay(3), Duration::from_millis(400));
        assert_eq!(restart_delay(100), RESTART_MAX_DELAY);
    }
}

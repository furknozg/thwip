use proxy_common::{read_config, AsyncRuntimeConfig, Config};
use std::{fs, path::Path};

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
        AsyncRuntimeConfig::Epoll { max_events } => {
            println!("Runtime: epoll (max events: {})", max_events);
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

    //  (Fork) Yönetimi
    for cpu_id in 0..total_workers {
        // config'i child process'e güvenli şekilde klonlayarak taşıyoruz
        let worker_config = config.clone();

        match unsafe { nix::unistd::fork() } {
            Ok(nix::unistd::ForkResult::Parent { child }) => {
                println!("[Parent] Worker #{} (PID: {}) fork edildi.", cpu_id, child);
            }
            Ok(nix::unistd::ForkResult::Child) => {
                // Okuduğumuz 'worker_config' ve 'cpu_id'yi işçi fonksiyona paslıyoruz.
                run_child_worker(cpu_id, worker_config);
                std::process::exit(0);
            }
            Err(err) => panic!("Fork başarısız oldu: {}", err),
        }
    }

    // Parent süreci hayatta tut ve ölen süreçleri izle (Supervision)
    loop {
        if let Ok(status) = nix::sys::wait::wait() {
            println!(
                "[Parent] Bir worker süreci kapandı veya çöktü: {:?}",
                status
            );
            // İlerleyen aşamalarda buraya ölen worker'ı config ile yeniden kaldırma mantığı eklenebilir.
        }
    }
}

fn run_child_worker(cpu_id: usize, config: Config) {
    if let Err(error) = slave::start_worker(cpu_id, &config) {
        eprintln!("[Worker {}] failed to start: {}", cpu_id, error);
    }
}

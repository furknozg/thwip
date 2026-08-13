use proxy_common::{read_config, Config};
use std::{fs, path::Path};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config_path = Path::new("rginx.conf");

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
    println!(
        "io_uring SQ Deep: {}, CQ Deep: {}",
        config.io_uring.sq_entries, config.io_uring.cq_entries
    );

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
    // Çekirdek sabitleme (Affinity)
    let core_ids = core_affinity::get_core_ids().unwrap();
    if let Some(core) = core_ids.get(cpu_id % core_ids.len()) {
        core_affinity::set_for_current(*core);
    }

    // Tek thread üzerinde koşan izole Tokio Runtime ayağa kaldırılıyor
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    runtime.block_on(async {
        println!(
            "[Worker {}] Soket dinleniyor. io_uring CQEntries: {}",
            cpu_id, config.io_uring.cq_entries
        );

        // io_uring halkası (io_uring::IoUring::builder() veya tokio_uring)
        // config.io_uring.sq_entries ve config.io_uring.cq_entries değerleriyle burada başlatılır.

        // Örnek asenkron proxy döngüsü...
    });
}

use std::os::unix::io::AsRawFd;
use nix::sys::wait::WaitStatus;
use tokio::runtime::Builder;
use core_affinity::CoreId;

fn main() {
    let cpu_count = 12; // 12 işlemci için 12 worker process

    for cpu_id in 0..cpu_count {
        match unsafe { nix::unistd::fork() } {
            Ok(nix::unistd::ForkResult::Parent { child }) => {
                // Parent process: Child'ları takip et, ölen olursa yeniden kaldır.
                println!("Worker {} (PID: {}) başlatıldı.", cpu_id, child);
            }
            Ok(nix::unistd::ForkResult::Child) => {
                // CHILD PROCESS: Burası tamamen izole bir OS sürecidir!
                
                // 1. Adım: Bu process'i ilgili CPU çekirdeğine sabitle (Pinning)
                let core_ids = core_affinity::get_core_ids().unwrap();
                if let Some(core) = core_ids.get(cpu_id) {
                    core_affinity::set_for_current(*core);
                }

                // 2. Adım: TEK BİR THREAD üzerinde çalışan Tokio Runtime'ı kur
                let runtime = Builder::new_current_thread() // <--- Sadece tek bir thread!
                    .enable_all()
                    .build()
                    .unwrap();

                // 3. Adım: Bu izole process'in asenkron döngüsünü başlat
                runtime.block_on(async {
                    // io_uring halkasını burada başlatıyoruz. 
                    // Sadece bu process'e ve bu thread'e özel izole bir ring olur!
                    run_proxy_worker(cpu_id).await;
                });

                // Child process işini bitirirse veya çökerse parent'a dönmesin diye çıkış yapıyoruz
                std::process::exit(0);
            }
            Err(err) => panic!("Fork hatası: {}", err),
        }
    }

    // Parent process burada bekler (zombie process olmaması için waitpid döngüsü)
    loop {
        run_parent_loop();
    }
}


async fn run_parent_loop() {
    
       if let Ok(status) = nix::sys::wait::wait() {
            println!("Bir worker kapandı: {:?}", status);
            // Gerçek senaryoda burada ölen worker yerine yenisi fork edilir.
        }
}

async fn run_proxy_worker(cpu_id: usize) {
    println!("Worker {} tamamen izole single-thread modda çalışıyor.", cpu_id);
    // TCP Listener açarken SO_REUSEPORT kullanmalısın!
    // io_uring operasyonları (tokio-uring vb.) burada döner.
}

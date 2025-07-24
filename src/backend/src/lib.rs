use getrandom_v03::Error;
use ic_cdk::{init, management_canister, post_upgrade};
use ic_cdk_timers::set_timer;
use ic_stable_structures::{
    memory_manager::{MemoryId, MemoryManager},
    DefaultMemoryImpl,
};
use ic_turso_bindings::{Builder, Connection};
use rand::{rngs::StdRng, RngCore, SeedableRng};
use std::{cell::RefCell, time::Duration};

thread_local! {
    static RNG: RefCell<Option<StdRng>> = const { RefCell::new(None) };
    static CONNECTION: RefCell<Option<Connection>> = const { RefCell::new(None) };
    static MEMORY_MANAGER: RefCell<MemoryManager<DefaultMemoryImpl>> =
         RefCell::new(MemoryManager::init(DefaultMemoryImpl::default()));
}

fn vec_to_array32(vec: Vec<u8>) -> Option<[u8; 32]> {
    if vec.len() == 32 {
        let mut array = [0u8; 32];
        array.copy_from_slice(&vec);
        Some(array)
    } else {
        None
    }
}

fn init_timer() {
    set_timer(Duration::ZERO, || {
        ic_cdk::futures::spawn(async {
            let seed = management_canister::raw_rand().await.unwrap();
            RNG.with_borrow_mut(|rng| {
                *rng = Some(StdRng::from_seed(vec_to_array32(seed).unwrap()))
            });
        })
    });
}

#[init]
fn init() {
    init_timer();
}

#[post_upgrade]
fn post_upgrade() {
    init_timer();
}

#[no_mangle]
unsafe extern "Rust" fn __getrandom_v03_custom(dest: *mut u8, len: usize) -> Result<(), Error> {
    RNG.with_borrow_mut(|rng| match rng {
        None => Err(Error::new_custom(0)),
        Some(rng) => {
            let buf: &mut [u8] = unsafe {
                core::ptr::write_bytes(dest, 0, len);
                core::slice::from_raw_parts_mut(dest, len)
            };
            rng.fill_bytes(buf);
            Ok(())
        }
    })
}

getrandom::register_custom_getrandom!(custom_getrandom);
fn custom_getrandom(buf: &mut [u8]) -> Result<(), getrandom::Error> {
    RNG.with(|rng| rng.borrow_mut().as_mut().unwrap().fill_bytes(buf));
    Ok(())
}

#[ic_cdk::query]
async fn greet(name: String) -> String {
    let memory = MEMORY_MANAGER.with_borrow(|m| m.get(MemoryId::new(1)));
    let db = Builder::with_memory(memory).build().await.unwrap();
    let conn = db.connect().unwrap();

    conn.execute("CREATE TABLE IF NOT EXISTS users (name TEXT)", ())
        .await
        .unwrap();

    conn.execute("INSERT INTO users (name) VALUES (?1)", [name.clone()])
        .await
        .unwrap();

    let mut stmt = conn
        .prepare("SELECT * FROM users WHERE name = ?1")
        .await
        .unwrap();

    let mut rows = stmt.query([name.clone()]).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    let value = row.get_value(0).unwrap();

    format!("Hello {}", value.as_text().unwrap())
}

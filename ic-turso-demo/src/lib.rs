use getrandom_v03::Error;
use ic_cdk::{init, management_canister, post_upgrade};
use ic_cdk_timers::set_timer;
use ic_stable_structures::{
    memory_manager::{MemoryId, MemoryManager},
    DefaultMemoryImpl,
};
use ic_turso_bindings::{Builder, Connection};
use rand::{rngs::StdRng, RngCore, SeedableRng};
use std::{cell::RefCell, rc::Rc, time::Duration};

mod test;

thread_local! {
    static RNG: RefCell<Option<StdRng>> = const { RefCell::new(None) };
    static CONNECTION: RefCell<Option<Rc<Connection>>> = const { RefCell::new(None) };
    static MEMORY_MANAGER: RefCell<MemoryManager<DefaultMemoryImpl>> =
         RefCell::new(MemoryManager::init(DefaultMemoryImpl::default()));
}

fn init_rng() {
    set_timer(Duration::ZERO, || {
        ic_cdk::futures::spawn(async {
            let seed = management_canister::raw_rand().await.unwrap();
            RNG.with_borrow_mut(|rng| {
                *rng = Some(StdRng::from_seed(seed.try_into().unwrap()));
            });
        })
    });
}

pub async fn init_db() -> Rc<Connection> {
    let memory = MEMORY_MANAGER.with_borrow(|m| m.get(MemoryId::new(0)));
    let db = Builder::with_memory(memory).build().await.unwrap();
    let connection = Rc::new(db.connect().unwrap());
    CONNECTION.with_borrow_mut(|c| {
        *c = Some(Rc::clone(&connection));
    });
    connection
}

pub async fn get_connection() -> Rc<Connection> {
    if let Some(conn) = CONNECTION.with_borrow(|c| c.clone()) {
        Rc::clone(&conn)
    } else {
        init_db().await
    }
}

#[init]
fn init() {
    init_rng();
}

#[post_upgrade]
fn post_upgrade() {
    init_rng();
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

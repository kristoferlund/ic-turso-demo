use ic_stable_structures::{memory_manager::VirtualMemory, Ic0StableMemory, Memory};
use turso_core::{Buffer, Clock, Completion, File, Instant, MemoryIO, OpenFlags, Result, IO};

use std::{cell::RefCell, sync::Arc};
use tracing::debug;

pub struct StableIO {
    virtual_memory: VirtualMemory<Ic0StableMemory>,
}
unsafe impl Send for StableIO {}
unsafe impl Sync for StableIO {}

impl StableIO {
    #[allow(clippy::arc_with_non_send_sync)]
    pub fn new(virtual_memory: VirtualMemory<Ic0StableMemory>) -> Self {
        debug!("StableIO initializing with VirtualMemory");
        Self { virtual_memory }
    }
}

impl Clock for StableIO {
    fn now(&self) -> Instant {
        let now_ns = ic_cdk::api::time();
        Instant {
            secs: (now_ns / 1_000_000_000) as i64,
            micros: ((now_ns % 1_000_000_000) / 1_000) as u32,
        }
    }
}

impl IO for StableIO {
    fn open_file(&self, _path: &str, _flags: OpenFlags, _direct: bool) -> Result<Arc<dyn File>> {
        Ok(Arc::new(StableFile {
            virtual_memory: self.virtual_memory.clone(),
        }))
    }

    fn run_once(&self) -> Result<()> {
        // nop
        Ok(())
    }

    fn wait_for_completion(&self, _c: Arc<Completion>) -> Result<()> {
        todo!();
    }

    fn generate_random_number(&self) -> i64 {
        let mut buf = [0u8; 8];
        getrandom::getrandom(&mut buf).unwrap();
        i64::from_ne_bytes(buf)
    }

    fn get_memory_io(&self) -> Arc<MemoryIO> {
        Arc::new(MemoryIO::new())
    }
}

pub struct StableDatabaseStorage {
    file: Arc<dyn File>,
}

unsafe impl Send for StableDatabaseStorage {}
unsafe impl Sync for StableDatabaseStorage {}

impl StableDatabaseStorage {
    pub fn new(file: Arc<dyn turso_core::File>) -> Self {
        Self { file }
    }
}

impl turso_core::DatabaseStorage for StableDatabaseStorage {
    fn read_page(&self, page_idx: usize, c: turso_core::Completion) -> Result<()> {
        let r = match c.completion_type {
            turso_core::CompletionType::Read(ref r) => r,
            _ => unreachable!(),
        };
        let size = r.buf().len();
        assert!(page_idx > 0);
        if !(512..=65536).contains(&size) || size & (size - 1) != 0 {
            return Err(turso_core::LimboError::NotADB);
        }
        let pos = (page_idx - 1) * size;
        self.file.pread(pos, c)?;
        Ok(())
    }

    fn write_page(
        &self,
        page_idx: usize,
        buffer: Arc<std::cell::RefCell<turso_core::Buffer>>,
        c: turso_core::Completion,
    ) -> Result<()> {
        let size = buffer.borrow().len();
        let pos = (page_idx - 1) * size;
        self.file.pwrite(pos, buffer, c)?;
        Ok(())
    }

    fn sync(&self, c: turso_core::Completion) -> Result<()> {
        let _ = self.file.sync(c)?;
        Ok(())
    }

    fn size(&self) -> Result<u64> {
        self.file.size()
    }
}

pub struct StableFile {
    virtual_memory: VirtualMemory<Ic0StableMemory>,
}
unsafe impl Send for StableFile {}
unsafe impl Sync for StableFile {}

impl File for StableFile {
    fn lock_file(&self, _exclusive: bool) -> Result<()> {
        Ok(())
    }
    fn unlock_file(&self) -> Result<()> {
        Ok(())
    }

    fn pread(&self, pos: usize, c: Completion) -> Result<Arc<Completion>> {
        let nr = {
            let r = c.as_read();
            let mut buf = r.buf_mut();
            let buf = buf.as_mut_slice();
            self.virtual_memory.read(pos as u64, buf);
            buf.len() as i32
        };
        c.complete(nr);
        #[allow(clippy::arc_with_non_send_sync)]
        Ok(Arc::new(c))
    }

    fn pwrite(
        &self,
        pos: usize,
        buffer: Arc<RefCell<Buffer>>,
        c: Completion,
    ) -> Result<Arc<Completion>> {
        let buf = buffer.borrow();
        let buf = buf.as_slice();

        // Determine if memory needs to grow
        let required_end = pos + buf.len();
        let current_size_bytes = self.virtual_memory.size() as usize * 65536;

        if required_end > current_size_bytes {
            let required_pages = ((required_end + 65535) / 65536) as u64;
            let current_pages = self.virtual_memory.size();
            let pages_to_grow = required_pages.saturating_sub(current_pages);

            if pages_to_grow > 0 {
                let grown = self.virtual_memory.grow(pages_to_grow);
                if grown == -1 {
                    return Err(turso_core::LimboError::InternalError(
                        "Could not grow memory.".to_string(),
                    ));
                }
            }
        }

        self.virtual_memory.write(pos as u64, buf);

        c.complete(buf.len() as i32);
        #[allow(clippy::arc_with_non_send_sync)]
        Ok(Arc::new(c))
    }

    fn sync(&self, c: Completion) -> Result<Arc<Completion>> {
        // no-op
        c.complete(0);
        #[allow(clippy::arc_with_non_send_sync)]
        Ok(Arc::new(c))
    }

    fn size(&self) -> Result<u64> {
        Ok(self.virtual_memory.size())
    }
}

impl Drop for StableFile {
    fn drop(&mut self) {
        // no-op
    }
}

pub mod cli;
pub mod evidence;
pub mod input;
pub mod pipeline;
pub mod realtime;
pub mod sync;

pub use evidence::allocator;
pub use evidence::metrics;
pub use evidence::reporting;
pub use input::event;
pub use input::source;
pub use realtime::failsafe;
pub use realtime::priority;
pub use realtime::queue;
pub use sync::leaderboard;

#[global_allocator]
static GLOBAL: allocator::CountingAllocator = allocator::CountingAllocator;

pub type Result<T> = anyhow::Result<T>;

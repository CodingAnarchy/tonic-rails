//! `Mutex` and `Condvar` over `std::sync`, with parking_lot's API: `lock`
//! returns the guard, and a panic while holding a lock doesn't poison it.
//!
//! Why not parking_lot: under contention it spins and then calls `Sleep`,
//! and on Windows (mingw) the extension's `Sleep` import binds to the one
//! Ruby's DLL exports.  Ruby's `Sleep` expects a Ruby thread, so on a tokio
//! thread it crashed the process (#41).  std's locks use SRW locks and
//! never call `Sleep`.

use std::sync::{MutexGuard, PoisonError};

#[derive(Default)]
pub struct Mutex<T>(std::sync::Mutex<T>);

impl<T> Mutex<T> {
    pub const fn new(value: T) -> Self {
        Self(std::sync::Mutex::new(value))
    }

    pub fn lock(&self) -> MutexGuard<'_, T> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn get_mut(&mut self) -> &mut T {
        self.0.get_mut().unwrap_or_else(PoisonError::into_inner)
    }
}

#[derive(Default)]
pub struct Condvar(std::sync::Condvar);

impl Condvar {
    /// Block until notified, releasing `guard` meanwhile.  Wakes can be
    /// spurious: wait in a loop on the condition.
    pub fn wait<'a, T>(&self, guard: MutexGuard<'a, T>) -> MutexGuard<'a, T> {
        self.0.wait(guard).unwrap_or_else(PoisonError::into_inner)
    }

    pub fn notify_one(&self) {
        self.0.notify_one();
    }
}

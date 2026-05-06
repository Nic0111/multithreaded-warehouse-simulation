use serde::Serialize;
use std::collections::VecDeque;

#[derive(Debug, Clone, Serialize)]
pub struct OverflowEvent {
    pub timestamp_ns: u128,
    pub capacity: usize,
    pub dropped_seq: u64,
    pub reason: &'static str,
}

#[derive(Debug)]
pub struct BoundedDropOldestChannel<T> {
    capacity: usize,
    items: VecDeque<T>,
    overflow_events: Vec<OverflowEvent>,
}

pub type DropOldestQueue<T> = BoundedDropOldestChannel<T>;

pub trait QueueItem {
    fn seq(&self) -> u64;
}

impl<T: QueueItem> BoundedDropOldestChannel<T> {
    pub fn new(capacity: usize) -> Self {
        assert!(
            capacity > 0,
            "bounded channel capacity must be greater than zero"
        );
        Self {
            capacity,
            items: VecDeque::with_capacity(capacity),
            overflow_events: Vec::new(),
        }
    }

    /// Sends into a bounded channel with real-time drop-oldest backpressure.
    ///
    /// Standard Rust channels do not directly expose "drop the oldest item and
    /// log the overflow" semantics, so this assignment uses a small custom
    /// channel wrapper around VecDeque to make that policy explicit.
    pub fn send_drop_oldest(&mut self, item: T, timestamp_ns: u128) {
        if self.items.len() == self.capacity {
            if let Some(dropped) = self.items.pop_front() {
                self.overflow_events.push(OverflowEvent {
                    timestamp_ns,
                    capacity: self.capacity,
                    dropped_seq: dropped.seq(),
                    reason: "bounded-channel-overflow-drop-oldest",
                });
            }
        }
        self.items.push_back(item);
    }

    pub fn try_recv(&mut self) -> Option<T> {
        self.items.pop_front()
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn overflow_events(&self) -> &[OverflowEvent] {
        &self.overflow_events
    }

    pub fn take_overflow_events(&mut self) -> Vec<OverflowEvent> {
        std::mem::take(&mut self.overflow_events)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct Item(u64);

    impl QueueItem for Item {
        fn seq(&self) -> u64 {
            self.0
        }
    }

    #[test]
    fn drops_oldest_when_full() {
        let mut queue = DropOldestQueue::new(2);
        queue.send_drop_oldest(Item(1), 10);
        queue.send_drop_oldest(Item(2), 20);
        queue.send_drop_oldest(Item(3), 30);

        assert_eq!(queue.overflow_events()[0].dropped_seq, 1);
        assert_eq!(queue.try_recv().unwrap().seq(), 2);
        assert_eq!(queue.try_recv().unwrap().seq(), 3);
    }
}

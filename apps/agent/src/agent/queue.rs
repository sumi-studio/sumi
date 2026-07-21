//! Bounded, one-at-a-time FIFO queues used by an active run.
#![allow(
    dead_code,
    reason = "the queue is consumed by the intentionally unwired T15 Session actor"
)]

use std::collections::VecDeque;

use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
#[error("message queue is full (capacity {capacity})")]
pub(crate) struct QueueFull {
    capacity: usize,
}

/// A deliberately small queue abstraction: callers can take only the oldest
/// item, so ordinary follow-ups cannot accidentally be drained as a group.
#[derive(Debug)]
pub(crate) struct MessageQueue<T> {
    entries: VecDeque<T>,
    capacity: usize,
}

impl<T> MessageQueue<T> {
    pub(crate) fn bounded(capacity: usize) -> Self {
        assert!(capacity > 0, "message queue capacity must be non-zero");
        Self {
            entries: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    pub(crate) fn push(&mut self, value: T) -> Result<(), QueueFull> {
        if self.entries.len() == self.capacity {
            return Err(QueueFull {
                capacity: self.capacity,
            });
        }
        self.entries.push_back(value);
        Ok(())
    }

    pub(crate) fn push_front(&mut self, value: T) -> Result<(), QueueFull> {
        if self.entries.len() == self.capacity {
            return Err(QueueFull {
                capacity: self.capacity,
            });
        }
        self.entries.push_front(value);
        Ok(())
    }

    pub(crate) fn pop_one(&mut self) -> Option<T> {
        self.entries.pop_front()
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_fifo_exposes_only_one_item_per_pop() {
        let mut queue = MessageQueue::bounded(2);
        queue.push(10).expect("first entry");
        queue.push(20).expect("second entry");
        assert_eq!(queue.len(), 2);
        assert_eq!(queue.push(30), Err(QueueFull { capacity: 2 }));

        assert_eq!(queue.pop_one(), Some(10));
        assert_eq!(queue.len(), 1);
        assert_eq!(queue.pop_one(), Some(20));
        assert!(queue.is_empty());
        assert_eq!(queue.pop_one(), None);
    }
}

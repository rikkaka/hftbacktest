use std::mem;

#[derive(Clone, Copy)]
#[repr(C, align(32))]
pub struct EventIntent {
    pub timestamp: i64,
    pub asset_no: usize,
    pub kind: EventIntentKind,
}

/// This is constructed by using transmute in `EventSet::next`.
#[allow(dead_code)]
#[derive(Eq, PartialEq, Clone, Copy)]
#[repr(usize)]
pub enum EventIntentKind {
    LocalData = 0,
    LocalOrder = 1,
    ExchData = 2,
    ExchOrder = 3,
}

/// Manages the event timestamps to determine the next event to be processed.
pub struct EventSet {
    timestamp: Vec<i64>,
    heap: Vec<usize>,
    pos: Vec<usize>,
}

impl EventSet {
    /// Constructs an instance of `EventSet`.
    pub fn new(num_assets: usize) -> Self {
        if num_assets == 0 {
            panic!();
        }
        let len = num_assets * 4;
        Self {
            timestamp: vec![i64::MAX; len],
            heap: (0..len).collect(),
            pos: (0..len).collect(),
        }
    }

    /// Returns the next event to be processed, which has the earliest timestamp.
    pub fn next(&self) -> Option<EventIntent> {
        let evst_no = self.heap[0];
        let timestamp = self.timestamp[evst_no];
        // Returns None if no valid events are found.
        if timestamp == i64::MAX {
            return None;
        }
        let asset_no = evst_no >> 2;
        let kind = unsafe { mem::transmute::<usize, EventIntentKind>(evst_no & 3) };
        Some(EventIntent {
            timestamp,
            asset_no,
            kind,
        })
    }

    #[inline]
    fn update(&mut self, evst_no: usize, timestamp: i64) {
        self.timestamp[evst_no] = timestamp;
        let pos = self.pos[evst_no];
        self.sift_up(pos);
        let pos = self.pos[evst_no];
        self.sift_down(pos);
    }

    #[inline]
    pub fn update_local_data(&mut self, asset_no: usize, timestamp: i64) {
        self.update(4 * asset_no, timestamp);
    }

    #[inline]
    pub fn update_local_order(&mut self, asset_no: usize, timestamp: i64) {
        self.update(4 * asset_no + 1, timestamp);
    }

    #[inline]
    pub fn update_exch_data(&mut self, asset_no: usize, timestamp: i64) {
        self.update(4 * asset_no + 2, timestamp);
    }

    #[inline]
    pub fn update_exch_order(&mut self, asset_no: usize, timestamp: i64) {
        self.update(4 * asset_no + 3, timestamp);
    }

    #[inline]
    fn invalidate(&mut self, evst_no: usize) {
        self.timestamp[evst_no] = i64::MAX;
        self.sift_down(self.pos[evst_no]);
    }

    #[inline]
    pub fn invalidate_local_data(&mut self, asset_no: usize) {
        self.invalidate(4 * asset_no);
    }

    #[inline]
    pub fn invalidate_exch_data(&mut self, asset_no: usize) {
        self.invalidate(4 * asset_no + 2);
    }

    fn less(&self, lhs: usize, rhs: usize) -> bool {
        let lhs_ts = self.timestamp[lhs];
        let rhs_ts = self.timestamp[rhs];
        lhs_ts < rhs_ts || (lhs_ts == rhs_ts && lhs < rhs)
    }

    fn swap_heap(&mut self, lhs: usize, rhs: usize) {
        self.heap.swap(lhs, rhs);
        self.pos[self.heap[lhs]] = lhs;
        self.pos[self.heap[rhs]] = rhs;
    }

    fn sift_up(&mut self, mut pos: usize) {
        while pos > 0 {
            let parent = (pos - 1) / 2;
            if !self.less(self.heap[pos], self.heap[parent]) {
                break;
            }
            self.swap_heap(pos, parent);
            pos = parent;
        }
    }

    fn sift_down(&mut self, mut pos: usize) {
        loop {
            let left = pos * 2 + 1;
            let right = left + 1;
            let mut best = pos;

            if left < self.heap.len() && self.less(self.heap[left], self.heap[best]) {
                best = left;
            }
            if right < self.heap.len() && self.less(self.heap[right], self.heap[best]) {
                best = right;
            }
            if best == pos {
                break;
            }

            self.swap_heap(pos, best);
            pos = best;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct LinearEventSet {
        timestamp: Vec<i64>,
    }

    impl LinearEventSet {
        fn new(num_assets: usize) -> Self {
            Self {
                timestamp: vec![i64::MAX; num_assets * 4],
            }
        }

        fn update(&mut self, evst_no: usize, timestamp: i64) {
            self.timestamp[evst_no] = timestamp;
        }

        fn invalidate(&mut self, evst_no: usize) {
            self.timestamp[evst_no] = i64::MAX;
        }

        fn next(&self) -> Option<EventIntent> {
            let mut evst_no = 0;
            let mut timestamp = self.timestamp[0];
            for (i, &ev_timestamp) in self.timestamp[1..].iter().enumerate() {
                if ev_timestamp < timestamp {
                    timestamp = ev_timestamp;
                    evst_no = i + 1;
                }
            }
            if timestamp == i64::MAX {
                return None;
            }
            let asset_no = evst_no >> 2;
            let kind = unsafe { mem::transmute::<usize, EventIntentKind>(evst_no & 3) };
            Some(EventIntent {
                timestamp,
                asset_no,
                kind,
            })
        }
    }

    fn event_key(event: Option<EventIntent>) -> Option<(i64, usize, usize)> {
        event.map(|event| (event.timestamp, event.asset_no, event.kind as usize))
    }

    fn event_slot(event: EventIntent) -> usize {
        event.asset_no * 4 + event.kind as usize
    }

    #[test]
    fn heap_event_set_matches_linear_scan_order() {
        let mut heap = EventSet::new(4);
        let mut linear = LinearEventSet::new(4);
        let updates = [
            (8, 10),
            (1, 5),
            (4, 5),
            (2, 7),
            (7, 5),
            (0, 10),
            (10, 7),
            (13, 5),
        ];

        for (slot, timestamp) in updates {
            heap.update(slot, timestamp);
            linear.update(slot, timestamp);
            assert_eq!(event_key(heap.next()), event_key(linear.next()));
        }

        while let Some(next) = linear.next() {
            assert_eq!(
                event_key(heap.next()),
                Some((next.timestamp, next.asset_no, next.kind as usize))
            );
            let slot = event_slot(next);
            heap.invalidate(slot);
            linear.invalidate(slot);
        }
        assert_eq!(event_key(heap.next()), None);
    }
}

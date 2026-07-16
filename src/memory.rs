use crate::protocol::ResourceResponse;
use std::fmt;
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub const DEFAULT_MEMORY_BUDGET_SIZE: usize = 4 * crate::MAX_BODY_SIZE;

#[derive(Clone, Debug)]
pub struct MemoryBudget {
    inner: Arc<MemoryBudgetInner>,
}

#[derive(Debug)]
struct MemoryBudgetInner {
    limit: usize,
    used: AtomicUsize,
    high_watermark: AtomicUsize,
}

#[derive(Debug, Eq, PartialEq)]
pub struct MemoryBudgetError {
    requested: usize,
    available: usize,
}

impl fmt::Display for MemoryBudgetError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "response memory budget exceeded: requested {} bytes with {} bytes available",
            self.requested, self.available
        )
    }
}

impl std::error::Error for MemoryBudgetError {}

impl MemoryBudget {
    pub fn new(limit: usize) -> Self {
        Self {
            inner: Arc::new(MemoryBudgetInner {
                limit,
                used: AtomicUsize::new(0),
                high_watermark: AtomicUsize::new(0),
            }),
        }
    }

    pub fn try_reserve(&self, bytes: usize) -> Result<MemoryReservation, MemoryBudgetError> {
        self.try_acquire(bytes)?;
        Ok(MemoryReservation {
            budget: self.clone(),
            bytes,
        })
    }

    fn try_acquire(&self, bytes: usize) -> Result<(), MemoryBudgetError> {
        let result = self
            .inner
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes)
                    .filter(|next| *next <= self.inner.limit)
            });
        match result {
            Ok(previous) => {
                self.inner
                    .high_watermark
                    .fetch_max(previous + bytes, Ordering::Relaxed);
                Ok(())
            }
            Err(used) => Err(MemoryBudgetError {
                requested: bytes,
                available: self.inner.limit.saturating_sub(used),
            }),
        }
    }

    pub fn limit(&self) -> usize {
        self.inner.limit
    }

    pub fn used(&self) -> usize {
        self.inner.used.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn high_watermark(&self) -> usize {
        self.inner.high_watermark.load(Ordering::Acquire)
    }
}

#[derive(Debug)]
pub struct MemoryReservation {
    budget: MemoryBudget,
    bytes: usize,
}

impl MemoryReservation {
    pub fn try_grow(&mut self, additional: usize) -> Result<(), MemoryBudgetError> {
        self.budget.try_acquire(additional)?;
        self.bytes += additional;
        Ok(())
    }

    pub fn shrink_to(&mut self, bytes: usize) {
        assert!(
            bytes <= self.bytes,
            "cannot grow a reservation by shrinking it"
        );
        let released = self.bytes - bytes;
        self.bytes = bytes;
        self.budget.inner.used.fetch_sub(released, Ordering::AcqRel);
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

impl Drop for MemoryReservation {
    fn drop(&mut self) {
        self.budget
            .inner
            .used
            .fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

#[derive(Debug)]
pub struct BudgetedResponse {
    response: ResourceResponse,
    retained: MemoryReservation,
}

impl BudgetedResponse {
    pub fn try_new(
        budget: &MemoryBudget,
        uid: String,
        status: u16,
        body: String,
    ) -> Result<Self, MemoryBudgetError> {
        let retained_bytes = retained_response_bytes(&uid, &body)?;
        let retained = budget.try_reserve(retained_bytes)?;
        Ok(Self {
            response: ResourceResponse::new(uid, status, body),
            retained,
        })
    }

    pub(crate) fn from_reserved(
        uid: String,
        status: u16,
        body: String,
        mut retained: MemoryReservation,
    ) -> Self {
        let retained_bytes = uid
            .capacity()
            .checked_add(body.capacity())
            .expect("validated response sizes fit in usize");
        debug_assert!(retained.bytes() >= retained_bytes);
        retained.shrink_to(retained_bytes);
        Self {
            response: ResourceResponse::new(uid, status, body),
            retained,
        }
    }

    pub fn downgrade_to_error(&mut self) {
        self.response.body = String::new();
        self.response.status = 500;
        self.retained.shrink_to(self.response.uid.capacity());
    }

    pub fn retained_bytes(&self) -> usize {
        self.retained.bytes()
    }
}

impl Deref for BudgetedResponse {
    type Target = ResourceResponse;

    fn deref(&self) -> &Self::Target {
        &self.response
    }
}

fn retained_response_bytes(uid: &String, body: &String) -> Result<usize, MemoryBudgetError> {
    uid.capacity()
        .checked_add(body.capacity())
        .ok_or(MemoryBudgetError {
            requested: usize::MAX,
            available: 0,
        })
}

#[cfg(test)]
mod tests {
    use super::{BudgetedResponse, MemoryBudget};

    #[test]
    fn shared_reservations_enforce_one_limit_and_release_exact_counters() {
        let budget = MemoryBudget::new(10);
        let first = budget.try_reserve(6).unwrap();
        assert_eq!(budget.used(), 6);
        assert!(budget.clone().try_reserve(5).is_err());
        assert_eq!(budget.used(), 6);

        let mut second = budget.clone().try_reserve(4).unwrap();
        assert_eq!(budget.used(), 10);
        assert!(second.try_grow(1).is_err());
        assert_eq!(budget.used(), 10);

        second.shrink_to(2);
        assert_eq!(budget.used(), 8);
        drop(first);
        assert_eq!(budget.used(), 2);
        drop(second);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn budgeted_responses_are_byte_bounded_below_item_capacity() {
        let budget = MemoryBudget::new(8);
        let (sender, mut receiver) = tokio::sync::mpsc::channel(8);
        let first =
            BudgetedResponse::try_new(&budget, "uid-0001".into(), 503, String::new()).unwrap();
        sender.try_send(first).unwrap();
        assert_eq!(sender.capacity(), 7);
        assert_eq!(budget.used(), 8);

        assert!(BudgetedResponse::try_new(&budget, "uid-0002".into(), 503, String::new()).is_err());
        assert_eq!(sender.capacity(), 7);
        assert_eq!(budget.used(), 8);

        drop(receiver.try_recv().unwrap());
        assert_eq!(budget.used(), 0);
    }
}

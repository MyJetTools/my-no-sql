use std::collections::BTreeMap;

use rust_extensions::{date_time::DateTimeAsMicroseconds, lazy::LazyVec};

use rust_extensions::auto_shrink::VecAutoShrink;

pub trait ExpirationItem {
    fn get_id(&self) -> &str;

    fn are_same(&self, other_one: &Self) -> bool {
        self.get_id() == other_one.get_id()
    }
}

pub struct ExpirationIndex<T: Clone + ExpirationItem> {
    index: BTreeMap<i64, VecAutoShrink<T>>,
    amount: usize,
}

impl<T: Clone + ExpirationItem> ExpirationIndex<T> {
    pub fn new() -> Self {
        Self {
            index: BTreeMap::new(),
            amount: 0,
        }
    }

    pub fn add(&mut self, expiration_moment: Option<DateTimeAsMicroseconds>, item: &T) {
        if expiration_moment.is_none() {
            return;
        }

        let expire_moment = expiration_moment.unwrap().unix_microseconds;

        match self.index.get_mut(&expire_moment) {
            Some(items) => {
                items.push(item.clone());
            }
            None => {
                self.index.insert(
                    expire_moment,
                    VecAutoShrink::new_with_element(32, item.clone()),
                );
            }
        }

        self.amount += 1;
    }

    pub fn remove(&mut self, expiration_moment: DateTimeAsMicroseconds, item: &T) {
        let expire_moment = expiration_moment.unix_microseconds;

        let mut is_empty = false;
        if let Some(items) = self.index.get_mut(&expire_moment) {
            items.retain(|f| !item.are_same(f));
            is_empty = items.is_empty();
        }

        if is_empty {
            self.index.remove(&expire_moment);
        }

        self.amount -= 1;
    }

    pub fn get_items_to_expire(&self, now: DateTimeAsMicroseconds) -> Option<Vec<T>> {
        let mut result = LazyVec::new();
        for (expiration_time, items) in &self.index {
            if *expiration_time > now.unix_microseconds {
                break;
            }

            for itm in items.iter() {
                result.add(itm.clone());
            }
        }

        result.get_result()
    }

    pub fn get_items_to_expire_cloned(&self, now: DateTimeAsMicroseconds) -> Option<Vec<T>> {
        let mut result = LazyVec::new();
        for (expiration_time, items) in &self.index {
            if *expiration_time > now.unix_microseconds {
                break;
            }

            for itm in items.iter() {
                result.add(itm.clone());
            }
        }

        result.get_result()
    }

    pub fn has_data_with_expiration_moment(&self, expiration_moment: i64) -> bool {
        self.index.contains_key(&expiration_moment)
    }

    pub fn len(&self) -> usize {
        self.amount
    }

    pub fn clear(&mut self) {
        self.index.clear();
    }
}

extern crate bincode;
extern crate fuzzy_log_util;
extern crate fuzzy_log_client;
pub extern crate serde;
#[macro_use] extern crate serde_derive;

#[cfg(test)] extern crate fuzzy_log_server;

use std::collections::HashMap;

use bincode::{serialize, deserialize, Infinite};

use fuzzy_log_util::range_tree::RangeTree;

pub use fuzzy_log_client::fuzzy_log::log_handle::{
    order,
    OrderIndex,
    GetRes,
    LogHandle,
};

pub type Handle = LogHandle<[u8]>;

#[derive(Debug, Serialize, Deserialize)]
struct Allocation<'a> {
    num_colors: u64,
    key: &'a [u8],
}

pub struct Allocator {
    used_keys: HashMap<Vec<u8>, (u64, u64)>,
    allocated: RangeTree<bool>,
    alloc_color: order,
}

impl Allocator {
    pub fn new() -> Self {
        Self::with_allocator_color(order::from(1))
    }

    fn with_allocator_color(alloc_color: order) -> Self {
        Allocator {
            alloc_color,
            allocated: RangeTree::with_default_val(false),
            used_keys: Default::default(),
        }
    }

    pub fn allocate(&mut self, handle: &mut Handle, num_colors: u64, key: &[u8]) -> Vec<order> {
        let data = serialize(&Allocation{ num_colors, key }, Infinite).unwrap();
        handle.append(self.alloc_color, &data[..], &[]);
        self.update_allocations(handle).get_allocation_for(key).unwrap()
    }

    pub fn update_allocations(&mut self, handle: &mut Handle) -> &mut Self {
        handle.snapshot(self.alloc_color);
        loop {
            match handle.get_next() {
                Err(GetRes::Done) => return self,
                Err(e) => panic!("{:?}", e),
                Ok((bytes, _)) => {
                    let Allocation {key, num_colors} = deserialize(bytes).unwrap();
                    let alloc_start = self.allocated.iter()
                        .filter_map(|(range, &alloc)| if alloc {
                            Some(range.last() + 1)
                        } else {
                            None
                        })
                        .next()
                        .unwrap_or(2);
                    if self.used_keys.get(key).is_some() {
                        continue
                    }

                    self.used_keys.insert(key.to_vec(), (alloc_start, num_colors));
                    self.allocated.set_range_as(alloc_start, alloc_start+num_colors-1, true);
                },
            }
        }
    }

    pub fn get_allocation_for(&self, key: &[u8]) -> Option<Vec<order>> {
        self.used_keys.get(key).map(|&(start, end)|
            (start..end).map(|i| order::from(i as u32)).collect()
        )
    }
}

pub fn allocate(handle: &mut Handle, num_colors: u64, key: &[u8]) -> Vec<order> {
    let mut alloc = Allocator::new();
    handle.rewind(OrderIndex(alloc.alloc_color, 0.into()));
    alloc.allocate(handle, num_colors, key)
}

pub fn get_allocation_for(handle: &mut Handle, key: &[u8]) -> Option<Vec<order>> {
    let mut alloc = Allocator::new();
    handle.rewind(OrderIndex(alloc.alloc_color, 0.into()));
    alloc.update_allocations(handle).get_allocation_for(key)
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
    }
}

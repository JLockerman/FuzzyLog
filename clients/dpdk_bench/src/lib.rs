use std::collections::HashMap;

#[no_mangle]
pub extern "C" fn take_time(map: &mut HashMap<u64, i64>, packet_num: u64, end_time: i64) -> i64 {
    map.remove(&packet_num).unwrap_or_else(|| panic!("at packet num {}, {:?}", packet_num, map))
    //map.remove(&packet_num).map_or(0, |start_time| end_time - start_time)
}

#[no_mangle]
pub extern "C" fn put_time(map: &mut HashMap<u64, i64>, packet_num: u64, time: i64) {
    map.insert(packet_num, time);
}

#[no_mangle]
pub extern "C" fn init_map() -> Box<HashMap<u64, i64>> {
    Box::new(HashMap::with_capacity(1000))
}

#[cfg(test)]
mod test {
    #[test]
    fn it_works() {
    }
}

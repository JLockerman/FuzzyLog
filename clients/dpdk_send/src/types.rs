
use std::mem;

use fuzzy_log::prelude::*;

use self::ReceiveEvent::*;

pub type WaitKey = u16;//TODO?
#[allow(non_camel_case_types)]
pub type ns = u64;

pub enum ReceiveEvent {
    Message(MessageBuffer),
    Timeout(MessageBuffer, WaitKey),
}

impl ReceiveEvent {
    pub fn wait_key(&self) -> &WaitKey {
        match self {
            &Message(ref buf) => buf.wait_key(),
            &Timeout(_, ref key) => key,
        }
    }

    pub fn into_message_buffer(self) -> MessageBuffer {
        match self {
            Message(buf) => buf,
            Timeout(buf, _) => buf,
        }
    }
}

pub enum Mbuf {}

pub struct MessageBuffer {
    pub buffer: *mut Mbuf,
}

impl MessageBuffer {

    pub fn as_nothing(&mut self) {
        let buffer = self.buffer;
        assert!(buffer != ::std::ptr::null_mut());
        assert!(buffer as usize != 1);
        unsafe {
            let packet = &mut *mbuf_into_read_packet(buffer);
            packet.kind = EntryKind::Invalid;
        }
    }

    pub fn as_read(&mut self, key: OrderIndex) {
        let buffer = self.buffer;
        assert!(buffer != ::std::ptr::null_mut());
        assert!(buffer as usize != 1);
        let size = unsafe {
            let packet = &mut *mbuf_into_read_packet(buffer);
            packet.kind = EntryKind::Read;
            packet.flex.loc = key;
            packet.id = Uuid::new_v4();
            entry_size(packet)
        };
        unsafe { prep_mbuf(buffer, size, 0) };
    }

    pub fn as_append<V: ?Sized + Storeable>(&mut self, chain: order, val: &V, deps: &[OrderIndex]) {
        let buffer = self.buffer;
        assert!(buffer != ::std::ptr::null_mut());
        assert!(buffer as usize != 1);
        let size = unsafe {
            let packet = &mut *mbuf_into_write_packet(buffer);
            {
                packet.kind = EntryKind::Data;
                let packet = transmute_ref_mut(packet);
                *packet = EntryContents::Data(val, deps).clone_entry();
            }
            packet.flex.loc = (chain, 0.into());
            packet.id = Uuid::new_v4();
            entry_size(packet)
        };
        trace!("entry size: {}, min size {}", size, mem::size_of::<Entry<(), DataFlex<()>>>());
        assert!(size as usize >= mem::size_of::<Entry<(), DataFlex<()>>>());
        unsafe { prep_mbuf(buffer, size, 0) };
    }

    pub fn as_multiappend<V: ?Sized + Storeable>(&mut self, chains: &[OrderIndex], val: &V, deps: &[OrderIndex]) {
        let buffer = self.buffer;
        assert!(buffer != ::std::ptr::null_mut());
        assert!(buffer as usize != 1);
        let size = unsafe {
            let packet = &mut *mbuf_into_multi_packet(buffer);
            packet.kind = EntryKind::Multiput;
            let mut packet = transmute_ref_mut(packet);
            *packet = EntryContents::Multiput{data: val, uuid: mem::zeroed(), columns: chains, deps: deps}.clone_entry();
            packet.id = Uuid::new_v4();
            entry_size(packet)
        };
        unsafe { prep_mbuf(buffer, size, 0) };
    }

    pub fn wait_key(&self) -> &WaitKey {
        unsafe {
            mbuf_get_src_port_ptr(&*self.buffer)
        }
    }

    pub fn kind(&self) -> EntryKind::Kind {
        unsafe {
            (*mbuf_as_packet(self.buffer)).kind
        }
    }

    pub fn layout(&self) -> EntryKind::Kind {
        self.kind() & EntryKind::Layout
    }

    pub fn read_loc(&self) -> OrderIndex {
        assert_eq!(self.layout(), EntryKind::Read);
        unsafe {
            let packet = &*mbuf_as_packet(self.buffer);
            packet.as_data_entry().flex.loc
        }
    }

    pub fn set_read_entry(&mut self, e: entry) {
        assert_eq!(self.layout(), EntryKind::Read);
        unsafe {
            let packet = &mut *mbuf_as_packet(self.buffer);
            packet.as_data_entry_mut().flex.loc.1 = e;
        }
    }

    pub fn get_id(&self) -> Uuid {
        unsafe {
            (*mbuf_as_packet(self.buffer)).id
        }
    }

    pub fn locs(&self) -> &[OrderIndex] {
        unsafe {
            (*mbuf_as_packet(self.buffer)).locs()
        }
    }

    pub fn val_locs_and_deps<V: ?Sized + Storeable>(&self) -> (&V, &[OrderIndex], &[OrderIndex]) {
        unsafe { transmute_ref::<_, Entry<V>>(&*mbuf_as_packet(self.buffer)).val_locs_and_deps() }
    }
}

impl Drop for MessageBuffer {
    fn drop(&mut self) {
        unsafe {
            //panic!("");
            dpdk_free_mbuf(self.buffer);
        }
    }
}


#[inline(always)]
fn entry_size<V: ?Sized + Storeable, F>(e: &Entry<V, F>) -> u16 {
    use fuzzy_log::prelude::EntryKind;
    unsafe {
        if e.kind & EntryKind::Layout == EntryKind::Data {
            data_entry_size(mem::transmute::<&Entry<V, F>, &Entry<V, _>>(e))
        }
        else if e.kind & EntryKind::Layout == EntryKind::Multiput {
            multi_entry_size(mem::transmute::<&Entry<V, F>, &Entry<V, _>>(e))
        }
        else if e.kind & EntryKind::Layout == EntryKind::Read {
            //TODO
            mem::size_of::<Entry<()>>() as u16
        }
        else {
            panic!("invalid layout {:?}", e.kind & EntryKind::Layout);
        }
    }
}

#[inline(always)]
fn data_entry_size<V: ?Sized + Storeable>(e: &Entry<V, DataFlex>) -> u16 {
    assert!(e.kind & EntryKind::Layout == EntryKind::Data);
    let size = mem::size_of::<Entry<(), DataFlex<()>>>() + e.data_bytes as usize
        + e.dependency_bytes as usize;
    assert!(size <= 8192);
    size as u16
}

#[inline(always)]
fn multi_entry_size<V: ?Sized + Storeable>(e: &Entry<V, MultiFlex>) -> u16 {
    assert!(e.kind & EntryKind::Layout == EntryKind::Multiput);
    let size = mem::size_of::<Entry<(), MultiFlex<()>>>() + e.data_bytes as usize
        + e.dependency_bytes as usize + (e.flex.cols as usize * mem::size_of::<OrderIndex>());
    assert!(size <= 8192);
    size as u16
}

#[inline(always)]
unsafe fn transmute_ref<T, U>(t: &T) -> &U {
    mem::transmute(t)
}

#[inline(always)]
unsafe fn transmute_ref_mut<T, U>(t: &mut T) -> &mut U {
    mem::transmute(t)
}

#[allow(improper_ctypes)]
extern "C" {
    fn dpdk_free_mbuf(_: *mut Mbuf);
    fn prep_mbuf(_: *mut Mbuf, data_size: u16, src_port: u16);
    fn mbuf_get_src_port_ptr(_: &Mbuf) -> &u16; //TODO should be dest port
    fn mbuf_into_write_packet(_ :*mut Mbuf) -> *mut Entry<(), DataFlex>;
    fn mbuf_into_read_packet(_ :*mut Mbuf) -> *mut Entry<(), DataFlex>;
    fn mbuf_into_multi_packet(_ :*mut Mbuf) -> *mut Entry<(), MultiFlex>;
    fn mbuf_as_packet(_ :*mut Mbuf) -> *mut Entry<()>;
}

#[inline(always)]
pub fn curr_time() -> ns {
    //unsafe { rdtsc() }
    unsafe {
        let (low, high): (u32, u32);
        asm!("rdtsc\n\tmovl %edx, $0\n\tmovl %eax, $1\n\t"
            : "=r"(high), "=r"(low) :: "{rax}", "{rdx}" : "volatile" );
        ((high as u64) << 32) | (low as u64)
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use std::mem;

    #[test]
    fn test_sizes() {
        assert_eq!(mem::size_of::<Mbuf>(), 0);
        assert_eq!(mem::size_of::<*mut Mbuf>(), mem::size_of::<*mut u8>());
        assert_eq!(mem::size_of::<MessageBuffer>(), 16);
    }
}


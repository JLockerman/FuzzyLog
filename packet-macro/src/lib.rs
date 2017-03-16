/*TODO
    pub fn write_to_starting_at<W: Write>(&self, start: usize write: &mut W)
    -> Result<usize, (usize, io::Error)> {
        let &Ref { $($field),* } = self;
        $(packet!(write_starting_at $field:$typ, bytes);)+
    }


    (write_stating_at $start:expr, $offset:ident, $field:ident:[$typ:path; $count:ident], $bytes:ident) => {
        unsafe {
            let count = *$count as usize;
            let size = mem::size_of::<$typ>() * count;
            if $start < $offset + size {
                let beginning = if $start < $offset {
                    $offset
                }
                else {
                    $start - $offest;
                }
                let field_bytes = $field.as_ptr() as *const u8;
                let field_slice = slice::from_raw_parts(field_bytes, size);
                try!($bytes.write(field_slice[beginning..]));
            }
            else {
                offset + size
            }


            .extend_from_slice(field_slice)
            offset
        }
    };

    (write_stating_at $start:ident, $field:ident:$typ:path, $bytes:ident) => {
        unsafe {
            let size = mem::size_of::<$typ>();
            let field_bytes = $field as *const $typ as *const u8;
            let field_slice = slice::from_raw_parts(field_bytes, size);
            $bytes.extend_from_slice(field_slice)
        };
    };

    UnalignedRef/Slice?
*/

pub trait Wrap<'a> {
    type Ref;
    type Mut;

    unsafe fn wrap(bytes: &'a [u8]) -> (Self::Ref, &'a [u8]);
    unsafe fn wrap_mut(bytes: &'a mut [u8]) -> (Self::Mut, &'a mut [u8]);

    unsafe fn try_wrap(bytes: &'a [u8]) -> Result<(Self::Ref, &'a [u8]), WrapErr>;
    unsafe fn try_wrap_mut(bytes: &'a mut [u8]) -> Result<(Self::Mut, &'a mut [u8]), usize>;
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum WrapErr {
    BadTag,
    NotEnoughBytes(usize),
}

impl WrapErr {
    pub fn unwrap_min_bytes(self) -> usize {
        match self {
            WrapErr::NotEnoughBytes(min_bytes) => min_bytes,
            BadTag => panic!("bad tag."),
        }
    }
}

#[macro_export]
macro_rules! packet {
    (struct $name:ident { $($field:ident: $typ:tt),+ $(,)* }) => {

        #[allow(non_snake_case)]
        pub mod $name {
            #[allow(unused_imports)]
            use super::*;

            #[derive(Copy, Clone, PartialEq, Eq, Debug)]
            pub struct Ref<'a> {
                $(pub $field: &'a packet!(type $typ)),*
            }

            #[derive(PartialEq, Eq, Debug)]
            pub struct Mut<'a> {
                $(pub $field: &'a mut packet!(type $typ)),*
            }

            impl<'a> Ref<'a> {
                pub unsafe fn wrap(bytes: &[u8]) -> Self {
                    let _offset = 0;
                    $(packet!(ref $field:$typ, _offset, bytes);)+
                    Ref {
                        $($field: $field),*
                    }
                }

                #[allow(unused_assignments)]
                pub unsafe fn try_wrap(mut __bytes: &[u8]) -> Result<(Self, &[u8]), $crate::WrapErr> {
                    let _min_total_len = 0usize;
                    $(let mut $field: Option<&packet!(type $typ)> = None;)*
                    'tryref: loop {
                       $(packet!(tryref $field:$typ; _min_total_len, __bytes; 'tryref);)*
                        let _ref = Ref { $($field: $field.unwrap()),* };
                        return Ok((_ref, __bytes))
                    }
                    Err($crate::WrapErr::NotEnoughBytes(0 $(+ packet!(err_size $typ))*))
                }

                pub fn fill_vec(&self, mut bytes: &mut Vec<u8>) {
                    let &Ref { $($field),* } = self;
                    $(packet!(write $field:$typ, bytes);)+
                }
            }

            impl<'a> Mut<'a> {
                pub unsafe fn wrap_mut(bytes: &mut [u8]) -> Self {
                    let _offset = 0;
                    $(packet!(mut $field:$typ, _offset, bytes);)+
                    Mut {
                        $($field: $field),*
                    }
                }

                #[allow(unused_assignments)]
                pub unsafe fn try_wrap_mut(__bytes: &mut [u8])
                -> Result<(Self, &mut [u8]), usize> {
                    let _min_total_len = 0usize;
                    $(let mut $field: Option<&mut packet!(type $typ)> = None;)*
                    'trymut: loop {
                       $(packet!(trymut $field:$typ; _min_total_len, __bytes; 'trymut);)*
                        let _mut = Mut { $($field: $field.unwrap()),* };
                        return Ok((_mut, __bytes))
                    }
                    Err(0 $(+ packet!(err_size $typ))*)
                }

                pub fn to_ref(&self) -> Ref {
                    let &Mut { $(ref $field),* } = self;
                    Ref { $($field: &**$field),* }
                }
            }

            pub fn min_size() -> usize {
                $(packet!(min_size $typ) +)* 0usize
            }

            packet!(impl Packet: Ref, Mut);
        }
    };

    (enum $name:ident { $tag:ident: $tag_typ:path,
        $($variant:ident: $tag_val:expr =>  { $($field:ident: $typ:tt),* $(,)* }),* $(,)*
    }) => {

        #[allow(non_snake_case)]
        pub mod $name {
            #[allow(unused_imports)]
            use super::*;

            #[derive(Copy, Clone, PartialEq, Eq, Debug)]
            pub enum Ref<'a> {
                $($variant { $($field: &'a packet!(type $typ)),* }),*
            }

            #[derive(PartialEq, Eq, Debug)]
            pub enum Mut<'a> {
                $($variant { $($field: &'a mut packet!(type $typ)),* }),*
            }

            impl<'a> Ref<'a> {
                pub unsafe fn wrap(bytes: &[u8]) -> Self {
                    let _offset = 0;
                    packet!(ref $tag: $tag_typ, _offset, bytes);
                    match $tag {
                        $(&$tag_val => {
                            $(packet!(ref $field:$typ, _offset, bytes);)+
                            Ref::$variant{ $($field: $field),* }
                        }),*
                        t => panic!("invlaid variant {:?}", t),
                    }
                }

                #[allow(unused_assignments)]
                pub unsafe fn try_wrap(mut __bytes: &[u8]) -> Result<(Self, &[u8]), $crate::WrapErr> {
                    let obytes = __bytes;
                    let _min_total_len = 0usize;
                    let mut $tag = None;
                    'tag: loop {
                       packet!(tryref $tag:$tag_typ; _min_total_len, __bytes; 'tag);
                        match $tag.unwrap() {
                            $(&$tag_val => {
                                $(let mut $field: Option<&packet!(type $typ)> = None;)*
                                'tryref: loop {
                                    $(packet!(tryref $field:$typ; _min_total_len, __bytes; 'tryref);)*
                                    let _ref = Ref::$variant{ $($field: $field.unwrap()),* };
                                    return Ok((_ref, __bytes))
                                }
                                return Err($crate::WrapErr::NotEnoughBytes(
                                    packet!(err_size $tag_typ) $(+ packet!(err_size $typ))*)
                                )
                            }),*
                            _ => return Err($crate::WrapErr::BadTag),
                        }
                    }
                    Err($crate::WrapErr::NotEnoughBytes(min_size()))
                }

                pub fn fill_vec(&self, mut bytes: &mut Vec<u8>) {
                    match self {
                        $(&Ref::$variant{ $($field),* } => {
                            let $tag: &$tag_typ = &$tag_val;
                            packet!(write $tag: $tag_typ, bytes);
                            $(packet!(write $field: $typ, bytes);)+
                        }),*
                    }
                }

                pub fn tag(&self) -> $tag_typ {
                    match self {
                        $(&Ref::$variant{..} => $tag_val),*
                    }
                }

                #[allow(dead_code)]
                fn __check_all_variants_are_unique() {
                    enum Variants {
                        $($variant = $tag_val),*
                    }
                }
            }

            impl<'a> Mut<'a> {
                pub unsafe fn wrap_mut(bytes: &mut [u8]) -> Self {
                    let _offset = 0;
                    packet!(mut $tag: $tag_typ, _offset, bytes);
                    match $tag {
                        $(&mut $tag_val => {
                            $(packet!(mut $field:$typ, _offset, bytes);)+
                            Mut::$variant{ $($field: $field),* }
                        }),*
                        t => panic!("invalid variant {:?}", t),
                    }
                }

                #[allow(unused_assignments)]
                pub unsafe fn try_wrap_mut(__bytes: &mut [u8])
                -> Result<(Self, &mut [u8]), usize> {
                    let _min_total_len = 0usize;
                    let mut $tag: Option<&mut $tag_typ> = None;
                    'tag: loop {
                       packet!(trymut $tag:$tag_typ; _min_total_len, __bytes; 'tag);
                        match $tag.unwrap() {
                            $(&mut $tag_val => {
                                $(let mut $field: Option<&mut packet!(type $typ)> = None;)*
                                'trymut: loop {
                                    $(packet!(trymut $field:$typ; _min_total_len, __bytes; 'trymut);)*
                                    let _mut = Mut::$variant{ $($field: $field.unwrap()),* };
                                    return Ok((_mut, __bytes))
                                }
                                return Err(packet!(err_size $tag_typ) $(+ packet!(err_size $typ))*)
                            }),*
                            t => panic!("invalid variant {:?} @ {:?}", t, __bytes),
                        }
                    }
                    Err(min_size())
                }

                pub fn to_ref(&self) -> Ref {
                    match self {
                        $(
                            &Mut::$variant{ $(ref $field),* } =>
                                Ref::$variant{ $($field: &**$field),* }
                        ),*
                    }
                }
            }

            pub fn min_size() -> usize {
                let variant_size = ::std::usize::MAX;
                $(let variant_size = ::std::cmp::min(
                    variant_size,
                    $(packet!(min_size $typ) +)* 0usize
                );)*;
                ::std::mem::size_of::<$tag_typ>() + variant_size
            }

            packet!(impl Packet: Ref, Mut);
        }
    };

    (impl Packet: $Ref:ident, $Mut:ident) => {
        pub struct Packet;

        impl<'a> $crate::Wrap<'a> for Packet {
            type Ref = $Ref<'a>;
            type Mut = $Mut<'a>;

            unsafe fn wrap(bytes: &'a [u8]) -> (Self::Ref, &'a [u8]) {
                $Ref::try_wrap(bytes).unwrap()
            }

            unsafe fn wrap_mut(bytes: &'a mut [u8]) -> (Self::Mut, &'a mut [u8]) {
                $Mut::try_wrap_mut(bytes).unwrap()
            }

            unsafe fn try_wrap(bytes: &'a [u8]) -> Result<(Self::Ref, &'a [u8]), $crate::WrapErr> {
                $Ref::try_wrap(bytes)
            }

            unsafe fn try_wrap_mut(bytes: &'a mut [u8]) -> Result<(Self::Mut, &'a mut [u8]), usize> {
                $Mut::try_wrap_mut(bytes)
            }
        }
    };

    (ref $field:ident:[$typ:path| $count:ident], $offset:ident, $bytes:ident) => {
        let ($field, $offset): (&[$typ], usize) = {
            let count = *$count as usize;
            let size = ::std::mem::size_of::<$typ>() * count;
            let end = $offset + size;
            let bytes = (&$bytes[$offset..end]).as_ptr();
            let field = ::std::slice::from_raw_parts(bytes as *const $typ, count);
            debug_assert_eq!(
                bytes.offset(size as isize) as usize,
                field.as_ptr().offset(count as isize) as usize
            );
            (field, end)
        };
    };

    (ref $field:ident:[$typ:path; $count:expr], $offset:ident, $bytes:ident) => {
        let ($field, $offset): (&[$typ; $count], usize) = {
            let size = ::std::mem::size_of::<[$typ; $count]>();
            let end = $offset + size;
            let field = (&$bytes[$offset..end]).as_ptr()
                as *const [$typ; $count];
            (field.as_ref().unwrap(), end)
        };
    };

    (ref $field:ident:$typ:path, $offset:ident, $bytes:ident) => {
        let ($field, $offset): (&$typ, usize) = {
            let size = ::std::mem::size_of::<$typ>();
            let end = $offset + size;
            let field = (&$bytes[$offset..end]).as_ptr()
                as *const $typ;
            (field.as_ref().unwrap(), end)
        };
    };

    (mut $field:ident:[$typ:path| $count:ident], $offset:ident, $bytes:ident) => {
        let ($field, $offset): (&mut [$typ], usize) = {
            let count = *$count as usize;
            let size = ::std::mem::size_of::<$typ>() * count;
            let end = $offset + size;
            let bytes = (&mut $bytes[$offset..end]).as_mut_ptr();
            let field = ::std::slice::from_raw_parts_mut(bytes as *mut $typ, count);
            debug_assert_eq!(
                bytes.offset(size as isize) as usize,
                field.as_mut_ptr().offset(count as isize) as usize
            );
            (field, end)
        };
    };

    (mut $field:ident:[$typ:path; $count:expr], $offset:ident, $bytes:ident) => {
        let ($field, $offset): (&mut [$typ; $count], usize) = {
            let size = ::std::mem::size_of::<[$typ; $count]>();
            let end = $offset + size;
            let field = (&mut $bytes[$offset..end]).as_mut_ptr()
                as *mut [$typ; $count];
            (field.as_mut().unwrap(), end)
        };
    };

    (mut $field:ident:$typ:path, $offset:ident, $bytes:ident) => {
        let ($field, $offset): (&mut $typ, usize) = {
            let size = ::std::mem::size_of::<$typ>();
            let end = $offset + size;
            let field = (&mut $bytes[$offset..end]).as_mut_ptr()
                as *mut $typ;
            (field.as_mut().unwrap(), end)
        };
    };

    (tryrefs_t $($field:ident: $typ:tt);* - $len:ident, $bytes:ident) => {
        'tag: loop {
            $(packet!(tryref $field:$typ; $len, $bytes; 'tag);)*
            break 'tag
        }
    };

    (tryrefs $($field:ident: $typ:tt);* - $len:ident, $bytes:ident) => {
        'tryref: loop {
            $(packet!(tryref $field:$typ; $len, $bytes; 'tryref);)*
            break 'tryref
        }
    };

    (tryrefs_v $($field:ident: $typ:tt);* - $len:ident, $bytes:ident; $label:ident) => {
        $label: loop {
            $(packet!(tryref $field:$typ; $len, $bytes; $label);)*
            break $label
        }
    };

    (tryref $field:ident:[$typ:path| $count:ident]; $len:ident, $bytes:ident; $label:tt) => {
        let $len: usize = {
            let count = $count.cloned().unwrap() as usize;
            let size = ::std::mem::size_of::<$typ>() * count;
            let $len = $len + size;
            if $bytes.len() < size {
                packet!(break $label)
            }
            let (field_bytes, rem_bytes) = $bytes.split_at(size);
            let field_ptr = field_bytes.as_ptr();
            let field = ::std::slice::from_raw_parts(field_ptr as *const $typ, count);
            debug_assert_eq!(
                field_ptr.offset(size as isize) as usize,
                field.as_ptr().offset(count as isize) as usize
            );
            $bytes = rem_bytes;
            $field = Some(field);
            $len
        };
    };

    (tryref $field:ident:[$typ:path; $count:expr]; $len:ident, $bytes:ident; $label:tt) => {
        let $len: usize = {
            let size = ::std::mem::size_of::<[$typ; $count]>();
            let $len = $len + size;
            if !$bytes.len() < size {
                packet!(break $label)
            }
            let (field, rem_bytes) = $bytes.split_at(size);
            let field: &[$typ; $count] = ::std::mem::transmute(field.as_ptr());
            $bytes = rem_bytes;
            $field = Some(field);
            $len
        };
    };

    (tryref $field:ident:$typ:path; $len:ident, $bytes:ident; $label:tt) => {
        let $len: usize = {
            let size = ::std::mem::size_of::<$typ>();
            let $len = $len + size;
            if $bytes.len() < size {
                packet!(break $label)
            }
            let (field, rem_bytes) = $bytes.split_at(size);
            let field: &$typ = ::std::mem::transmute(field.as_ptr());
            $bytes = rem_bytes;
            $field = Some(field);
            $len
        };
    };

    (trymut $field:ident:[$typ:path| $count:ident]; $len:ident, $bytes:ident; $label:tt) => {
        let ($len, $bytes): (usize, &mut [u8]) = {
            fn opt_mut_clone<T: Clone>(o: &Option<&mut T>) -> Option<T> {
               o.as_ref().map(|t| <T as Clone>::clone(t))
            }
            let count = opt_mut_clone(&$count).unwrap() as usize;
            let size = ::std::mem::size_of::<$typ>() * count;
            let $len = $len + size;
            if $bytes.len() < size {
                packet!(break $label)
            }
            let (field_bytes, rem_bytes) = $bytes.split_at_mut(size);
            let field_ptr = field_bytes.as_mut_ptr();
            let field = ::std::slice::from_raw_parts_mut(field_ptr as *mut $typ, count);
            debug_assert_eq!(
                field_ptr.offset(size as isize) as usize,
                field.as_ptr().offset(count as isize) as usize
            );
            $field = Some(field);
            ($len, rem_bytes)
        };
    };

    (trymut $field:ident:[$typ:path; $count:expr]; $len:ident, $bytes:ident; $label:tt) => {
        let ($len, $bytes): (usize, &mut [u8]) = {
            let size = ::std::mem::size_of::<[$typ; $count]>();
            let $len = $len + size;
            if !$bytes.len() < size {
                packet!(break $label)
            }
            let (field, rem_bytes) = $bytes.split_at_mut(size);
            let field: &mut [$typ; $count] = ::std::mem::transmute(field.as_mut_ptr());
            $field = Some(field);
            ($len, rem_bytes)
        };
    };

    (trymut $field:ident:$typ:path; $len:ident, $bytes:ident; $label:tt) => {
        let ($len, $bytes): (usize, &mut [u8]) = {
            let size = ::std::mem::size_of::<$typ>();
            let $len = $len + size;
            if $bytes.len() < size {
                packet!(break $label)
            }
            let (field, rem_bytes) = $bytes.split_at_mut(size);
            let field: &mut $typ = ::std::mem::transmute(field.as_mut_ptr());
            $field = Some(field);
            ($len, rem_bytes)
        };
    };

    (write $field:ident:[$typ:path| $count:ident], $bytes:ident) => {
        unsafe {
            let count = *$count as usize;
            let size = ::std::mem::size_of::<$typ>() * count;
            let field_bytes = $field.as_ptr() as *const u8;
            let field_slice = ::std::slice::from_raw_parts(field_bytes, size);
            $bytes.extend_from_slice(field_slice)
        };
    };

    (write $field:ident:[$typ:path; $count:expr], $bytes:ident) => {
        unsafe {
            let size = ::std::mem::size_of::<[$typ; $count]>();
            let field_bytes = $field as *const [$typ; $count] as *const u8;
            let field_slice = ::std::slice::from_raw_parts(field_bytes, size);
            $bytes.extend_from_slice(field_slice)
        };
    };

    (write $field:ident:$typ:path, $bytes:ident) => {
        unsafe {
            let size = ::std::mem::size_of::<$typ>();
            let field_bytes = $field as *const $typ as *const u8;
            let field_slice = ::std::slice::from_raw_parts(field_bytes, size);
            $bytes.extend_from_slice(field_slice)
        };
    };

    (type [$typ:path; $count:expr]) => {
        [$typ; $count]
    };

    (type [$typ:path| $count:ident]) => {
        [$typ]
    };

    (type $typ:path) => {
        $typ
    };

    (min_size [$typ:path| $count:ident]) => {
        0
    };

    (min_size [$typ:path; $count:expr]) => {
        ::std::mem::size_of::<[$typ; $count]>()
    };

    (min_size $typ:path) => {
        ::std::mem::size_of::<$typ>()
    };

    //(err_sizes ($t:tt)) => {
    //    (packet!(err_size $t))
    //};

    (err_size [$typ:path| $count:ident]) => {
        (::std::mem::size_of::<$typ>() * ($count.as_ref().map(|c| **c).unwrap_or(0)) as usize)
    };

    (err_size $t:tt) => {
        (packet!(min_size $t))
    };

    /*(err_size_mut [$typ:path| $count:ident]) => {
        (::std::mem::size_of::<$typ>() * $coun.as_ref().unwrap_or(&&mut 0).clone() as usize)
    };

    (err_size_mut $t:tt) => {
        (packet!(err_size $t))
    };*/

    (break $label:tt) => { break $label };
}

packet!{
    enum Test {
        tag: u8,
        Variant1: 1 => { size: u16,  bytes: [u8| size] },
        Variant2: 2 => { flags: u32 },
        Variant4: 4 => { gloob: [u32; 3] },
        Variant3: 3 => { size: u32,  bytes: [u8| size], words: [u16| size], end: u8 },
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    packet!{
        struct TestField {
            field: u16,
        }
    }

    packet!{
        struct TestVarSize {
            size: u16,
            bytes: [u8| size]
        }
    }

    packet!{
        struct TestFixSize {
            bytes: [u8; 8]
        }
    }

    packet!{
        enum TestEnum {
            tag: u8,
            Variant1: 1 => { size: u16,  bytes: [u8| size] },
            Variant2: 2 => { flags: u32 },
        }
    }

    pub type OrderIndex = (u64, u64);
    pub type Uuid = [u8; 16];

    packet! {
        enum Log {
            tag: u8,
            Read: 3 => { flags: u8, loc: OrderIndex, },
            Data: 1 => {
                flags: u8,
                data_bytes: u16,
                num_locs: u16, // read as 1
                num_deps: u16,
                id: Uuid,
                loc: OrderIndex,
                deps: [OrderIndex| num_deps],
                data: [u8| data_bytes],
            },
            Multi: 2 => {
                flags: u8,
                data_bytes: u16,
                num_locs: u16,
                num_deps: u16,
                id: Uuid,
                locs: [OrderIndex| num_locs],
                deps: [OrderIndex| num_deps],
                data: [u8| data_bytes],
            },
            //TODO
            Senti: 6 => {
                flags: u8,
                data_bytes: u16,
                num_locs: u16,
                num_deps: u16,
                id: Uuid,
                locs: [OrderIndex| num_locs],
                lock_num: u64,
            },
            NoData: 7 => { loc: OrderIndex, last_valid: OrderIndex },
            Unlock: 4 => { num_locs: u16, locs: [OrderIndex| num_locs] },
        }
    }

    #[test]
    fn struct_test() {
        let val = TestVarSize::Ref{ size: &3, bytes: &[2, 7, 0xf] };
        let mut vec = Vec::new();
        val.fill_vec(&mut vec);
        let wrap = unsafe { TestVarSize::Ref::wrap(&vec[..]) };
        assert_eq!(val, wrap);
    }

    #[test]
    fn struct_mut_test() {
        let val = TestVarSize::Mut{ size: &mut 3, bytes: &mut [2, 7, 0xf] };
        let mut vec = Vec::new();
        val.to_ref().fill_vec(&mut vec);
        let wrap = unsafe { TestVarSize::Ref::wrap(&vec[..]) };
        assert_eq!(val.to_ref(), wrap);
    }

    #[test]
    fn fix_struct_test() {
        let val = TestFixSize::Ref{bytes: &[2, 7, 0xf, 12, 42, 11, 0, 1] };
        let mut vec = Vec::new();
        val.fill_vec(&mut vec);
        assert_eq!(vec.len(), 8);
        let wrap = unsafe { TestFixSize::Ref::wrap(&vec[..]) };
        assert_eq!(val, wrap);
    }

    #[test]
    fn struct_empty_test1() {
        let val = TestVarSize::Ref{ size: &0, bytes: &[2, 7, 0xf] };
        let mut vec = Vec::new();
        val.fill_vec(&mut vec);
        let wrap = unsafe { TestVarSize::Ref::wrap(&vec[..]) };
        assert_eq!(wrap, TestVarSize::Ref{ size: &0, bytes: &[] });
    }

    #[test]
    fn struct_empty_test2() {
        let val = TestVarSize::Ref{ size: &0, bytes: &[] };
        let mut vec = Vec::new();
        val.fill_vec(&mut vec);
        let wrap = unsafe { TestVarSize::Ref::wrap(&vec[..]) };
        assert_eq!(wrap, val);
    }

    #[test]
    fn struct_size_try_ref_ok_test() {
        let val = TestVarSize::Ref{ size: &3, bytes: &[2, 7, 0xf] };
        let mut vec = Vec::new();
        val.fill_vec(&mut vec);
        let wrap = unsafe { TestVarSize::Ref::try_wrap(&vec[..]) };
        assert_eq!(Ok((val, &[][..])), wrap);
    }

    #[test]
    fn struct_size_try_ref_err_test() {
        let vec: Vec<_> = (0..(TestVarSize::min_size() - 1)).map(|_| 0u8).collect();
        let wrap = unsafe { TestVarSize::Ref::try_wrap(&vec[..]) };
        assert_eq!(Err(WrapErr::NotEnoughBytes(2)), wrap);
    }

    #[test]
    fn struct_size_try_ref_var_err_test() {
        let val = TestVarSize::Ref{ size: &3, bytes: &[2, 7, 0xf] };
        let mut vec = Vec::new();
        val.fill_vec(&mut vec);
        {
            let wrap = unsafe { TestVarSize::Ref::try_wrap(&vec[..3]) };
            assert_eq!(Err(WrapErr::NotEnoughBytes(5)), wrap);
        }
        let wrap = unsafe { TestVarSize::Ref::try_wrap(&vec[..]) };
        assert_eq!(Ok((val, &[][..])), wrap);
    }

    #[test]
    fn struct_size_try_ref_var_extra_test() {
        let val = TestVarSize::Ref{ size: &3, bytes: &[2, 7, 0xf] };
        let mut vec = Vec::new();
        val.fill_vec(&mut vec);
        vec.extend_from_slice(&[0xfe, 0xef, 0x9]);
        {
            let wrap = unsafe { TestVarSize::Ref::try_wrap(&vec[..4]) };
            assert_eq!(Err(WrapErr::NotEnoughBytes(5)), wrap);
        }
        let wrap = unsafe { TestVarSize::Ref::try_wrap(&vec[..]) };
        assert_eq!(Ok((val, &[0xfe, 0xef, 0x9][..])), wrap);
    }

    #[test]
    fn struct_size_min_size_test() {
        assert_eq!(TestField::min_size(), 2);
    }

    #[test]
    fn struct_var_size_min_size_test() {
        assert_eq!(TestVarSize::min_size(), 2);
    }

    #[test]
    fn enum_test1() {
        let val = TestEnum::Ref::Variant1{ size: &3, bytes: &[2, 7, 0xf] };
        let mut vec = Vec::new();
        val.fill_vec(&mut vec);
        let wrap = unsafe { TestEnum::Ref::wrap(&vec[..]) };
        assert_eq!(val, wrap);
    }

    #[test]
    fn enum_test2() {
        let val = TestEnum::Ref::Variant2{ flags: &0xdeadbeef };
        let mut vec = Vec::new();
        val.fill_vec(&mut vec);
        let wrap = unsafe { TestEnum::Ref::wrap(&vec[..]) };
        assert_eq!(val, wrap);
    }

    #[test]
    fn enum_test_size() {
        assert_eq!(TestEnum::min_size(), 3);
    }

    #[test]
    fn enum_test_log_size() {
        assert_eq!(Log::min_size(), 3);
    }

    packet!{
        enum TestEnum2 {
            tag: u8,
            Variant1: 1 => { size: u16, padding: u32,  bytes: [u8| size] },
            Variant2: 2 => { flags: u32 },
        }
    }

    #[test]
    fn test_partial_wraps() {
        let val = TestEnum2::Ref::Variant1{size: &4, padding: &0xfeed, bytes: &[7, 22, 1, 255]};
        let mut vec = Vec::new();
        val.fill_vec(&mut vec);
        vec.extend_from_slice(&[1, 2, 3, 4, 5, 6]);
        let wrap = unsafe { TestEnum2::Ref::try_wrap(&vec[..0]) };
        assert_eq!(Err(WrapErr::NotEnoughBytes(5)), wrap);
        let wrap = unsafe { TestEnum2::Ref::try_wrap(&vec[..1]) };
        assert_eq!(Err(WrapErr::NotEnoughBytes(7)), wrap);
        let wrap = unsafe { TestEnum2::Ref::try_wrap(&vec[..2]) };
        assert_eq!(Err(WrapErr::NotEnoughBytes(7)), wrap);
        for i in 3..7 {
            let wrap = unsafe { TestEnum2::Ref::try_wrap(&vec[..i]) };
            assert_eq!(Err(WrapErr::NotEnoughBytes(11)), wrap);
        }
        for i in 7..11 {
            let wrap = unsafe { TestEnum2::Ref::try_wrap(&vec[..i]) };
            assert_eq!(Err(WrapErr::NotEnoughBytes(11)), wrap);
        }
        let wrap = unsafe { TestEnum2::Ref::try_wrap(&vec[..11]) };
        assert_eq!(Ok((val, &[][..])), wrap);
        let wrap = unsafe { TestEnum2::Ref::try_wrap(&vec[..12]) };
        assert_eq!(Ok((val, &[1][..])), wrap);
        let wrap = unsafe { TestEnum2::Ref::try_wrap(&vec[..13]) };
        assert_eq!(Ok((val, &[1, 2][..])), wrap);
        let wrap = unsafe { TestEnum2::Ref::try_wrap(&vec[..14]) };
        assert_eq!(Ok((val, &[1, 2, 3][..])), wrap);
        let wrap = unsafe { TestEnum2::Ref::try_wrap(&vec[..]) };
        assert_eq!(Ok((val, &[1, 2, 3, 4, 5, 6][..])), wrap);
    }

}

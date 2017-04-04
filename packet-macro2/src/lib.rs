//based on proc-macro-hack and procedural-masquerade

#[allow(unused_imports)]
#[macro_use] extern crate packet_macro_impl;

#[macro_export]
macro_rules! define_packet {
    (struct $name:ident $t:tt) => {
        #[allow(non_snake_case)]
        pub mod $name {
            #![allow(unused_variables)]

            #[allow(unused_imports)]
            use super::*;

            #[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
            pub enum WrapErr {
                BadTag,
                NotEnoughBytes(usize),
            }

            #[derive(packet_macro_impl)]
            #[allow(unused)]
            enum ProceduralMasqueradeDummyType {
                Input = (0, stringify!(struct $t) ).0
            }
        }
    };
    (enum $name:ident $t:tt) => {
        #[allow(non_snake_case)]
        pub mod $name {
            #![allow(unused_variables)]

            #[allow(unused_imports)]
            use super::*;

            #[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
            pub enum WrapErr {
                BadTag,
                NotEnoughBytes(usize),
            }

            #[derive(packet_macro_impl)]
            #[allow(unused)]
            enum ProceduralMasqueradeDummyType {
                Input = (0, stringify!(enum $t) ).0
            }
        }
    };
}
/*
define_packet!{
    struct TestVarA {
        counters: u16,
        bytes: [u8 | counters]
    }
}*/

#[cfg(test)]
mod tests {
    #[allow(dead_code)]
    define_packet!{
        struct TestField {
            field: u16,
        }
    }

    #[allow(dead_code)]
    define_packet!{
        struct TestArray {
            field: u16,
            bytes: [u8; 8]
        }
    }

    #[allow(dead_code)]
    define_packet!{
        struct TestVarArray {
            size: u16,
            bytes: [u8 | size]
        }
    }

    #[test]
    fn array_struct_test() {
        let val = TestArray::Ref{field: &13, bytes: &[2, 7, 0xf, 12, 42, 11, 0, 1] };
        let mut vec = Vec::new();
        val.fill_vec(&mut vec);
        assert_eq!(vec.len(), 10);
        let wrap = unsafe { TestArray::Ref::try_ref(&vec[..]).unwrap().0 };
        assert_eq!(val, wrap);
        assert_eq!(val.len(), vec.len());
    }

    #[test]
    fn struct_test() {
        let val = TestVarArray::Ref{bytes: &[2, 7, 0xf] };
        let mut vec = Vec::new();
        val.fill_vec(&mut vec);
        let wrap = unsafe { TestVarArray::Ref::try_ref(&vec[..]).unwrap().0 };
        assert_eq!(val, wrap);
        assert_eq!(val.len(), vec.len());
    }

    #[test]
    fn struct_empty_test1() {
        let mut vec: Vec<_> = (0..5).map(|_| 0u8).collect();
        let wrap = unsafe { TestVarArray::Ref::try_ref(&vec[..]).unwrap().0 };
        assert_eq!(wrap, TestVarArray::Ref{ bytes: &[] });
    }

    #[test]
    fn struct_empty_test2() {
        let val = TestVarArray::Ref{ bytes: &[] };
        let mut vec = Vec::new();
        val.fill_vec(&mut vec);
        let wrap = unsafe { TestVarArray::Ref::try_ref(&vec[..]).unwrap().0 };
        assert_eq!(wrap, val);
        assert_eq!(val.len(), vec.len());
    }

    #[test]
    fn struct_size_try_ref_ok_test() {
        let val = TestVarArray::Ref{ bytes: &[2, 7, 0xf] };
        let mut vec = Vec::new();
        val.fill_vec(&mut vec);
        let wrap = unsafe { TestVarArray::Ref::try_ref(&vec[..]) };
        assert_eq!(Ok((val, &[][..])), wrap);
        assert_eq!(val.len(), vec.len());
    }

    #[test]
    fn struct_size_try_ref_err_test() {
        use self::TestVarArray::WrapErr;
        let vec: Vec<_> = (0..(TestVarArray::min_len() - 1)).map(|_| 0u8).collect();
        let wrap = unsafe { TestVarArray::Ref::try_ref(&vec[..]) };
        assert_eq!(Err(WrapErr::NotEnoughBytes(2)), wrap);
    }

    #[test]
    fn struct_size_try_ref_var_err_test() {
        use self::TestVarArray::WrapErr;
        let val = TestVarArray::Ref{ bytes: &[2, 7, 0xf] };
        let mut vec = Vec::new();
        val.fill_vec(&mut vec);
        {
            let wrap = unsafe { TestVarArray::Ref::try_ref(&vec[..3]) };
            assert_eq!(Err(WrapErr::NotEnoughBytes(5)), wrap);
        }
        let wrap = unsafe { TestVarArray::Ref::try_ref(&vec[..]) };
        assert_eq!(Ok((val, &[][..])), wrap);
        assert_eq!(val.len(), vec.len());
    }

    #[test]
    fn struct_size_try_ref_var_extra_test() {
        use self::TestVarArray::WrapErr;
        let val = TestVarArray::Ref{ bytes: &[2, 7, 0xf] };
        let mut vec = Vec::new();
        val.fill_vec(&mut vec);
        vec.extend_from_slice(&[0xfe, 0xef, 0x9]);
        {
            let wrap = unsafe { TestVarArray::Ref::try_ref(&vec[..4]) };
            assert_eq!(Err(WrapErr::NotEnoughBytes(5)), wrap);
        }
        let wrap = unsafe { TestVarArray::Ref::try_ref(&vec[..]) };
        assert_eq!(Ok((val, &[0xfe, 0xef, 0x9][..])), wrap);
    }

    #[test]
    fn struct_size_min_size_test() {
        assert_eq!(TestField::min_len(), 2);
    }

    #[test]
    fn struct_var_size_min_size_test() {
        assert_eq!(TestArray::min_len(), 10);
    }

    #[test]
    fn struct_ar_size_min_size_test() {
        assert_eq!(TestVarArray::min_len(), 2);
    }

    /*#[test]
    fn print_string0() {
        TestField::print_string()
    }

    #[test]
    fn print_string1() {
        TestArray::print_string()
    }

    #[test]
    fn print_string2() {
        TestVarArray::print_string()
    }*/

    define_packet!{
        enum TestEnum {
            tag: u8,
            Variant1: 1 => { size: u16,  bytes: [u8| size] },
            Variant2: 2 => { flags: u32 },
        }
    }

    define_packet!{
        enum TestEmptyVariant {
            tag: u8,
            Variant1: 1 => { size: u16,  bytes: [u8| size] },
            Variant2: 2 => { flags: u32 },
            Variant3: 3 => {},
        }
    }

    #[test]
    fn enum_test_min_len() {
        assert_eq!(TestEnum::min_len(), 3);
        assert_eq!(TestEmptyVariant::min_len(), 1);
    }

    #[test]
    fn enum_test1() {
        let val = TestEnum::Ref::Variant1{bytes: &[2, 7, 0xf, 0xb] };
        let mut vec = Vec::new();
        val.fill_vec(&mut vec);
        let wrap = unsafe { TestEnum::Ref::try_ref(&vec[..]).unwrap().0 };
        assert_eq!(val, wrap);
        assert_eq!(val.len(), vec.len());
    }

    #[test]
    fn enum_test2() {
        let val = TestEnum::Ref::Variant2{flags: &0xf00fbad };
        let mut vec = Vec::new();
        val.fill_vec(&mut vec);
        let wrap = unsafe { TestEnum::Ref::try_ref(&vec[..]).unwrap().0 };
        assert_eq!(val, wrap);
        assert_eq!(val.len(), vec.len());
    }

    #[test]
    fn enum_test_mut1() {
        let val = TestEnum::Mut::Variant1{bytes: &mut [2, 7, 0xf, 0xb] };
        let mut vec = Vec::new();
        val.as_ref().fill_vec(&mut vec);
        let wrap = unsafe { TestEnum::Mut::try_mut(&mut vec[..]).unwrap().0 };
        assert_eq!(val, wrap);
        assert_eq!(val.as_ref().len(), vec.len());
    }

    #[test]
    fn enum_test_mut2() {
        let val = TestEnum::Mut::Variant2{flags: &mut 0xf00fbad };
        let mut vec = Vec::new();
        val.as_ref().fill_vec(&mut vec);
        let wrap = unsafe { TestEnum::Mut::try_mut(&mut vec[..]).unwrap().0 };
        assert_eq!(val, wrap);
        assert_eq!(val.as_ref().len(), vec.len());
    }

    /*#[test]
    fn print_enum() {
        TestEnum::print_string()
    }*/
}

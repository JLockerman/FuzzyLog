
macro_rules! async_tests {
    (test $new_thread_log:ident) => (

        use packets::*;
        use async::fuzzy_log::*;
        use async::fuzzy_log::log_handle::LogHandle;

        use std::collections::{HashMap, HashSet};

        //TODO move to crate root under cfg...
        extern crate env_logger;

        #[test]
        #[inline(never)]
        fn test_get_none() {
            let _ = env_logger::init();
            let mut lh = $new_thread_log::<()>(vec![1.into()]);
            assert_eq!(lh.get_next(), None);
        }

        #[test]
        #[inline(never)]
        fn test_get_none2() {
            let _ = env_logger::init();
            let mut lh = $new_thread_log::<()>(vec![1.into()]);
            lh.snapshot(1.into());
            assert_eq!(lh.get_next(), None);
        }

        #[test]
        #[inline(never)]
        pub fn test_1_column() {
            let _ = env_logger::init();
            trace!("TEST 1 column");
            let mut lh = $new_thread_log::<i32>(vec![3.into()]);
            let _ = lh.append(3.into(), &1, &[]);
            let _ = lh.append(3.into(), &17, &[]);
            let _ = lh.append(3.into(), &32, &[]);
            let _ = lh.append(3.into(), &-1, &[]);
            lh.snapshot(3.into());
            assert_eq!(lh.get_next(), Some((&1,  &[OrderIndex(3.into(), 1.into())][..])));
            assert_eq!(lh.get_next(), Some((&17, &[OrderIndex(3.into(), 2.into())][..])));
            assert_eq!(lh.get_next(), Some((&32, &[OrderIndex(3.into(), 3.into())][..])));
            assert_eq!(lh.get_next(), Some((&-1, &[OrderIndex(3.into(), 4.into())][..])));
            assert_eq!(lh.get_next(), None);
        }

        #[test]
        #[inline(never)]
        pub fn test_3_column() {
            let _ = env_logger::init();
            trace!("TEST 3 column");

            let mut lh = $new_thread_log::<i32>(vec![4.into(), 5.into(), 6.into()]);
            let cols = vec![vec![12, 19, 30006, 122, 9],
                vec![45, 111111, -64, 102, -10101],
                vec![-1, -2, -9, 16, -108]];
            for (j, col) in cols.iter().enumerate() {
                for i in col.iter() {
                    let _ = lh.append(((j + 4) as u32).into(), i, &[]);
                }
            }
            lh.snapshot(4.into());
            lh.snapshot(6.into());
            lh.snapshot(5.into());
            let mut is = [0u32, 0, 0, 0];
            let total_len = cols.iter().fold(0, |len, col| len + col.len());
            for _ in 0..total_len {
                let next = lh.get_next();
                assert!(next.is_some());
                let (&n, ois) = next.unwrap();
                assert_eq!(ois.len(), 1);
                let OrderIndex(o, i) = ois[0];
                let off: u32 = (o - 4).into();
                is[off as usize] = is[off as usize] + 1;
                let i: u32 = i.into();
                assert_eq!(is[off as usize], i);
                let c = is[off as usize] - 1;
                assert_eq!(n, cols[off as usize][c as usize]);
            }
            assert_eq!(lh.get_next(), None);
        }

        #[test]
        #[inline(never)]
        pub fn test_read_deps() {
            let _ = env_logger::init();
            trace!("TEST read deps");

            let mut lh = $new_thread_log::<i32>(vec![7.into(), 8.into()]);

            let _ = lh.append(7.into(), &63,  &[]);
            let _ = lh.append(8.into(), &-2,  &[OrderIndex(7.into(), 1.into())]);
            let _ = lh.append(8.into(), &-56, &[]);
            let _ = lh.append(7.into(), &111, &[OrderIndex(8.into(), 2.into())]);
            let _ = lh.append(8.into(), &0,   &[OrderIndex(7.into(), 2.into())]);
            lh.snapshot(8.into());
            lh.snapshot(7.into());
            assert_eq!(lh.get_next(), Some((&63,  &[OrderIndex(7.into(), 1.into())][..])));
            assert_eq!(lh.get_next(), Some((&-2,  &[OrderIndex(8.into(), 1.into())][..])));
            assert_eq!(lh.get_next(), Some((&-56, &[OrderIndex(8.into(), 2.into())][..])));
            assert_eq!(lh.get_next(), Some((&111, &[OrderIndex(7.into(), 2.into())][..])));
            assert_eq!(lh.get_next(), Some((&0,   &[OrderIndex(8.into(), 3.into())][..])));
            assert_eq!(lh.get_next(), None);
        }

        #[test]
        #[inline(never)]
        pub fn test_long() {
            let _ = env_logger::init();
            trace!("TEST long");

            let mut lh = $new_thread_log::<i32>(vec![9.into()]);
            for i in 0..19i32 {
                trace!("LONG append {}", i);
                let _ = lh.append(9.into(), &i, &[]);
            }
            lh.snapshot(9.into());
            for i in 0..19i32 {
                let u = i as u32;
                trace!("LONG read {}", i);
                assert_eq!(lh.get_next(), Some((&i,  &[OrderIndex(9.into(), (u + 1).into())][..])));
            }
            assert_eq!(lh.get_next(), None);
        }

        #[test]
        #[inline(never)]
        pub fn test_wide() {
            let _ = env_logger::init();
            trace!("TEST wide");

            let interesting_chains: Vec<_> = (10..21).map(|i| i.into()).collect();
            let mut lh = $new_thread_log(interesting_chains.clone());
            for &i in &interesting_chains {
                if i > 10.into() {
                    let _ = lh.append(i.into(), &i, &[OrderIndex(i - 1, 1.into())]);
                }
                else {
                    let _ = lh.append(i.into(), &i, &[]);
                }

            }
            lh.snapshot(20.into());
            for &i in &interesting_chains {
                assert_eq!(lh.get_next(), Some((&i,  &[OrderIndex(i, 1.into())][..])));
            }
            assert_eq!(lh.get_next(), None);
        }

        #[test]
        #[inline(never)]
        pub fn test_append_after_fetch() {
            let _ = env_logger::init();
            trace!("TEST append after fetch");

            let mut lh = $new_thread_log(vec![21.into()]);
            for i in 0u32..10 {
                let _ = lh.append(21.into(), &i, &[]);
            }
            lh.snapshot(21.into());
            for i in 0u32..10 {
                assert_eq!(lh.get_next(), Some((&i,  &[OrderIndex(21.into(), (i + 1).into())][..])));
            }
            assert_eq!(lh.get_next(), None);
            for i in 10u32..21 {
                let _ = lh.append(21.into(), &i, &[]);
            }
            lh.snapshot(21.into());
            for i in 10u32..21 {
                assert_eq!(lh.get_next(), Some((&i,  &[OrderIndex(21.into(), (i + 1).into())][..])));
            }
            assert_eq!(lh.get_next(), None);
        }

        #[test]
        #[inline(never)]
        pub fn test_append_after_fetch_short() {
            let _ = env_logger::init();
            trace!("TEST append after fetch short");

            let mut lh = $new_thread_log(vec![22.into()]);
            for i in 0u32..2 {
                let _ = lh.append(22.into(), &i, &[]);
            }
            lh.snapshot(22.into());
            for i in 0u32..2 {
                assert_eq!(lh.get_next(), Some((&i,  &[OrderIndex(22.into(), (i + 1).into())][..])));
            }
            assert_eq!(lh.get_next(), None);
            for i in 2u32..4 {
                let _ = lh.append(22.into(), &i, &[]);
            }
            lh.snapshot(22.into());
            for i in 2u32..4 {
                assert_eq!(lh.get_next(), Some((&i,  &[OrderIndex(22.into(), (i + 1).into())][..])));
            }
            assert_eq!(lh.get_next(), None);
        }

        #[test]
        #[inline(never)]
        pub fn test_multi1() {
            let _ = env_logger::init();
            trace!("TEST multi");

            let columns = vec![23.into(), 24.into(), 25.into()];
            let mut lh = $new_thread_log::<u64>(columns.clone());
            let _ = lh.multiappend(&columns, &0xfeed, &[]);
            let _ = lh.multiappend(&columns, &0xbad , &[]);
            let _ = lh.multiappend(&columns, &0xcad , &[]);
            let _ = lh.multiappend(&columns, &13    , &[]);
            lh.snapshot(24.into());
            assert_eq!(lh.get_next(), Some((&0xfeed, &[OrderIndex(23.into(), 1.into()),
                OrderIndex(24.into(), 1.into()), OrderIndex(25.into(), 1.into())][..])));
            assert_eq!(lh.get_next(), Some((&0xbad , &[OrderIndex(23.into(), 2.into()),
                OrderIndex(24.into(), 2.into()), OrderIndex(25.into(), 2.into())][..])));
            assert_eq!(lh.get_next(), Some((&0xcad , &[OrderIndex(23.into(), 3.into()),
                OrderIndex(24.into(), 3.into()), OrderIndex(25.into(), 3.into())][..])));
            assert_eq!(lh.get_next(), Some((&13    , &[OrderIndex(23.into(), 4.into()),
                OrderIndex(24.into(), 4.into()), OrderIndex(25.into(), 4.into())][..])));
            assert_eq!(lh.get_next(), None);
        }

        #[test]
        #[inline(never)]
        pub fn test_multi_shingled() {
            let _ = env_logger::init();
            trace!("TEST multi shingled");

            let columns = vec![26.into(), 27.into(), 28.into(), 29.into(), 30.into()];
            let mut lh = $new_thread_log::<u64>(columns.clone());
            for (i, cols) in columns.windows(2).rev().enumerate() {
                let i = i as u64;
                let _ = lh.multiappend(&cols, &((i + 1) * 2), &[]);
            }
            lh.snapshot(26.into());
            assert_eq!(lh.get_next(),
                Some((&2, &[OrderIndex(29.into(), 1.into()), OrderIndex(30.into(), 1.into())][..])));
            assert_eq!(lh.get_next(),
                Some((&4, &[OrderIndex(28.into(), 1.into()), OrderIndex(29.into(), 2.into())][..])));
            assert_eq!(lh.get_next(),
                Some((&6, &[OrderIndex(27.into(), 1.into()), OrderIndex(28.into(), 2.into())][..])));
            assert_eq!(lh.get_next(),
                Some((&8, &[OrderIndex(26.into(), 1.into()), OrderIndex(27.into(), 2.into())][..])));
            assert_eq!(lh.get_next(), None);
        }

        #[test]
        #[inline(never)]
        pub fn test_multi_wide() {
            let _ = env_logger::init();
            trace!("TEST multi wide");

            let columns: Vec<_> = (31..45).map(Into::into).collect();
            let mut lh = $new_thread_log::<u64>(columns.clone());
            let _ = lh.multiappend(&columns, &82352  , &[]);
            let _ = lh.multiappend(&columns, &018945 , &[]);
            let _ = lh.multiappend(&columns, &119332 , &[]);
            let _ = lh.multiappend(&columns, &0      , &[]);
            let _ = lh.multiappend(&columns, &17     , &[]);
            lh.snapshot(33.into());
            let locs: Vec<_> = columns.iter().map(|&o| OrderIndex(o, 1.into())).collect();
            assert_eq!(lh.get_next(), Some((&82352 , &locs[..])));
            let locs: Vec<_> = columns.iter().map(|&o| OrderIndex(o, 2.into())).collect();
            assert_eq!(lh.get_next(), Some((&018945, &locs[..])));
            let locs: Vec<_> = columns.iter().map(|&o| OrderIndex(o, 3.into())).collect();
            assert_eq!(lh.get_next(), Some((&119332, &locs[..])));
            let locs: Vec<_> = columns.iter().map(|&o| OrderIndex(o, 4.into())).collect();
            assert_eq!(lh.get_next(), Some((&0     , &locs[..])));
            let locs: Vec<_> = columns.iter().map(|&o| OrderIndex(o, 5.into())).collect();
            assert_eq!(lh.get_next(), Some((&17    , &locs[..])));
            assert_eq!(lh.get_next(), None);
        }

        #[test]
        #[inline(never)]
        pub fn test_multi_deep() {
            let _ = env_logger::init();
            trace!("TEST multi deep");

            let columns: Vec<_> = (45..49).map(Into::into).collect();
            let mut lh = $new_thread_log::<u32>(columns.clone());
            for i in 1..32 {
                let _ = lh.multiappend(&columns, &i, &[]);
            }
            lh.snapshot(48.into());
            for i in 1..32 {
                let locs: Vec<_> = columns.iter()
                    .map(|&o| OrderIndex(o, i.into())).collect();
                assert_eq!(lh.get_next(), Some((&i , &locs[..])));
            }
            assert_eq!(lh.get_next(), None);
        }

        #[test]
        #[inline(never)]
        pub fn test_dependent_multi1() {
            let _ = env_logger::init();
            trace!("TEST multi");

            let columns = vec![49.into(), 50.into(), 51.into()];
            let mut lh = $new_thread_log::<u64>(columns.clone());
            let _ = lh.append(50.into(), &22, &[]);
            let _ = lh.append(51.into(), &11, &[]);
            let _ = lh.append(49.into(), &0xf0000, &[]);
            let _ = lh.dependent_multiappend(&[49.into()], &[50.into(), 51.into()], &0xbaaa, &[]);
            lh.snapshot(49.into());
            {
                let potential_vals: [_; 3] =
                    [(22     , vec![OrderIndex(50.into(), 1.into())]),
                     (11     , vec![OrderIndex(51.into(), 1.into())]),
                     (0xf0000, vec![OrderIndex(49.into(), 1.into())])
                    ];
                let mut potential_vals: HashMap<_, _> = potential_vals.into_iter().cloned().collect();
                for _ in 0..3 {
                    let next_val = &lh.get_next().expect("should find val");
                    let locs = potential_vals.remove(next_val.0).expect("must be expected");
                    assert_eq!(next_val.1, &locs[..]);
                }
            }
            assert_eq!(lh.get_next(),
                Some((&0xbaaa,
                    &[OrderIndex(49.into(), 2.into()),
                      OrderIndex( 0.into(), 0.into()),
                      OrderIndex(50.into(), 2.into()),
                      OrderIndex(51.into(), 2.into())
                     ][..])));
            assert_eq!(lh.get_next(), None);
        }

        #[test]
        #[inline(never)]
        pub fn test_dependent_multi_with_early_fetch() {
            let _ = env_logger::init();
            trace!("TEST multi");

            let columns = vec![52.into(), 53.into(), 54.into()];
            let mut lh = $new_thread_log::<i64>(columns.clone());
            let _ = lh.append(52.into(), &99999, &[]);
            let _ = lh.append(53.into(), &101, &[]);
            let _ = lh.append(54.into(), &-99, &[]);
            let _ = lh.dependent_multiappend(&[53.into()], &[52.into(), 54.into()], &-7777, &[]);
            lh.snapshot(52.into());
            lh.snapshot(54.into());
            {
                let potential_vals =
                    [(99999, vec![OrderIndex(52.into(), 1.into())]),
                     (-99  , vec![OrderIndex(54.into(), 1.into())]),
                    ];
                let mut potential_vals: HashMap<_, _> = potential_vals.into_iter().cloned().collect();
                for _ in 0..2 {
                    let next_val = &lh.get_next().expect("should find val");
                    match potential_vals.remove(next_val.0) {
                        Some(locs) => assert_eq!(next_val.1, &locs[..]),
                        None => panic!("unexpected val {:?}", next_val),
                    }

                }
            }
            lh.snapshot(53.into());
            assert_eq!(lh.get_next(), Some((&101, &[OrderIndex(53.into(), 1.into())][..])));
            assert_eq!(lh.get_next(),
                Some((&-7777,
                    &[OrderIndex(53.into(), 2.into()),
                      OrderIndex( 0.into(), 0.into()),
                      OrderIndex(52.into(), 2.into()),
                      OrderIndex(54.into(), 2.into())
                     ][..])));
            assert_eq!(lh.get_next(), None);
        }

        #[test]
        #[inline(never)]
        pub fn test_dependent_multi_with_partial_early_fetch() {
            let _ = env_logger::init();
            trace!("TEST multi");

            let columns = vec![55.into(), 56.into(), 57.into()];
            let mut lh = $new_thread_log::<i64>(columns.clone());
            let _ = lh.append(55.into(), &99999, &[]);
            let _ = lh.append(56.into(), &101, &[]);
            let _ = lh.append(57.into(), &-99, &[]);
            let _ = lh.dependent_multiappend(&[55.into()], &[56.into(), 57.into()], &-7777, &[]);
            lh.snapshot(56.into());
            assert_eq!(lh.get_next(), Some((&101, &[OrderIndex(56.into(), 1.into())][..])));
            lh.snapshot(55.into());
            assert_eq!(lh.get_next(), Some((&99999, &[OrderIndex(55.into(), 1.into())][..])));
            assert_eq!(lh.get_next(), Some((&-99, &[OrderIndex(57.into(), 1.into())][..])));
            assert_eq!(lh.get_next(),
                Some((&-7777,
                    &[OrderIndex(55.into(), 2.into()),
                      OrderIndex( 0.into(), 0.into()),
                      OrderIndex(56.into(), 2.into()),
                      OrderIndex(57.into(), 2.into())
                     ][..])));
            assert_eq!(lh.get_next(), None);
        }

        #[test]
        #[inline(never)]
        pub fn test_multi_boring() {
            let _ = env_logger::init();
            trace!("TEST multi");

            let _columns = &[order::from(58), order::from(59), order::from(60)];
            let interesting_columns = vec![58.into(), 59.into()];
            let mut lh = $new_thread_log::<i64>(interesting_columns);
            //1. even if one of the columns is boring we can still read the multi
            let _ = lh.multiappend(&[58.into(), 60.into()], &0xfeed, &[]);
            //2. transitives are obeyed beyond boring columns
            let _ = lh.multiappend(&[59.into(), 60.into()], &0xbad , &[]);
            let _ = lh.append(60.into(), &-1 , &[]);
            let _ = lh.multiappend(&[58.into(), 60.into()], &0xdeed, &[]);
            lh.snapshot(58.into());
            assert_eq!(lh.get_next(), Some((&0xfeed, &[OrderIndex(58.into(), 1.into()),
                OrderIndex(60.into(), 1.into())][..])));
            assert_eq!(lh.get_next(), Some((&0xbad, &[OrderIndex(59.into(), 1.into()),
                OrderIndex(60.into(), 2.into())][..])));
            assert_eq!(lh.get_next(), Some((&0xdeed, &[OrderIndex(58.into(), 2.into()),
                OrderIndex(60.into(), 4.into())][..])));
        }

        #[test]
        #[inline(never)]
        pub fn test_zero_sized() {
            let _ = env_logger::init();
            trace!("TEST no bytes");

            let interesting_columns = vec![61.into(), 62.into()];
            let mut lh = $new_thread_log::<()>(interesting_columns);
            let _ = lh.append(61.into(), &(), &[]);
            let _ = lh.multiappend(&[61.into(), 62.into()], &(), &[]);
            let _ = lh.dependent_multiappend(&[61.into()], &[62.into()], &(), &[]);
            let _ = lh.color_append(&(), &mut [61.into()], &mut [], false);
            lh.snapshot(61.into());
            assert_eq!(lh.get_next(),
                Some((&(), &[OrderIndex(61.into(), 1.into())][..])));
            assert_eq!(lh.get_next(),
                Some((&(), &[OrderIndex(61.into(), 2.into()),
                    OrderIndex(62.into(), 1.into())][..])));
            assert_eq!(lh.get_next(),
                Some((&(), &[OrderIndex(61.into(), 3.into()),
                    OrderIndex(0.into(), 0.into()), OrderIndex(62.into(), 2.into())][..])));
            assert_eq!(lh.get_next(),
                Some((&(), &[OrderIndex(61.into(), 4.into())][..])));
            assert_eq!(lh.get_next(), None);
        }

        #[test]
        #[inline(never)]
        pub fn test_async_1_column() {
            let _ = env_logger::init();
            trace!("TEST async 1 column");
            let mut lh = $new_thread_log::<i32>(vec![63.into()]);
            let _ = lh.async_append(63.into(), &1, &[]);
            let _ = lh.async_append(63.into(), &17, &[]);
            let _ = lh.async_append(63.into(), &32, &[]);
            let _ = lh.async_append(63.into(), &-1, &[]);
            lh.wait_for_all_appends();
            lh.snapshot(63.into());
            assert_eq!(lh.get_next(), Some((&1,  &[OrderIndex(63.into(), 1.into())][..])));
            assert_eq!(lh.get_next(), Some((&17, &[OrderIndex(63.into(), 2.into())][..])));
            assert_eq!(lh.get_next(), Some((&32, &[OrderIndex(63.into(), 3.into())][..])));
            assert_eq!(lh.get_next(), Some((&-1, &[OrderIndex(63.into(), 4.into())][..])));
            assert_eq!(lh.get_next(), None);
        }

        #[test]
        #[inline(never)]
        pub fn test_no_remote_multi1() {
            let _ = env_logger::init();
            trace!("TEST nrmulti");

            let columns = vec![64.into(), 65.into(), 66.into()];
            let mut lh = $new_thread_log::<u64>(columns.clone());
            let _ = lh.no_remote_multiappend(&columns, &0xfeed, &[]);
            let _ = lh.no_remote_multiappend(&columns, &0xbad , &[]);
            let _ = lh.no_remote_multiappend(&columns, &0xcad , &[]);
            let _ = lh.no_remote_multiappend(&columns, &13    , &[]);
            for col in 64..67u32 {
                let col = col.into();
                lh.snapshot(col);
                assert_eq!(
                    lh.get_next().map(
                        |(&v, l)| (v, *l.iter().find(|oi| oi.0 == col).unwrap())
                    ),
                    Some((0xfeed, OrderIndex(col, 1.into())))
                );
                assert_eq!(
                    lh.get_next().map(|(&v, l)|
                        (v, *l.iter().find(|oi| oi.0 == col).unwrap())
                    ),
                    Some((0xbad, OrderIndex(col, 2.into())))
                );
                assert_eq!(
                    lh.get_next().map(|(&v, l)|
                        (v, *l.iter().find(|oi| oi.0 == col).unwrap())
                    ),
                    Some((0xcad, OrderIndex(col, 3.into())))
                );
                assert_eq!(
                    lh.get_next().map(|(&v, l)|
                        (v, *l.iter().find(|oi| oi.0 == col).unwrap())
                    ),
                    Some((13, OrderIndex(col, 4.into())))
                );
                assert_eq!(lh.get_next(), None);
            }
        }

        #[test]
        #[inline(never)]
        pub fn test_simple_causal1() {
            trace!("TEST simple causal 1");

            let mut lh = $new_thread_log::<i32>(vec![67.into(), 68.into(), 69.into()]);
            let mut lh2 = $new_thread_log::<i32>(vec![67.into(), 68.into(), 69.into()]);

            let _ = lh.append(67.into(), &63,  &[]);
            let _ = lh.append(68.into(), &-2,  &[]);
            let _ = lh.append(68.into(), &-56, &[]);
            lh.snapshot(68.into());
            lh.snapshot(67.into());

            while let Some(..) = lh.get_next() {}
            let id = lh.simple_causal_append(&111, &mut [69.into()], &mut [67.into(), 68.into()]);
            lh.wait_for_all_appends();


            let _ = lh2.snapshot(69.into());

            let mut seen = HashSet::new();

            while let Some((_, locs)) = lh2.get_next() {
                for &loc in locs {
                    seen.insert(loc);
                }
            }

            let should_see =
                [OrderIndex(67.into(), 1.into()),
                OrderIndex(68.into(), 1.into()),
                OrderIndex(68.into(), 2.into()),
                OrderIndex(69.into(), 1.into())].into_iter().cloned().collect();

            assert_eq!(seen, should_see);
        }

        #[test]
        #[inline(never)]
        pub fn test_simple_causal2() {
            trace!("TEST simple causal 2");

            let mut lh = $new_thread_log::<i32>(vec![70.into(), 71.into(), 72.into()]);

            let _ = lh.append(70.into(), &63,  &[]);
            let _ = lh.append(71.into(), &-2,  &[]);
            let _ = lh.append(71.into(), &-56, &[]);
            let id = lh.simple_causal_append(&111, &mut [72.into()], &mut [70.into(), 71.into()]);
            lh.wait_for_all_appends();


            let _ = lh.snapshot(72.into());

            let mut seen = HashSet::new();

            while let Some((_, locs)) = lh.get_next() {
                for &loc in locs {
                    seen.insert(loc);
                }
            }

            let should_see =
                [OrderIndex(70.into(), 1.into()),
                OrderIndex(71.into(), 1.into()),
                OrderIndex(71.into(), 2.into()),
                OrderIndex(72.into(), 1.into())].into_iter().cloned().collect();

            assert_eq!(seen, should_see);
        }

        //TODO test append after prefetch but before read
    );
    () => {
        async_tests!(tcp);
        async_tests!(udp);
        async_tests!(stcp);
        async_tests!(rtcp);
    };
    (tcp) => (
        mod tcp {
            async_tests!(test new_thread_log);

            #[allow(non_upper_case_globals)]
            const lock_str: &'static str = "0.0.0.0:13389";
            #[allow(non_upper_case_globals)]
            const addr_strs: &'static [&'static str] = &["0.0.0.0:13390", "0.0.0.0:13391"];

            //TODO move back up
            #[test]
            #[inline(never)]
            pub fn test_big() {
                let _ = env_logger::init();
                trace!("TEST multi");

                let interesting_columns = vec![1_000_01.into()];
                start_tcp_servers();
                let mut lh = LogHandle::spawn_tcp_log2(lock_str.parse().unwrap(),
                    addr_strs.into_iter().map(|s| s.parse().unwrap()),
                    interesting_columns.into_iter(),
                );
                let _ = lh.append(1_000_01.into(), &[32u8; 6000][..], &[]);
                let _ = lh.append(1_000_01.into(), &[0x0fu8; 8000][..], &[]);
                lh.snapshot(1_000_01.into());
                assert_eq!(lh.get_next(), Some((&[32u8; 6000][..],
                    &[OrderIndex(1_000_01.into(), 1.into())][..])));
                assert_eq!(lh.get_next(), Some((&[0x0fu8; 8000][..],
                    &[OrderIndex(1_000_01.into(), 2.into())][..])));
                assert_eq!(lh.get_next(), None);
            }

            #[test]
            #[inline(never)]
            pub fn test_no_bytes() {
                let _ = env_logger::init();
                trace!("TEST no bytes");

                let interesting_columns = vec![1_000_02.into(), 1_000_03.into()];
                start_tcp_servers();
                let mut lh = LogHandle::<[u8]>::spawn_tcp_log2(lock_str.parse().unwrap(),
                    addr_strs.into_iter().map(|s| s.parse().unwrap()),
                    interesting_columns.into_iter(),
                );
                let _ = lh.append(1_000_02.into(), &[], &[]);
                let _ = lh.multiappend(&[1_000_02.into(), 1_000_03.into()], &[] , &[]);
                let _ = lh.dependent_multiappend(&[1_000_02.into()], &[1_000_03.into()], &[] , &[]);
                let _ = lh.color_append(&[], &mut [1_000_02.into()], &mut [], false);
                lh.snapshot(1_000_02.into());
                assert_eq!(lh.get_next(),
                    Some((&[][..], &[OrderIndex(1_000_02.into(), 1.into())][..])));
                assert_eq!(lh.get_next(),
                    Some((&[][..], &[OrderIndex(1_000_02.into(), 2.into()),
                        OrderIndex(1_000_03.into(), 1.into())][..])));
                assert_eq!(lh.get_next(),
                    Some((&[][..], &[OrderIndex(1_000_02.into(), 3.into()),
                        OrderIndex(0.into(), 0.into()), OrderIndex(1_000_03.into(), 2.into())][..])));
                assert_eq!(lh.get_next(),
                    Some((&[][..], &[OrderIndex(1_000_02.into(), 4.into())][..])));
                assert_eq!(lh.get_next(), None);
            }

            //FIXME should be V: ?Sized
            fn new_thread_log<V>(interesting_chains: Vec<order>) -> LogHandle<V> {
                start_tcp_servers();

                LogHandle::spawn_tcp_log(lock_str.parse().unwrap(),
                    addr_strs.into_iter().map(|s| s.parse().unwrap()),
                    interesting_chains.into_iter(),
                )
            }


            fn start_tcp_servers()
            {
                use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT, Ordering};
                use std::{thread, iter};

                use mio;

                static SERVERS_READY: AtomicUsize = ATOMIC_USIZE_INIT;

                for (i, &addr_str) in iter::once(&lock_str).chain(addr_strs.iter()).enumerate() {
                    let addr = addr_str.parse().expect("invalid inet address");
                    let acceptor = mio::tcp::TcpListener::bind(&addr);
                    if let Ok(acceptor) = acceptor {
                        thread::spawn(move || {
                            trace!("starting server");
                            if i == 0 {
                                ::servers2::tcp::run(acceptor, 0, 1, 2, &SERVERS_READY)
                            }
                            else {
                                ::servers2::tcp::run(
                                    acceptor,
                                    i as u32 - 1,
                                    addr_strs.len() as u32,
                                    1,
                                    &SERVERS_READY,
                                )
                            }
                        });
                    }
                    else {
                        trace!("server already started");
                    }
                }

                while SERVERS_READY.load(Ordering::Acquire) < addr_strs.len() + 1 {}
            }
        }
    );
    (stcp) => {
        mod stcp {
            async_tests!(test new_thread_log);

            #[allow(non_upper_case_globals)]
            const addr_strs: &'static [&'static str] = &["0.0.0.0:13490", "0.0.0.0:13491"];

            fn new_thread_log<V>(interesting_chains: Vec<order>) -> LogHandle<V> {
                start_tcp_servers();

                LogHandle::new_tcp_log(
                    addr_strs.into_iter().map(|s| s.parse().unwrap()),
                    interesting_chains.into_iter(),
                )
            }


            fn start_tcp_servers()
            {
                use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT, Ordering};
                use std::{thread, iter};

                use mio;

                static SERVERS_READY: AtomicUsize = ATOMIC_USIZE_INIT;

                for (i, &addr_str) in addr_strs.iter().enumerate() {
                    let addr = addr_str.parse().expect("invalid inet address");
                    let acceptor = mio::tcp::TcpListener::bind(&addr);
                    if let Ok(acceptor) = acceptor {
                        thread::spawn(move || {
                            trace!("starting server");
                            ::servers2::tcp::run(
                                acceptor,
                                i as u32,
                                addr_strs.len() as u32,
                                1,
                                &SERVERS_READY,
                            )
                        });
                    }
                    else {
                        trace!("server already started");
                    }
                }

                while SERVERS_READY.load(Ordering::Acquire) < addr_strs.len() {}
            }
        }
    };
    (rtcp) => {
        mod rtcp {
            use std::sync::{Arc, Mutex};
            use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT, Ordering};
            use std::thread;
            use async::store::AsyncTcpStore;
            use std::sync::mpsc;
            use std::mem;
            use std::net::SocketAddr;

            use mio;

            async_tests!(test new_thread_log);

            fn new_thread_log<V>(interesting_chains: Vec<order>) -> LogHandle<V> {
                start_tcp_servers();

                LogHandle::with_store(interesting_chains.into_iter(), |client| {
                    let client: ::std::sync::mpsc::Sender<Message> = client;
                    let to_store_m = Arc::new(Mutex::new(None));
                    let tsm = to_store_m.clone();
                    #[allow(non_upper_case_globals)]
                    thread::spawn(move || {
                        const addr_str1: &'static str = "127.0.0.1:13395";
                        const addr_str2: &'static str = "127.0.0.1:13396";
                        let addrs: (SocketAddr, SocketAddr) = (addr_str1.parse().unwrap(), addr_str2.parse().unwrap());
                        let mut event_loop = mio::Poll::new().unwrap();
                        trace!("RTCP make store");
                        let (store, to_store) = ::async::store::AsyncTcpStore::replicated_tcp(
                            None::<SocketAddr>,
                            ::std::iter::once::<(SocketAddr, SocketAddr)>(addrs),
                            client, &mut event_loop
                        ).expect("");
                        *tsm.lock().unwrap() = Some(to_store);
                        trace!("RTCP store setup");
                        store.run(event_loop)
                    });
                    let to_store;
                    loop {
                        let ts = mem::replace(&mut *to_store_m.lock().unwrap(), None);
                        if let Some(s) = ts {
                            to_store = s;
                            break
                        }
                    }
                    to_store
                })
            }

            fn start_tcp_servers() {
                use std::net::{IpAddr, Ipv4Addr};
                const addr_strs: &'static [&'static str] = &[&"0.0.0.0:13395", &"0.0.0.0:13396"];

                static SERVERS_READY: AtomicUsize = ATOMIC_USIZE_INIT;
                let local_host = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
                for i in 0..addr_strs.len() {
                    let prev_server: Option<SocketAddr> =
                        if i > 0 { Some(addr_strs[i-1]) } else { None }
                        .map(|s| s.parse().unwrap());
                    let prev_server = prev_server.map(|mut s| {s.set_ip(local_host); s});
                    let next_server: Option<SocketAddr> = addr_strs.get(i+1)
                        .map(|s| s.parse().unwrap());
                    let next_server = next_server.map(|mut s| {s.set_ip(local_host); s});
                    let next_server = next_server.map(|s| s.ip());
                    let addr = addr_strs[i].parse().unwrap();
                    let acceptor = mio::tcp::TcpListener::bind(&addr);
                    if let Ok(acceptor) = acceptor {
                        thread::spawn(move || {
                            trace!("starting replica server {}", i);
                            ::servers2::tcp::run_with_replication(acceptor, 0, 1,
                                prev_server, next_server,
                                2, &SERVERS_READY)
                        });
                    }
                    else {
                        trace!("server already started @ {}", addr_strs[i]);
                    }
                }

                while SERVERS_READY.load(Ordering::Acquire) < addr_strs.len() {}
            }
        }
    };
    (udp) => (
        mod udp {
            use std::sync::{Arc, Mutex};
            use std::thread;
            use async::store::AsyncTcpStore;
            use std::sync::mpsc;
            use std::mem;

            use mio;

            async_tests!(test new_thread_log);

            //TODO make UDP server multi server aware
            //#[allow(non_upper_case_globals)]
            //const lock_str: &'static str = "0.0.0.0:13393";
            //#[allow(non_upper_case_globals)]
            //const addr_strs: &'static [&'static str] = &["0.0.0.0:13394", "0.0.0.0:13395"];
            #[allow(non_upper_case_globals)]
            const addr_str: &'static str = "0.0.0.0:13393";

            fn new_thread_log<V>(interesting_chains: Vec<order>) -> LogHandle<V> {
                use std::iter;
                start_udp_servers();

                let to_store_m = Arc::new(Mutex::new(None));
                let tsm = to_store_m.clone();
                let (to_log, from_outside) = mpsc::channel();
                let client = to_log.clone();
                let (ready_reads_s, ready_reads_r) = mpsc::channel();
                let (finished_writes_s, finished_writes_r) = mpsc::channel();
                thread::spawn(move || {
                    let mut event_loop = mio::Poll::new().unwrap();
                    let (store, to_store) = AsyncTcpStore::udp(addr_str.parse().unwrap(),
                        iter::once(addr_str).map(|s| s.parse().unwrap()),
                        client, &mut event_loop).expect("");
                    *tsm.lock().unwrap() = Some(to_store);
                    store.run(event_loop);
                });
                let to_store;
                loop {
                    let ts = mem::replace(&mut *to_store_m.lock().unwrap(), None);
                    if let Some(s) = ts {
                        to_store = s;
                        break
                    }
                }
                thread::spawn(move || {
                    let log = ThreadLog::new(to_store, from_outside, ready_reads_s, finished_writes_s,
                        interesting_chains.into_iter());
                    log.run()
                });

                LogHandle::new(to_log, ready_reads_r, finished_writes_r)
            }


            fn start_udp_servers()
            {
                use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT, Ordering};
                use std::thread;

                use servers::udp::Server;

                static SERVERS_READY: AtomicUsize = ATOMIC_USIZE_INIT;

                {
                    let handle = thread::spawn(move || {

                        let addr = addr_str.parse().expect("invalid inet address");
                        //let mut event_loop = EventLoop::new().unwrap();
                        let server = Server::new(&addr);
                        if let Ok(mut server) = server {
                            trace!("starting server");
                            //event_loop.run(&mut server).expect("server should never stop");
                            server.run(&SERVERS_READY)
                        }
                        trace!("server already started");
                        return;
                    });
                    mem::forget(handle);
                }

                //while SERVERS_READY.load(Ordering::Acquire) < addr_strs.len() + 1 {}
                while SERVERS_READY.load(Ordering::Acquire) < 1 {}
                trace!("server started, client starting");
            }
        }
    );
}

async_tests!();

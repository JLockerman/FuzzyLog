extern crate bincode;
extern crate fuzzy_log_client;
pub extern crate serde;

#[cfg(test)] extern crate fuzzy_log_server;
#[cfg(test)] #[macro_use] extern crate matches;
#[cfg(test)] #[macro_use] extern crate serde_derive;

pub use std::io;
pub use fuzzy_log_client::fuzzy_log::log_handle::{
    entry,
    order,
    GetRes,
    LogHandle,
    OrderIndex,
    TryWaitRes,
    Uuid,
};

use bincode::{serialize, deserialize, Infinite};

use serde::{Serialize, Deserialize};

#[macro_export]
macro_rules! filtered {

    (
        mod $modname: ident;
        type = $type:path;
        {
            $first_filter_name:ident = $first_filter:expr
            $(,$filter_name:ident = $filter:expr)* $(,)*
        }
    ) => {
        filtered!{
            mod $modname;
            type = $type;
            base = 1;
            {
                $first_filter_name = $first_filter
                $(,$filter_name = $filter)*
            }
        }
    };

    (
        mod $modname: ident;
        type = $type:path;
        state: $state_ty:path = $start_state:expr;
        {
            $first_filter_name:ident = $first_filter:expr
            $(,$filter_name:ident = $filter:expr)* $(,)*
        }
    ) => {
        filtered!{
            mod $modname;
            state $state_ty = $start_state;
            type = $type;
            base = 1;
            {
                $first_filter_name = $first_filter
                $(,$filter_name = $filter)*
            }
        }
    };

    (
        type = $type:path;
        {
            $first_filter_name:ident = $first_filter:expr
            $(,$filter_name:ident = $filter:expr)* $(,)*
        }
    ) => {
        filtered!{
            type = $type;
            base = 1;
            {
                $first_filter_name = $first_filter
                $(,$filter_name = $filter)*
            }
        }
    };

    (
        type = $type:path;
        state: $state_ty:path = $start_state:expr;
        {
            $first_filter_name:ident = $first_filter:expr
            $(,$filter_name:ident = $filter:expr)* $(,)*
        }
    ) => {
        filtered!{
            state $state_ty = $start_state;
            type = $type;
            base = 1;
            {
                $first_filter_name = $first_filter
                $(,$filter_name = $filter)*
            }
        }
    };

    (
        type = $type:path;
        base = $first_color:expr;
        {
            $first_filter_name:ident = $first_filter:expr
            $(,$filter_name:ident = $filter:expr)* $(,)*
        }
    ) => {
        filtered!{
            mod filtered;
            type = $type;
            base = $first_color;
            {
                $first_filter_name = $first_filter
                $(,$filter_name = $filter)*
            }
        }
    };

    (
        type = $type:path;
        state: $state_ty:path = $start_state:expr;
        base = $first_color:expr;
        {
            $first_filter_name:ident = $first_filter:expr
            $(,$filter_name:ident = $filter:expr)* $(,)*
        }
    ) => {
        filtered!{
            mod filtered;
            state $state_ty = $start_state;
            type = $type;
            base = $first_color;
            {
                $first_filter_name = $first_filter
                $(,$filter_name = $filter)*
            }
        }
    };

    (
        mod $modname: ident;
        type = $type:path;
        base = $first_color:expr;
        {
            $first_filter_name:ident = $first_filter:expr
            $(,$filter_name:ident = $filter:expr)* $(,)*
        }
    ) => {
        mod $modname {
            use super::*;
            use std::iter;
            use std::net::SocketAddr;
            pub use $crate::*;

            #[repr(u32)]
            #[allow(non_camel_case_types)]
            #[derive(Copy, Clone, PartialEq, Eq)]
            pub enum Filter {
                $first_filter_name = $first_color,
                $($filter_name),*
            }

            impl From<Filter> for order {
                fn from(f: Filter) -> Self {
                    order::from(f as u32)
                }
            }

            impl<'a> From<&'a Filter> for order {
                fn from(f: &'a Filter) -> Self {
                    order::from(*f as u32)
                }
            }

            pub fn filtered_view(head_server: SocketAddr, tail_server: Option<SocketAddr>)
            -> View<$type> {
                let (handle, colors) = filtered!(handle and colors:
                    $first_filter_name = $first_filter,
                    $($filter_name = $filter),*;
                    head_server, tail_server
                );
                View {
                    handle,
                    filter: filtered!(function:
                        $first_filter_name = $first_filter,
                        $($filter_name = $filter),*
                    ),
                    colors: colors.to_vec(),
                }
            }

            impl IntoIterator for Filter {
                type Item = order;
                type IntoIter = iter::Once<order>;

                fn into_iter(self) -> Self::IntoIter {
                    iter::once(self.into())
                }
            }
        }
    };

    (
        mod $modname: ident;
        type = $type:path;
        state: $state_ty:path = $start_state:expr;
        base = $first_color:expr;
        {
            $first_filter_name:ident = $first_filter:expr
            $(,$filter_name:ident = $filter:expr)* $(,)*
        }
    ) => {
        mod $modname {
            use super::*;
            use std::iter;
            use std::net::SocketAddr;
            use std::ops::Add;
            pub use $crate::*;

            #[repr(u32)]
            #[allow(non_camel_case_types)]
            #[derive(Copy, Clone, PartialEq, Eq)]
            pub enum Filter {
                $first_filter_name = $first_color,
                $($filter_name),*
            }

            impl From<Filter> for order {
                fn from(f: Filter) -> Self {
                    order::from(f as u32)
                }
            }

            impl<'a> From<&'a Filter> for order {
                fn from(f: &'a Filter) -> Self {
                    order::from(*f as u32)
                }
            }

            pub fn filtered_view(head_server: SocketAddr, tail_server: Option<SocketAddr>)
            -> View<$type> {
                let (handle, colors) = filtered!(handle and colors:
                    $first_filter_name = $first_filter,
                    $($filter_name = $filter),*;
                    head_server, tail_server
                );
                View {
                    handle,
                    filter: filtered!(function with state:
                        $state_ty = $start_state;
                        $first_filter_name = $first_filter,
                        $($filter_name = $filter),*
                    ),
                    colors: colors.to_vec(),
                }
            }

            impl IntoIterator for Filter {
                type Item = order;
                type IntoIter = iter::Once<order>;

                fn into_iter(self) -> Self::IntoIter {
                    iter::once(self.into())
                }
            }
        }
    };

    (handle and colors:
        $first_filter_name:ident = $first_filter:expr,
        $($filter_name:ident = $filter:expr),*;
        $head_server:ident,
        $tail_server:ident
    ) => {
        {
            let colors = [
                order::from(Filter::$first_filter_name as u32),
                $(order::from(Filter::$filter_name as u32)),*
            ];
            let handle = if let Some(tail_server) = $tail_server {
                LogHandle::replicated_with_servers(
                        iter::once(($head_server, tail_server)))
                    .chains(colors.iter().cloned())
                    .build()
            } else {
                LogHandle::unreplicated_with_servers(iter::once($head_server))
                    .chains(colors.iter().cloned())
                    .build()
            };
            (handle, colors.to_vec())
        }
    };

    (function: $first_filter_name:ident = $first_filter:expr,
        $($filter_name:ident = $filter:expr),*) => {
        Box::new(|val, colors| {
            if $first_filter(&mut *val) {
                colors.push(order::from(Filter::$first_filter_name as u32))
            }
            $(
                if $filter(&mut *val) {
                    colors.push(order::from(Filter::$filter_name as u32))
                }
            )*
        })
    };

    (function with state: $state_ty:path = $start_state:expr;
        $first_filter_name:ident = $first_filter:expr,
        $($filter_name:ident = $filter:expr),*) => {
        {
            let mut state: $state_ty = $start_state;
            Box::new(move |val, colors| {
                if $first_filter(&mut state, &mut *val) {
                    colors.push(order::from(Filter::$first_filter_name as u32))
                }
                $(
                    if $filter(&mut state, &mut *val) {
                        colors.push(order::from(Filter::$filter_name as u32))
                    }
                )*
            })
        }
    };
}

pub struct View<V: ?Sized> {
    handle: LogHandle<[u8]>,
    filter: Box<FnMut(&mut V, &mut Vec<order>)>,
    //TODO only for dynamic alloc collors
    // filters_to_colors: Vec<order>,
    // base_color: order,
    //cached vector to store colors for appends or snaps
    colors: Vec<order>,
}

/*TODO
#[repr(C)]
struct WithAux<V, A> {
    val: V,
    aux: A,
}*/

/*impl<V> View<V> {
    pub fn async_append_raw(&mut self, v: &mut V) -> Uuid {
        (self.filter)(v, &mut self.colors);
        let write_id = if self.colors.len() == 1 {
            self.handle.async_append(self.colors[0], &v, &[])
        } else {
            self.handle.async_no_remote_multiappend(&self.colors[..], &v, &[])
        };
        self.colors.clear();
        write_id
    }

    pub fn append_raw(&mut self, v: &mut V)-> Result<Vec<OrderIndex>, TryWaitRes> {
        let id = self.async_append_raw(v);
        self.wait_for_a_specific_append(id)
    }
}*/

impl<V: ?Sized> View<V>
where V: Serialize {
    pub fn async_append(&mut self, v: &mut V) -> Result<Uuid, bincode::Error> {
        (self.filter)(v, &mut self.colors);
        let data = serialize(v, Infinite)?;
        let write_id = if self.colors.len() == 1 {
            self.handle.async_append(self.colors[0], &data[..], &[])
        } else {
            self.handle.async_no_remote_multiappend(&self.colors[..], &data[..], &[])
        };
        self.colors.clear();
        Ok(write_id)
    }

    pub fn append(&mut self, v: &mut V)-> Result<Vec<OrderIndex>, TryWaitRes> {
        let id = self.async_append(v).unwrap();
        self.wait_for_a_specific_append(id)
    }
}

#[derive(Debug)]
pub enum UpdateErr {
    Deserialize(Vec<u8>, bincode::Error),
    Log(GetRes),
}

impl<V: ?Sized> View<V>
where V: for<'a> Deserialize<'a> {

    pub fn update<I, T, F>(&mut self, filters: I, apply: F) -> Result<(), UpdateErr>
    where I: IntoIterator<Item=T>, T: Into<order>,
        F: FnMut(V, &[OrderIndex]) {
        self.colors.clear();
        self.colors.extend(filters.into_iter().map(Into::into));
        if self.colors.len() == 0 { return Ok(()) }

        if self.colors.len() == 1 {
            self.handle.snapshot(self.colors[0])
        } else {
            self.handle.strong_snapshot(&self.colors[..])
        }

        self.continue_update(apply)
    }

    pub fn update_color<F>(&mut self, color: order, apply: F) -> Result<(), UpdateErr>
    where F: FnMut(V, &[OrderIndex]) {
        self.handle.snapshot(color);
        self.continue_update(apply)
    }

    pub fn continue_update<F>(&mut self, mut apply: F) -> Result<(), UpdateErr>
    where F: FnMut(V, &[OrderIndex]) {
        loop {
            match self.handle.get_next() {
                Err(GetRes::Done) => return Ok(()),
                Err(e) => return Err(UpdateErr::Log(e)),
                Ok((bytes, locs)) => {
                    let e = match deserialize(bytes){
                        Err(e) => e,
                        Ok(v) => {
                            apply(v, locs);
                            continue
                        },
                    };
                    return Err(UpdateErr::Deserialize(bytes.to_vec(), e))
                },
            }
        }
    }

    pub fn get_next(&mut self) -> Result<(V, &[OrderIndex]), UpdateErr> {
        match self.handle.get_next() {
            Err(e) => return Err(UpdateErr::Log(e)),
            Ok((bytes, locs)) => {
                let e = match deserialize(bytes){
                    Err(e) => e,
                    Ok(v) => return Ok((v, locs)),
                };
                return Err(UpdateErr::Deserialize(bytes.to_vec(), e))
            },
        }
    }

    pub fn snapshot(&mut self, color: order) {
        self.handle.snapshot(color);
    }
}

impl<V: ?Sized> View<V> {
    pub fn wait_for_all_appends(&mut self) -> Result<(), TryWaitRes> {
        self.handle.wait_for_all_appends()
    }

    pub fn wait_for_any_append(&mut self) -> Result<(Uuid, Vec<OrderIndex>), TryWaitRes> {
        self.handle.wait_for_any_append()
    }

    pub fn wait_for_a_specific_append(&mut self, write_id: Uuid)
    -> Result<Vec<OrderIndex>, TryWaitRes> {
        self.handle.wait_for_a_specific_append(write_id)
    }

    pub fn try_wait_for_any_append(&mut self)
    -> Result<(Uuid, Vec<OrderIndex>), TryWaitRes> {
        self.handle.try_wait_for_any_append()
    }

    pub fn flush_completed_appends(&mut self) -> Result<usize, (io::ErrorKind, usize)> {
        self.handle.flush_completed_appends()
    }
}

// pub struct Colors<Rest> {
//     color: order,
//     rest: Rest,
// }

// pub trait TakeColor<'colors> {
//     fn take_color<'s>(&'s mut self) -> Option<order>;
//     fn next(&'colors mut self) -> &'colors mut TakeColor<'colors>;
// }

// impl<'colors> TakeColor<'colors> for () {
//     fn take_color<'s>(&'s mut self) -> Option<order> {
//         None
//     }

//     fn next(&'colors mut self) -> &'colors mut TakeColor<'colors> {
//         COLORS_END
//     }
// }

// impl<'colors, T: TakeColor<'colors>> TakeColor<'colors> for Colors<T> {
//     fn take_color<'s>(&'s mut self) -> Option<order> {
//         Some(self.color)
//     }

//     fn next(&'colors mut self) -> &'colors mut TakeColor<'colors> {
//         &mut self.rest
//     }
// }

// static COLORS_END: &'static mut TakeColor<'static> = &mut ();

// pub trait PutColor {
//     type NewColors;
//     fn put_color(self, color: order) -> Self::NewColors;
// }


// impl PutColor for Colors<()> {
//     type NewColors = Colors<Colors<()>>;

//     fn put_color(self, color: order) -> Self::NewColors {
//         let NewColor = Colors{ color, rest: ()};
//         let Colors {color, ..} = self;
//         Colors{ color, rest: NewColor, }
//     }
// }

// impl<T: PutColor> PutColor for Colors<T> {
//     type NewColors = Colors<T::NewColors>;

//     fn put_color(self, color: order) -> Self::NewColors {
//         let Colors {color: my_color, rest} = self;
//         let NewColor = rest.put_color(color);
//         Colors{ color: my_color, rest: NewColor, }
//     }
// }

// pub struct ColorsIter<'colors> {
//     colors: &'colors mut (TakeColor<'colors> + 'colors),
// }

// impl<'colors> Iterator for ColorsIter<'colors> {
//     type Item = order;
//     fn next(&mut self) -> Option<Self::Item> {
//         let color = { self.colors.take_color() };
//         let old = ::std::mem::replace(&mut self.colors, COLORS_END);
//         ::std::mem::replace(&mut self.colors, old.next());
//         color
//     }
// }

// pub trait ToColors {
//     type Iter: Iterator<Item=order>;

//     fn to_colors(self) -> Self::Iter;
// }

// impl ToColors for order {
//     type Iter = ::std::iter::Once<order>;

//     fn to_colors(self) -> Self::Iter {
//         ::std::iter::once(self)
//     }
// }

// impl<'a> ToColors for &'a [order] {
//     type Iter = ::std::iter::Cloned<::std::slice::Iter<'a, order>>;

//     fn to_colors(self) -> Self::Iter {
//         self.iter().cloned()
//     }
// }

#[cfg(test)]
mod tests {

    filtered!{
        type = u64;
        base = 1;
        {
            filter1 = |_| true,
            filter2 = |&mut a| (a + a < 2 * a),
            filter3 = |_| false,
        }
    }

    filtered!{
        mod default_first_color;
        type = u64;
        base = 1;
        {
            filter1 = |_| true,
            filter2 = |&mut a| (a + a > 2 * a),
        }
    }

    filtered!{
        mod test1;
        type = u64;
        base = 1;
        {
            filter1 = |_| true,
            filter2 = |&mut a| (a + a < a),
        }
    }

    filtered!{
        mod test_state;
        type = u64;
        state: bool = false;
        base = 1;
        {
            filter1 = |_, _| true,
            filter2 = |s: &mut bool, &mut a| (a + a < a) && {*s = !*s; *s},
        }
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub enum MapEvent<K, V> {
        Insert(K, V),
        Update(K, V),
        Remove(K),
    }

    filtered!{
        mod map_filter;
        type = MapEvent<u64, u64>;
        {
            everything = |_| true,
            inserts = |e| matches!(e, &MapEvent::Insert(_, _)),
            removals = |e| matches!(e, &MapEvent::Remove(_)),
        }
    }

    #[test]
    fn map() {
        use tests::map_filter::*;
        use tests::MapEvent::*;
        use std::collections::HashMap;

        let mut view = filtered_view(unimplemented!(), None);
        view.append(&mut Insert(0, 1));
        view.append(&mut Insert(1, 17));
        view.append(&mut Insert(22, 4));
        view.append(&mut Update(0, 31));
        view.append(&mut Update(1, 117));
        view.append(&mut Update(22, 9));
        view.append(&mut Remove(0));

        let mut map = HashMap::new();
        view.update(Filter::inserts, |e, _| match e {
            Insert(k, v) => {
                let old = map.insert(k, v);
                assert!(old.is_none());
            }
            _ => {},
        });
        assert_eq!(false, true);
    }

    #[test]
    fn map2() {
        use tests::map_filter::*;
        use tests::MapEvent::*;
        use std::collections::HashMap;

        let mut view = filtered_view(unimplemented!(), None);
        view.append(&mut Insert(0, 1));
        view.append(&mut Insert(1, 17));
        view.append(&mut Insert(22, 4));
        view.append(&mut Update(0, 31));
        view.append(&mut Update(1, 117));
        view.append(&mut Update(22, 9));
        view.append(&mut Remove(0));

        let mut map = HashMap::new();
        view.update(&[Filter::inserts, Filter::removals], |e, _| match e {
            Insert(k, v) => {
                let old = map.insert(k, v);
                assert!(old.is_none());
            }
            _ => {},
        });
        assert_eq!(false, true);
    }
}

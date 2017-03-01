macro_rules! counters {
    (struct $name:ident { $($field:ident: $typ:tt),* $(,)* }) => {
        #[derive(Copy, Clone, PartialEq, Eq, Debug, Default)]
        struct $name {
            $(#[cfg(print_stats)] $field: $typ),*
        }

        impl $name {
            $(
                #[inline(always)]
                fn $field(&mut self, _increment: $typ) {
                    #[cfg(print_stats)]
                    {
                        self.$field += _increment
                    }
                }
            )*
        }
    };
}

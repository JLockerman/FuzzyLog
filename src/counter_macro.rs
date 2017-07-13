macro_rules! counters {
    (struct $name:ident { $($field:ident: $typ:tt),* $(,)* }) => {
        #[derive(Copy, Clone, PartialEq, Eq, Debug, Default)]
        pub struct $name {
            $(#[cfg(feature = "print_stats")] $field: $typ),*
        }

        impl $name {
            $(
                #[inline(always)]
                fn $field(&mut self, _increment: $typ) {
                    #[cfg(feature = "print_stats")]
                    {
                        self.$field += _increment
                    }
                }
            )*
        }
    };
}

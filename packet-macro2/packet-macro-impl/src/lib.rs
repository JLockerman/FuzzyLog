#![recursion_limit = "128"]

extern crate proc_macro;
extern crate syn;
#[macro_use] extern crate quote;
#[macro_use] extern crate synom;

use proc_macro::TokenStream;

use synom::{IResult, space};

use parser::*;

use syn::Ident;

use std::collections::{HashMap, HashSet};

mod parser;

#[proc_macro_derive(packet_macro_impl)]
pub fn packet_macro_impl(input: TokenStream) -> TokenStream {
    let input = input.to_string();
    let input = extract_input(&input);
    let parsed = match parser::packet_body(input) {
        IResult::Done(mut rest, t) => {
            rest = space::skip_whitespace(rest);
            if rest.is_empty() {
                t
            } else if rest.len() == input.len() {
                // parsed nothing
                panic!("failed to parse {:?}", rest)
            } else {
                panic!("unparsed tokens after {:?}", rest)
            }
        }
        IResult::Error => panic!("failed to parse: {:?}", input),
    };
    let body = impl_ref(parsed);
    //let tokens = quote!{

    //};
    //let string = format!("{}", &body);
    let q = quote! {
        #body

        //pub fn print_string() {
        //    println!("body {}", #string);
        //}
    };
    q.parse().unwrap()
}

fn impl_ref(body: PacketBody) -> quote::Tokens {
    match body {
        PacketBody::Struct(fields) => {
            let try_wrap = struct_try_wrap(&fields);
            let fill_vec = struct_fill_vec(&fields);
            let get_len = struct_get_len(&fields);

            let try_mut = struct_try_mut(&fields);
            let as_ref = struct_mut_as_ref(&fields);

            let size_vars = size_vars(&fields);
            let (field, typ): (Vec<_>, Vec<_>) = fields.iter()
                .filter_map(|f| match size_vars.contains(&f.ident) {
                    true => None,
                    false => Some((&f.ident, f.ty.ref_type())),
                }).unzip();
            let (field1, typ1) = (field.iter(), typ.iter());
            let (field2, typ2) = (field.iter(), typ.iter());
            quote! {
                #[derive(Copy, Clone, PartialEq, Eq, Debug)]
                pub struct Ref<'a> {
                    #(pub #field1: &'a #typ1),*
                }

                #[derive(PartialEq, Eq, Debug)]
                pub struct Mut<'a> {
                    #(pub #field2: &'a mut #typ2),*
                }

                impl<'a> Ref<'a> {
                    #try_wrap

                    #fill_vec

                    #get_len
                }

                impl<'a> Mut<'a> {
                    #try_mut

                    #as_ref
                }

                pub fn min_len() -> usize {
                    match unsafe { Ref::try_ref(&[]) } {
                        Err(WrapErr::NotEnoughBytes(n)) => n,
                        _ => 0,
                    }
                }
            }
        },
        PacketBody::Enum(KindFlag(tag_ident, tag_ty), variants) => {
            let ref_variant = variants.iter().map(|v| {
                let variant = &v.ident;
                let size_vars = size_vars(&v.body);
                let (field, typ): (Vec<_>, Vec<_>) = v.body.iter()
                    .filter_map(|f| match size_vars.contains(&f.ident) {
                        true => None,
                        false => Some((&f.ident, f.ty.ref_type())),
                    }).unzip();
                quote!( #variant { #(#field: &'a #typ),* } )
            });

            let mut_variant = variants.iter().map(|v| {
                let variant = &v.ident;
                let size_vars = size_vars(&v.body);
                let (field, typ): (Vec<_>, Vec<_>) = v.body.iter()
                    .filter_map(|f| match size_vars.contains(&f.ident) {
                        true => None,
                        false => Some((&f.ident, f.ty.ref_type())),
                    }).unzip();
                quote!( #variant { #(#field: &'a mut #typ),* } )
            });

            let try_wrap = enum_try_wrap(&tag_ident, &tag_ty, &variants);
            let fill_vec = enum_fill_vec(&tag_ident, &tag_ty, &variants);
            let get_len = enum_get_len(&tag_ty, &variants);

            let try_mut = enum_try_mut(&tag_ident, &tag_ty, &variants);
            let as_ref = enum_mut_as_ref(&variants);

            let min_len = enum_minsize(&tag_ty, &variants);
            quote!{
                #[derive(Copy, Clone, PartialEq, Eq, Debug)]
                pub enum Ref<'a> {
                    #(#ref_variant),*
                }

                #[derive(PartialEq, Eq, Debug)]
                pub enum Mut<'a> {
                    #(#mut_variant),*
                }

                impl<'a> Ref<'a> {
                    #try_wrap

                    #fill_vec

                    #get_len
                }

                impl<'a> Mut<'a> {
                    #try_mut

                    #as_ref
                }

                #min_len
            }
        },
    }
}

struct TryWrapBody {
    vars: quote::Tokens,
    body: quote::Tokens,
    set_fields: quote::Tokens,
    err_size: quote::Tokens,
}

fn struct_try_wrap(fields: &[Field]) -> quote::Tokens {
    let TryWrapBody{ vars, body, set_fields, err_size} =
        try_wrap_body(fields, &"'tryref".into());
    quote!{
        #[allow(unused_assignments)]
        pub unsafe fn try_ref(mut __packet_macro_bytes: &[u8]) -> Result<(Self, &[u8]), WrapErr> {
            let __packet_macro_read_len = 0usize;
            #vars
            'tryref: loop {
                #body
                let _ref = Ref { #set_fields };
                return Ok((_ref, __packet_macro_bytes))
            }
            Err(WrapErr::NotEnoughBytes(0 #err_size))
        }
    }
}

fn enum_try_wrap(tag_ident: &Ident, tag_ty: &Ty, variants: &[Variant]) -> quote::Tokens {
    let bodies = variants.iter().enumerate().map(|(i, v)| {
        let &Variant{ident: ref variant, flag: ref tag_val, ref body} = v;
        let break_label = format!("'tryref_{}", i).into();
        let TryWrapBody{ vars, body, set_fields, err_size} =
            try_wrap_body(body, &break_label);
        let tag_size = tag_ty.err_size();
        quote!{
            Some(&#tag_val) => {
                #vars
                #break_label: loop {
                    #body
                    let _ref = Ref::#variant { #set_fields };
                    return Ok((_ref, __packet_macro_bytes))
                }
                return Err(WrapErr::NotEnoughBytes(#tag_size #err_size))
            }
        }
    });
    let try_wrap_tag = try_wrap_field(tag_ident, tag_ty, &"'tryref_tag".into());
    quote!{
        #[allow(unused_assignments)]
        pub unsafe fn try_ref(mut __packet_macro_bytes: &[u8]) -> Result<(Self, &[u8]), WrapErr> {
            let __packet_macro_read_len = 0usize;
            let mut #tag_ident = None;
            'tryref_tag: loop {
                #try_wrap_tag;
                match #tag_ident {
                    #(#bodies),*
                    _ => return Err(WrapErr::BadTag),
                }
            }
            Err(WrapErr::NotEnoughBytes(min_len()))
        }
    }
}

fn try_wrap_body(fields: &[Field], break_label: &Ident) -> TryWrapBody {
    let size_vars = size_vars(fields);

    let (field, typ): (Vec<_>, Vec<_>) = fields.iter().map(|f| (&f.ident, &f.ty)).unzip();
    let (field1, typ1) = (field.iter(), typ.iter());
    let field2 = field.iter().filter(|f| !size_vars.contains(*f));
    let field3 = field.iter().filter(|f| !size_vars.contains(*f));

    let field_vars = quote!( #(let mut #field1: Option<&#typ1> = None;)* );

    let try_wrap_fields = fields.iter().map(|f|
        try_wrap_field(&f.ident, &f.ty, break_label));
    let try_wrap_body = quote! ( #(#try_wrap_fields)* );

    let vars_set = quote!( #(#field2: #field3.unwrap()),* );

    let err_size = typ.iter().map(|t| t.err_size());
    let err_size = quote!( #( + #err_size)* );
    TryWrapBody {
        vars: field_vars, body: try_wrap_body, set_fields: vars_set, err_size: err_size,
    }
}

fn try_wrap_field(fields: &Ident, typ: &Ty, label: &Ident) -> quote::Tokens {
    use Ty::*;
    match typ {
        typ @ &Path(..) | typ @ &Tuple(..) | typ @ &Array(..) => quote!{
            let __packet_macro_read_len: usize = {
                let __packet_macro_size = ::std::mem::size_of::<#typ>();
                let __packet_macro_read_len = __packet_macro_read_len + __packet_macro_size;
                if __packet_macro_bytes.len() < __packet_macro_size {
                    break #label
                }
                let (__packet_macro_field, __packet_macro_rem_bytes) =
                    __packet_macro_bytes.split_at(__packet_macro_size);
                let __packet_macro_field: &#typ =
                    ::std::mem::transmute(__packet_macro_field.as_ptr());
                __packet_macro_bytes = __packet_macro_rem_bytes;
                #fields = Some(__packet_macro_field);
                __packet_macro_read_len
            };
        },
        &VarArray(ref ty, ref count) => quote!{
            let __packet_macro_read_len: usize = {
                let __packet_macro_count = #count.cloned().unwrap() as usize;
                let __packet_macro_size = ::std::mem::size_of::<#ty>() * __packet_macro_count;
                let __packet_macro_read_len = __packet_macro_read_len + __packet_macro_size;
                if __packet_macro_bytes.len() < __packet_macro_size {
                    break #label
                }
                let (__packet_macro_field_bytes, __packet_macro_rem_bytes) =
                    __packet_macro_bytes.split_at(__packet_macro_size);
                let __packet_macro_field_ptr = __packet_macro_field_bytes.as_ptr();
                let __packet_macro_field = ::std::slice::from_raw_parts(
                    __packet_macro_field_ptr as *const #ty, __packet_macro_count);
                debug_assert_eq!(
                    __packet_macro_field_ptr.offset(__packet_macro_size as isize) as usize,
                    __packet_macro_field.as_ptr().offset(__packet_macro_count as isize) as usize
                );
                __packet_macro_bytes = __packet_macro_rem_bytes;
                #fields = Some(__packet_macro_field);
                __packet_macro_read_len
            };
        },
    }
}

fn struct_try_mut(fields: &[Field]) -> quote::Tokens {
    let TryWrapBody{ vars, body, set_fields, err_size} =
        try_mut_body(fields, &"'trymut".into());
    quote!{
        #[allow(unused_assignments)]
        pub unsafe fn try_mut(__packet_macro_bytes: &mut [u8]) -> Result<(Self, &mut [u8]), WrapErr> {
            let __packet_macro_read_len = 0usize;
            #vars
            'trymut: loop {
                #body
                let _mut = Mut { #set_fields };
                return Ok((_mut, __packet_macro_bytes))
            }
            //Err(WrapErr::NotEnoughBytes(0 $(+ packet!(err_size $typ))*))
            Err(WrapErr::NotEnoughBytes(0 #err_size))
        }
    }
}

fn enum_try_mut(tag_ident: &Ident, tag_ty: &Ty, variants: &[Variant]) -> quote::Tokens {
    let bodies = variants.iter().enumerate().map(|(i, v)| {
        let &Variant{ident: ref variant, flag: ref tag_val, ref body} = v;
        let break_label = format!("'trymut_{}", i).into();
        let TryWrapBody{ vars, body, set_fields, err_size} =
            try_mut_body(body, &break_label);
        let tag_size = tag_ty.err_size();
        quote!{
            Some(&mut #tag_val) => {
                #vars
                #break_label: loop {
                    #body
                    let _ref = Mut::#variant { #set_fields };
                    return Ok((_ref, __packet_macro_bytes))
                }
                return Err(WrapErr::NotEnoughBytes(#tag_size #err_size))
            }
        }
    });
    let try_mut_tag = try_mut_field(tag_ident, tag_ty, &"'trymut_tag".into());
    quote!{
        #[allow(unused_assignments)]
        pub unsafe fn try_mut(mut __packet_macro_bytes: &mut [u8]) -> Result<(Self, &mut [u8]), WrapErr> {
            let __packet_macro_read_len = 0usize;
            let mut #tag_ident = None;
            'trymut_tag: loop {
                #try_mut_tag
                match #tag_ident {
                    #(#bodies),*
                    _ => return Err(WrapErr::BadTag),
                }
            }
            Err(WrapErr::NotEnoughBytes(min_len()))
        }
    }
}

fn try_mut_body(fields: &[Field], break_label: &Ident) -> TryWrapBody {
    let size_vars = size_vars(fields);

    let (field, typ): (Vec<_>, Vec<_>) = fields.iter().map(|f| (&f.ident, &f.ty)).unzip();
    let (field1, typ1) = (field.iter(), typ.iter());
    let field2 = field.iter().filter(|f| !size_vars.contains(*f));
    let field3 = field.iter().filter(|f| !size_vars.contains(*f));

    let field_vars = quote!( #(let mut #field1: Option<&mut #typ1> = None;)* );

    let try_mut_field = fields.iter().map(|f|
        try_mut_field(&f.ident, &f.ty, break_label));
    let try_mut_body = quote! ( #(#try_mut_field)* );

    let vars_set = quote!( #(#field2: #field3.unwrap()),* );

    let err_size = typ.iter().map(|t| t.err_size());
    let err_size = quote!( #( + #err_size)* );
    TryWrapBody {
        vars: field_vars, body: try_mut_body, set_fields: vars_set, err_size: err_size,
    }
}

fn try_mut_field(fields: &Ident, typ: &Ty, label: &Ident) -> quote::Tokens {
    use Ty::*;
    match typ {
        typ @ &Path(..) | typ @ &Tuple(..) | typ @ &Array(..) => quote!{
            let (__packet_macro_read_len, __packet_macro_bytes): (usize, &mut [u8]) = {
                let __packet_macro_size = ::std::mem::size_of::<#typ>();
                let __packet_macro_read_len = __packet_macro_read_len + __packet_macro_size;
                if __packet_macro_bytes.len() < __packet_macro_size {
                    break #label
                }
                let (__packet_macro_field, __packet_macro_rem_bytes) =
                    __packet_macro_bytes.split_at_mut(__packet_macro_size);
                let __packet_macro_field: &mut #typ =
                    ::std::mem::transmute(__packet_macro_field.as_ptr());
                #fields = Some(__packet_macro_field);
                (__packet_macro_read_len, __packet_macro_rem_bytes)
            };
        },
        &VarArray(ref ty, ref count) => quote!{
            let (__packet_macro_read_len, __packet_macro_bytes): (usize, &mut [u8]) = {
                fn __opt_mut_clone<T: Clone>(o: &Option<&mut T>) -> Option<T> {
                   o.as_ref().map(|t| <T as Clone>::clone(t))
                }
                let __packet_macro_count = __opt_mut_clone(&#count).unwrap() as usize;
                let __packet_macro_size = ::std::mem::size_of::<#ty>() * __packet_macro_count;
                let __packet_macro_read_len = __packet_macro_read_len + __packet_macro_size;
                if __packet_macro_bytes.len() < __packet_macro_size {
                    break #label
                }
                let (__packet_macro_field_bytes, __packet_macro_rem_bytes) =
                    __packet_macro_bytes.split_at_mut(__packet_macro_size);
                let __packet_macro_field_ptr = __packet_macro_field_bytes.as_ptr();
                let __packet_macro_field = ::std::slice::from_raw_parts_mut(
                    __packet_macro_field_ptr as *mut #ty, __packet_macro_count);
                debug_assert_eq!(
                    __packet_macro_field_ptr.offset(__packet_macro_size as isize) as usize,
                    __packet_macro_field.as_ptr().offset(__packet_macro_count as isize) as usize
                );
                #fields = Some(__packet_macro_field);
                (__packet_macro_read_len, __packet_macro_rem_bytes)
            };
        },
    }
}

fn struct_mut_as_ref(fields: &[Field]) -> quote::Tokens {
    let size_vars = size_vars(fields);
    let field0 = fields.iter().map(|f| &f.ident).filter(|f| !size_vars.contains(*f));
    let field1 = fields.iter().map(|f| &f.ident).filter(|f| !size_vars.contains(*f));
    let field2 = fields.iter().map(|f| &f.ident).filter(|f| !size_vars.contains(*f));
    quote!{
        #[allow(unused_assignments)]
        pub fn as_ref(&self) -> Ref {
            let &Mut { #(ref #field0),* } = self;
            Ref { #(#field1: &**#field2),* }
        }
    }
}

fn enum_mut_as_ref(variants: &[Variant]) -> quote::Tokens {
    let bodies = variants.iter().map(|v| {
        let &Variant{ident: ref variant, flag: _, ref body} = v;
        let size_vars = size_vars(body);
        let field0 = body.iter().map(|f| &f.ident).filter(|f| !size_vars.contains(*f));
        let field1 = body.iter().map(|f| &f.ident).filter(|f| !size_vars.contains(*f));
        let field2 = body.iter().map(|f| &f.ident).filter(|f| !size_vars.contains(*f));
        quote!{
            &Mut::#variant { #(ref #field0),* } => {
                Ref::#variant { #(#field1: &**#field2),* }
            }
        }
    });
    quote!{
        #[allow(unused_assignments)]
        pub fn as_ref(&self) -> Ref {
            match self {
                #(#bodies),*
            }
        }
    }
}

fn struct_fill_vec(fields: &[Field]) -> quote::Tokens {
    let (fields, counters, fill_vec_with) = fill_vec_body(fields);
    quote!{
        #[allow(unused_assignments)]
        pub fn fill_vec(&self, mut __packet_macro_bytes: &mut Vec<u8>) {
            __packet_macro_bytes.reserve_exact(self.len());
            let &Ref { #fields } = self;
            #counters
            #fill_vec_with
        }
    }
}

fn enum_fill_vec(tag_ident: &Ident, tag_ty: &Ty, variants: &[Variant]) -> quote::Tokens {
    let bodies = variants.iter().map(|v| {
        let &Variant{ident: ref variant, flag: ref tag_val, ref body} = v;
        let fill_vec_tag = fill_vec_with_field(
            &Field{ident: tag_ident.clone(), ty: tag_ty.clone()});
        let (fields, counters, fill_vec_with) = fill_vec_body(body);
        quote!{
            &Ref::#variant { #fields } => {
                let #tag_ident: &#tag_ty = &#tag_val;
                #counters
                #fill_vec_tag
                #fill_vec_with
            }
        }
    });
    quote!{
        #[allow(unused_assignments)]
        pub fn fill_vec(&self, mut __packet_macro_bytes: &mut Vec<u8>) {
            __packet_macro_bytes.reserve_exact(self.len());
            match self {
                #(#bodies),*
            }
        }
    }
}


fn fill_vec_body(fields: &[Field]) -> (quote::Tokens, quote::Tokens, quote::Tokens) {
    let size_vars: HashMap<_, _> = fields.iter()
        .filter_map(|f| f.ty.size_var().map(|s| (s, &f.ident))).collect();
    let counters = size_vars.iter().map(|(s, i)| quote!( let #s = &#i.len(); ));
    let counters = quote!( #(#counters);* );

    //FIXME assert multiple values of counters are equal...
    let fill_vec_with = fields.iter().map(|f| match size_vars.contains_key(&f.ident) {
        true => fill_vec_with_counter(f),
        false => fill_vec_with_field(f),
    });
    let fill_vec_with = quote!( #(#fill_vec_with);* );

    let field = fields.iter()
        .filter(|f| !size_vars.contains_key(&f.ident)).map(|f| &f.ident);
    let fields = quote!( #(#field),* );
    (fields, counters, fill_vec_with)
}

fn fill_vec_with_field(&Field{ ref ident, ref ty }: &Field) -> quote::Tokens {
    use Ty::*;
    match ty {
        ty @ &Path(..) | ty @ &Array(..) | ty @ &Tuple(..)  => quote! {
            unsafe {
                let __packet_field_size = ::std::mem::size_of::<#ty>();
                let __packet_field_bytes = #ident as *const #ty as *const u8;
                let __packet_field_slice =
                    ::std::slice::from_raw_parts(__packet_field_bytes, __packet_field_size);
                __packet_macro_bytes.extend_from_slice(__packet_field_slice)
            }
        },
        &VarArray(ref ty, ref count) => quote! {
            unsafe {
                let __packet_field_count = *#count as usize;
                let __packet_field_size =
                    ::std::mem::size_of::<#ty>() * __packet_field_count;
                let __packet_field_field_bytes = #ident.as_ptr() as *const u8;
                let __packet_field_field_slice =
                    ::std::slice::from_raw_parts(__packet_field_field_bytes, __packet_field_size);
                __packet_macro_bytes.extend_from_slice(__packet_field_field_slice)
            }
        },
    }
}

fn fill_vec_with_counter(&Field{ ref ident, ref ty }: &Field) -> quote::Tokens {
    use Ty::*;
    match ty {
        ty @ &Path(..)  => quote! {
            unsafe {
                let __packet_field_size = ::std::mem::size_of::<#ty>();
                let __packet_field_bytes = (*#ident) as #ty;
                let __packet_field_bytes = (&__packet_field_bytes)
                    as *const #ty as *const u8;
                let __packet_field_slice =
                    ::std::slice::from_raw_parts(__packet_field_bytes, __packet_field_size);
                __packet_macro_bytes.extend_from_slice(__packet_field_slice)
            }
        },
        ty @ &VarArray(..) | ty @ &Array(..) | ty @ &Tuple(..) =>
            panic!("cannot use a {:?} as an array size", ty),
    }
}

fn struct_get_len(field: &[Field]) -> quote::Tokens {
    let counters: HashMap<_, _> = field.iter()
        .filter_map(|f| f.ty.size_var().map(|s| (s, &f.ident))).collect();
    let counters = counters.iter().map(|(s, i)| quote!( let #s = &#i.len(); ));
    let size_vars = size_vars(field);
    let size = field.iter().map(|f| f.ty.size());
    let field = field.iter().filter(|f| !size_vars.contains(&f.ident)).map(|f| &f.ident);
    quote!{
        pub fn len(&self) -> usize {
            let &Ref { #(#field),* } = self;
            #(#counters);*
            0usize #(+ #size)*
        }
    }
}

fn enum_get_len(tag_ty: &Ty, variants: &[Variant]) -> quote::Tokens {
    let bodies = variants.iter().map(|v| {
        let &Variant{ident: ref variant, flag: _, ref body} = v;
        let counters: HashMap<_, _> = body.iter()
            .filter_map(|f| f.ty.size_var().map(|s| (s, &f.ident))).collect();
        let counters = counters.iter().map(|(s, i)| quote!( let #s = &#i.len(); ));

        let size_vars = size_vars(&body);
        let size = body.iter().map(|f| f.ty.size());
        let tag_size = tag_ty.size();
        let field = body.iter()
            .filter(|f| !size_vars.contains(&f.ident)).map(|f| &f.ident);
        quote!{
            &Ref::#variant { #(#field),* } => {
                #(#counters);*
                #tag_size #( + #size)*
            }
        }
    });
    quote!{
        #[allow(unused_assignments)]
        pub fn len(&self) -> usize {
            match self {
                #(#bodies),*
            }
        }
    }
}

fn enum_minsize(tag_ty: &Ty, variants: &[Variant]) -> quote::Tokens {
    let size = variants.iter().map(|v| {
        let &Variant{ident: _, flag: _, ref body} = v;
        let tag_size = tag_ty.min_size();
        let sizes = minsize(body);
        quote!{
            #tag_size #sizes
        }
    });
    quote!{
        #[allow(unused_assignments)]
        pub fn min_len() -> usize {
            let variant_size = ::std::usize::MAX;
            #(let variant_size = ::std::cmp::min(
                variant_size,
                #size
            );)*
            variant_size
        }
    }
}

fn minsize(fields: &[Field]) -> quote::Tokens {
    let sizes = fields.iter().map(|f| f.ty.min_size());
    quote!{
        #(+ #sizes)*
    }
}

fn size_vars(fields: &[Field]) -> HashSet<&Ident> {
    fields.iter().filter_map(|f| f.ty.size_var()).collect()
}

impl Ty {
    fn ref_type(&self) -> quote::Tokens {
        use Ty::*;
        match self {
            &Path(is_global, ref paths) => {
                let paths = paths.iter().map(PathFragment::quote);
                if is_global {
                    quote!( ::#(#paths)::* )
                } else {
                    quote!( #(#paths)::* )
                }
            },
            &Array(ref ty, ref size) => {
                let ty = ty.ref_type();
                quote!( [ #ty; #size ] )
            },
            &VarArray(ref ty, ref _ident) => {
                let ty = ty.ref_type();
                quote!( [#ty] )
            },
            &Tuple(ref tys) => {
                let tys = tys.iter().map(Ty::ref_type);
                quote!( ( #(#tys),* ) )
            },
        }
    }

    fn min_size(&self) -> quote::Tokens {
        use Ty::*;
        match self {
            ty @ &Path(..) | ty @ &Array(..) | ty @ &Tuple(..)  => {
                quote!( ::std::mem::size_of::<#ty>() )
            },
            &VarArray(..) => quote!( 0 ),
        }
    }

    fn err_size(&self) -> quote::Tokens {
        use Ty::*;
        match self {
            ty @ &Path(..) | ty @ &Array(..) | ty @ &Tuple(..)  => {
                quote!( ::std::mem::size_of::<#ty>() )
            },
            &VarArray(ref ty, ref count) => quote! {
                (::std::mem::size_of::<#ty>()
                    * (#count.as_ref().map(|c| **c).unwrap_or(0)) as usize)
            },
        }
    }

    fn size(&self) -> quote::Tokens {
        use Ty::*;
        match self {
            ty @ &Path(..) | ty @ &Array(..) | ty @ &Tuple(..)  => {
                quote!( ::std::mem::size_of::<#ty>() )
            },
            &VarArray(ref ty, ref count) => quote! {
                (::std::mem::size_of::<#ty>() * ((*#count) as usize))
            },
        }
    }

    fn size_var(&self) -> Option<&Ident> {
        use Ty::*;
        match self {
            &VarArray(ref _ty, ref count) => Some(count),
            &Path(..) | &Array(..) | &Tuple(..) => None,
        }
    }
}

impl quote::ToTokens for Ty {
    fn to_tokens(&self, toks: &mut quote::Tokens) {
        toks.append(self.ref_type())
    }
}

impl PathFragment {
    fn quote(&self) -> quote::Tokens {
        use PathFragment::*;
        match self {
            &Fish(ref i1, ref i2) => quote!(< #i1 as #i2>),
            &Ident(ref id) => quote!(#id),
        }
    }
}

fn extract_input(derive_input: &str) -> &str {
    let mut input = derive_input;

    for expected in &["#[allow(unused)]", "enum", "ProceduralMasqueradeDummyType", "{",
                     "Input", "=", "(0,", "stringify!", "("] {
        input = input.trim_left();
        assert!(input.starts_with(expected),
                "expected prefix {:?} not found in {:?}", expected, derive_input);
        input = &input[expected.len()..];
    }

    for expected in [")", ").0,", "}"].iter().rev() {
        input = input.trim_right();
        assert!(input.ends_with(expected),
                "expected suffix {:?} not found in {:?}", expected, derive_input);
        let end = input.len() - expected.len();
        input = &input[..end];
    }

    input
}

//use Packet;
//use PacketBody;

use syn::{Ident, Expr};
use syn::parse::ident as ident;
use syn::parse::expr as expr;

// a hacked version of the syn parsers which allows for an enlarged type syntax

#[derive(Debug, Clone)]
pub enum PacketBody {
    Struct(Vec<Field>),
    Enum(KindFlag, Vec<Variant>),
}

#[derive(Debug, Clone)]
pub struct KindFlag(pub Ident, pub Ty);

#[derive(Debug, Clone)]
pub struct Variant {
    pub ident: Ident,
    pub flag: Expr,
    pub body: Vec<Field>,
}

#[derive(Debug, Clone)]
pub struct Field {
    pub ident: Ident,
    pub ty: Ty,
}

#[derive(Debug, Clone)]
pub enum Ty {
    Path(bool, Vec<PathFragment>),
    Array(Box<Ty>, Expr),
    VarArray(Box<Ty>, Ident),
    Tuple(Vec<Ty>),
}

#[derive(Debug, Clone)]
pub enum PathFragment {
    Fish(Ident, Ident),
    Ident(Ident),
}


named!(pub packet_body -> PacketBody, do_parse!(
    which: alt!(keyword!("struct") | keyword!("enum")) >>
    item: switch!(value!(which),
        "struct" => map!(struct_like_body, move |body| PacketBody::Struct(body))
        |
        "enum" => map!(enum_body, move |(kind, body)| PacketBody::Enum(kind, body))
    ) >>
    (item)
));

named!(pub enum_body -> (KindFlag, Vec<Variant>), do_parse!(
    punct!("{") >>
    kind_name: ident >>
    //call!(inspect, "after tag id") >>
    punct!(":") >>
    kind_type: ty >>
    punct!(",") >>
    variants: terminated_list!(punct!(","), variant) >>
    punct!("}") >>
    (KindFlag(kind_name, kind_type), variants)
));

named!(variant -> Variant, do_parse!(
    id: ident >>
    punct!(":") >>
    flag: expr >>
    punct!("=>") >>
    body: struct_like_body >>
    (Variant {
        ident: id,
        flag: flag,
        body: body,
    })
));

named!(pub struct_like_body -> Vec<Field>, do_parse!(
    punct!("{") >>
    fields: terminated_list!(punct!(","), struct_field) >>
    punct!("}") >>
    (fields)
));

named!(struct_field -> Field, do_parse!(
    id: ident >>
    punct!(":") >>
    ty: ty >>
    (Field {
        ident: id,
        ty: ty,
    })
));

named!(pub ty -> Ty, alt!(
    ty_paren // must be before ty_tup
    |
    ty_path // must be before ty_poly_trait_ref
    |
    ty_array
    |
    ty_var_len
    |
    ty_tup
));

named!(ty_path -> Ty, do_parse!(
    global: option!(punct!("::")) >>
    path: separated_nonempty_list!(punct!("::"), ty_path_fragment) >>
    (Ty::Path(global.is_some(), path))
));

named!(ty_path_fragment -> PathFragment, alt!(
    ty_fish
    |
    ty_ident
));

named!(ty_fish -> PathFragment, do_parse!(
    punct!("<") >>
    ident0: ident >>
    punct!("as") >>
    ident1: ident >>
    punct!("<") >>
    (PathFragment::Fish(ident0, ident1))
));

named!(ty_ident -> PathFragment, do_parse!(
    ident: ident >>
    (PathFragment::Ident(ident))
));

named!(ty_paren -> Ty, do_parse!(
    punct!("(") >>
    elem: ty >>
    punct!(")") >>
    (elem)
));

named!(ty_array -> Ty, do_parse!(
    punct!("[") >>
    elem: ty >>
    punct!(";") >>
    len: expr >>
    punct!("]") >>
    (Ty::Array(Box::new(elem), len))
));

named!(ty_var_len -> Ty, do_parse!(
    punct!("[") >>
    elem: ty >>
    punct!("|") >>
    len: ident >>
    punct!("]") >>
    (Ty::VarArray(Box::new(elem), len))
));

named!(ty_tup -> Ty, do_parse!(
    punct!("(") >>
    elems: terminated_list!(punct!(","), ty) >>
    punct!(")") >>
    (Ty::Tuple(elems))
));

#[allow(dead_code)]
fn inspect<'a>(input: &'a str, print: &str) -> ::synom::IResult<&'a str, ()> {
    println!("{:?}", print);
    ::synom::IResult::Done(&input, ())
}

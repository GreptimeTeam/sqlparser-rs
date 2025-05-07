// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Procedural macros for sqlparser.
//!
//! This crate provides:
//! - [`Visit`] and [`VisitMut`] derive macros for AST traversal.
//! - [`derive_dialect!`] macro for creating custom SQL dialects.

use proc_macro2::Literal;
use quote::quote;
use quote::{quote_spanned, ToTokens};
use syn::parse_macro_input;
use syn::spanned::Spanned;
use syn::{
    AngleBracketedGenericArguments, DataEnum, DataStruct, FieldsNamed, FieldsUnnamed,
    GenericArgument, MetaList, Path, PathArguments, PathSegment,
};
use syn::{Attribute, Data, DeriveInput, Fields, Ident, Meta, Type, TypePath};

mod dialect;
mod visit;

/// Implementation of `#[derive(VisitMut)]`
#[proc_macro_derive(VisitMut, attributes(visit))]
pub fn derive_visit_mut(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as syn::DeriveInput);
    visit::derive_visit(
        input,
        &visit::VisitType {
            visit_trait: quote!(VisitMut),
            visitor_trait: quote!(VisitorMut),
            modifier: Some(quote!(mut)),
        },
    )
}

/// Implementation of `#[derive(Visit)]`
#[proc_macro_derive(Visit, attributes(visit))]
pub fn derive_visit_immutable(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as syn::DeriveInput);
    visit::derive_visit(
        input,
        &visit::VisitType {
            visit_trait: quote!(Visit),
            visitor_trait: quote!(Visitor),
            modifier: None,
        },
    )
}

/// Procedural macro for deriving new SQL dialects.
#[proc_macro]
pub fn derive_dialect(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as dialect::DeriveDialectInput);
    dialect::derive_dialect(input)
}

/// Determine the variable type to decide which method in the `Convert` trait to use
fn get_var_type(ty: &Type) -> proc_macro2::TokenStream {
    let span = ty.span();
    if let Type::Path(TypePath {
        path: Path { segments, .. },
        ..
    }) = ty
    {
        if let Some(PathSegment { ident, arguments }) = segments.first() {
            return match ident.to_string().as_str() {
                "Option" => {
                    if let PathArguments::AngleBracketed(AngleBracketedGenericArguments {
                        args,
                        ..
                    }) = arguments
                    {
                        if let Some(GenericArgument::Type(Type::Path(TypePath {
                            path: Path { segments, .. },
                            ..
                        }))) = args.first()
                        {
                            if let Some(PathSegment { ident, .. }) = segments.first() {
                                return match ident.to_string().as_str() {
                                    "Box" => quote_spanned!(span => Convert::convert_option_box),
                                    "Vec" => quote_spanned!(span => Convert::convert_option_vec),
                                    _ => quote_spanned!(span => Convert::convert_option),
                                };
                            }
                        }
                    }
                    quote_spanned!(span => Convert::convert_option)
                }
                "Vec" => {
                    if let PathArguments::AngleBracketed(AngleBracketedGenericArguments {
                        args,
                        ..
                    }) = arguments
                    {
                        if let Some(GenericArgument::Type(Type::Path(TypePath {
                            path: Path { segments, .. },
                            ..
                        }))) = args.first()
                        {
                            if let Some(PathSegment { ident, .. }) = segments.first() {
                                return match ident.to_string().as_str() {
                                    "Vec" => quote_spanned!(span => Convert::convert_matrix),
                                    "Box" => quote_spanned!(span => Convert::convert_vec_box),
                                    _ => quote_spanned!(span => Convert::convert_vec),
                                };
                            }
                        }
                    }
                    quote_spanned!(span => Convert::convert_vec)
                }
                "Box" => quote_spanned!(span => Convert::convert_box),
                _ => quote_spanned!(span => Convert::convert),
            };
        }
    }
    quote_spanned!(span => Convert::convert)
}

/// Obtain the struct path where `datafusion` `sqlparser` is located from derive macro helper attribute `df_path`,
/// if value not given, the default return is `df_sqlparser::ast`
fn get_crate_path(st: &syn::DeriveInput) -> proc_macro2::TokenStream {
    let span = st.span();
    for attr in &st.attrs {
        let Meta::List(MetaList {
            path: Path { segments, .. },
            tokens,
            ..
        }) = &attr.meta
        else {
            continue;
        };
        if let Some(PathSegment { ident, .. }) = segments.first() {
            if ident.to_string().as_str() == "df_path" {
                return tokens.clone();
            }
        }
    }
    quote_spanned!(span => df_sqlparser::ast)
}

/// Check whether the attribute `ignore_item` exists. If the attribute exists,
/// the corresponding convert method will not be generated.
/// If exist attribute `ignore_item`
/// 1. enum conversion returns panic
/// 2. struct conversion does not generate the corresponding field
fn ignore_convert(attrs: &Vec<Attribute>) -> bool {
    for attr in attrs {
        let Meta::Path(Path { segments, .. }) = &attr.meta else {
            continue;
        };
        if let Some(PathSegment { ident, .. }) = segments.first() {
            if ident.to_string().as_str() == "ignore_item" {
                return true;
            }
        }
    }
    false
}

fn convert_struct(st: &syn::DeriveInput) -> proc_macro2::TokenStream {
    let name = &st.ident;
    let path = get_crate_path(st);
    // for struct pattern like
    // struct xxx {
    //  xxx: xxx
    // }
    if let Data::Struct(DataStruct {
        fields: Fields::Named(FieldsNamed { named, .. }),
        ..
    }) = &st.data
    {
        let span = named.span();
        let mut fields: Vec<proc_macro2::TokenStream> = Vec::with_capacity(named.len());
        for field in named {
            if ignore_convert(&field.attrs) {
                continue;
            }
            let field_name = field.ident.clone().unwrap();
            let var_type = get_var_type(&field.ty);
            let span = field_name.span();
            let code = quote_spanned! { span =>
                #field_name: #var_type(value.#field_name),
            };
            fields.push(code);
        }
        return quote_spanned! { span =>
            impl From<#name> for #path::#name {
                #[allow(unused_variables)]
                fn from(value: #name) -> Self {
                    Self {
                        #(#fields)*
                    }
                }
            }
        };
    }
    // for struct pattern like
    // struct xxx(xxxx);
    if let Data::Struct(DataStruct {
        fields: Fields::Unnamed(FieldsUnnamed { unnamed, .. }),
        ..
    }) = &st.data
    {
        let span = unnamed.span();
        let mut fields: Vec<proc_macro2::TokenStream> = Vec::with_capacity(unnamed.len());
        for i in 0..unnamed.len() {
            if ignore_convert(&unnamed[i].attrs) {
                continue;
            }
            let field_name = Literal::usize_unsuffixed(i);
            let var_type = get_var_type(&unnamed[i].ty);
            let span = unnamed[i].span();
            let code = quote_spanned! { span =>
                #var_type(value.#field_name),
            };
            fields.push(code);
        }
        return quote_spanned! { span =>
            impl From<#name> for #path::#name {
                #[allow(unused_variables)]
                fn from(value: #name) -> Self {
                    Self(#(#fields)*)
                }
            }
        };
    }
    panic!("Unrecognised Struct Type{}", st.to_token_stream())
}

fn convert_enum(st: &DeriveInput) -> proc_macro2::TokenStream {
    let name = &st.ident;
    let path = get_crate_path(st);
    if let Data::Enum(DataEnum { variants, .. }) = &st.data {
        let span = variants.span();
        let mut fields: Vec<proc_macro2::TokenStream> = Vec::with_capacity(variants.len());
        for field in variants {
            let enum_name = &field.ident;
            let span = enum_name.span();
            let ignore_convert = ignore_convert(&field.attrs);
            // for enum item like xxxxxx(xxx)
            if let Fields::Unnamed(FieldsUnnamed { unnamed, .. }) = &field.fields {
                let inner_names = ('a'..='z')
                    .map(|x| Ident::new(x.to_string().as_str(), unnamed.span()))
                    .collect::<Vec<_>>()[..unnamed.len()]
                    .to_vec();
                let mut codes: Vec<proc_macro2::TokenStream> = Vec::with_capacity(unnamed.len());
                let inner_fields: Vec<_> = inner_names.iter().map(|x| quote!(#x,)).collect();
                for (inner_name, field) in inner_names.iter().zip(unnamed.iter()) {
                    let var_type = get_var_type(&field.ty);
                    let span = field.span();
                    codes.push(quote_spanned! { span =>
                        #var_type(#inner_name),
                    });
                }
                fields.push(if ignore_convert {
                    quote_spanned! { span =>
                        #name::#enum_name(#(#inner_fields)*) => panic!("Convert on this item is ignored"),
                    }
                } else {
                    quote_spanned! { span =>
                        #name::#enum_name(#(#inner_fields)*) => Self::#enum_name(#(#codes)*),
                    }
                });
            }
            //  for enum item like
            //  xxxxxx {
            //     xxx: xxxx,
            // },
            if let Fields::Named(FieldsNamed { named, .. }) = &field.fields {
                let mut inner_fields: Vec<proc_macro2::TokenStream> =
                    Vec::with_capacity(named.len());
                let mut codes: Vec<proc_macro2::TokenStream> = Vec::with_capacity(named.len());
                let span = named.span();
                for field in named {
                    let field_name = field.ident.clone().unwrap();
                    let span = field_name.span();
                    let var_type = get_var_type(&field.ty);
                    inner_fields.push(quote_spanned!(span => #field_name,));
                    codes.push(quote_spanned! { span =>
                        #field_name: #var_type(#field_name),
                    });
                }
                fields.push(if ignore_convert {
                    quote_spanned! { span =>
                        #name::#enum_name{#(#inner_fields)*} => panic!("Convert on this item is ignored"),
                    }
                } else {
                    quote_spanned! { span =>
                        #name::#enum_name{#(#inner_fields)*} => Self::#enum_name{#(#codes)*},
                    }
                });
            }
            //  for enum item like
            // xxxxxx
            if let Fields::Unit = &field.fields {
                let span = field.span();
                fields.push(if ignore_convert {
                    quote_spanned! { span =>
                        #name::#enum_name => panic!("Convert on this item is ignored"),
                    }
                } else {
                    quote_spanned! { span =>
                        #name::#enum_name => Self::#enum_name,
                    }
                });
            }
        }
        return quote_spanned! { span =>
            impl From<#name> for #path::#name {
                #[allow(unused_variables)]
                fn from(value: #name) -> Self {
                    match value{
                        #(#fields)*
                    }
                }
            }
        };
    }
    panic!("Unrecognised Enum Type{}", st.to_token_stream())
}

fn convert_union(st: &DeriveInput) -> proc_macro2::TokenStream {
    let name = &st.ident;
    let path = get_crate_path(st);

    if let Data::Union(data_union) = &st.data {
        let span = data_union.fields.span();
        let mut fields: Vec<proc_macro2::TokenStream> =
            Vec::with_capacity(data_union.fields.named.len());

        for field in &data_union.fields.named {
            if ignore_convert(&field.attrs) {
                continue;
            }
            let field_name = field.ident.clone().unwrap();
            let var_type = get_var_type(&field.ty);
            let span = field_name.span();
            let code = quote_spanned! { span =>
                #field_name: unsafe { #var_type(value.#field_name) },
            };
            fields.push(code);
        }

        quote_spanned! { span =>
            impl From<#name> for #path::#name {
                #[allow(unused_variables)]
                fn from(value: #name) -> Self {
                    unsafe {
                        Self {
                            #(#fields)*
                        }
                    }
                }
            }
        }
    } else {
        panic!("Expected Union type")
    }
}

fn expand_df_convert(st: &DeriveInput) -> proc_macro2::TokenStream {
    match st.data {
        syn::Data::Struct(_) => convert_struct(st),
        syn::Data::Enum(_) => convert_enum(st),
        syn::Data::Union(_) => convert_union(st),
    }
}

/// Derive macro to implement `From` Trait. Convert the current sqlparser struct to the struct used by datafusion sqlparser.
/// There are two helper attributes that can be marked on the derive struct/enum, affecting the generated Convert function
/// 1. `#[df_path(....)]`: Most structures are defined in `df_sqlparser::ast`, if the path of some structures is not in this path,
///    user need to specify `df_path` to tell the compiler the location of this struct/enum
/// 2. `#[ignore_item]`: Marked on the field of the struct/enum, indicating that the Convert method of the field of the struct/enum is not generated·
#[proc_macro_derive(DFConvert, attributes(df_path, ignore_item))]
pub fn derive_df_convert(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    expand_df_convert(&parse_macro_input!(input as DeriveInput)).into()
}

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

//! Derives for Iceberg's string-keyed property maps.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{
    Attribute, Data, DeriveInput, Error, Expr, ExprLit, ExprPath, Field, Fields, Ident, Lit, Meta,
    Path, Type, parse_macro_input,
};

/// Derive parsing, defaults, JSON serialization, and getters for a typed property map.
///
/// Each field must declare the table-property key and its default:
///
/// ```ignore
/// #[derive(Properties)]
/// struct Properties {
///     #[key = "write.format.default"]
///     #[default(DataFileFormat::Parquet)]
///     write_format_default: DataFileFormat,
/// }
/// ```
///
/// `parse_with` may be used for property types that do not implement `FromStr` or need
/// validation. `serialize_with` supplies the string representation used in JSON. Optional
/// fields are omitted from JSON when they are `None`. Fields must implement `Clone`; they also
/// need `FromStr` and `ToString` unless the relevant custom parsing or serialization attribute is
/// supplied.
#[proc_macro_derive(Properties, attributes(key, default, parse_with, serialize_with))]
pub fn derive_properties(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    match expand_properties(input) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.into_compile_error().into(),
    }
}

struct PropertyField {
    ident: Ident,
    ty: Type,
    docs: Vec<Attribute>,
    key: syn::LitStr,
    default: Expr,
    parse_with: Option<Path>,
    serialize_with: Option<Path>,
    is_option: bool,
}

fn expand_properties(input: DeriveInput) -> syn::Result<TokenStream2> {
    let struct_name = input.ident;
    let fields = match input.data {
        Data::Struct(data) => match data.fields {
            Fields::Named(fields) => fields.named,
            _ => {
                return Err(Error::new_spanned(
                    struct_name,
                    "Properties can only be derived for structs with named fields",
                ));
            }
        },
        _ => {
            return Err(Error::new_spanned(
                struct_name,
                "Properties can only be derived for structs",
            ));
        }
    };

    let fields = fields
        .iter()
        .map(parse_property_field)
        .collect::<syn::Result<Vec<_>>>()?;

    let defaults = fields.iter().map(|field| {
        let ident = &field.ident;
        let default = &field.default;
        quote!(#ident: #default)
    });

    let getters = fields.iter().map(|field| {
        let ident = &field.ident;
        let ty = &field.ty;
        let docs = &field.docs;
        quote! {
            #(#docs)*
            pub fn #ident(&self) -> #ty {
                self.#ident.clone()
            }
        }
    });

    let parses = fields.iter().map(|field| {
        let ident = &field.ident;
        let ty = &field.ty;
        let key = &field.key;
        let default = &field.default;
        let parse = match &field.parse_with {
            Some(parse_with) => quote! {
                #parse_with(value).map_err(|error| {
                    format!("Invalid value for {}: {error}", #key)
                })?
            },
            None => quote! {
                value.parse::<#ty>().map_err(|error| {
                    format!("Invalid value for {}: {error}", #key)
                })?
            },
        };

        quote! {
            #ident: match properties.get(#key) {
                Some(value) => #parse,
                None => #default,
            }
        }
    });

    let serializes = fields.iter().map(serialize_field);

    Ok(quote! {
        impl ::std::default::Default for #struct_name {
            fn default() -> Self {
                Self {
                    #(#defaults,)*
                }
            }
        }

        impl #struct_name {
            pub(crate) fn from_properties(
                properties: &::std::collections::HashMap<::std::string::String, ::std::string::String>,
            ) -> ::std::result::Result<Self, ::std::string::String> {
                Ok(Self {
                    #(#parses,)*
                })
            }

            #(#getters)*
        }

        impl ::serde::Serialize for #struct_name {
            fn serialize<S>(&self, serializer: S) -> ::std::result::Result<S::Ok, S::Error>
            where
                S: ::serde::Serializer,
            {
                use ::serde::ser::SerializeMap as _;

                let mut map = serializer.serialize_map(None)?;
                #(#serializes)*
                map.end()
            }
        }

        impl<'de> ::serde::Deserialize<'de> for #struct_name {
            fn deserialize<D>(deserializer: D) -> ::std::result::Result<Self, D::Error>
            where
                D: ::serde::Deserializer<'de>,
            {
                let properties = <::std::collections::HashMap<::std::string::String, ::std::string::String> as ::serde::Deserialize>::deserialize(deserializer)?;
                Self::from_properties(&properties).map_err(::serde::de::Error::custom)
            }
        }
    })
}

fn parse_property_field(field: &Field) -> syn::Result<PropertyField> {
    let ident = field
        .ident
        .clone()
        .ok_or_else(|| Error::new_spanned(field, "Properties fields must be named"))?;
    let key = attribute_string_value(&field.attrs, "key")?.ok_or_else(|| {
        Error::new_spanned(field, "Properties fields must declare #[key = \"...\"]")
    })?;
    let default = attribute_expression_value(&field.attrs, "default")?.ok_or_else(|| {
        Error::new_spanned(field, "Properties fields must declare #[default(...)]")
    })?;

    Ok(PropertyField {
        ident,
        ty: field.ty.clone(),
        docs: field
            .attrs
            .iter()
            .filter(|attribute| attribute.path().is_ident("doc"))
            .cloned()
            .collect(),
        key,
        default,
        parse_with: attribute_path_value(&field.attrs, "parse_with")?,
        serialize_with: attribute_path_value(&field.attrs, "serialize_with")?,
        is_option: is_option_type(&field.ty),
    })
}

fn attribute_string_value(
    attributes: &[Attribute],
    name: &str,
) -> syn::Result<Option<syn::LitStr>> {
    let Some(attribute) = find_attribute(attributes, name)? else {
        return Ok(None);
    };

    match &attribute.meta {
        Meta::NameValue(name_value) => match &name_value.value {
            Expr::Lit(ExprLit {
                lit: Lit::Str(value),
                ..
            }) => Ok(Some(value.clone())),
            _ => Err(Error::new_spanned(
                attribute,
                format!("{name} must be a string literal"),
            )),
        },
        _ => Err(Error::new_spanned(
            attribute,
            format!("{name} must use the form #[{name} = ...]"),
        )),
    }
}

fn attribute_expression_value(attributes: &[Attribute], name: &str) -> syn::Result<Option<Expr>> {
    let Some(attribute) = find_attribute(attributes, name)? else {
        return Ok(None);
    };

    match &attribute.meta {
        Meta::NameValue(name_value) => Ok(Some(name_value.value.clone())),
        Meta::List(_) => attribute.parse_args::<Expr>().map(Some),
        _ => Err(Error::new_spanned(
            attribute,
            format!("{name} must use the form #[{name}(...)]"),
        )),
    }
}

fn attribute_path_value(attributes: &[Attribute], name: &str) -> syn::Result<Option<Path>> {
    let Some(expression) = attribute_expression_value(attributes, name)? else {
        return Ok(None);
    };

    match expression {
        Expr::Path(ExprPath { path, .. }) => Ok(Some(path)),
        _ => Err(Error::new_spanned(
            expression,
            format!("{name} must be a path"),
        )),
    }
}

fn find_attribute<'a>(
    attributes: &'a [Attribute],
    name: &str,
) -> syn::Result<Option<&'a Attribute>> {
    let mut matching = attributes
        .iter()
        .filter(|attribute| attribute.path().is_ident(name));
    let first = matching.next();
    if let Some(duplicate) = matching.next() {
        return Err(Error::new_spanned(
            duplicate,
            format!("duplicate #[{name}] attribute"),
        ));
    }
    Ok(first)
}

fn is_option_type(ty: &Type) -> bool {
    let Type::Path(type_path) = ty else {
        return false;
    };

    type_path
        .path
        .segments
        .last()
        .is_some_and(|segment| segment.ident == "Option")
}

fn serialize_field(field: &PropertyField) -> TokenStream2 {
    let ident = &field.ident;
    let key = &field.key;
    if field.is_option {
        let value = match &field.serialize_with {
            Some(serialize_with) => quote!(#serialize_with(&self.#ident)),
            None => quote!(::std::string::ToString::to_string(
                self.#ident.as_ref().expect("checked is_some above")
            )),
        };
        quote! {
            if self.#ident.is_some() {
                map.serialize_entry(#key, &#value)?;
            }
        }
    } else {
        let value = match &field.serialize_with {
            Some(serialize_with) => quote!(#serialize_with(&self.#ident)),
            None => quote!(::std::string::ToString::to_string(&self.#ident)),
        };
        quote! {
            map.serialize_entry(#key, &#value)?;
        }
    }
}

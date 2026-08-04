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
use quote::{format_ident, quote};
use syn::{
    Attribute, Data, DeriveInput, Error, Expr, ExprPath, Field, Fields, Ident, Meta, Path, Type,
    parse_macro_input,
};

/// Derive parsing, defaults, JSON serialization, and getters for a typed property map.
///
/// Each field must declare the table-property key and its default:
///
/// ```ignore
/// #[derive(Properties)]
/// struct Properties {
///     #[key(TableProperties::DEFAULT_FILE_FORMAT)]
///     #[default(DataFileFormat::Parquet)]
///     #[doc = "Default file format"]
///     write_format_default: DataFileFormat,
/// }
/// ```
///
/// `prefix` captures a family of properties in a `HashMap<String, T>`, keyed by the suffix after
/// the declared prefix. `parse_with` may be used for exact-key property types that do not implement
/// `FromStr` or need validation. `serialize_with` supplies their string representation in JSON.
/// Optional fields are omitted from JSON when they are `None`. Fields must implement `Clone`; they
/// also need `FromStr` and `ToString` unless the relevant custom parsing or serialization attribute
/// is supplied.
#[proc_macro_derive(
    Properties,
    attributes(key, prefix, default, parse_with, serialize_with)
)]
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
    key: Option<Expr>,
    prefix: Option<Expr>,
    default: Expr,
    parse_with: Option<Path>,
    serialize_with: Option<Path>,
    option_inner_type: Option<Type>,
    map_value_type: Option<Type>,
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

    let accessors = fields.iter().map(|field| {
        let ident = &field.ident;
        let ty = &field.ty;
        let docs = &field.docs;
        let with_ident = format_ident!("with_{ident}");
        let getter_doc = format!("Returns the `{ident}` property.");
        let with_doc = format!("Sets the `{ident}` property.");
        let getter_docs = if docs.is_empty() {
            quote!(#[doc = #getter_doc])
        } else {
            quote!(#(#docs)*)
        };
        quote! {
            #getter_docs
            pub fn #ident(&self) -> #ty {
                self.#ident.clone()
            }

            #[doc = #with_doc]
            pub fn #with_ident(mut self, value: #ty) -> Self {
                self.#ident = value;
                self
            }
        }
    });

    let parses = fields.iter().map(parse_field);

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

            #(#accessors)*
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
    let key = attribute_expression_value(&field.attrs, "key")?;
    let prefix = attribute_expression_value(&field.attrs, "prefix")?;
    if key.is_some() == prefix.is_some() {
        return Err(Error::new_spanned(
            field,
            "Properties fields must declare exactly one of #[key(...)] or #[prefix(...)]",
        ));
    }
    let default = attribute_expression_value(&field.attrs, "default")?.ok_or_else(|| {
        Error::new_spanned(field, "Properties fields must declare #[default(...)]")
    })?;

    let map_value_type = map_value_type(&field.ty);
    if prefix.is_some() && map_value_type.is_none() {
        return Err(Error::new_spanned(
            &field.ty,
            "#[prefix(...)] fields must have type HashMap<String, T>",
        ));
    }
    if prefix.is_some()
        && (attribute_path_value(&field.attrs, "parse_with")?.is_some()
            || attribute_path_value(&field.attrs, "serialize_with")?.is_some())
    {
        return Err(Error::new_spanned(
            field,
            "#[prefix(...)] fields do not support parse_with or serialize_with",
        ));
    }

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
        prefix,
        default,
        parse_with: attribute_path_value(&field.attrs, "parse_with")?,
        serialize_with: attribute_path_value(&field.attrs, "serialize_with")?,
        option_inner_type: option_inner_type(&field.ty),
        map_value_type,
    })
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

fn parse_field(field: &PropertyField) -> TokenStream2 {
    let ident = &field.ident;
    let default = &field.default;

    if let Some(prefix) = &field.prefix {
        let value_type = field
            .map_value_type
            .as_ref()
            .expect("prefix fields are validated as maps");
        return quote! {
            #ident: {
                let parsed = properties
                    .iter()
                    .filter_map(|(key, value)| {
                        key.strip_prefix(#prefix).map(|suffix| {
                            value.parse::<#value_type>()
                                .map(|parsed| (suffix.to_string(), parsed))
                                .map_err(|error| format!("Invalid value for {key}: {error}"))
                        })
                    })
                    .collect::<::std::result::Result<
                        ::std::collections::HashMap<_, _>,
                        ::std::string::String,
                    >>()?;
                if parsed.is_empty() {
                    #default
                } else {
                    parsed
                }
            }
        };
    }

    let ty = &field.ty;
    let key = field.key.as_ref().expect("exact-key fields have a key");
    let parse = match (&field.parse_with, &field.option_inner_type) {
        (Some(parse_with), _) => quote! {
            #parse_with(value).map_err(|error| {
                format!("Invalid value for {}: {error}", #key)
            })?
        },
        (None, Some(inner_type)) => quote! {
            Some(value.parse::<#inner_type>().map_err(|error| {
                format!("Invalid value for {}: {error}", #key)
            })?)
        },
        (None, None) => quote! {
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
}

fn option_inner_type(ty: &Type) -> Option<Type> {
    let Type::Path(type_path) = ty else {
        return None;
    };

    let segment = type_path.path.segments.last()?;
    if segment.ident != "Option" {
        return None;
    }

    let syn::PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return None;
    };
    let Some(syn::GenericArgument::Type(inner_type)) = arguments.args.first() else {
        return None;
    };

    Some(inner_type.clone())
}

fn map_value_type(ty: &Type) -> Option<Type> {
    let Type::Path(type_path) = ty else {
        return None;
    };

    let segment = type_path.path.segments.last()?;
    if segment.ident != "HashMap" {
        return None;
    }

    let syn::PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return None;
    };
    let Some(syn::GenericArgument::Type(value_type)) = arguments.args.iter().nth(1) else {
        return None;
    };

    Some(value_type.clone())
}

fn serialize_field(field: &PropertyField) -> TokenStream2 {
    let ident = &field.ident;
    if let Some(prefix) = &field.prefix {
        return quote! {
            for (suffix, value) in &self.#ident {
                let key = format!("{}{}", #prefix, suffix);
                map.serialize_entry(&key, &::std::string::ToString::to_string(value))?;
            }
        };
    }

    let key = field.key.as_ref().expect("exact-key fields have a key");
    if field.option_inner_type.is_some() {
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

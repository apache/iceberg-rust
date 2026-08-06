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
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{
    Attribute, Data, DeriveInput, Error, Expr, ExprLit, ExprPath, Field, Fields, Ident, Lit, Meta,
    Path, Token, Type, parenthesized, parse_macro_input,
};

/// Derive parsing, defaults, and JSON serialization for a typed property map.
///
/// Leaf fields must declare the table-property key and its default:
///
/// ```
/// use iceberg_property_macro::Properties;
///
/// #[derive(Properties)]
/// struct Properties {
///     #[key = "write.format.default"]
///     #[default = "parquet"]
///     #[doc = "Default file format"]
///     #[property(pub(getter), pub(setter))]
///     write_format_default: String,
/// }
///
/// let mut properties = Properties::default();
/// assert_eq!(properties.write_format_default(), "parquet");
/// properties.set_write_format_default("orc".to_string());
/// assert_eq!(properties.write_format_default(), "orc");
/// ```
///
/// `prefix` captures a family of properties in a `HashMap<String, T>`, keyed by the suffix after
/// the declared prefix. `nested` embeds another `Properties` struct while keeping its serialized
/// property map flat. `parse_with` may be used for exact-key property types that do not implement
/// `FromStr` or need validation. `serialize_with` supplies their string representation in JSON.
/// `parse_properties_with` and `write_properties_with` provide access to the complete property map
/// for fields represented by more than one key. `additional_key` declares a second key and passes
/// it to those hooks after the primary key. Write hooks are also passed the field default and are
/// responsible for omitting or removing default-valued properties.
/// Optional fields are omitted from JSON when they are `None`. Fields need `FromStr` and `ToString`
/// unless the relevant custom parsing or serialization attribute is supplied. Leaf fields also
/// need `PartialEq` so values equal to their defaults can be omitted from JSON. String-literal and
/// path defaults are converted into their field type with `Into`. Boolean property values are
/// parsed case-insensitively.
///
/// Fields remain private unless their struct declaration makes them public. The
/// `#[property(pub(getter))]` and `#[property(pub(setter))]` options generate a public getter and
/// setter respectively. Getters borrow the field, and setters are named `set_<field>`.
#[proc_macro_derive(
    Properties,
    attributes(
        key,
        additional_key,
        prefix,
        nested,
        default,
        parse_with,
        serialize_with,
        parse_properties_with,
        write_properties_with,
        property
    )
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
    key: Option<Expr>,
    additional_key: Option<Expr>,
    prefix: Option<Expr>,
    nested: bool,
    default: Option<Expr>,
    parse_with: Option<Path>,
    serialize_with: Option<Path>,
    parse_properties_with: Option<Path>,
    write_properties_with: Option<Path>,
    option_inner_type: Option<Type>,
    map_value_type: Option<Type>,
    public_getter: bool,
    public_setter: bool,
    doc_attributes: Vec<Attribute>,
}

enum PublicAccessor {
    Getter,
    Setter,
}

impl Parse for PublicAccessor {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        input.parse::<Token![pub]>()?;
        let content;
        parenthesized!(content in input);
        let accessor = content.parse::<Ident>()?;
        if !content.is_empty() {
            return Err(content.error("expected getter or setter"));
        }

        match accessor.to_string().as_str() {
            "getter" => Ok(Self::Getter),
            "setter" => Ok(Self::Setter),
            _ => Err(Error::new_spanned(accessor, "expected getter or setter")),
        }
    }
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
        if field.nested {
            quote!(#ident: ::std::default::Default::default())
        } else {
            let default = field.default.as_ref().expect("leaf fields have defaults");
            let ty = &field.ty;
            let default = default_value(default, ty);
            quote!(#ident: #default)
        }
    });

    let parses = fields.iter().map(parse_field);

    let property_writes = fields.iter().map(write_field);

    let accessors = fields.iter().map(field_accessors);

    Ok(quote! {
        impl ::std::default::Default for #struct_name {
            fn default() -> Self {
                Self {
                    #(#defaults,)*
                }
            }
        }

        impl #struct_name {
            #(#accessors)*

            pub(crate) fn from_properties(
                properties: &::std::collections::HashMap<::std::string::String, ::std::string::String>,
            ) -> ::std::result::Result<Self, ::std::string::String> {
                Ok(Self {
                    #(#parses,)*
                })
            }

            pub(crate) fn write_properties(
                &self,
                properties: &mut ::std::collections::HashMap<
                    ::std::string::String,
                    ::std::string::String,
                >,
            ) {
                #(#property_writes)*
            }

            fn to_properties(
                &self,
            ) -> ::std::collections::HashMap<
                ::std::string::String,
                ::std::string::String,
            > {
                let mut properties = ::std::collections::HashMap::new();
                self.write_properties(&mut properties);
                properties
            }
        }

        impl ::serde::Serialize for #struct_name {
            fn serialize<S>(&self, serializer: S) -> ::std::result::Result<S::Ok, S::Error>
            where
                S: ::serde::Serializer,
            {
                ::serde::Serialize::serialize(&self.to_properties(), serializer)
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
    let additional_key = attribute_expression_value(&field.attrs, "additional_key")?;
    let prefix = attribute_expression_value(&field.attrs, "prefix")?;
    let nested = marker_attribute(&field.attrs, "nested")?;
    if usize::from(key.is_some()) + usize::from(prefix.is_some()) + usize::from(nested) != 1 {
        return Err(Error::new_spanned(
            field,
            "Properties fields must declare exactly one of #[key(...)], #[prefix(...)], or #[nested]",
        ));
    }
    let default = attribute_expression_value(&field.attrs, "default")?;
    if nested && default.is_some() {
        return Err(Error::new_spanned(
            field,
            "#[nested] fields use the nested type's Default implementation and cannot declare #[default(...)]",
        ));
    }
    if !nested && default.is_none() {
        return Err(Error::new_spanned(
            field,
            "Properties leaf fields must declare #[default(...)]",
        ));
    }

    let map_value_type = map_value_type(&field.ty);
    if prefix.is_some() && map_value_type.is_none() {
        return Err(Error::new_spanned(
            &field.ty,
            "#[prefix(...)] fields must have type HashMap<String, T>",
        ));
    }
    let parse_with = attribute_path_value(&field.attrs, "parse_with")?;
    let serialize_with = attribute_path_value(&field.attrs, "serialize_with")?;
    let parse_properties_with = attribute_path_value(&field.attrs, "parse_properties_with")?;
    let write_properties_with = attribute_path_value(&field.attrs, "write_properties_with")?;
    if additional_key.is_some()
        && parse_properties_with.is_none()
        && write_properties_with.is_none()
    {
        return Err(Error::new_spanned(
            field,
            "#[additional_key(...)] requires parse_properties_with or write_properties_with",
        ));
    }
    if (prefix.is_some() || nested)
        && (additional_key.is_some()
            || parse_with.is_some()
            || serialize_with.is_some()
            || parse_properties_with.is_some()
            || write_properties_with.is_some())
    {
        return Err(Error::new_spanned(
            field,
            "#[prefix(...)] and #[nested] fields do not support custom parse or write functions",
        ));
    }
    if parse_with.is_some() && parse_properties_with.is_some() {
        return Err(Error::new_spanned(
            field,
            "fields cannot declare both parse_with and parse_properties_with",
        ));
    }
    if serialize_with.is_some() && write_properties_with.is_some() {
        return Err(Error::new_spanned(
            field,
            "fields cannot declare both serialize_with and write_properties_with",
        ));
    }
    let (public_getter, public_setter) = property_accessors(&field.attrs)?;

    Ok(PropertyField {
        ident,
        ty: field.ty.clone(),
        key,
        additional_key,
        prefix,
        nested,
        default,
        parse_with,
        serialize_with,
        parse_properties_with,
        write_properties_with,
        option_inner_type: option_inner_type(&field.ty),
        map_value_type,
        public_getter,
        public_setter,
        doc_attributes: field
            .attrs
            .iter()
            .filter(|attribute| attribute.path().is_ident("doc"))
            .cloned()
            .collect(),
    })
}

fn property_accessors(attributes: &[Attribute]) -> syn::Result<(bool, bool)> {
    let Some(attribute) = find_attribute(attributes, "property")? else {
        return Ok((false, false));
    };

    let accessors =
        attribute.parse_args_with(Punctuated::<PublicAccessor, Token![,]>::parse_terminated)?;
    if accessors.is_empty() {
        return Err(Error::new_spanned(
            attribute,
            "property must declare pub(getter), pub(setter), or both",
        ));
    }

    let mut public_getter = false;
    let mut public_setter = false;
    for accessor in accessors {
        let selected = match accessor {
            PublicAccessor::Getter => &mut public_getter,
            PublicAccessor::Setter => &mut public_setter,
        };
        if *selected {
            return Err(Error::new_spanned(attribute, "duplicate property accessor"));
        }
        *selected = true;
    }

    Ok((public_getter, public_setter))
}

fn field_accessors(field: &PropertyField) -> TokenStream2 {
    let ident = &field.ident;
    let ty = &field.ty;
    let docs = &field.doc_attributes;
    let getter = field.public_getter.then(|| {
        quote! {
            #(#docs)*
            pub fn #ident(&self) -> &#ty {
                &self.#ident
            }
        }
    });
    let setter = field.public_setter.then(|| {
        let setter_ident = format_ident!("set_{}", ident);
        let setter_doc = format!("Sets `{ident}`.");
        quote! {
            #[doc = #setter_doc]
            pub fn #setter_ident(&mut self, value: #ty) {
                self.#ident = value;
            }
        }
    });

    quote! {
        #getter
        #setter
    }
}

fn marker_attribute(attributes: &[Attribute], name: &str) -> syn::Result<bool> {
    let Some(attribute) = find_attribute(attributes, name)? else {
        return Ok(false);
    };

    match &attribute.meta {
        Meta::Path(_) => Ok(true),
        _ => Err(Error::new_spanned(
            attribute,
            format!("{name} must use the form #[{name}]"),
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

fn parse_field(field: &PropertyField) -> TokenStream2 {
    let ident = &field.ident;
    if field.nested {
        let ty = &field.ty;
        return quote!(#ident: <#ty>::from_properties(properties)?);
    }

    let ty = &field.ty;
    let default = default_value(
        field.default.as_ref().expect("leaf fields have defaults"),
        ty,
    );
    let default = quote!({
        let value: #ty = #default;
        value
    });

    if let Some(parse_properties_with) = &field.parse_properties_with {
        let key = field.key.as_ref().expect("exact-key fields have a key");
        let parse = match &field.additional_key {
            Some(additional_key) => {
                quote!(#parse_properties_with(properties, #key, #additional_key, #default))
            }
            None => quote!(#parse_properties_with(properties, #key, #default)),
        };
        return quote! {
            #ident: #parse.map_err(|error| {
                format!("Invalid value for {}: {error}", #key)
            })?
        };
    }

    if let Some(prefix) = &field.prefix {
        let value_type = field
            .map_value_type
            .as_ref()
            .expect("prefix fields are validated as maps");
        let parse = if is_bool(value_type) {
            quote!(value.to_ascii_lowercase().parse::<#value_type>())
        } else {
            quote!(value.parse::<#value_type>())
        };
        return quote! {
            #ident: {
                let parsed = properties
                    .iter()
                    .filter_map(|(key, value)| {
                        key.strip_prefix(#prefix).map(|suffix| {
                            #parse
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
        (None, Some(inner_type)) if is_bool(inner_type) => quote! {
            Some(value.to_ascii_lowercase().parse::<#inner_type>().map_err(|error| {
                format!("Invalid value for {}: {error}", #key)
            })?)
        },
        (None, Some(inner_type)) => quote! {
            Some(value.parse::<#inner_type>().map_err(|error| {
                format!("Invalid value for {}: {error}", #key)
            })?)
        },
        (None, None) if is_bool(ty) => quote! {
            value.to_ascii_lowercase().parse::<#ty>().map_err(|error| {
                format!("Invalid value for {}: {error}", #key)
            })?
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

fn default_value(default: &Expr, ty: &Type) -> TokenStream2 {
    if matches!(
        default,
        Expr::Lit(ExprLit {
            lit: Lit::Str(_),
            ..
        }) | Expr::Path(_)
    ) {
        quote!(::std::convert::Into::<#ty>::into(#default))
    } else {
        quote!(#default)
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

fn is_bool(ty: &Type) -> bool {
    let Type::Path(type_path) = ty else {
        return false;
    };

    type_path
        .path
        .segments
        .last()
        .is_some_and(|segment| segment.ident == "bool")
}

fn write_field(field: &PropertyField) -> TokenStream2 {
    let ident = &field.ident;
    if field.nested {
        return quote! {
            self.#ident.write_properties(properties);
        };
    }

    let ty = &field.ty;
    let default = default_value(
        field.default.as_ref().expect("leaf fields have defaults"),
        ty,
    );
    let default = quote!({
        let value: #ty = #default;
        value
    });

    if let Some(write_properties_with) = &field.write_properties_with {
        let key = field.key.as_ref().expect("exact-key fields have a key");
        let write = match &field.additional_key {
            Some(additional_key) => {
                quote!(#write_properties_with(&self.#ident, properties, #key, #additional_key, &#default))
            }
            None => quote!(#write_properties_with(&self.#ident, properties, #key, &#default)),
        };
        return quote! {
            #write;
        };
    }

    if let Some(prefix) = &field.prefix {
        return quote! {
            if self.#ident != #default {
                for (suffix, value) in &self.#ident {
                    let key = format!("{}{}", #prefix, suffix);
                    properties.insert(key, ::std::string::ToString::to_string(value));
                }
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
            if self.#ident != #default && self.#ident.is_some() {
                properties.insert((#key).to_string(), #value);
            }
        }
    } else {
        let value = match &field.serialize_with {
            Some(serialize_with) => quote!(#serialize_with(&self.#ident)),
            None => quote!(::std::string::ToString::to_string(&self.#ident)),
        };
        quote! {
            if self.#ident != #default {
                properties.insert((#key).to_string(), #value);
            }
        }
    }
}

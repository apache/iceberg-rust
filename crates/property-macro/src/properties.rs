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

use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{
    Attribute, Data, DeriveInput, Error, Expr, ExprLit, ExprPath, Field, Fields, GenericArgument,
    Ident, Lit, Meta, Path, PathArguments, Token, Type, parenthesized,
};

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

enum PropertyOption {
    Key(Expr),
    AdditionalKey(Expr),
    Prefix(Expr),
    Nested,
    Default(Expr),
    ParseWith(Path),
    SerializeWith(Path),
    ParsePropertiesWith(Path),
    WritePropertiesWith(Path),
    Accessor(PublicAccessor),
}

#[derive(Default)]
struct PropertyOptions {
    key: Option<Expr>,
    additional_key: Option<Expr>,
    prefix: Option<Expr>,
    nested: bool,
    default: Option<Expr>,
    parse_with: Option<Path>,
    serialize_with: Option<Path>,
    parse_properties_with: Option<Path>,
    write_properties_with: Option<Path>,
    public_getter: bool,
    public_setter: bool,
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

impl Parse for PropertyOption {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        if input.peek(Token![pub]) {
            return input.parse().map(Self::Accessor);
        }

        let name = input.parse::<Ident>()?;
        let option_name = name.to_string();
        if option_name == "nested" {
            return Ok(Self::Nested);
        }

        input.parse::<Token![=]>()?;
        let expression = input.parse::<Expr>()?;
        match option_name.as_str() {
            "key" => Ok(Self::Key(expression)),
            "additional_key" => Ok(Self::AdditionalKey(expression)),
            "prefix" => Ok(Self::Prefix(expression)),
            "default" => Ok(Self::Default(expression)),
            "parse_with" => expression_path(expression, "parse_with").map(Self::ParseWith),
            "serialize_with" => {
                expression_path(expression, "serialize_with").map(Self::SerializeWith)
            }
            "parse_properties_with" => {
                expression_path(expression, "parse_properties_with").map(Self::ParsePropertiesWith)
            }
            "write_properties_with" => {
                expression_path(expression, "write_properties_with").map(Self::WritePropertiesWith)
            }
            _ => Err(Error::new_spanned(name, "unknown property option")),
        }
    }
}

pub(crate) fn expand_properties(input: DeriveInput) -> syn::Result<TokenStream2> {
    let struct_name = input.ident;
    let generics = input.generics;
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
    let parses = fields.iter().map(parse_field);
    let property_writes = fields.iter().map(write_field);
    let accessors = fields.iter().map(field_accessors);
    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();

    Ok(quote! {
        impl #impl_generics #struct_name #type_generics #where_clause {
            #(#accessors)*

            pub(crate) fn from_properties(
                properties: &::std::collections::HashMap<
                    ::std::string::String,
                    ::std::string::String,
                >,
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
        }
    })
}

fn parse_property_field(field: &Field) -> syn::Result<PropertyField> {
    let ident = field
        .ident
        .clone()
        .ok_or_else(|| Error::new_spanned(field, "Properties fields must be named"))?;
    let property_options = property_options(&field.attrs)?;
    let key = merge_attribute_option(
        attribute_expression_value(&field.attrs, "key")?,
        property_options.key,
        field,
        "key",
    )?;
    let additional_key = merge_attribute_option(
        attribute_expression_value(&field.attrs, "additional_key")?,
        property_options.additional_key,
        field,
        "additional_key",
    )?;
    let prefix = merge_attribute_option(
        attribute_expression_value(&field.attrs, "prefix")?,
        property_options.prefix,
        field,
        "prefix",
    )?;
    let standalone_nested = marker_attribute(&field.attrs, "nested")?;
    if standalone_nested && property_options.nested {
        return Err(Error::new_spanned(
            field,
            "duplicate nested property option",
        ));
    }
    let nested = standalone_nested || property_options.nested;

    if usize::from(key.is_some()) + usize::from(prefix.is_some()) + usize::from(nested) != 1 {
        return Err(Error::new_spanned(
            field,
            "Properties fields must declare exactly one of #[key(...)], #[prefix(...)], or #[nested]",
        ));
    }

    let default = merge_attribute_option(
        attribute_expression_value(&field.attrs, "default")?,
        property_options.default,
        field,
        "default",
    )?;
    if nested && default.is_some() {
        return Err(Error::new_spanned(
            field,
            "#[nested] fields obtain defaults from their own property annotations and cannot declare #[default(...)]",
        ));
    }
    if !nested && default.is_none() {
        return Err(Error::new_spanned(
            field,
            "Properties leaf fields must declare #[default(...)]",
        ));
    }

    let map_value_type = hash_map_value_type(&field.ty);
    if prefix.is_some() && map_value_type.is_none() {
        return Err(Error::new_spanned(
            &field.ty,
            "#[prefix(...)] fields must have type HashMap<String, T>",
        ));
    }

    let parse_with = merge_attribute_option(
        attribute_path_value(&field.attrs, "parse_with")?,
        property_options.parse_with,
        field,
        "parse_with",
    )?;
    let serialize_with = merge_attribute_option(
        attribute_path_value(&field.attrs, "serialize_with")?,
        property_options.serialize_with,
        field,
        "serialize_with",
    )?;
    let parse_properties_with = merge_attribute_option(
        attribute_path_value(&field.attrs, "parse_properties_with")?,
        property_options.parse_properties_with,
        field,
        "parse_properties_with",
    )?;
    let write_properties_with = merge_attribute_option(
        attribute_path_value(&field.attrs, "write_properties_with")?,
        property_options.write_properties_with,
        field,
        "write_properties_with",
    )?;

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
        public_getter: property_options.public_getter,
        public_setter: property_options.public_setter,
        doc_attributes: field
            .attrs
            .iter()
            .filter(|attribute| attribute.path().is_ident("doc"))
            .cloned()
            .collect(),
    })
}

fn property_options(attributes: &[Attribute]) -> syn::Result<PropertyOptions> {
    let Some(attribute) = find_attribute(attributes, "property")? else {
        return Ok(PropertyOptions::default());
    };

    let parsed =
        attribute.parse_args_with(Punctuated::<PropertyOption, Token![,]>::parse_terminated)?;
    if parsed.is_empty() {
        return Err(Error::new_spanned(
            attribute,
            "property must declare at least one option",
        ));
    }

    let mut options = PropertyOptions::default();
    for option in parsed {
        match option {
            PropertyOption::Key(value) => {
                set_property_option(&mut options.key, value, attribute, "key")?
            }
            PropertyOption::AdditionalKey(value) => set_property_option(
                &mut options.additional_key,
                value,
                attribute,
                "additional_key",
            )?,
            PropertyOption::Prefix(value) => {
                set_property_option(&mut options.prefix, value, attribute, "prefix")?
            }
            PropertyOption::Nested => {
                if options.nested {
                    return Err(Error::new_spanned(
                        attribute,
                        "duplicate nested property option",
                    ));
                }
                options.nested = true;
            }
            PropertyOption::Default(value) => {
                set_property_option(&mut options.default, value, attribute, "default")?
            }
            PropertyOption::ParseWith(value) => {
                set_property_option(&mut options.parse_with, value, attribute, "parse_with")?
            }
            PropertyOption::SerializeWith(value) => set_property_option(
                &mut options.serialize_with,
                value,
                attribute,
                "serialize_with",
            )?,
            PropertyOption::ParsePropertiesWith(value) => set_property_option(
                &mut options.parse_properties_with,
                value,
                attribute,
                "parse_properties_with",
            )?,
            PropertyOption::WritePropertiesWith(value) => set_property_option(
                &mut options.write_properties_with,
                value,
                attribute,
                "write_properties_with",
            )?,
            PropertyOption::Accessor(accessor) => {
                let selected = match accessor {
                    PublicAccessor::Getter => &mut options.public_getter,
                    PublicAccessor::Setter => &mut options.public_setter,
                };
                if *selected {
                    return Err(Error::new_spanned(attribute, "duplicate property accessor"));
                }
                *selected = true;
            }
        }
    }

    Ok(options)
}

fn set_property_option<T>(
    target: &mut Option<T>,
    value: T,
    attribute: &Attribute,
    name: &str,
) -> syn::Result<()> {
    if target.is_some() {
        return Err(Error::new_spanned(
            attribute,
            format!("duplicate {name} property option"),
        ));
    }
    *target = Some(value);
    Ok(())
}

fn merge_attribute_option<T>(
    standalone: Option<T>,
    grouped: Option<T>,
    field: &Field,
    name: &str,
) -> syn::Result<Option<T>> {
    match (standalone, grouped) {
        (Some(_), Some(_)) => Err(Error::new_spanned(
            field,
            format!("duplicate {name} property option"),
        )),
        (Some(value), None) | (None, Some(value)) => Ok(Some(value)),
        (None, None) => Ok(None),
    }
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

    expression_path(expression, name).map(Some)
}

fn expression_path(expression: Expr, name: &str) -> syn::Result<Path> {
    match expression {
        Expr::Path(ExprPath { path, .. }) => Ok(path),
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
    let default = typed_default(field);

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

fn typed_default(field: &PropertyField) -> TokenStream2 {
    let ty = &field.ty;
    let default = default_value(
        field.default.as_ref().expect("leaf fields have defaults"),
        ty,
    );
    quote!({
        let value: #ty = #default;
        value
    })
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

    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return None;
    };
    let Some(GenericArgument::Type(inner_type)) = arguments.args.first() else {
        return None;
    };

    Some(inner_type.clone())
}

fn hash_map_value_type(ty: &Type) -> Option<Type> {
    let Type::Path(type_path) = ty else {
        return None;
    };

    let segment = type_path.path.segments.last()?;
    if segment.ident != "HashMap" {
        return None;
    }

    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return None;
    };
    let mut arguments = arguments.args.iter();
    let Some(GenericArgument::Type(key_type)) = arguments.next() else {
        return None;
    };
    let Some(GenericArgument::Type(value_type)) = arguments.next() else {
        return None;
    };
    if !is_named_type(key_type, "String") {
        return None;
    }

    Some(value_type.clone())
}

fn is_bool(ty: &Type) -> bool {
    is_named_type(ty, "bool")
}

fn is_named_type(ty: &Type, name: &str) -> bool {
    let Type::Path(type_path) = ty else {
        return false;
    };

    type_path
        .path
        .segments
        .last()
        .is_some_and(|segment| segment.ident == name)
}

fn write_field(field: &PropertyField) -> TokenStream2 {
    let ident = &field.ident;
    if field.nested {
        return quote! {
            self.#ident.write_properties(properties);
        };
    }

    let default = typed_default(field);

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
            properties.retain(|key, _| !key.starts_with(#prefix));
            if self.#ident != #default {
                for (suffix, value) in &self.#ident {
                    let key = format!("{}{}", #prefix, suffix);
                    properties.insert(key, ::std::string::ToString::to_string(value));
                }
            }
        };
    }

    let key = field.key.as_ref().expect("exact-key fields have a key");
    let value = match (&field.serialize_with, &field.option_inner_type) {
        (Some(serialize_with), _) => quote!(#serialize_with(&self.#ident)),
        (None, Some(_)) => quote!(::std::string::ToString::to_string(
            self.#ident.as_ref().expect("checked is_some above")
        )),
        (None, None) => quote!(::std::string::ToString::to_string(&self.#ident)),
    };
    let insert = if field.option_inner_type.is_some() {
        quote! {
            if self.#ident != #default && self.#ident.is_some() {
                properties.insert((#key).to_string(), #value);
            }
        }
    } else {
        quote! {
            if self.#ident != #default {
                properties.insert((#key).to_string(), #value);
            }
        }
    };

    quote! {
        properties.remove(#key);
        #insert
    }
}

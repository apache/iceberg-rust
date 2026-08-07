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
use quote::quote;
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
    additional_keys: Option<Vec<Expr>>,
    prefix: Option<Expr>,
    nested: bool,
    default: Option<Expr>,
    parse_with: Option<Path>,
    parse_properties_with: Option<Path>,
    option_inner_type: Option<Type>,
    map_value_type: Option<Type>,
    public_getter: bool,
    doc_attributes: Vec<Attribute>,
}

struct PublicGetter;

enum PropertyOption {
    Key(Expr),
    AdditionalKeys(Vec<Expr>),
    Prefix(Expr),
    Nested,
    Default(Expr),
    ParseWith(Path),
    ParsePropertiesWith(Path),
    Getter(PublicGetter),
}

#[derive(Default)]
struct PropertyOptions {
    key: Option<Expr>,
    additional_keys: Option<Vec<Expr>>,
    prefix: Option<Expr>,
    nested: bool,
    default: Option<Expr>,
    parse_with: Option<Path>,
    parse_properties_with: Option<Path>,
    public_getter: bool,
}

impl Parse for PublicGetter {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        input.parse::<Token![pub]>()?;
        let content;
        parenthesized!(content in input);
        let accessor = content.parse::<Ident>()?;
        if !content.is_empty() {
            return Err(content.error("expected getter"));
        }

        if accessor == "getter" {
            Ok(Self)
        } else {
            Err(Error::new_spanned(accessor, "expected getter"))
        }
    }
}

impl Parse for PropertyOption {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        if input.peek(Token![pub]) {
            return input.parse().map(Self::Getter);
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
            "additional_keys" => {
                expression_list(expression, "additional_keys").map(Self::AdditionalKeys)
            }
            "prefix" => Ok(Self::Prefix(expression)),
            "default" => Ok(Self::Default(expression)),
            "parse_with" => expression_path(expression, "parse_with").map(Self::ParseWith),
            "parse_properties_with" => {
                expression_path(expression, "parse_properties_with").map(Self::ParsePropertiesWith)
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
        .map(|field| parse_property_field(field, property_options(&field.attrs)?))
        .collect::<syn::Result<Vec<_>>>()?;
    let parses = fields.iter().map(parse_field);
    let accessors = fields.iter().map(field_getter);
    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();

    Ok(quote! {
        impl #impl_generics #struct_name #type_generics #where_clause {
            #(#accessors)*

            pub fn from_properties(
                properties: &::std::collections::HashMap<
                    ::std::string::String,
                    ::std::string::String,
                >,
            ) -> ::std::result::Result<Self, ::std::string::String> {
                Ok(Self {
                    #(#parses,)*
                })
            }
        }
    })
}

fn parse_property_field(
    field: &Field,
    property_options: PropertyOptions,
) -> syn::Result<PropertyField> {
    let ident = field
        .ident
        .clone()
        .ok_or_else(|| Error::new_spanned(field, "Properties fields must be named"))?;
    let key = merge_attribute_option(
        attribute_expression_value(&field.attrs, "key")?,
        property_options.key,
        field,
        "key",
    )?;
    let additional_keys = merge_attribute_option(
        attribute_expression_list(&field.attrs, "additional_keys")?,
        property_options.additional_keys,
        field,
        "additional_keys",
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
    let parse_properties_with = merge_attribute_option(
        attribute_path_value(&field.attrs, "parse_properties_with")?,
        property_options.parse_properties_with,
        field,
        "parse_properties_with",
    )?;

    if additional_keys.is_some() && parse_properties_with.is_none() {
        return Err(Error::new_spanned(
            field,
            "#[additional_keys(...)] requires parse_properties_with",
        ));
    }
    if (prefix.is_some() || nested)
        && (additional_keys.is_some() || parse_with.is_some() || parse_properties_with.is_some())
    {
        return Err(Error::new_spanned(
            field,
            "#[prefix(...)] and #[nested] fields do not support custom parse functions",
        ));
    }
    if parse_with.is_some() && parse_properties_with.is_some() {
        return Err(Error::new_spanned(
            field,
            "fields cannot declare both parse_with and parse_properties_with",
        ));
    }
    Ok(PropertyField {
        ident,
        ty: field.ty.clone(),
        key,
        additional_keys,
        prefix,
        nested,
        default,
        parse_with,
        parse_properties_with,
        option_inner_type: option_inner_type(&field.ty),
        map_value_type,
        public_getter: property_options.public_getter,
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
            PropertyOption::AdditionalKeys(value) => set_property_option(
                &mut options.additional_keys,
                value,
                attribute,
                "additional_keys",
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
            PropertyOption::ParsePropertiesWith(value) => set_property_option(
                &mut options.parse_properties_with,
                value,
                attribute,
                "parse_properties_with",
            )?,
            PropertyOption::Getter(_) => {
                if options.public_getter {
                    return Err(Error::new_spanned(attribute, "duplicate property accessor"));
                }
                options.public_getter = true;
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

fn field_getter(field: &PropertyField) -> TokenStream2 {
    if !field.public_getter {
        return TokenStream2::new();
    }
    let ident = &field.ident;
    let ty = &field.ty;
    let docs = &field.doc_attributes;
    if is_copy_type(ty) {
        quote! {
            #(#docs)*
            pub fn #ident(&self) -> #ty {
                self.#ident
            }
        }
    } else {
        quote! {
            #(#docs)*
            pub fn #ident(&self) -> &#ty {
                &self.#ident
            }
        }
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

fn attribute_expression_list(
    attributes: &[Attribute],
    name: &str,
) -> syn::Result<Option<Vec<Expr>>> {
    let Some(attribute) = find_attribute(attributes, name)? else {
        return Ok(None);
    };

    let expressions = match &attribute.meta {
        Meta::NameValue(name_value) => expression_list(name_value.value.clone(), name)?,
        Meta::List(_) => attribute
            .parse_args_with(Punctuated::<Expr, Token![,]>::parse_terminated)?
            .into_iter()
            .collect(),
        _ => {
            return Err(Error::new_spanned(
                attribute,
                format!("{name} must contain a non-empty list of keys"),
            ));
        }
    };

    if expressions.is_empty() {
        return Err(Error::new_spanned(
            attribute,
            format!("{name} must contain at least one key"),
        ));
    }
    Ok(Some(expressions))
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

fn expression_list(expression: Expr, name: &str) -> syn::Result<Vec<Expr>> {
    let Expr::Array(array) = expression else {
        return Err(Error::new_spanned(
            expression,
            format!("{name} must be an array of keys"),
        ));
    };
    if array.elems.is_empty() {
        return Err(Error::new_spanned(
            array,
            format!("{name} must contain at least one key"),
        ));
    }
    Ok(array.elems.into_iter().collect())
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
        let parse = match &field.additional_keys {
            Some(additional_keys) => {
                quote!(#parse_properties_with(properties, #key, &[#(#additional_keys),*], #default))
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

fn is_copy_type(ty: &Type) -> bool {
    match ty {
        Type::Array(array) => is_copy_type(&array.elem),
        Type::BareFn(_) | Type::Never(_) | Type::Ptr(_) => true,
        Type::Group(group) => is_copy_type(&group.elem),
        Type::Paren(paren) => is_copy_type(&paren.elem),
        Type::Reference(reference) => reference.mutability.is_none(),
        Type::Tuple(tuple) => tuple.elems.iter().all(is_copy_type),
        Type::Path(type_path) if type_path.qself.is_none() => {
            let Some(segment) = type_path.path.segments.last() else {
                return false;
            };
            if matches!(
                segment.ident.to_string().as_str(),
                "bool"
                    | "char"
                    | "f32"
                    | "f64"
                    | "i8"
                    | "i16"
                    | "i32"
                    | "i64"
                    | "i128"
                    | "isize"
                    | "u8"
                    | "u16"
                    | "u32"
                    | "u64"
                    | "u128"
                    | "usize"
            ) {
                return true;
            }
            if segment.ident != "Option" && segment.ident != "Result" {
                return false;
            }
            let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
                return false;
            };
            arguments.args.iter().all(|argument| match argument {
                GenericArgument::Lifetime(_) => true,
                GenericArgument::Type(ty) => is_copy_type(ty),
                _ => false,
            })
        }
        _ => false,
    }
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

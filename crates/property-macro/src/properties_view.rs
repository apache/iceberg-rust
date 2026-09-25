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
use syn::{Error, Fields, GenericArgument, ItemStruct, Lifetime, PathArguments, Type};

use crate::properties::{
    ParseTarget, PropertyField, parse_field_value, parse_property_field, property_options,
};

pub(crate) fn expand_properties_view(input: ItemStruct) -> syn::Result<TokenStream2> {
    if !input.generics.params.is_empty() || input.generics.where_clause.is_some() {
        return Err(Error::new_spanned(
            input.generics,
            "properties_view! does not support generic declarations",
        ));
    }

    let attributes = input.attrs;
    let visibility = input.vis;
    let struct_name = input.ident;
    let fields = match input.fields {
        Fields::Named(fields) => fields.named,
        _ => {
            return Err(Error::new_spanned(
                struct_name,
                "properties_view! requires a struct-shaped declaration with named fields",
            ));
        }
    };
    let fields = fields
        .iter()
        .map(|field| parse_property_field(field, property_options(field)?))
        .collect::<syn::Result<Vec<_>>>()?;
    let properties_lifetime: Lifetime = syn::parse_quote!('properties);
    let getters = fields
        .iter()
        .map(|field| view_field_getter(field, &properties_lifetime))
        .collect::<syn::Result<Vec<_>>>()?;

    Ok(quote! {
        #(#attributes)*
        #visibility struct #struct_name<#properties_lifetime> {
            properties: &#properties_lifetime ::std::collections::HashMap<
                ::std::string::String,
                ::std::string::String,
            >,
        }

        impl<#properties_lifetime> #struct_name<#properties_lifetime> {
            /// Creates a property view without parsing any values.
            #visibility fn new(
                properties: &#properties_lifetime ::std::collections::HashMap<
                    ::std::string::String,
                    ::std::string::String,
                >,
            ) -> Self {
                Self { properties }
            }

            #(#getters)*
        }
    })
}

fn view_field_getter(
    field: &PropertyField,
    properties_lifetime: &Lifetime,
) -> syn::Result<TokenStream2> {
    let ident = &field.ident;
    let ty = view_field_type(field, properties_lifetime)?;
    let docs = &field.doc_attributes;
    let visibility = if field.public_getter {
        quote!(pub)
    } else {
        TokenStream2::new()
    };
    let parse = parse_field_value(field, ParseTarget::View)?;

    Ok(quote! {
        #(#docs)*
        #visibility fn #ident(&self) -> ::iceberg::Result<#ty> {
            let properties = self.properties;
            let value = #parse;
            Ok(value)
        }
    })
}

fn view_field_type(field: &PropertyField, properties_lifetime: &Lifetime) -> syn::Result<Type> {
    let mut ty = field.ty.clone();
    if !field.nested {
        return Ok(ty);
    }

    let Type::Path(type_path) = &mut ty else {
        return Err(Error::new_spanned(
            &field.ty,
            "nested property view types must have a lifetime argument",
        ));
    };
    let Some(segment) = type_path.path.segments.last_mut() else {
        return Err(Error::new_spanned(
            &field.ty,
            "nested property view types must have a lifetime argument",
        ));
    };
    let PathArguments::AngleBracketed(arguments) = &mut segment.arguments else {
        return Err(Error::new_spanned(
            &field.ty,
            "nested property view types must have a lifetime argument",
        ));
    };
    let Some(lifetime) = arguments
        .args
        .iter_mut()
        .find_map(|argument| match argument {
            GenericArgument::Lifetime(lifetime) => Some(lifetime),
            _ => None,
        })
    else {
        return Err(Error::new_spanned(
            &field.ty,
            "nested property view types must have a lifetime argument",
        ));
    };
    *lifetime = properties_lifetime.clone();
    Ok(ty)
}

#[cfg(test)]
mod tests {
    use syn::{ImplItem, Item, Visibility, parse_quote, parse2};

    use super::*;

    fn method_visibility(input: ItemStruct, method_name: &str) -> Visibility {
        let expanded = expand_properties_view(input).unwrap();
        let file = parse2::<syn::File>(expanded).unwrap();

        file.items
            .into_iter()
            .find_map(|item| match item {
                Item::Impl(item_impl) => item_impl.items.into_iter().find_map(|item| match item {
                    ImplItem::Fn(method) if method.sig.ident == method_name => Some(method.vis),
                    _ => None,
                }),
                _ => None,
            })
            .unwrap()
    }

    #[test]
    fn constructor_uses_the_struct_visibility() {
        let visibility = method_visibility(
            parse_quote! {
                pub(crate) struct TestProperties {
                    #[property(key = "retries", default = 4)]
                    retries: u64,
                }
            },
            "new",
        );

        assert!(matches!(visibility, Visibility::Restricted(_)));
    }

    #[test]
    fn getter_option_overrides_explicit_visibility() {
        let visibility = method_visibility(
            parse_quote! {
                struct TestProperties {
                    #[property(key = "retries", default = 4, getter)]
                    pub(crate) retries: u64,
                }
            },
            "retries",
        );

        assert!(matches!(visibility, Visibility::Public(_)));
    }

    #[test]
    fn field_visibility_does_not_make_the_getter_public() {
        let visibility = method_visibility(
            parse_quote! {
                struct TestProperties {
                    #[property(key = "retries", default = 4)]
                    pub retries: u64,
                }
            },
            "retries",
        );

        assert!(matches!(visibility, Visibility::Inherited));
    }
}

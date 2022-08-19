// Copyright (C) 2022 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

extern crate proc_macro;

use std::collections::HashMap;

use heck::{ToShoutySnakeCase, ToUpperCamelCase};
use proc_macro::{TokenStream};
use quote::{format_ident, quote};

#[proc_macro_attribute]
pub fn qw_default(_attrs: TokenStream, input: TokenStream) -> TokenStream {
    let item_fn = syn::parse_macro_input!(input as syn::ItemFn);
    let fn_name = item_fn.sig.ident;
    let return_type = match item_fn.sig.output {
        syn::ReturnType::Type(_, ty) => {
            quote! { -> crate::config_value::ConfigValueBuilder<#ty, Q> }
        }
        syn::ReturnType::Default => quote! { -> crate::config_value::ConfigValueBuilder<(), Q> },
    };
    let block = &item_fn.block;
    let output = quote! {
        fn #fn_name<Q>() #return_type {
            crate::config_value::ConfigValueBuilder {
                qw_default: Some(#block),
                ..Default::default()
            }
        }
    };
    output.into()
}

#[proc_macro_attribute]
pub fn qw_env_var(_attrs: TokenStream, input: TokenStream) -> TokenStream {
    let item_struct = syn::parse_macro_input!(input as syn::ItemStruct);
    let struct_attrs: Vec<syn::Attribute> = item_struct.attrs;
    let struct_vis = item_struct.vis;
    let struct_ident = item_struct.ident;
    let qw_env_var_key = struct_ident.to_string().to_shouty_snake_case();
    let output = quote!(
        #[derive(Debug, Eq, PartialEq)]
        #struct_vis struct #struct_ident;

        impl crate::config_value::QwEnvVar for #struct_ident {
            fn env_var_key() -> Option<&'static str> {
                Some(#qw_env_var_key)
            }
        }
    );
    output.into()
}

/// ```rust
/// [Config]
/// struct MyConfig {
///     version: usize,
///     cluster_id: String,
///     node_id: String,
///     foo: Option<PathBuf>,
///     #[ignore]
///     indexer: IndexerConfig,
/// }
/// ```
/// expands to:
/// ```rust
/// struct MyConfig {
///     version: ConfigValue<usize>,
///     cluster_id: ConfigValue<String>,
///     node_id: ConfigValue<String>,
///     foo: Option<ConfigValue<PathBuf>>,
///     indexer: IndexerConfig,
/// }
/// ```
#[proc_macro_attribute]
pub fn Config(_attrs: TokenStream, input: TokenStream) -> TokenStream {
    let item_struct = syn::parse_macro_input!(input as syn::ItemStruct);
    let struct_attrs: Vec<syn::Attribute> = item_struct.attrs;
    let struct_vis = item_struct.vis;
    let struct_ident = item_struct.ident;
    let struct_fields = item_struct.fields.into_iter().map(|field| {
        if field.attrs.iter().any(|attr| attr.path.is_ident("ignore")) {
            quote!(
                #field
            )
        } else {
            let field_attrs = field.attrs;
            let field_vis = field.vis;
            let field_ident = field
                .ident
                .expect("This macro should not be used for a tuple struct.");
            let field_ty = field.ty;
            quote!(
                #(#field_attrs)*
                #field_vis #field_ident: crate::config_value::ConfigValue<#field_ty>
            )
        }
    });
    let output = quote! {
        #(#struct_attrs)*
        #struct_vis struct #struct_ident {
            #(#struct_fields),*
        }
    };
    output.into()
}

/// ```rust
/// [ConfigBuilder]
/// struct MyConfigBuilder {
///     version: usize,
///     #[qw_env_var]
///     cluster_id: String,
///     #[qw_env_var]
///     node_id: String,
///     rest_port: u16,
///     #[optional]
///     grpc_port: Option<u16>,
///     #[ignore]
///     indexer: IndexerConfig,
/// }
/// ```
/// expands to:
/// ```rust
/// struct MyConfig {
///     version: ConfigValueBuilder<usize, NoQwEnvVar>,
///     cluster_id: ConfigValueBuilder<String, QwClusterIdEnVar>,
///     node_id: ConfigValueBuilder<String, QwNodeIdEnVar>,
///     rest_port: ConfigValueBuilder<usize, NoQwEnvVar>,
///     grpc_port: Option<ConfigValueBuilder<usize, NoQwEnvVar>>,
///     indexer: IndexerConfig,
/// }
/// ```
#[proc_macro_attribute]
pub fn ConfigBuilder(_attrs: TokenStream, input: TokenStream) -> TokenStream {
    let item_struct = syn::parse_macro_input!(input as syn::ItemStruct);
    let struct_attrs: Vec<syn::Attribute> = item_struct.attrs;
    let struct_vis = item_struct.vis;
    let struct_ident = item_struct.ident;
    let qw_env_vars: HashMap<syn::Ident, (String, String)> = item_struct
        .fields
        .iter()
        .filter(|field| {
            field
                .attrs
                .iter()
                .any(|attr| attr.path.is_ident("qw_env_var"))
        })
        .map(|field| {
            let field_ident = field.ident.clone().expect("");
            let field_name = field_ident.to_string();
            let marker_name = format!("Qw{}", field_name.to_upper_camel_case());
            let env_var_key = format!("QW_{}", field_name.to_uppercase());
            (field_ident, (marker_name, env_var_key))
        })
        .collect();
    let qw_env_var_markers = qw_env_vars
        .values()
        .map(|(marker_name, env_var_key)| {
            let struct_ident = format_ident!("{}", marker_name);
            quote!(
                #[derive(Debug, Eq, PartialEq)]
                struct #struct_ident;

                impl crate::config_value::QwEnvVar for #struct_ident {
                    fn env_var_key() -> Option<&'static str> {
                        Some(#env_var_key)
                    }
                }
            )
        })
        .collect::<Vec<_>>();
    let struct_fields = item_struct.fields.into_iter().map(|field| {
        if field.attrs.iter().any(|attr| attr.path.is_ident("ignore")) {
            quote!(
                #field
            )
        } else {
            let mut field_attrs = field.attrs;
            let field_vis = field.vis;
            let field_ident = field.ident.expect("");
            let env_var_ty = if let Some((marker_name, _)) = qw_env_vars.get(&field_ident) {
                quote!(#marker_name)
            } else {
                quote!(crate::config_value::NoQwEnvVar)
            };
            let field_ty = if field_attrs
                .iter()
                .any(|attr| attr.path.is_ident("optional"))
            {
                let inner_ty = extract_inner_type(&field.ty);
                quote!(
                    Option<crate::config_value::ConfigValueBuilder<#inner_ty, #env_var_ty>>
                )
            } else {
                let field_ty = field.ty;
                quote!(
                    crate::config_value::ConfigValueBuilder<#field_ty, #env_var_ty>
                )
            };
            field_attrs.retain(|attr| !attr.path.is_ident("qw_env_var") && !attr.path.is_ident("optional"));
            quote!(
                #(#field_attrs)*
                #field_vis #field_ident: #field_ty
            )
        }
    });
    let output = quote! {
        #(#qw_env_var_markers)*

        #(#struct_attrs)*
        #struct_vis struct #struct_ident {
            #(#struct_fields),*
        }
    };
    output.into()
}

fn extract_inner_type(ty: &syn::Type) -> syn::Type {
    if let syn::Type::Path(type_path) = ty {
        let segment = &type_path.path.segments.last().unwrap().arguments;
        if let syn::PathArguments::AngleBracketed(path_args) = segment {
            if let syn::GenericArgument::Type(inner_ty) = path_args.args.last().unwrap() {
                return inner_ty.clone();
            }
        }
    }
    panic!("Failed to extract inner type.")
}

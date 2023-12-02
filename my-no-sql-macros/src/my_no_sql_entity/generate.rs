use quote::quote;

extern crate proc_macro;

use proc_macro::TokenStream;
use types_reader::TokensObject;

pub fn generate(attr: TokenStream, input: TokenStream) -> Result<TokenStream, syn::Error> {
    let ast = proc_macro2::TokenStream::from(input);

    let mut result: Vec<proc_macro2::TokenTree> = Vec::new();

    let mut struct_name = None;
    let mut passed_struct_name = false;

    let attr: proc_macro2::TokenStream = attr.into();
    let params = TokensObject::new(attr.into(), &|| None)?;

    let table_name: &str = params.get_from_single_or_named("table_name")?.try_into()?;

    for item in ast {
        if struct_name.is_none() {
            if let proc_macro2::TokenTree::Ident(ident) = &item {
                if passed_struct_name {
                    struct_name = Some(ident.clone());
                } else {
                    if ident.to_string() == "struct" {
                        passed_struct_name = true;
                    }
                }
            }
            result.push(item);
        } else {
            if let proc_macro2::TokenTree::Group(group) = &item {
                if group.delimiter() == proc_macro2::Delimiter::Brace {
                    let mut first = true;

                    let mut result_tokens: Vec<proc_macro2::TokenTree> = Vec::new();

                    for token in group.stream() {
                        if first {
                            populate_tokens(&mut result_tokens);
                            first = false;
                        }
                        result_tokens.push(token);
                    }

                    if result_tokens.len() == 0 {
                        populate_tokens(&mut result_tokens);
                    }

                    result.push(proc_macro2::TokenTree::Group(proc_macro2::Group::new(
                        proc_macro2::Delimiter::Brace,
                        result_tokens.into_iter().collect(),
                    )));
                }
            }
        }
    }

    let struct_name = struct_name.unwrap();

    let result = quote! {

        #(#result)*

        impl my_no_sql_sdk::abstractions::MyNoSqlEntity for #struct_name {

            const TABLE_NAME: &'static str = #table_name;

            fn get_partition_key(&self) -> &str {
                &self.partition_key
            }

            fn get_row_key(&self) -> &str {
                &self.row_key
            }

            fn get_time_stamp(&self) -> i64 {
                my_no_sql_sdk::rust_extensions::date_time::DateTimeAsMicroseconds::parse_iso_string(self.time_stamp.as_str())
                    .unwrap()
                    .unix_microseconds
            }
        }
    };

    Ok(result.into())
}

fn populate_tokens(result_tokens: &mut Vec<proc_macro2::TokenTree>) {
    let token: proc_macro2::TokenStream = quote! {
        #[serde(rename = "PartitionKey")]
        pub partition_key: String,
    }
    .into();
    result_tokens.extend(token);

    let token: proc_macro2::TokenStream = quote! {
        #[serde(rename = "RowKey")]
        pub row_key: String,
    }
    .into();
    result_tokens.extend(token);

    let token: proc_macro2::TokenStream = quote! {
        #[serde(rename = "TimeStamp")]
        pub time_stamp: String,
    }
    .into();
    result_tokens.extend(token);
}

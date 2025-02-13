use quote::ToTokens;

pub fn compile_struct_with_new_fields(
    ast: &proc_macro2::TokenStream,
    add_pk: bool,
    add_rk: bool,
    add_timestamp: bool,
) -> (syn::Ident, proc_macro2::TokenStream) {
    let mut result: Vec<proc_macro2::TokenTree> = Vec::new();
    let mut struct_name = None;
    let mut passed_struct_name = false;

    for item in ast.into_token_stream() {
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
                            populate_tokens(&mut result_tokens, add_pk, add_rk, add_timestamp);
                            first = false;
                        }
                        result_tokens.push(token);
                    }

                    if result_tokens.len() == 0 {
                        populate_tokens(&mut result_tokens, add_pk, add_rk, add_timestamp);
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
    (struct_name, quote::quote!(#(#result)*))
}

fn populate_tokens(
    result_tokens: &mut Vec<proc_macro2::TokenTree>,
    add_pk: bool,
    add_rk: bool,
    add_timestamp: bool,
) {
    if add_pk {
        result_tokens.extend(get_partition_key_token());
    }

    if add_rk {
        result_tokens.extend(get_row_key_token());
    }

    if add_timestamp {
        result_tokens.extend(get_time_stamp_token());
    }
}

pub fn get_partition_key_token() -> proc_macro2::TokenStream {
    quote::quote! {
        #[serde(rename = "PartitionKey")]
        pub partition_key: String,
    }
}

pub fn get_row_key_token() -> proc_macro2::TokenStream {
    quote::quote! {
        #[serde(rename = "RowKey")]
        pub row_key: String,
    }
}
pub fn get_time_stamp_token() -> proc_macro2::TokenStream {
    quote::quote! {
        #[serde(rename = "TimeStamp")]
        pub time_stamp: my_no_sql_sdk::abstractions::Timestamp,
    }
}

pub fn get_fn_get_time_stamp_token() -> proc_macro2::TokenStream {
    quote::quote! {
        fn get_time_stamp(&self) -> my_no_sql_sdk::abstractions::Timestamp {
            self.time_stamp
        }
    }
}

pub fn get_fn_standard_serialize_deserialize() -> proc_macro2::TokenStream {
    quote::quote! {
        fn serialize_entity(&self) -> Vec<u8> {
            my_no_sql_sdk::core::entity_serializer::serialize(self)
        }


        fn deserialize_entity(src: &[u8]) -> Result<Self, String> {
          my_no_sql_sdk::core::entity_serializer::deserialize(src)
        }
    }
}

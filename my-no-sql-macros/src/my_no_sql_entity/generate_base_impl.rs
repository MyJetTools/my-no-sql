use crate::entity_utils::*;

pub fn generate_base_impl(
    ast: &proc_macro2::TokenStream,
    table_name: &str,
    has_f64_param: bool,
) -> Result<proc_macro2::TokenStream, syn::Error> {
    let (struct_name, new_struct) = compile_struct_with_new_fields(ast, true, true, true);

    let fn_get_time_stamp = get_fn_get_time_stamp_token();

    let fn_serialize_deserialize = get_fn_standard_serialize_deserialize(has_f64_param);

    let result = quote::quote! {

        #new_struct

        impl my_no_sql_sdk::abstractions::MyNoSqlEntity for #struct_name {

            const TABLE_NAME: &'static str = #table_name;

            fn get_partition_key(&self) -> &str {
                &self.partition_key
            }

            fn get_row_key(&self) -> &str {
                &self.row_key
            }

            #fn_get_time_stamp


        }

        impl my_no_sql_sdk::abstractions::MyNoSqlEntitySerializer for #struct_name {
            #fn_serialize_deserialize
        }

    };

    Ok(result)
}

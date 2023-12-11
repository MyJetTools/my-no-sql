use types_reader::TokensObject;

use crate::{entity_utils::*, EnumModelParameters};

pub fn generate(
    attr: proc_macro2::TokenStream,
    input: proc_macro2::TokenStream,
) -> Result<proc_macro::TokenStream, syn::Error> {
    let parameters: TokensObject = attr.try_into()?;

    let parameters = EnumModelParameters::try_from(&parameters)?;

    let partition_key = parameters.partition_key;
    let row_key = parameters.row_key;

    let (struct_name, new_struct) = compile_struct_with_new_fields(&input, false, false, true);

    let fn_get_time_stamp = get_fn_get_time_stamp_token();

    let fn_serialize_deserialize = get_fn_standard_serialize_deserialize();

    let result = quote::quote! {
        #new_struct

        impl #struct_name{
            const PARTITION_KEY:&'static str = #partition_key;
            const ROW_KEY:&'static str = #row_key;
        }


        impl my_no_sql_sdk::abstractions::MyNoSqlEntity for #struct_name {

        const TABLE_NAME: &'static str = "";

        fn get_partition_key(&self) -> &str {
            Self::PARTITION_KEY
        }

        fn get_row_key(&self) -> &str {
            Self::ROW_KEY
        }

        #fn_get_time_stamp

        #fn_serialize_deserialize

       }

    };

    Ok(result.into())
}

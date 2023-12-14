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

    let (struct_name, new_struct) =
        compile_struct_with_new_fields(&input, false, row_key.is_none(), true);

    let fn_get_time_stamp = get_fn_get_time_stamp_token();

    let fn_serialize_deserialize = get_fn_standard_serialize_deserialize();

    let (row_key, fn_get_row_key_body) = if let Some(row_key) = row_key {
        let row_key = quote::quote!(const ROW_KEY: Option<&'static str> = Some(#row_key););

        let fn_get_row_key_body = quote::quote!(Self::ROW_KEY.unwrap());

        (row_key, fn_get_row_key_body)
    } else {
        let row_key = quote::quote!(
            const ROW_KEY: Option<&'static str> = None;
        );
        let fn_get_row_key_body = quote::quote!(&self.row_key);

        (row_key, fn_get_row_key_body)
    };

    let result = quote::quote! {
        #new_struct

        impl #struct_name{
            const PARTITION_KEY:&'static str = #partition_key;
            #row_key
        }


        impl my_no_sql_sdk::abstractions::MyNoSqlEntity for #struct_name {

        const TABLE_NAME: &'static str = "";

        fn get_partition_key(&self) -> &str {
            Self::PARTITION_KEY
        }

        fn get_row_key(&self) -> &str {
            #fn_get_row_key_body
        }

        #fn_get_time_stamp

        #fn_serialize_deserialize

       }

    };

    Ok(result.into())
}

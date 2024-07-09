extern crate proc_macro;

use proc_macro::TokenStream;
use types_reader::TokensObject;

use crate::MyNoSqlEntityParameters;

pub fn generate(
    attr: proc_macro2::TokenStream,
    input: proc_macro2::TokenStream,
) -> Result<TokenStream, syn::Error> {
    let ast = proc_macro2::TokenStream::from(input);

    let attr: TokensObject = attr.try_into()?;

    let has_f64_param = crate::entity_utils::has_f64_parameter(&attr);

    let params = MyNoSqlEntityParameters::try_from(&attr)?;

    let result = super::generate_base_impl(&ast, params.table_name, has_f64_param)?;

    Ok(result.into())
}

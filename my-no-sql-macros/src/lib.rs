use proc_macro::TokenStream;

extern crate proc_macro;
mod my_no_sql_entity;

#[proc_macro_attribute]
pub fn my_no_sql_entity(attr: TokenStream, input: TokenStream) -> TokenStream {
    match crate::my_no_sql_entity::generate(attr, input) {
        Ok(result) => result.into(),
        Err(err) => err.into_compile_error().into(),
    }
}

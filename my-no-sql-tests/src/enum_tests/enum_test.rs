use my_no_sql_macros::*;
use serde::*;

#[enum_of_my_no_sql_entity("Test")]
pub enum MyNoSqlEnumEntityTest {
    Case1(Struct1),
    Case2(Struct2),
}

impl MyNoSqlEnumEntityTest {
    pub fn unwrap_case1(&self) -> &Struct1 {
        match self {
            MyNoSqlEnumEntityTest::Case1(v) => v,
            _ => panic!("Not case 1"),
        }
    }

    pub fn unwrap_case2(&self) -> &Struct2 {
        match self {
            MyNoSqlEnumEntityTest::Case2(v) => v,
            _ => panic!("Not case 2"),
        }
    }
}

#[enum_model(partition_key:"pk1", row_key: "rk1")]
#[derive(Serialize, Deserialize, Clone)]
pub struct Struct1 {
    pub field1: String,
    pub field2: i32,
}

#[enum_model(partition_key:"pk2", row_key: "rk2")]
#[derive(Serialize, Deserialize, Clone)]
pub struct Struct2 {
    pub field3: String,
    pub field4: i32,
}

#[test]
fn test_serialize_deserialize_case_1() {
    let src_model = Struct1 {
        time_stamp: "".to_string(),
        field1: "test".to_string(),
        field2: 123,
    };
    let entity = MyNoSqlEnumEntityTest::Case1(src_model.clone());

    use my_no_sql_sdk::abstractions::MyNoSqlEntity;

    let vec = entity.serialize_entity();

    let dest = MyNoSqlEnumEntityTest::deserialize_entity(&vec);

    let model = dest.unwrap_case1();

    assert_eq!(src_model.field1, model.field1);
    assert_eq!(src_model.field2, model.field2);
}

#[test]
fn test_serialize_deserialize_case_2() {
    let src_model = Struct2 {
        time_stamp: "".to_string(),
        field3: "test3".to_string(),
        field4: 1234,
    };
    let entity = MyNoSqlEnumEntityTest::Case2(src_model.clone());

    use my_no_sql_sdk::abstractions::MyNoSqlEntity;

    let vec = entity.serialize_entity();

    let dest = MyNoSqlEnumEntityTest::deserialize_entity(&vec);

    let model = dest.unwrap_case2();

    assert_eq!(src_model.field3, model.field3);
    assert_eq!(src_model.field4, model.field4);
}

use my_json::json_reader::{JsonContentOffset, JsonValue};

#[derive(Debug, Clone)]
pub struct KeyValueContentPosition {
    pub start: usize,
    pub end: usize,
}

impl KeyValueContentPosition {
    pub fn len(&self) -> usize {
        self.end - self.start
    }

    pub fn get_value<'s>(&self, raw: &'s [u8]) -> &'s str {
        std::str::from_utf8(&raw[self.start..self.end]).unwrap()
    }

    pub fn get_str_value<'s>(&self, raw: &'s [u8]) -> &'s str {
        std::str::from_utf8(&raw[self.start + 1..self.end - 1]).unwrap()
    }

    pub fn is_null(&self, raw: &[u8]) -> bool {
        self.get_value(raw) == "null"
    }
}

#[derive(Debug, Clone)]
pub struct JsonKeyValuePosition {
    pub key: KeyValueContentPosition,
    pub value: KeyValueContentPosition,
}

impl JsonKeyValuePosition {
    pub fn new(name: &JsonContentOffset, value: &JsonValue) -> Self {
        Self {
            key: KeyValueContentPosition {
                start: name.start,
                end: name.end,
            },

            value: KeyValueContentPosition {
                start: value.start,
                end: value.end,
            },
        }
    }
}

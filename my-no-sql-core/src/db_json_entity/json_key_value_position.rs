use my_json::json_reader::JsonKeyValue;

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
    pub fn new(src: &JsonKeyValue) -> Self {
        Self {
            key: KeyValueContentPosition {
                start: src.name.start,
                end: src.name.end,
            },

            value: KeyValueContentPosition {
                start: src.value.start,
                end: src.value.end,
            },
        }
    }
}

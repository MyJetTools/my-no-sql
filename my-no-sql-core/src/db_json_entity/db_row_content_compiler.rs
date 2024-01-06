use my_json::json_reader::JsonFirstLine;

use super::{JsonKeyValuePosition, KeyValueContentPosition};

pub struct DbRowContentCompiler {
    first_line: bool,
    pub content: Vec<u8>,
}

impl DbRowContentCompiler {
    pub fn new() -> Self {
        Self {
            content: Vec::new(),
            first_line: false,
        }
    }

    fn append_first_line(&mut self) {
        if self.first_line {
            self.content.push(b',');
        } else {
            self.content.push(b'{');
            self.first_line = true;
        }
    }

    pub fn append(&mut self, src: &[u8], line: &JsonFirstLine) -> JsonKeyValuePosition {
        self.append_first_line();
        let mut key = KeyValueContentPosition {
            start: self.content.len(),
            end: 0,
        };

        self.content
            .extend_from_slice(&src[line.name_start..line.name_end]);

        key.end = self.content.len();

        self.content.push(b':');

        let mut value = KeyValueContentPosition {
            start: self.content.len(),
            end: 0,
        };
        self.content
            .extend_from_slice(&src[line.value_start..line.value_end]);

        value.end = self.content.len();

        JsonKeyValuePosition { key, value }
    }

    pub fn append_str_value(&mut self, name: &str, value: &str) -> JsonKeyValuePosition {
        self.append_first_line();
        let mut key = KeyValueContentPosition {
            start: self.content.len(),
            end: 0,
        };

        self.content.push(b'"');
        self.content.extend_from_slice(name.as_bytes());
        self.content.push(b'"');
        key.end = self.content.len();

        self.content.push(b':');

        let mut value_pos = KeyValueContentPosition {
            start: self.content.len(),
            end: 0,
        };
        self.content.push(b'"');
        self.content.extend_from_slice(value.as_bytes());
        self.content.push(b'"');
        key.end = self.content.len();

        value_pos.end = self.content.len();

        JsonKeyValuePosition {
            key,
            value: value_pos,
        }
    }

    pub fn into_vec(mut self) -> Vec<u8> {
        self.content.push(b'}');
        self.content
    }
}

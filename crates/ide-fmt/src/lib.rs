//! A pattern-based formatter (WIP)

use syntax::{AstNode, Edition, SourceFile};

pub fn format_text(text: &str) -> String {
    let parse = SourceFile::parse(text, Edition::CURRENT);
    parse.tree().syntax().to_string()
}

#[cfg(test)]
mod tests {
    use expect_test::{Expect, expect};

    use super::format_text;

    fn check(input: &str, expect: Expect) {
        expect.assert_eq(&format_text(input));
    }

    #[test]
    fn preserves_whitespace_and_comments() {
        check(
            "fn main() {  // hi\n    let x=1;\n}\n",
            expect![[r#"
fn main() {  // hi
    let x=1;
}
"#]],
        );
    }

    #[test]
    fn accepts_broken_code() {
        check(
            "fn main( {",
            expect![[r#"
fn main( {"#]],
        );
    }
}

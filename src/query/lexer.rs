//! Lightweight SQL tokenizer used by the analyzer.
//!
//! This is **not** a full SQL parser. It only needs to:
//!   1. Skip whitespace and `--` line comments (so we can detect the
//!      `-- dactyl: <datastore>` inline override).
//!   2. Recognize single-word lexemes (case-insensitive) like `returning`,
//!      `ilike`, `json_each`, `without`, `rowid`, `strict`, `gen_random_uuid`,
//!      `now`.
//!   3. Recognize multi-char operator lexemes (`->>`, `->`, `@>`, `<@`).
//!
//! The lexer is total: it never panics on arbitrary input and never raises
//! errors. Anything it cannot classify becomes an `Other` token, which the
//! analyzer ignores.

/// A single token emitted by the lexer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Token {
    /// `select`, `from`, `where`, `returning`, etc.
    Word(String),
    /// Multi-char operator lexeme (`->>`, `->`, `@>`, `<@`).
    Op(String),
    /// A single `?` (Postgres JSON existence operator in this context).
    Question,
    /// `;` statement separator.
    Semi,
    /// Any other punctuation / character the analyzer doesn't care about.
    Other(char),
}

/// Tokenize a SQL string.
pub fn tokenize(input: &str) -> Vec<Token> {
    let mut out = Vec::new();
    let bytes = input.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        let c = bytes[i] as char;

        // line comment: -- ... \n
        if c == '-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }

        // block comment: /* ... */ (unterminated runs to EOF — matches sqlite)
        if c == '/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
            i += 2;
            while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            if i + 1 < bytes.len() {
                i += 2;
            } else {
                i = bytes.len();
            }
            continue;
        }

        // string literal: '...' with '' escapes
        if c == '\'' {
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\'' && i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                if bytes[i] == b'\'' {
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }

        // quoted identifier: "..." with "" escapes
        if c == '"' {
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'"' && i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                    i += 2;
                    continue;
                }
                if bytes[i] == b'"' {
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }

        // whitespace
        if c.is_whitespace() {
            i += 1;
            continue;
        }

        // identifier / keyword: [A-Za-z_][A-Za-z0-9_]*
        if c.is_ascii_alphabetic() || c == '_' {
            let start = i;
            i += 1;
            while i < bytes.len() {
                let b = bytes[i];
                if (b as char).is_ascii_alphanumeric() || b == b'_' {
                    i += 1;
                } else {
                    break;
                }
            }
            let word = std::str::from_utf8(&bytes[start..i])
                .unwrap_or("")
                .to_ascii_lowercase();
            out.push(Token::Word(word));
            continue;
        }

        // digit start of a number literal — skip without emitting.
        if c.is_ascii_digit() {
            i += 1;
            while i < bytes.len()
                && ((bytes[i] as char).is_ascii_alphanumeric() || bytes[i] == b'.')
            {
                i += 1;
            }
            continue;
        }

        // multi-char operators first
        if i + 1 < bytes.len() {
            let two = &bytes[i..i + 2];
            if two == b"->" {
                if i + 2 < bytes.len() && bytes[i + 2] == b'>' {
                    out.push(Token::Op("->>".into()));
                    i += 3;
                    continue;
                }
                out.push(Token::Op("->".into()));
                i += 2;
                continue;
            }
            if two == b"@>" {
                out.push(Token::Op("@>".into()));
                i += 2;
                continue;
            }
            if two == b"<@" {
                out.push(Token::Op("<@".into()));
                i += 2;
                continue;
            }
        }

        match c {
            '?' => {
                out.push(Token::Question);
                i += 1;
            }
            ';' => {
                out.push(Token::Semi);
                i += 1;
            }
            _ => {
                out.push(Token::Other(c));
                i += 1;
            }
        }
    }

    out
}

/// If the SQL begins with a `-- dactyl: <datastore>` directive line, return
/// the recognized datastore plus the remainder of the input with the
/// directive line stripped. The remainder preserves all subsequent text
/// verbatim (including other comments).
pub fn strip_dactyl_directive(input: &str) -> (Option<&'static str>, String) {
    let bytes = input.as_bytes();
    let mut i = 0;

    // Skip leading whitespace.
    while i < bytes.len() && (bytes[i] as char).is_whitespace() {
        i += 1;
    }

    // Must start with `--`.
    if i + 1 >= bytes.len() || bytes[i] != b'-' || bytes[i + 1] != b'-' {
        return (None, input.to_string());
    }
    let directive_start = i;
    i += 2;

    // Skip whitespace after `--`.
    while i < bytes.len() && (bytes[i] as char).is_whitespace() {
        i += 1;
    }

    // Expect "dactyl".
    if i + 6 > bytes.len() || !bytes[i..i + 6].eq_ignore_ascii_case(b"dactyl") {
        return (None, input.to_string());
    }
    i += 6;

    // Skip whitespace before `:`.
    while i < bytes.len() && (bytes[i] as char).is_whitespace() {
        i += 1;
    }
    if i >= bytes.len() || bytes[i] != b':' {
        return (None, input.to_string());
    }
    i += 1;

    // Skip whitespace after `:`.
    while i < bytes.len() && (bytes[i] as char).is_whitespace() {
        i += 1;
    }

    // Read identifier (alpha + alnum + _ + -).
    let id_start = i;
    while i < bytes.len() {
        let b = bytes[i];
        if (b as char).is_ascii_alphanumeric() || b == b'_' || b == b'-' {
            i += 1;
        } else {
            break;
        }
    }
    if id_start == i {
        return (None, input.to_string());
    }
    let id = std::str::from_utf8(&bytes[id_start..i]).unwrap_or("");
    let mapped = match id.to_ascii_lowercase().as_str() {
        "sqlite" => Some("sqlite"),
        "neon" | "postgres" | "postgresql" | "pg" => Some("neon"),
        _ => return (None, input.to_string()),
    };

    // Consume to end of line / start of next directive.
    while i < bytes.len() && bytes[i] != b'\n' {
        i += 1;
    }
    let line_end = i;
    // Skip the trailing newline if any.
    let after_directive = if line_end < bytes.len() {
        line_end + 1
    } else {
        line_end
    };

    // Drop the directive line. Also drop preceding whitespace-only lines so
    // we don't leave an awkward blank at the head of the result.
    let mut line_begin = directive_start;
    while line_begin > 0 && bytes[line_begin - 1] != b'\n' {
        line_begin -= 1;
    }
    // Walk backward over lines that are entirely whitespace before the
    // directive line.
    let mut trim_to = line_begin;
    let mut scan = 0usize;
    while scan < line_begin {
        let next = input[scan..line_begin].find('\n').map(|o| scan + o);
        let line_end_idx = next.unwrap_or(line_begin);
        let line = &input[scan..line_end_idx];
        if !line.chars().all(|c| c.is_whitespace()) {
            trim_to = line_begin;
            break;
        }
        trim_to = line_end_idx + 1; // skip past newline
        scan = line_end_idx + 1;
    }

    let mut remainder = String::with_capacity(input.len());
    remainder.push_str(&input[..trim_to]);
    remainder.push_str(&input[after_directive..]);
    (mapped, remainder)
}

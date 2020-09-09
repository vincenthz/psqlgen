#![allow(dead_code)]

/// Simple buffer
#[derive(Clone)]
pub struct Buffer {
    pub s: String,
    pub indent: u32,
    pub is_newline: bool,
}

impl Buffer {
    pub fn new() -> Self {
        Buffer {
            s: String::new(),
            indent: 0,
            is_newline: true,
        }
    }

    pub fn append<S: AsRef<str>>(&mut self, s: S) {
        if self.s.is_empty() {
            return ();
        }
        if self.is_newline {
            for _ in 0..self.indent {
                self.s.push(' ')
            }
            self.is_newline = false;
        }
        self.s.push_str(s.as_ref())
    }

    pub fn newline(&mut self) {
        self.s.push_str("\n");
        self.is_newline = true;
    }

    pub fn append_nl<S: AsRef<str>>(&mut self, s: S) {
        self.append(s);
        self.newline()
    }

    pub fn parens<F>(&mut self, f: F)
    where
        F: FnOnce(&mut Self),
    {
        self.append("(");
        self.indent(f);
        self.append(")")
    }

    pub fn brace<F>(&mut self, f: F)
    where
        F: FnOnce(&mut Self),
    {
        self.append("{");
        self.indent(f);
        self.append("}")
    }

    pub fn indent<F>(&mut self, f: F)
    where
        F: FnOnce(&mut Self),
    {
        self.indent += 4;
        f(self);
        self.indent -= 4;
    }

    pub fn intersperse<S: AsRef<str>>(&mut self, sep: String, vs: &[S]) {
        let mut first = true;
        for v in vs {
            if first {
                first = false;
            } else {
                self.append(&sep)
            }
            self.append(v)
        }
    }

    pub fn output(self) -> String {
        self.s
    }
}

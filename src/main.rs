use core::iter::Peekable;
use sqlparser::dialect::{Dialect, MySqlDialect};
use sqlparser::tokenizer::{Token, Tokenizer, Word};

use std::fs::File;

type StatementTokens = Vec<Token>;

#[derive(Debug, Clone)]
pub struct CreateStatement {
    table_name: String,
    columns: Vec<Column>,
    constraints: Vec<Constraints>,
}

#[derive(Debug, Clone)]
pub struct Column {
    field: String,
    v: Vec<Token>,
}

#[derive(Debug, Clone)]
pub struct Constraints {}

#[derive(Debug, Clone)]
pub enum Statement {
    Create(CreateStatement),
    Other(StatementTokens),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error(String);

fn make_statement(toks: &[Token]) -> Vec<StatementTokens> {
    let mut stmts = Vec::new();
    let mut stmt = Vec::new();

    for tok in toks {
        match tok {
            Token::Whitespace(_) => {}
            Token::SemiColon => {
                //println!("end");
                let mut swapped = Vec::new();
                std::mem::swap(&mut swapped, &mut stmt);
                if swapped.len() > 0 {
                    stmts.push(swapped);
                }
            }
            tok => stmt.push(tok.clone()),
        }
    }
    stmts
}

fn word<'a, I>(it: &mut I) -> Result<String, Error>
where
    I: Iterator<Item = &'a Token>,
{
    let n = it.next();
    match n {
        None => Err(Error("expected word, got EOF".to_string())),
        Some(Token::Word(w)) => Ok(w.value.clone()),
        Some(t) => Err(Error(format!("expected word, got {:?}", t))),
    }
}

fn eat_until_comma_or_rparen<'a, I>(it: &mut Peekable<I>) -> Result<Vec<Token>, Error>
where
    I: Iterator<Item = &'a Token>,
{
    let mut r = Vec::new();
    let mut level = 0;

    loop {
        match it.peek() {
            None => break,
            Some(&Token::RParen) if level == 0 => break,
            Some(&Token::Comma) => break,
            Some(t) => {
                if t == &&Token::RParen {
                    level -= 1;
                } else if t == &&Token::LParen {
                    level += 1;
                };

                r.push(it.next().unwrap().clone())
            }
        }
    }
    Ok(r)
}

fn parse_column(w: &Word, c: Vec<Token>) -> Result<Option<Column>, Error> {
    match w.value.as_ref() {
        "PRIMARY" => Ok(None),
        "KEY" => Ok(None),
        "CONSTRAINT" => Ok(None),
        "UNIQUE" => Ok(None),
        other => Ok(Some(Column {
            field: other.to_string(),
            v: c,
        })),
    }
}

fn parse_create<'a, I>(it: &mut Peekable<I>) -> Result<CreateStatement, Error>
where
    I: Iterator<Item = &'a Token>,
{
    if word(it)? != "TABLE" {
        return Err(Error("expecting TABLE".to_string()));
    }
    let table_name = word(it)?;

    if it.next().ok_or(Error("missing columns".to_string()))? != &Token::LParen {
        return Err(Error("expecting (".to_string()));
    }

    let mut columns = Vec::new();
    loop {
        let c = it.next();
        match c {
            None => {
                return Err(Error(format!(
                    "while parsing {}, EOF during columns",
                    table_name
                )))
            }
            Some(Token::RParen) => {
                break;
            }
            Some(Token::Word(w)) => {
                let c = eat_until_comma_or_rparen(it)?;
                match parse_column(w, c)? {
                    Some(c) => columns.push(c),
                    None => {}
                };
                match it.peek() {
                    Some(Token::Comma) => {
                        let _ = it.next();
                        ()
                    }
                    _ => {}
                }
            }
            Some(tok) => {
                return Err(Error(format!(
                    "unknown token {:?} at start of column field",
                    tok
                )))
            }
        }
    }

    Ok(CreateStatement {
        table_name,
        columns,
        constraints: Vec::new(),
    })
}

fn parse_statement(stmt: &StatementTokens) -> Option<Statement> {
    let mut it = stmt.iter().peekable();

    match word(&mut it) {
        Ok(verb) => match verb.as_ref() {
            "CREATE" => match parse_create(&mut it).map(Statement::Create) {
                Err(e) => {
                    println!("during CREATE: {:?}", e);
                    None
                }
                Ok(c) => Some(c),
            },
            v => {
                println!("statement: unknown start: {}", v);
                None
            }
        },
        Err(_) => None,
    }
}

fn p<D: Dialect>(dialect: &D, sql: &str) {
    let mut tokenizer = Tokenizer::new(dialect, &sql);
    let tokens = tokenizer.tokenize().expect("token failed");

    let stmts = make_statement(&tokens);

    for stmt in stmts {
        match parse_statement(&stmt) {
            Some(stmt) => println!("c: {:?}", stmt),
            None => {}
        }
    }
}

fn main() {
    let dialect = MySqlDialect {}; // or AnsiDialect

    let contents = {
        use std::io::Read;

        let mut file = File::open("SCHEMA").unwrap();
        let mut contents = String::new();
        file.read_to_string(&mut contents).unwrap();
        contents
    };

    p(&dialect, &contents);

}

use core::iter::Peekable;
use sqlparser::dialect::{Dialect, MySqlDialect};
use sqlparser::tokenizer::{Token, Tokenizer, Word};

use std::fs::File;
use clap::{App, Arg};

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
    sqltype: SqlTy,
    qualifiers: Vec<Constraint>,
}

#[derive(Debug, Clone)]
pub enum SqlParam {
    LitString(String),
    LitNumber(String),
    LitChar(char),
}

#[derive(Debug, Clone)]
pub enum SqlTyAst {
    Basic(String),
    Function(String, Vec<SqlParam>),
}

#[derive(Debug, Clone)]
pub enum SqlTy {
    Int,
    BigInt,
    Char(Option<String>),
    VarChar(Option<String>),
    Decimal(String, String),
    Timestamp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Constraint {
    OnUpdate(SqlValue),
    Default(SqlValue),
    NotNull,
    Unsigned,
    AutoIncrement,
    CharacterSet(String),
    U,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlValue {
    Number(String),
    FctCallVoid(String),
    SQuoteString(String),
    Null,
}

impl SqlValue {
    pub fn as_string(&self) -> String {
        match self {
            SqlValue::Number(s) => s.clone(),
            SqlValue::Null => "null".to_string(),
            SqlValue::FctCallVoid(s) => format!("\"{}()\"", s),
            SqlValue::SQuoteString(s) => format!("'{}'", s),
        }
    }
}

fn parse_type(ast: SqlTyAst) -> SqlTy {
    match &ast {
        SqlTyAst::Basic(b) => match b.as_ref() {
            "int" => SqlTy::Int,
            "tinyint" => SqlTy::Int,
            "timestamp" => SqlTy::Timestamp,
            _ => panic!("type not supported {:?}", ast),
        },
        SqlTyAst::Function(b, p) => match b.as_ref() {
            "int" => SqlTy::Int,
            "tinyint" => SqlTy::Int,
            "bigint" => SqlTy::BigInt,
            "varchar" => {
                if let SqlParam::LitNumber(n) = &p[0] {
                    SqlTy::VarChar(Some(n.clone()))
                } else {
                    panic!("unknown varchar function {:?}", p)
                }
            }
            "char" => {
                if let SqlParam::LitNumber(n) = &p[0] {
                    SqlTy::Char(Some(n.clone()))
                } else {
                    panic!("unknown char function {:?}", p)
                }
            }
            "decimal" => {
                if let SqlParam::LitNumber(n1) = &p[0] {
                    if let SqlParam::LitNumber(n2) = &p[1] {
                        SqlTy::Decimal(n1.clone(), n2.clone())
                    } else {
                        panic!("decimal missing second param {:?}", p)
                    }
                } else {
                    panic!("decimal missing first param {:?}", p)
                }
            }
            _ => panic!("type not supported {:?}", ast),
        },
    }
}

fn parse_constraints(toks: &[Token]) -> Vec<Constraint> {
    let mut start = 0;
    let mut constraints = Vec::new();
    loop {
        if start == toks.len() {
            break;
        }

        match &toks[start] {
            Token::Word(w) => match w.value.as_ref() {
                "unsigned" => {
                    constraints.push(Constraint::Unsigned);
                    start += 1;
                }
                "NOT" => {
                    if let Token::Word(w2) = &toks[start + 1] {
                        if w2.value.as_str() == "NULL" {
                            constraints.push(Constraint::NotNull)
                        } else {
                            panic!("unknown word token after NOT: {}", w2)
                        }
                    } else {
                        panic!("unknown token after NOT")
                    }
                    start += 2;
                }
                "DEFAULT" => match &toks[start + 1] {
                    Token::Word(w) => {
                        if w.value.as_str() == "NULL" {
                            constraints.push(Constraint::Default(SqlValue::Null));
                            start += 2;
                        } else {
                            if toks[start + 2] == Token::LParen && toks[start + 3] == Token::RParen
                            {
                                constraints.push(Constraint::Default(SqlValue::FctCallVoid(
                                    w.value.clone(),
                                )))
                            } else {
                                panic!("unknown DEFAULT value {}", w)
                            }
                            start += 4;
                        }
                    }
                    Token::Number(n) => {
                        constraints.push(Constraint::Default(SqlValue::Number(n.clone())));
                        start += 2;
                    }
                    Token::SingleQuotedString(s) => {
                        constraints.push(Constraint::Default(SqlValue::SQuoteString(s.clone())));
                        start += 2;
                    }
                    tok => panic!("unknown token in default {:?}", tok),
                },
                "ON" => {
                    if let Token::Word(w) = &toks[start + 1] {
                        if w.value.as_str() == "UPDATE" {
                            if let Token::Word(w) = &toks[start + 2] {
                                if toks[start + 3] == Token::LParen
                                    && toks[start + 4] == Token::RParen
                                {
                                    constraints.push(Constraint::OnUpdate(SqlValue::FctCallVoid(
                                        w.value.clone(),
                                    )));
                                    start += 5;
                                }
                            } else {
                                panic!("unknown function after ON UPDATE {:?}", toks[start + 2])
                            }
                        } else {
                            panic!("on not followed by update")
                        }
                    }
                }
                "CHARACTER" => {
                    if let Token::Word(w) = &toks[start + 1] {
                        if w.value.as_str() == "SET" {
                            if let Token::Word(w) = &toks[start + 2] {
                                constraints.push(Constraint::CharacterSet(w.value.clone()));
                                start += 3;
                            } else {
                                panic!("CHARACTER SET not followed by word")
                            }
                        } else {
                            panic!("CHARACTER not followed by set")
                        }
                    }
                }
                "AUTO_INCREMENT" => {
                    constraints.push(Constraint::AutoIncrement);
                    start += 1
                }
                w => panic!("unknown token: {} => {:?}", w, toks),
            },
            _ => {
                panic!(
                    "unknown column constraint qualifier {} at {}",
                    toks[start], start
                );
            }
        }

        //start += 1;
    }
    constraints
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
            Some(&Token::Comma) if level == 0 => break,
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

fn parse_params(tokens: &[Token]) -> Vec<SqlParam> {
    let mut params = Vec::new();
    let mut start = 0;
    loop {
        let p = match &tokens[start] {
            Token::Number(n) => SqlParam::LitNumber(n.clone()),
            Token::Char(c) => SqlParam::LitChar(*c),
            Token::NationalStringLiteral(s) => SqlParam::LitString(s.clone()),
            Token::SingleQuotedString(s) => SqlParam::LitString(s.clone()),
            token => panic!("unknown token while parsing params: {:?}", token),
        };
        params.push(p);
        if start + 1 < tokens.len() && tokens[start + 1] == Token::Comma {
            start += 2;
        } else {
            break params;
        }
    }
}

fn parse_column(w: &Word, c: Vec<Token>) -> Result<Option<Column>, Error> {
    match w.value.as_ref() {
        "PRIMARY" => Ok(None),
        "KEY" => Ok(None),
        "CONSTRAINT" => Ok(None),
        "UNIQUE" => Ok(None),
        other => {
            let (ty, next) = {
                if let Token::Word(w) = &c[0] {
                    if c.len() >= 2 && c[1] == Token::LParen {
                        let mut end = 2;
                        while c[end] != Token::RParen {
                            end += 1
                        }
                        let params = &c[2..end];
                        let sqlparams = parse_params(params);
                        (SqlTyAst::Function(w.value.clone(), sqlparams), end + 1)
                    } else {
                        (SqlTyAst::Basic(w.value.clone()), 1)
                    }
                } else {
                    return Err(Error(format!(
                        "unknown token while parsing column type {:?}",
                        c[0]
                    )));
                }
            };

            let ty = parse_type(ty);

            let constraints = parse_constraints(&c[next..]);

            // return
            Ok(Some(Column {
                field: other.to_string(),
                sqltype: ty,
                qualifiers: constraints,
            }))
        }
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
            _v => {
                //println!("statement: unknown start: {}", v);
                None
            }
        },
        Err(_) => None,
    }
}

fn print_create_table(stmt: CreateStatement) {
    println!("TABLE {}", stmt.table_name);
    for col in stmt.columns {
        println!("   {:?}", col);
    }
}

pub enum SqlPType {}

fn knex_create_table(stmt: CreateStatement) {
    println!("createTable('{}', function(table) {{", stmt.table_name);
    for col in stmt.columns {
        let x = match &col.sqltype {
            SqlTy::Int => format!("table.integer('{}')", col.field),
            SqlTy::BigInt => format!("table.bigInteger('{}')", col.field),
            SqlTy::Char(limit) => match limit {
                Some(n) => format!("table.string('{}', {})", col.field, n),
                None => format!("table.string('{}')", col.field),
            },
            SqlTy::VarChar(limit) => match limit {
                Some(n) => format!("table.string('{}', {})", col.field, n),
                None => format!("table.string('{}')", col.field),
            },
            SqlTy::Decimal(precision, limit) => {
                format!("table.decimal('{}', {}, {})", col.field, precision, limit)
            }
            SqlTy::Timestamp => format!("table.timestamp('{}')", col.field),
        };

        let mut chain = String::new();
        if col.qualifiers.iter().any(|c| c == &Constraint::Unsigned) {
            chain.push_str(&format!(".unsigned()"))
        }
        if col.qualifiers.iter().any(|c| c == &Constraint::NotNull) {
            chain.push_str(&format!(".notNullable()"))
        }
        if col.qualifiers.iter().any(|c| c == &Constraint::AutoIncrement) {
            chain.push_str(&format!(".primary()"))
        }
        if let Some(value) = col.qualifiers.iter().find_map(|c| match c {
            Constraint::Default(v) => Some(v),
            _ => None,
        }) {
            chain.push_str(&format!(".defaultTo({})", value.as_string()))
        }
        if let Some(upd) = col.qualifiers.iter().find_map(|c| match c {
            Constraint::OnUpdate(v) => Some(v),
            _ => None,
        }) {
            chain.push_str(&format!(".onUpdate({})", upd.as_string()))
        }

        // println!("    {}{}; // {:?}", x, chain, col);
        println!("    {}{};", x, chain);
    }
    println!("}});");
}

fn p<D: Dialect>(output: OutputType, dialect: &D, sql: &str) {
    let mut tokenizer = Tokenizer::new(dialect, &sql);
    let tokens = tokenizer.tokenize().expect("token failed");

    let stmts = make_statement(&tokens);

    for stmt in stmts {
        match parse_statement(&stmt) {
            Some(Statement::Create(stmt)) =>
                match output {
                    OutputType::Knex => knex_create_table(stmt),
                    OutputType::Diesel => {},
                    OutputType::Debug => print_create_table(stmt),
                }
            Some(Statement::Other(_)) => {}
            None => {}
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum OutputType {
    Debug,
    Knex,
    Diesel,
}

fn main() {
    const ARG_KNEX: &str = "knex";
    const ARG_DIESEL: &str = "diesel";
    const ARG_DEBUG: &str = "debug";
    const ARG_SCHEMA: &str = "schema";

    let app = App::new("SQL rewriter")
        .version("0.1")
        .about("rewrite raw SQL")
        .arg(Arg::with_name(ARG_KNEX)
                .long("knex")
                .help("output KNEX.js DSL")
                .takes_value(false)
        )
        .arg(Arg::with_name(ARG_DIESEL)
                .long("diesel")
                .help("output rust DIESEL DSL")
                .takes_value(false)
        )
        .arg(Arg::with_name(ARG_DEBUG)
                .long("debug")
                .help("output debug stuff")
                .takes_value(false)
        )
        .arg(Arg::with_name(ARG_SCHEMA)
                .long("schema")
                .value_name("SCHEMA.sql")
                .help("SCHEMA SQL file to use as input")
                .required(true)
                .takes_value(true)
        );

    let matches = app.get_matches();
    let schema_file = matches.value_of(ARG_SCHEMA).expect("missing a schema file");
    let output_knex = matches.is_present(ARG_KNEX);
    let output_diesel = matches.is_present(ARG_DIESEL);
    let output_debug = matches.is_present(ARG_DEBUG);

    let dialect = MySqlDialect {}; // or AnsiDialect

    let contents = {
        use std::io::Read;
        let mut file = File::open(schema_file).unwrap();
        let mut contents = String::new();
        file.read_to_string(&mut contents).unwrap();
        contents
    };

    let output = if output_debug {
        OutputType::Debug
    } else if output_diesel {
        OutputType::Diesel
    } else if output_knex {
        OutputType::Knex
    } else {
        panic!("no output type selected")
    };

    p(output, &dialect, &contents);
}

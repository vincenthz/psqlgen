use crate::tok::{TokenError, TokenParser};
use core::iter::Peekable;
use either::Either;
use sqlparser::dialect::Dialect;
use sqlparser::tokenizer::{Token, Tokenizer, Word};
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct CreateStatement {
    pub table_name: String,
    pub metas: Vec<Meta>,
    pub columns: Vec<Column>,
    pub constraints: Vec<Constraints>,
}

#[derive(Debug, Clone)]
pub struct Column {
    pub field: String,
    pub sqltype: SqlTy,
    pub qualifiers: Vec<Constraint>,
}

impl CreateStatement {
    pub fn get_column_foreign(&self, t: &str) -> Option<ForeignMeta> {
        self.metas.iter().find_map(|m| match m {
            Meta::ForeignKey(fk) if &fk.field == t => Some(fk.clone()),
            _ => None,
        })
    }

    pub fn is_column_primary(&self, t: &str) -> bool {
        self.metas
            .iter()
            .find_map(|m| match m {
                Meta::PrimaryKeySimple(s) if s.as_str() == t => Some(true),
                _ => None,
            })
            .unwrap_or(false)
    }

    #[allow(dead_code)]
    pub fn is_column_foreign(&self, t: &str) -> bool {
        self.metas
            .iter()
            .find_map(|m| match m {
                Meta::ForeignKey(m) if m.field == t => Some(true),
                _ => None,
            })
            .unwrap_or(false)
    }

    pub fn get_column_primary(&self) -> Option<String> {
        self.metas.iter().find_map(|m| match m {
            Meta::PrimaryKeySimple(s) => Some(s.clone()),
            _ => None,
        })
    }

    pub fn get_columns_primary(&self) -> Vec<String> {
        self.metas
            .iter()
            .find_map(|m| match m {
                Meta::PrimaryKeySimple(s) => Some(vec![s.clone()]),
                Meta::PrimaryKeyComposite(v) => Some(v.clone()),
                _ => None,
            })
            .unwrap_or(vec![])
    }

    pub fn get_column_primary_colid(&self) -> Option<(usize, String)> {
        self.metas.iter().find_map(|m| match m {
            Meta::PrimaryKeySimple(s) => self.columns.iter().enumerate().find_map(|(colid, c)| {
                if &c.field == s {
                    Some((colid, s.clone()))
                } else {
                    None
                }
            }),
            _ => None,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ForeignMeta {
    pub field: String,
    pub table: String,
    pub table_field: String,
    pub on_delete_cascade: bool,
}

#[derive(Debug, Clone)]
pub enum Meta {
    PrimaryKeySimple(String),
    PrimaryKeyComposite(Vec<String>),
    ForeignKey(ForeignMeta),
}

#[derive(Debug, Clone)]
pub struct InsertStatement {
    pub table_name: String,
    pub rows: Vec<Vec<SqlValue>>,
}

#[derive(Clone, PartialEq, Eq)]
pub enum SqlParam {
    LitString(String),
    LitNumber(String),
    LitChar(char),
}

impl std::fmt::Debug for SqlParam {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SqlParam::LitString(s) => write!(f, "\"{}\"", s),
            SqlParam::LitChar(s) => write!(f, "'{}'", s),
            SqlParam::LitNumber(s) => write!(f, "{}", s),
        }
    }
}

#[derive(Debug, Clone)]
pub enum SqlTyAst {
    Basic(String),
    Function(String, Vec<SqlParam>),
}

#[derive(Clone)]
pub enum SqlTy {
    TinyInt,
    Int,
    BigInt,
    Char(Option<String>),
    VarChar(Option<String>),
    Decimal(String, String),
    Enum(Vec<SqlParam>),
    Timestamp,
}

impl std::fmt::Debug for SqlTy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SqlTy::TinyInt => write!(f, "TINYINT"),
            SqlTy::Int => write!(f, "INT"),
            SqlTy::BigInt => write!(f, "BIGINT"),
            SqlTy::Char(Some(s)) => write!(f, "CHAR({})", s),
            SqlTy::Char(None) => write!(f, "CHAR()"),
            SqlTy::VarChar(Some(s)) => write!(f, "VARCHAR({})", s),
            SqlTy::VarChar(None) => write!(f, "VARCHAR()"),
            SqlTy::Decimal(a, b) => write!(f, "DECIMAL({}, {})", a, b),
            SqlTy::Timestamp => write!(f, "TIMESTAMP"),
            SqlTy::Enum(params) => {
                write!(f, "ENUM {{")?;
                for (i, p) in params.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", {:?}", p)?;
                    } else {
                        write!(f, "{:?}", p)?;
                    }
                }
                write!(f, "}}")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Constraint {
    OnUpdate(SqlValue),
    Default(SqlValue),
    NotNull,
    Unsigned,
    AutoIncrement,
    CharacterSet(String),
    Comment(String),
    #[allow(dead_code)]
    Collate(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlValue {
    Number(String),
    FctCallVoid(String),
    SQuoteString(String),
    Null,
}

impl SqlValue {
    pub fn as_u32(&self) -> Option<u32> {
        match self {
            SqlValue::Number(s) => match u32::from_str(s) {
                Ok(n) => Some(n),
                Err(_) => None,
            },
            _ => None,
        }
    }

    #[allow(dead_code)]
    pub fn is_decimal(&self) -> bool {
        match self {
            SqlValue::Number(n) => {
                let v = n.split(".").collect::<Vec<_>>();
                if v.len() == 2 {
                    let ent = u128::from_str(v[0]);
                    let dec = u128::from_str(v[1]);
                    ent.is_ok() || dec.is_ok()
                } else {
                    false
                }
            }
            _ => false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Constraints {}

#[derive(Debug, Clone)]
pub enum Statement {
    Create(CreateStatement),
    Insert(InsertStatement),
    #[allow(dead_code)]
    Other(StatementTokens),
}

pub type StatementTokens = Vec<Token>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error(String);

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
            "tinyint" => SqlTy::TinyInt,
            "timestamp" => SqlTy::Timestamp,
            _ => panic!("type not supported {:?}", ast),
        },
        SqlTyAst::Function(b, p) => match b.as_ref() {
            "int" => SqlTy::Int,
            "tinyint" => SqlTy::TinyInt,
            "bigint" => SqlTy::BigInt,
            "varchar" => {
                if let SqlParam::LitNumber(n) = &p[0] {
                    SqlTy::VarChar(Some(n.clone()))
                } else {
                    panic!("unknown varchar function {:?}", p)
                }
            }
            "enum" => SqlTy::Enum(p.clone()),
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
                "COLLATE" => {
                    if let Token::Word(w) = &toks[start + 1] {
                        constraints.push(Constraint::CharacterSet(w.value.clone()));
                        start += 2;
                    } else {
                        panic!("COLLATE not followed by word")
                    }
                }
                "COMMENT" => match &toks[start + 1] {
                    Token::SingleQuotedString(s) => {
                        constraints.push(Constraint::Comment(s.clone()));
                        start += 2
                    }
                    w => {
                        panic!("unknown token after comment: {} => {:?}", w, toks)
                    }
                },
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

//#[derive(Debug, Clone, PartialEq, Eq)]
//pub struct Error(String);

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

fn parse_value<'a>(ts: &mut TokenParser<'a>) -> Result<SqlValue, TokenError> {
    match ts.next() {
        Token::Number(n) => Ok(SqlValue::Number(n.clone())),
        Token::Char(c) => Ok(SqlValue::SQuoteString(c.to_string())),
        Token::NationalStringLiteral(s) => Ok(SqlValue::SQuoteString(s.clone())),
        Token::SingleQuotedString(s) => Ok(SqlValue::SQuoteString(s.clone())),
        Token::Word(w) if w.value == "NULL" => Ok(SqlValue::Null),
        Token::Minus => match ts.next() {
            Token::Number(n) => Ok(SqlValue::Number(format!("-{}", n))),
            tok => Err(TokenError::UnexpectedToken(
                tok.clone(),
                "expecting number after minus in value context",
            )),
        },
        token => Err(TokenError::UnexpectedToken(
            token.clone(),
            "expecting number, char or string or NULL",
        )),
    }
}

fn parse_param<'a>(ts: &mut TokenParser<'a>) -> Result<SqlParam, TokenError> {
    match ts.next() {
        Token::Number(n) => Ok(SqlParam::LitNumber(n.clone())),
        Token::Char(c) => Ok(SqlParam::LitChar(*c)),
        Token::NationalStringLiteral(s) => Ok(SqlParam::LitString(s.clone())),
        Token::SingleQuotedString(s) => Ok(SqlParam::LitString(s.clone())),
        //Token::Word(w) if w.value == "NULL" => Ok(SqlParam::LitNull),
        token => Err(TokenError::UnexpectedToken(
            token.clone(),
            "expecting number, char or string or NULL",
        )),
    }
}

fn parse_params(tokens: &[Token]) -> Result<Vec<SqlParam>, TokenError> {
    let mut params = Vec::new();
    let mut p = TokenParser::new(tokens);
    loop {
        let param = parse_param(&mut p)?;
        params.push(param);
        if !p.is_end() && p.next() == &Token::Comma {
            ()
        } else {
            break Ok(params);
        }
    }
}

fn multiple_ids(p: &mut TokenParser) -> Result<Vec<String>, TokenError> {
    p.eat_lparen()?;
    let mut key_ids = vec![p.next_word()?.value.clone()];
    while p.is_next_comma()? {
        p.skip();
        key_ids.push(p.next_word()?.value.clone());
    }
    p.eat_rparen()?;
    Ok(key_ids)
}

fn parse_column(w: &Word, c: Vec<Token>) -> Result<Option<Either<Column, Meta>>, TokenError> {
    match w.value.as_ref() {
        "PRIMARY" => {
            let mut p = TokenParser::new(&c);
            p.eat_word("KEY")?;
            let key_ids = multiple_ids(&mut p)?;

            let meta = if key_ids.len() == 1 {
                Meta::PrimaryKeySimple(key_ids[0].clone())
            } else {
                Meta::PrimaryKeyComposite(key_ids)
            };
            Ok(Some(Either::Right(meta)))
        }
        "KEY" => Ok(None),
        "CONSTRAINT" => {
            let mut p = TokenParser::new(&c);
            let _ = p.next_word()?;
            p.eat_word("FOREIGN")?;
            p.eat_word("KEY")?;
            let key_ids = multiple_ids(&mut p)?;
            let tkey = key_ids[0].clone();
            p.eat_word("REFERENCES")?;
            let table = p.next_word()?.value.clone();
            let key_ids = multiple_ids(&mut p)?;
            let fkey = key_ids[0].clone();
            let on_delete_cascade = if !p.is_end() {
                p.eat_word("ON")?;
                p.eat_word("DELETE")?;
                p.eat_word("CASCADE")?;
                true
            } else {
                false
            };
            let fm = ForeignMeta {
                field: tkey,
                table,
                table_field: fkey,
                on_delete_cascade,
            };
            Ok(Some(Either::Right(Meta::ForeignKey(fm))))
        }
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
                        let sqlparams = parse_params(params).unwrap();
                        (SqlTyAst::Function(w.value.clone(), sqlparams), end + 1)
                    } else {
                        (SqlTyAst::Basic(w.value.clone()), 1)
                    }
                } else {
                    return Err(TokenError::ExpectingWordButGot(c[0].clone()));
                }
            };

            let ty = parse_type(ty);

            let constraints = parse_constraints(&c[next..]);

            // return
            Ok(Some(Either::Left(Column {
                field: other.to_string(),
                sqltype: ty,
                qualifiers: constraints,
            })))
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
    let mut metas = Vec::new();
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
                match parse_column(w, c).expect("column") {
                    Some(Either::Left(c)) => columns.push(c),
                    Some(Either::Right(m)) => metas.push(m),
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
        metas,
        columns,
        constraints: Vec::new(),
    })
}

fn parse_insert<'a, I>(it: &mut Peekable<I>) -> Result<InsertStatement, TokenError>
where
    I: Iterator<Item = &'a Token>,
{
    let stream = it.cloned().collect::<Vec<Token>>();
    let mut tp = TokenParser::new(&stream);

    tp.eat_word("INTO")?;
    let table_name = tp.next_word()?.value.clone();
    tp.eat_word("VALUES")?;

    let mut values = Vec::new();
    loop {
        tp.eat_lparen()?;
        let mut record = Vec::new();
        // first params
        {
            let p = parse_value(&mut tp)?;
            record.push(p);
        }

        // < , param > until ')'
        while tp.peek_next() != &Token::RParen {
            tp.comma()?;
            let p = parse_value(&mut tp)?;
            record.push(p)
        }

        tp.skip(); // skip ')'
        values.push(record);

        if tp.is_end() {
            break;
        } else {
            tp.comma()?;
        }
    }

    Ok(InsertStatement {
        table_name,
        rows: values,
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
            "INSERT" => match parse_insert(&mut it).map(Statement::Insert) {
                Err(e) => {
                    println!("during INSERT: {:?}", e);
                    None
                }
                Ok(c) => Some(c),
            },
            _v => {
                //Some(Statement::Other())
                //println!("statement: unknown start: {}", v);
                None
            }
        },
        Err(_) => None,
    }
}

pub fn parse<D: Dialect>(dialect: &D, sql: &str) -> Vec<Option<Statement>> {
    let mut tokenizer = Tokenizer::new(dialect, &sql);
    let tokens = tokenizer.tokenize().expect("token failed");

    let stmts = make_statement(&tokens);

    stmts
        .iter()
        .map(|stmt| parse_statement(&stmt))
        .collect::<Vec<_>>()
}

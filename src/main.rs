#[macro_use]
mod buffer;
mod extra;
mod integrity;
mod output;
mod parse;
mod tok;

use clap::{App, Arg};
use std::fs::File;

use output::{output_create, OutputType};
use parse::{parse, CreateStatement, Statement};
use sqlparser::dialect::MySqlDialect;
use std::fmt;
use std::path::Path;

fn read_file<P: AsRef<Path>>(file: P) -> String {
    use std::io::Read;
    let mut file = File::open(file).unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    contents
}

#[derive(Clone)]
pub struct State {}

#[derive(Clone, Debug)]
pub struct Table {
    c: CreateStatement,
}

pub struct TableInfo<'a> {
    table_name: &'a str,
    rowid: u32,
}

impl<'a> fmt::Display for TableInfo<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.table_name, self.rowid)
    }
}

fn main() {
    const ARG_KNEX: &str = "knex";
    const ARG_DIESEL: &str = "diesel";
    const ARG_DOT: &str = "dot";
    const ARG_DEBUG: &str = "debug";
    const ARG_SCHEMA: &str = "schema";
    const ARG_EXTRA: &str = "extra";
    const ARG_CHECK_INTEGRITY: &str = "check-integrity";

    let app = App::new("SQL rewriter")
        .version("0.1")
        .about("rewrite raw SQL")
        .arg(
            Arg::with_name(ARG_KNEX)
                .long("knex")
                .help("output KNEX.js DSL")
                .takes_value(false),
        )
        .arg(
            Arg::with_name(ARG_DIESEL)
                .long("diesel")
                .help("output rust DIESEL DSL")
                .takes_value(false),
        )
        .arg(
            Arg::with_name(ARG_DOT)
                .long("dot")
                .help("output dot format")
                .takes_value(false),
        )
        .arg(
            Arg::with_name(ARG_DEBUG)
                .long("debug")
                .help("output debug stuff")
                .takes_value(false),
        )
        .arg(
            Arg::with_name(ARG_CHECK_INTEGRITY)
                .long("check-integrity")
                .help("check-integrity")
                .takes_value(false),
        )
        .arg(
            Arg::with_name(ARG_EXTRA)
                .long("extra")
                .value_name("EXTRA")
                .help("EXTRA data")
                .required(false)
                .takes_value(true),
        )
        .arg(
            Arg::with_name(ARG_SCHEMA)
                .long("schema")
                .value_name("SCHEMA.sql")
                .help("SCHEMA SQL file to use as input")
                .required(true)
                .takes_value(true),
        );

    let matches = app.get_matches();
    let schema_file = matches.value_of(ARG_SCHEMA).expect("missing a schema file");
    let extra_file = matches.value_of(ARG_EXTRA);
    let output_knex = matches.is_present(ARG_KNEX);
    let output_diesel = matches.is_present(ARG_DIESEL);
    let output_dot = matches.is_present(ARG_DOT);
    let output_debug = matches.is_present(ARG_DEBUG);
    let check_integrity = matches.is_present(ARG_CHECK_INTEGRITY);

    let dialect = MySqlDialect {}; // or AnsiDialect

    let contents = read_file(schema_file);

    let extra_state = if let Some(extra_file) = extra_file {
        let extra_content = read_file(extra_file);
        Some(extra::parse(extra_content))
    } else {
        None
    };

    if check_integrity {
        let stmts = parse(&dialect, &contents);
        integrity::integrity(stmts, extra_state)
    } else {
        let output = if output_debug {
            OutputType::Debug
        } else if output_diesel {
            OutputType::Diesel
        } else if output_knex {
            OutputType::Knex
        } else if output_dot {
            OutputType::Dot
        } else {
            panic!("no output type selected")
        };

        let stmts = parse(&dialect, &contents);
        let creates = stmts
            .iter()
            .filter_map(|m| match m {
                Some(Statement::Create(s)) => Some(s.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();

        output_create(output, extra_state, &creates[..]);
    }
}

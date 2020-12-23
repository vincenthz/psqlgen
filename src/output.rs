use crate::buffer::Buffer;
use crate::extra::{self, ForeignRelation};
use crate::parse::{
    Column, Constraint, CreateStatement, InsertStatement, Meta, SqlParam, SqlTy, SqlValue,
};
use convert_case::{Case, Casing};
use std::collections::{HashMap, HashSet};

const TO_HIDE: [&str; 6] = [
    "create_date",
    "update_date",
    "created_by_user_id",
    "deleted_by_user_id",
    "updated_by_user_id",
    "delete_flag",
];

#[derive(Debug, Clone)]
pub enum OutputType {
    Debug,
    Knex,
    Diesel,
    Dot,
    Tables(bool, Vec<String>),
}

fn print_enum_params(ps: &[SqlParam]) -> String {
    let mut out = String::new();
    for p in ps {
        if out.len() > 0 {
            out.push_str(", ")
        }
        match p {
            SqlParam::LitString(s) => out.push_str(&s),
            _ => panic!("unknown enum type {:?}", &p),
        }
    }
    out
}

fn knex_create_table(stmt: &CreateStatement) {
    println!("createTable('{}', function(table) {{", stmt.table_name);
    for col in &stmt.columns {
        let x = match &col.sqltype {
            SqlTy::TinyInt => format!("table.boolean('{}')", col.field),
            SqlTy::Int => format!("table.integer('{}')", col.field),
            SqlTy::BigInt => format!("table.bigInteger('{}')", col.field),
            SqlTy::Enum(p) => format!("table.enum('{}', [{}])", col.field, print_enum_params(&p)),
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
        if col
            .qualifiers
            .iter()
            .any(|c| c == &Constraint::AutoIncrement)
        {
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

fn knex_create_tables(stmts: &[CreateStatement]) {
    for stmt in stmts {
        knex_create_table(stmt)
    }
}

fn diesel_column_type(extra: &extra::Relation, create: &CreateStatement, col: &Column) -> String {
    let not_null = col.qualifiers.iter().any(|x| x == &Constraint::NotNull);
    let unsigned = col.qualifiers.iter().any(|x| x == &Constraint::Unsigned);

    let is_primary = create.is_column_primary(&col.field);
    let is_foreign = create.get_column_foreign(&col.field);
    let is_extra_foreign = extra.get_key_foreign(&create.table_name, &col.field);

    let is_key = is_primary || is_foreign.is_some() || is_extra_foreign.is_some();

    if is_key {
        let ty = match col.sqltype {
            SqlTy::Int | SqlTy::BigInt => {
                if is_primary {
                    let _s = create.table_name.to_case(Case::UpperCamel);
                    //format!("KeyID<object::{}>", s)
                    format!("Unsigned<Integer>")
                } else if let Some(f) = is_foreign {
                    let _s = f.table.to_case(Case::UpperCamel);
                    //format!("KeyID<object::{}>", s)
                    format!("Unsigned<Integer>")
                } else if let Some(vtf) = is_extra_foreign {
                    if vtf.len() > 1 {
                        format!("Unsigned<Integer>")
                    } else {
                        format!("Unsigned<Integer>")
                    }
                } else {
                    format!("Unsigned<Integer>")
                    //panic!("shouldn't be there")
                }
            }
            _ => format!("KeyID for col.sqltype {:?}", col.sqltype),
        };
        let ty = if !not_null {
            format!("Nullable<{}>", ty)
        } else {
            format!("{}", ty)
        };
        ty
    } else {
        let ty = match col.sqltype {
            SqlTy::TinyInt => {
                if col.field == "delete_flag" || col.field == "whitelist_enabled" {
                    format!("Bool")
                } else {
                    format!("Tinyint")
                }
            }
            SqlTy::Int => {
                if unsigned {
                    format!("Unsigned<Integer>")
                } else {
                    format!("Integer")
                }
            }
            SqlTy::BigInt => {
                if unsigned {
                    format!("Unsigned<Integer>")
                } else {
                    format!("Integer")
                }
            }
            SqlTy::Char(_) => format!("Char"),
            SqlTy::VarChar(_) => format!("Varchar"),
            SqlTy::Decimal(_, _) => {
                if unsigned {
                    format!("Unsigned<Decimal>")
                } else {
                    format!("Decimal")
                }
            }
            SqlTy::Enum(_) => format!("Varchar"),
            SqlTy::Timestamp => format!("Timestamp"),
        };

        let ty = if !not_null {
            format!("Nullable<{}>", ty)
        } else {
            format!("{}", ty)
        };
        ty
    }
}

fn diesel_create_table(b: &mut Buffer, extra: &extra::Relation, create: &CreateStatement) {
    let primary_keys = create.get_columns_primary();
    if primary_keys.len() == 0 {
        b.append_nl(format!("// no primary key for {}", create.table_name));
        return;
    }

    let mut pkey = String::new();
    for k in primary_keys {
        if pkey.len() != 0 {
            pkey.push_str(", ");
        }
        pkey.push_str(&k)
    }

    b.append("table! ");
    b.brace(|b| {
        b.newline();
        b.append(format!("{} ({}) ", create.table_name, pkey));
        b.brace(|b| {
            b.newline();
            for col in create.columns.iter() {
                let ty = diesel_column_type(extra, create, col);
                let need_escaping = col.field == create.table_name || col.field == "type";
                //let ty = format!("Type {:?} C={:?}", col.sqltype, col.qualifiers);
                if need_escaping {
                    let row = format!("{}_ -> {},", col.field, ty);
                    b.append_nl(format!("#[sql_name = \"{}\"]", col.field));
                    b.append_nl(format!("{}", row));
                } else {
                    let row = format!("{} -> {},", col.field, ty);
                    b.append_nl(format!("{}", row));
                }
            }
        });
        b.newline()
    });
    b.newline();
    b.newline();

    //println!("table! {{");
    //if pkey.len() > 0 {
    //    println!("    use imports::{{KeyID, object}};");
    //    println!("    use diesel::{{mysql::types::Unsigned, sql_types::*}};");
    //}
    //println!("    {} ({}) {{", create.table_name, pkey);

    //println!("    }}");
    //println!("}}");
    //println!("")
}

fn diesel_create_tables(extra: &extra::Relation, stmts: &[CreateStatement]) {
    use std::io::Write;
    let mut b = Buffer::new();
    for stmt in stmts {
        diesel_create_table(&mut b, extra, stmt)
    }
    // print diesel models
    std::io::stdout().write_all(b.output().as_bytes()).unwrap()
}

fn print_create_tables(stmts: &[CreateStatement]) {
    for stmt in stmts {
        println!("TABLE {}", stmt.table_name);
        for col in &stmt.columns {
            println!("   {:?}", col);
        }
        for m in &stmt.metas {
            println!("   {:?}", m);
        }
    }
}

fn check_exist(
    ctx: &str,
    tables: &HashMap<String, HashSet<String>>,
    table_name: &str,
    field_name: &str,
) {
    match tables.get(table_name) {
        None => {
            panic!("{}\nTable name {} doesn't exist", ctx, table_name);
        }
        Some(fieldset) => match fieldset.get(field_name) {
            None => panic!(
                "{}\ntable name '{}' field name '{}' doesn't exist.\nExisting fields: {:?}",
                ctx, table_name, field_name, fieldset
            ),
            Some(_) => (),
        },
    }
}

fn dot_pseudo_randomized_colors(colors: &[&'static str], table_name: &str) -> &'static str {
    if colors.len() == 0 {
        "black"
    } else {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        table_name.hash(&mut hasher);
        let h = hasher.finish() as usize % colors.len();
        colors[h]
    }
}

fn dot_create_tables(state: Option<extra::Relation>, stmts: &[CreateStatement]) {
    let er = state.unwrap_or_else(|| extra::Relation::new());

    println!("digraph {{");
    println!("    graph [pad=\"0.5\", nodesep=\"0.5\", ranksep=\"2\", bgcolor=black];");
    println!("    node [shape=plain]");
    println!("    rankdir=LR;");

    let bg_color = "#221122";
    let title_bg_color = "#331155";
    let border_color = "#999999";
    let font_color = "#ffffff";
    let title_color = "#ffaaff";

    let mut tables = std::collections::HashMap::new();
    for stmt in stmts {
        println!("    {} [label=<", stmt.table_name);
        println!("    <table bgcolor=\"{}\" color=\"{}\" border=\"0\" cellborder=\"1\" cellspacing=\"0\">", bg_color, border_color);
        println!(
            "    <tr bgcolor=\"{}\"><td><i><font color=\"{}\">{}</font></i></td></tr>",
            title_bg_color, title_color, stmt.table_name
        );
        for (_, col) in stmt
            .columns
            .iter()
            .filter(|c| !TO_HIDE.contains(&c.field.as_str()))
            .enumerate()
        {
            println!(
                "<tr><td port=\"{}\"><font color=\"{}\">{}: {:?}</font></td></tr>",
                col.field, font_color, col.field, col.sqltype
            );
        }
        println!("     </table>>];");
        // all the columns
        let fields = stmt
            .columns
            .iter()
            .filter(|c| !TO_HIDE.contains(&c.field.as_str()))
            .map(|c| c.field.clone())
            .collect::<std::collections::HashSet<_>>();
        tables.insert(stmt.table_name.clone(), fields);
    }

    let link_colors = [
        "black", "#3496F0", "#ED33F0", "#F08E33", "#36F033", "#F0CD33", "#F04F33", "#938893",
        "#3B9682",
    ];

    println!("    // foreign keys");
    for stmt in stmts {
        let my_table = &stmt.table_name;
        for m in &stmt.metas {
            match m {
                Meta::ForeignKey(f) => {
                    let my_field = &f.field;

                    check_exist("foreign keys", &tables, my_table, my_field);
                    check_exist("foreign keys", &tables, &f.table, &f.table_field);

                    //
                    let color = dot_pseudo_randomized_colors(&link_colors, &f.table);
                    println!(
                        "    {}:{} -> {}:{} [color=\"{}\"];",
                        my_table, my_field, f.table, f.table_field, color
                    );
                }
                _ => {}
            }
        }
    }

    println!("    // extra foreign keys");
    for (table, vs) in er.foreigns.iter() {
        for frel in vs {
            match frel {
                ForeignRelation::Simple(extra::ForeignSimple {
                    table_field: my_field,
                    foreign,
                }) => {
                    let ctx = format!(
                        "extra from foreign key for table '{}' setting '{}' as foreign of {}.{}",
                        table, my_field, foreign.table, foreign.field
                    );
                    check_exist(&ctx, &tables, table, my_field);
                    let ctx = format!(
                        "extra to foreign key for table '{}' setting '{}' as foreign of {}.{}",
                        table, my_field, foreign.table, foreign.field
                    );
                    check_exist(&ctx, &tables, &foreign.table, &foreign.field);

                    let color = dot_pseudo_randomized_colors(&link_colors, &foreign.field);
                    println!(
                        "    {}:{} -> {}:{} [color=\"{}\"];",
                        table, my_field, foreign.table, foreign.field, color
                    );
                }
                ForeignRelation::Either(_) => {
                    //
                }
            }
        }
    }

    println!("}}")
}

fn show_tables(
    only_table_name: bool,
    filter_table_names: &[String],
    stmts: &[CreateStatement],
    istmts: &[InsertStatement],
) {
    use prettytable::{color, format, Attr, Cell, Row, Table};

    for c in stmts {
        if !filter_table_names.is_empty() && !filter_table_names.contains(&c.table_name) {
            continue;
        }

        let mut table = Table::new();
        table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);

        println!("## {}", c.table_name);

        if only_table_name {
            continue;
        }

        let mut row = Vec::new();

        let mut hide_column_indexes = Vec::new();
        for (i, col) in c.columns.iter().enumerate() {
            if TO_HIDE.contains(&col.field.as_str()) {
                hide_column_indexes.push(i);
                continue;
            }
            row.push(
                Cell::new(&col.field)
                    .with_style(Attr::Bold)
                    .with_style(Attr::ForegroundColor(color::GREEN)),
            );
        }
        table.add_row(Row::new(row));

        let mut is: Vec<Vec<SqlValue>> = Vec::new();
        for i in istmts.iter().filter(|i| i.table_name == c.table_name) {
            is.extend(i.rows.iter().cloned())
        }

        for row in is.iter() {
            let mut trow = Vec::new();
            for (i, value) in row.iter().enumerate() {
                if hide_column_indexes.contains(&i) {
                    continue;
                }
                trow.push(Cell::new(&format!("{}", value.to_display_string())));
            }
            table.add_row(Row::new(trow));
        }
        table.printstd();
    }
}

pub fn output_create(
    output: OutputType,
    extra_relation: Option<extra::Relation>,
    stmts: &[CreateStatement],
    istmts: &[InsertStatement],
) {
    match output {
        OutputType::Knex => knex_create_tables(stmts),
        OutputType::Diesel => {
            diesel_create_tables(&extra_relation.unwrap_or(extra::Relation::new()), stmts)
        }
        OutputType::Debug => print_create_tables(stmts),
        OutputType::Dot => dot_create_tables(extra_relation, stmts),
        OutputType::Tables(only_table_name, tables) => {
            show_tables(only_table_name, &tables, stmts, istmts)
        }
    }
}

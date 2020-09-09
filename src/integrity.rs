use std::str::FromStr;

use crate::extra;
use crate::parse::{Column, Constraint, CreateStatement, SqlParam, SqlTy, SqlValue, Statement};
use chrono::NaiveDateTime;
use std::collections::{HashMap, HashSet};

use super::{Table, TableInfo};

macro_rules! n_from_str {
    ($is_unsigned: ident, $info: ident, $col: ident, $number: ident, $um: ident, $im: ident) => {
        if $is_unsigned && $um::from_str($number).is_err() {
            println!(
                "{}:{} UNSIGNED NUMBER mismatch {}",
                $info, $col.field, $number
            );
        } else if $im::from_str($number).is_err() {
            println!(
                "{}:{} SIGNED NUMBER mismatch {}",
                $info, $col.field, $number
            );
        }
    };
}

fn number_check<'a>(info: &TableInfo<'a>, number: &str, col: &Column) {
    let unsigned = col.qualifiers.iter().any(|q| q == &Constraint::Unsigned);

    match &col.sqltype {
        SqlTy::TinyInt => n_from_str!(unsigned, info, col, number, u8, i8),
        SqlTy::Int => n_from_str!(unsigned, info, col, number, u32, i32),
        SqlTy::BigInt => n_from_str!(unsigned, info, col, number, u64, i64),
        SqlTy::Decimal(_, _) => {
            let v = number.split(".").collect::<Vec<_>>();
            if v.len() == 2 {
                let ent = u128::from_str(v[0]);
                let dec = u128::from_str(v[1]);
                if ent.is_err() || dec.is_err() {
                    println!("{}:{} DECIMAL mismatch {}", info, col.field, number);
                }
            } else {
                println!("{}:{} DECIMAL mismatch {}", info, col.field, number);
            }
        }
        t => println!(
            "{}:{} NUMBER mismatch {} type={:?}",
            info, col.field, number, t
        ),
    }
}

fn string_check<'a>(info: &TableInfo<'a>, s: &str, col: &Column) {
    match &col.sqltype {
        SqlTy::Char(_opt) => {}
        SqlTy::VarChar(_opt) => {}
        SqlTy::Enum(list) => {
            if list
                .iter()
                .any(|x| x == &SqlParam::LitString(s.to_string()))
            {
                ()
            } else {
                println!(
                    "{}:{} ENUM mismatch \"{}\" not in list of {:?}",
                    info, col.field, s, list
                )
            }
        }
        SqlTy::Timestamp => match NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
            Ok(_) => {}
            Err(_) => {
                if s != "0000-00-00 00:00:00" {
                    println!("{}:{} TIMESTAMP mismatch {}", info, col.field, s)
                } else {
                }
            }
        },
        t => println!("{}:{} STRING mismatch \"{}\" {:?}", info, col.field, s, t),
    }
}

fn row_match_schema<'a>(info: TableInfo<'a>, create_stmt: &CreateStatement, row: &Vec<SqlValue>) {
    for (col_id, col) in create_stmt.columns.iter().enumerate() {
        let col_def = &create_stmt.columns[col_id];
        let v = &row[col_id];
        match v {
            SqlValue::Number(n) => number_check(&info, n, col_def),
            SqlValue::SQuoteString(s) => string_check(&info, s, col_def),
            SqlValue::Null => {
                let not_null = col.qualifiers.iter().any(|x| x == &Constraint::NotNull);
                if not_null {
                    println!("{}:{} NULL mismatch {:?}", info, col.field, col_def.sqltype);
                } else {
                }
            }
            SqlValue::FctCallVoid(_) => {}
        }
        //println!("  {:?} {:?}", col_def, v);
    }
}

pub fn integrity(stmts: Vec<Option<Statement>>, extra: Option<extra::Relation>) {
    extra.as_ref().map(|e| println!("{}", e));
    let (creates, inserts) = {
        let mut creates = Vec::new();
        let mut inserts = Vec::new();
        for s in stmts.into_iter() {
            match s {
                Some(Statement::Create(c)) => creates.push(c),
                Some(Statement::Insert(i)) => inserts.push(i),
                Some(Statement::Other(_)) => {}
                None => {}
            }
        }
        (creates, inserts)
    };

    let mut db = HashMap::new();
    for c in creates.iter() {
        db.insert(c.table_name.clone(), Table { c: c.clone() });
    }

    // prepopulate table with primary keys
    let mut primaries = HashMap::new();
    for c in creates.iter() {
        match c.get_column_primary() {
            None => {}
            Some(pk_field) => {
                primaries.insert(c.table_name.clone(), (pk_field, HashSet::new()));
            }
        }
    }

    // check all values
    for i in inserts.iter() {
        println!("insert in {}: {} values", i.table_name, i.rows.len());
        match db.get(&i.table_name) {
            None => {
                println!("table {} not created", i.table_name);
            }
            Some(t) => {
                match primaries.get_mut(&i.table_name) {
                    None => {}
                    Some((_, prim)) => {
                        let (colid, _pkfield) = t.c.get_column_primary_colid().unwrap();
                        for (_, row) in i.rows.iter().enumerate() {
                            match row[colid].as_u32() {
                                None => println!(
                                    "table {} foreign key wrong {}: {:?}",
                                    i.table_name, colid, row[colid]
                                ),
                                Some(n) => {
                                    if !prim.insert(n) {
                                        println!("duplicated entry on {} for {}", i.table_name, n)
                                    }
                                }
                            }
                        }
                    }
                };
                let s = i.table_name.as_str();

                for (rowid, row) in i.rows.iter().enumerate() {
                    let table_info = TableInfo {
                        table_name: s,
                        rowid: rowid as u32,
                    };
                    row_match_schema(table_info, &t.c, &row);
                }
            }
        }
    }

    let mut success = 0;
    let mut err = 0;
    let mut skip = 0;
    for i in inserts {
        let c = &db.get(&i.table_name).unwrap().c;
        println!("checking foreign keys related to {}", i.table_name);
        let col_with_foreigns = c
            .columns
            .iter()
            .map(|col| {
                let fk = extra
                    .as_ref()
                    .map(|e| {
                        e.get_key_foreign(&i.table_name, &col.field)
                            .map(|x| x.iter().map(|y| y.table.clone()).collect::<Vec<_>>())
                    })
                    .unwrap_or(None);
                let finfo = c
                    .get_column_foreign(&col.field)
                    .map(|info| vec![info.table]);

                /*
                if i.table_name == "address_request" {
                    println!(" foreign col={} r = {:?} {:?}", col.field, fk, finfo)
                }
                */
                (col.clone(), finfo.or(fk))
            })
            .collect::<Vec<_>>();

        /*
        for x in col_with_foreigns.iter() {
            if x.1.is_none() {
                continue;
            }
            println!("     {:?}={:?}", &x.0, x.1.as_ref().unwrap())
        }
        */

        for row in i.rows.iter() {
            for (row_value, (col, finfo)) in row.iter().zip(col_with_foreigns.iter()) {
                match finfo {
                    None => skip += 1,
                    Some(foreign_tables) => {
                        /*
                        if i.table_name == "address_request" && foreign_table == "request" {
                            println!("OOOO col={} {:?}", col.field, row_value);
                        }
                        */
                        let fkey = match row_value.as_u32() {
                            None => {
                                continue;
                                //println!("foreign field {}={:?}", col.field, row_value);
                            }
                            Some(fkey) => fkey,
                        };
                        let mut found = false;
                        for foreign_table in foreign_tables {
                            let prim = match &primaries.get(foreign_table) {
                                None => {
                                    //println!("cannot get {} for primaries", foreign_table);
                                    continue;
                                }
                                Some(p) => &p.1,
                            };
                            if prim.get(&fkey).is_none() {
                                ()
                            } else {
                                found = true;
                                break;
                            }
                        }

                        if found {
                            success += 1;
                        } else {
                            err += 1;
                            println!("in table {} foreign field {} missing value {} in foreign tables={:?}", i.table_name, col.field, fkey, foreign_tables);
                        }
                        //println!("foreign field {}={:?}", col.field, row_value);
                    }
                };
            }
        }
    }
    println!(
        "foreign keys checked: success={} failed={} skip={}",
        success, err, skip
    )
}

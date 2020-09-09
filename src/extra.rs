use std::collections::HashMap;
use std::fmt;

#[derive(Clone)]
pub struct TF {
    pub table: String,
    pub field: String,
}

impl fmt::Debug for TF {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.table, self.field)
    }
}
impl fmt::Display for TF {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.table, self.field)
    }
}

#[derive(Debug, Clone)]
pub struct ForeignSimple {
    pub table_field: String,
    pub foreign: TF,
}

#[derive(Debug, Clone)]
pub struct ForeignEither {
    pub table_field: String,
    pub foreign1: TF,
    pub foreign2: TF,
}

#[derive(Debug, Clone)]
pub enum ForeignRelation {
    Simple(ForeignSimple),
    Either(ForeignEither),
}

impl ForeignRelation {
    pub fn table_field(&self) -> &str {
        match self {
            ForeignRelation::Simple(r) => r.table_field.as_ref(),
            ForeignRelation::Either(r) => r.table_field.as_ref(),
        }
    }
}

pub struct Relation {
    // extra foreign keys relation
    pub foreigns: HashMap<String, Vec<ForeignRelation>>,
}

impl fmt::Display for ForeignSimple {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} => {}", self.table_field, self.foreign)
    }
}

impl fmt::Display for ForeignEither {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "({} => {} | {})",
            self.table_field, self.foreign1, self.foreign2
        )
    }
}

impl fmt::Display for ForeignRelation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ForeignRelation::Simple(r) => write!(f, "{}", r),
            ForeignRelation::Either(r) => write!(f, "{}", r),
        }
    }
}

impl fmt::Display for Relation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "foreign:")?;
        for (k, vs) in self.foreigns.iter() {
            for frel in vs.iter() {
                writeln!(f, "    {} {}", k, frel)?
            }
        }
        Ok(())
    }
}

impl Relation {
    pub fn new() -> Self {
        Self {
            foreigns: HashMap::new(),
        }
    }

    #[allow(dead_code)]
    pub fn is_key_foreign(&self, table_name: &str, field_name: &str) -> bool {
        self.foreigns
            .get(table_name)
            .map(|v| v.iter().any(|x| x.table_field() == field_name))
            .unwrap_or(false)
    }

    pub fn get_key_foreign(&self, table_name: &str, field_name: &str) -> Option<Vec<TF>> {
        self.foreigns
            .get(table_name)
            .map(|v| {
                v.iter().find_map(|rel| match rel {
                    ForeignRelation::Simple(rel) if rel.table_field == field_name => {
                        Some(vec![rel.foreign.clone()])
                    }
                    ForeignRelation::Either(rel) if rel.table_field == field_name => {
                        Some(vec![rel.foreign1.clone(), rel.foreign2.clone()])
                    }
                    _ => None,
                })
            })
            .unwrap_or(None)
    }
}

/// Parse a triple:
///
///     TABLE_FIELD:FOREIGN_TABLE_NAME:FOREIGN_TABLE_FIELD
///
/// or:
///
///     FOREIGN_TABLE_NAME
///     FOREIGN_TABLE_NAME_id:FOREIGN_TABLE_NAME:"id"
///
fn parse_triple(tripl: &str) -> (String, TF) {
    if tripl.contains(":") {
        let splitted = tripl.split(":").collect::<Vec<_>>();
        if splitted.len() == 3 {
            let table_field = if splitted[0].is_empty() {
                format!("{}_id", splitted[1])
            } else {
                splitted[0].to_string()
            };

            let foreign_field = if splitted[2].is_empty() {
                "id".to_string()
            } else {
                splitted[2].to_string()
            };
            let foreign = TF {
                table: splitted[1].to_string(),
                field: foreign_field,
            };
            (table_field, foreign)
        } else {
            panic!("bad format \"{}\", expecting A:B:C", tripl)
        }
    } else {
        let table_field = format!("{}_id", tripl.to_string());
        let foreign = TF {
            table: tripl.to_string(),
            field: "id".to_string(),
        };
        (table_field, foreign)
    }
}

fn parse_foreign_relation(line: &str) -> ForeignRelation {
    let x = line.trim_start();
    if x.contains(" | ") {
        let v = x.split(" | ").collect::<Vec<_>>();
        if v.len() != 2 {
            panic!(
                "format for either not correct \"{}\": expecting 2 but got {}",
                line,
                v.len()
            )
        }
        let v0 = v[0].clone().trim();
        let v1 = v[1].clone().trim();
        let (tf1, f1) = parse_triple(&v0);
        let (tf2, f2) = parse_triple(&v1);
        if tf1 == tf2 {
            let fe = ForeignEither {
                table_field: tf1,
                foreign1: f1,
                foreign2: f2,
            };
            ForeignRelation::Either(fe)
        } else {
            panic!(
                "format for either not correct \"{}\": expecting same table but got {} {}",
                line, tf1, tf2
            )
        }
    } else {
        let (table_field, foreign) = parse_triple(x);
        let fs = ForeignSimple {
            table_field,
            foreign,
        };

        ForeignRelation::Simple(fs)
    }
}

pub fn parse_foreign(lines: &[&str]) -> HashMap<String, Vec<ForeignRelation>> {
    let mut table: Option<&str> = None;
    let mut table_state = vec![];
    let mut h = HashMap::new();

    for l in lines {
        if l.starts_with(" ") {
            let _t = table.expect("table is not empty");
            let frel = parse_foreign_relation(l);
            table_state.push(frel);
        } else {
            if let Some(table_name) = table {
                if table_state.len() == 0 {
                    panic!("empty: {}", table_name)
                }
                h.insert(table_name.to_string(), table_state);
                table_state = vec![];
                table = Some(l);
            } else {
                table = Some(l)
            }
        }
    }
    h
}

pub fn parse(xcontents: String) -> Relation {
    let mut sections = HashMap::new();
    let mut section = Vec::new();
    let mut section_name = None;

    for l in xcontents.lines() {
        if l.is_empty() || l.starts_with("#") {
            continue;
        } else if l.starts_with("[") && l.ends_with("]") {
            if let Some(section_name) = section_name {
                println!(" {} {}", section_name, section.len());
                sections.insert(section_name, section);
                section = Vec::new();
            }
            section_name = Some(l.clone());
        } else {
            section.push(l)
        }
    }

    if let Some(section_name) = section_name {
        if section.len() > 0 {
            sections.insert(section_name, section);
        }
    }

    let foreigns = sections
        .get("[foreign]")
        .map(|s| parse_foreign(s))
        .unwrap_or(HashMap::new());

    Relation { foreigns }
}

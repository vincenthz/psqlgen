use crate::parse::SqlType;

#[derive(Debug, Clone)]
pub enum Value {
    Primitive(SqlType),
}

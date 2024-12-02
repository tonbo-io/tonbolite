use rusqlite::types::ValueRef;
use rusqlite::vtab::Context;
use rusqlite::Error;
use sqlparser::ast::DataType;
use std::any::Any;
use std::sync::Arc;
use tonbo::record::{Column, Datatype};

pub(crate) fn parse_type(input: &str) -> rusqlite::Result<(Datatype, bool, bool)> {
    let input = input.trim();

    let is_nullable = input.contains("nullable");
    let is_primary_key = input.contains("primary key");

    let mut type_str = input.to_string();
    if is_nullable {
        type_str = type_str.replace("nullable", "");
    }
    if is_primary_key {
        type_str = type_str.replace("primary key", "");
    }
    let ty = match type_str.trim() {
        "int" => Datatype::Int64,
        "varchar" => Datatype::String,
        _ => {
            return Err(Error::ModuleError(format!(
                "unrecognized parameter '{input}'"
            )));
        }
    };

    Ok((ty, is_nullable, is_primary_key))
}

macro_rules! nullable_value {
    ($value:expr, $is_nullable:expr) => {
        if $is_nullable {
            Arc::new(Some($value)) as Arc<dyn Any + Send + Sync>
        } else {
            Arc::new($value) as Arc<dyn Any + Send + Sync>
        }
    };
}

// TODO: Value Cast
pub(crate) fn value_trans(
    value: ValueRef<'_>,
    ty: &Datatype,
    is_nullable: bool,
) -> rusqlite::Result<Arc<dyn Any + Send + Sync>> {
    match value {
        ValueRef::Null => {
            if !is_nullable {
                return Err(Error::ModuleError("value is not nullable".to_string()));
            }
            Ok(match ty {
                Datatype::UInt8 => Arc::new(Option::<u8>::None),
                Datatype::UInt16 => Arc::new(Option::<u16>::None),
                Datatype::UInt32 => Arc::new(Option::<u32>::None),
                Datatype::UInt64 => Arc::new(Option::<u64>::None),
                Datatype::Int8 => Arc::new(Option::<i8>::None),
                Datatype::Int16 => Arc::new(Option::<i16>::None),
                Datatype::Int32 => Arc::new(Option::<i32>::None),
                Datatype::Int64 => Arc::new(Option::<i64>::None),
                Datatype::String => Arc::new(Option::<String>::None),
                Datatype::Boolean => Arc::new(Option::<bool>::None),
                Datatype::Bytes => Arc::new(Option::<Vec<u8>>::None),
            })
        }
        ValueRef::Integer(v) => {
            let value = match ty {
                Datatype::UInt8 => nullable_value!(v as u8, is_nullable),
                Datatype::UInt16 => nullable_value!(v as u16, is_nullable),
                Datatype::UInt32 => nullable_value!(v as u32, is_nullable),
                Datatype::UInt64 => nullable_value!(v as u64, is_nullable),
                Datatype::Int8 => nullable_value!(v as i8, is_nullable),
                Datatype::Int16 => nullable_value!(v as i16, is_nullable),
                Datatype::Int32 => nullable_value!(v as i32, is_nullable),
                Datatype::Int64 => nullable_value!(v, is_nullable),
                _ => {
                    return Err(Error::ModuleError(format!(
                        "unsupported value: {:#?} cast to: {:#?}",
                        v, ty
                    )))
                }
            };

            Ok(value)
        }
        ValueRef::Real(_) => {
            todo!("tonbo f32/f64 unsupported yet")
        }
        ValueRef::Text(v) => {
            if let Datatype::Bytes = ty {
                return Ok(nullable_value!(v.to_vec(), is_nullable));
            }
            let v = String::from_utf8(v.to_vec()).unwrap();
            let value = match ty {
                Datatype::UInt8 => nullable_value!(v.parse::<u8>().unwrap(), is_nullable),
                Datatype::UInt16 => nullable_value!(v.parse::<u16>().unwrap(), is_nullable),
                Datatype::UInt32 => nullable_value!(v.parse::<u32>().unwrap(), is_nullable),
                Datatype::UInt64 => nullable_value!(v.parse::<u64>().unwrap(), is_nullable),
                Datatype::Int8 => nullable_value!(v.parse::<i8>().unwrap(), is_nullable),
                Datatype::Int16 => nullable_value!(v.parse::<i16>().unwrap(), is_nullable),
                Datatype::Int32 => nullable_value!(v.parse::<i32>().unwrap(), is_nullable),
                Datatype::Int64 => nullable_value!(v.parse::<i64>().unwrap(), is_nullable),
                Datatype::String => nullable_value!(v, is_nullable),
                Datatype::Boolean => nullable_value!(v.parse::<bool>().unwrap(), is_nullable),
                Datatype::Bytes => unreachable!(),
            };
            Ok(value)
        }
        ValueRef::Blob(v) => {
            let v = v.to_vec();
            Ok(match ty {
                Datatype::String => nullable_value!(String::from_utf8(v).unwrap(), is_nullable),
                Datatype::Bytes => nullable_value!(v, is_nullable),
                _ => {
                    return Err(Error::ModuleError(format!(
                        "unsupported value: {:#?} cast to: {:#?}",
                        v, ty
                    )))
                }
            })
        }
    }
}

pub(crate) fn set_result(ctx: &mut Context, col: &Column) -> rusqlite::Result<()> {
    match &col.datatype {
        Datatype::UInt8 => {
            if col.is_nullable {
                ctx.set_result(col.value.as_ref().downcast_ref::<Option<u8>>().unwrap())?;
            } else {
                ctx.set_result(col.value.as_ref().downcast_ref::<u8>().unwrap())?;
            }
        }
        Datatype::UInt16 => {
            if col.is_nullable {
                ctx.set_result(col.value.as_ref().downcast_ref::<Option<u16>>().unwrap())?;
            } else {
                ctx.set_result(col.value.as_ref().downcast_ref::<u16>().unwrap())?;
            }
        }
        Datatype::UInt32 => {
            if col.is_nullable {
                ctx.set_result(col.value.as_ref().downcast_ref::<Option<u32>>().unwrap())?;
            } else {
                ctx.set_result(col.value.as_ref().downcast_ref::<u32>().unwrap())?;
            }
        }
        Datatype::UInt64 => {
            if col.is_nullable {
                ctx.set_result(col.value.as_ref().downcast_ref::<Option<u64>>().unwrap())?;
            } else {
                ctx.set_result(col.value.as_ref().downcast_ref::<u64>().unwrap())?;
            }
        }
        Datatype::Int8 => {
            if col.is_nullable {
                ctx.set_result(col.value.as_ref().downcast_ref::<Option<i8>>().unwrap())?;
            } else {
                ctx.set_result(col.value.as_ref().downcast_ref::<i8>().unwrap())?;
            }
        }
        Datatype::Int16 => {
            if col.is_nullable {
                ctx.set_result(col.value.as_ref().downcast_ref::<Option<i16>>().unwrap())?;
            } else {
                ctx.set_result(col.value.as_ref().downcast_ref::<i16>().unwrap())?;
            }
        }
        Datatype::Int32 => {
            if col.is_nullable {
                ctx.set_result(col.value.as_ref().downcast_ref::<Option<i32>>().unwrap())?;
            } else {
                ctx.set_result(col.value.as_ref().downcast_ref::<i32>().unwrap())?;
            }
        }
        Datatype::Int64 => {
            if col.is_nullable {
                ctx.set_result(col.value.as_ref().downcast_ref::<Option<i64>>().unwrap())?;
            } else {
                ctx.set_result(col.value.as_ref().downcast_ref::<i64>().unwrap())?;
            }
        }
        Datatype::String => {
            if col.is_nullable {
                ctx.set_result(col.value.as_ref().downcast_ref::<Option<String>>().unwrap())?;
            } else {
                ctx.set_result(col.value.as_ref().downcast_ref::<String>().unwrap())?;
            }
        }
        Datatype::Boolean => {
            if col.is_nullable {
                ctx.set_result(col.value.as_ref().downcast_ref::<Option<bool>>().unwrap())?;
            } else {
                ctx.set_result(col.value.as_ref().downcast_ref::<bool>().unwrap())?;
            }
        }
        Datatype::Bytes => {
            if col.is_nullable {
                ctx.set_result(
                    col.value
                        .as_ref()
                        .downcast_ref::<Option<Vec<u8>>>()
                        .unwrap(),
                )?;
            } else {
                ctx.set_result(col.value.as_ref().downcast_ref::<Vec<u8>>().unwrap())?;
            }
        }
    }
    Ok(())
}

pub(crate) fn type_trans(ty: &DataType) -> Datatype {
    match ty {
        DataType::Int8(_) => Datatype::Int8,
        DataType::Int16 | DataType::SmallInt(_) => Datatype::Int16,
        DataType::Int(_) | DataType::Int32 | DataType::Integer(_) => Datatype::Int32,
        DataType::Int64 | DataType::BigInt(_) => Datatype::Int64,
        DataType::UnsignedInt(_) | DataType::UInt32 | DataType::UnsignedInteger(_) => {
            Datatype::UInt32
        }
        DataType::UInt8 | DataType::UnsignedInt8(_) => Datatype::UInt8,
        DataType::UInt16 => Datatype::UInt16,
        DataType::UInt64 | DataType::UnsignedBigInt(_) => Datatype::UInt64,
        DataType::Bool | DataType::Boolean => Datatype::Boolean,
        DataType::Bytes(_) => Datatype::Bytes,
        DataType::Varchar(_) => Datatype::String,
        _ => todo!(),
    }
}

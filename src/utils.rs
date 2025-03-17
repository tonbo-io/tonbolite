use rusqlite::types::ValueRef;
use rusqlite::vtab::Context;
use rusqlite::Error;
use sqlparser::ast::DataType;
use std::any::Any;
use std::sync::Arc;
use tonbo::record::Value;

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
    ty: &tonbo::record::DataType,
    is_nullable: bool,
) -> rusqlite::Result<Arc<dyn Any + Send + Sync>> {
    match value {
        ValueRef::Null => {
            if !is_nullable {
                return Err(Error::ModuleError("value is not nullable".to_string()));
            }
            Ok(match ty {
                tonbo::record::DataType::UInt8 => Arc::new(Option::<u8>::None),
                tonbo::record::DataType::UInt16 => Arc::new(Option::<u16>::None),
                tonbo::record::DataType::UInt32 => Arc::new(Option::<u32>::None),
                tonbo::record::DataType::UInt64 => Arc::new(Option::<u64>::None),
                tonbo::record::DataType::Int8 => Arc::new(Option::<i8>::None),
                tonbo::record::DataType::Int16 => Arc::new(Option::<i16>::None),
                tonbo::record::DataType::Int32 => Arc::new(Option::<i32>::None),
                tonbo::record::DataType::Int64 => Arc::new(Option::<i64>::None),
                tonbo::record::DataType::String => Arc::new(Option::<String>::None),
                tonbo::record::DataType::Boolean => Arc::new(Option::<bool>::None),
                tonbo::record::DataType::Bytes => Arc::new(Option::<Vec<u8>>::None),
            })
        }
        ValueRef::Integer(v) => {
            let value = match ty {
                tonbo::record::DataType::UInt8 => nullable_value!(v as u8, is_nullable),
                tonbo::record::DataType::UInt16 => nullable_value!(v as u16, is_nullable),
                tonbo::record::DataType::UInt32 => nullable_value!(v as u32, is_nullable),
                tonbo::record::DataType::UInt64 => nullable_value!(v as u64, is_nullable),
                tonbo::record::DataType::Int8 => nullable_value!(v as i8, is_nullable),
                tonbo::record::DataType::Int16 => nullable_value!(v as i16, is_nullable),
                tonbo::record::DataType::Int32 => nullable_value!(v as i32, is_nullable),
                tonbo::record::DataType::Int64 => nullable_value!(v, is_nullable),
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
            if let tonbo::record::DataType::Bytes = ty {
                return Ok(nullable_value!(v.to_vec(), is_nullable));
            }
            let v = String::from_utf8(v.to_vec()).unwrap();
            let value = match ty {
                tonbo::record::DataType::UInt8 => {
                    nullable_value!(v.parse::<u8>().unwrap(), is_nullable)
                }
                tonbo::record::DataType::UInt16 => {
                    nullable_value!(v.parse::<u16>().unwrap(), is_nullable)
                }
                tonbo::record::DataType::UInt32 => {
                    nullable_value!(v.parse::<u32>().unwrap(), is_nullable)
                }
                tonbo::record::DataType::UInt64 => {
                    nullable_value!(v.parse::<u64>().unwrap(), is_nullable)
                }
                tonbo::record::DataType::Int8 => {
                    nullable_value!(v.parse::<i8>().unwrap(), is_nullable)
                }
                tonbo::record::DataType::Int16 => {
                    nullable_value!(v.parse::<i16>().unwrap(), is_nullable)
                }
                tonbo::record::DataType::Int32 => {
                    nullable_value!(v.parse::<i32>().unwrap(), is_nullable)
                }
                tonbo::record::DataType::Int64 => {
                    nullable_value!(v.parse::<i64>().unwrap(), is_nullable)
                }
                tonbo::record::DataType::String => nullable_value!(v, is_nullable),
                tonbo::record::DataType::Boolean => {
                    nullable_value!(v.parse::<bool>().unwrap(), is_nullable)
                }
                tonbo::record::DataType::Bytes => unreachable!(),
            };
            Ok(value)
        }
        ValueRef::Blob(v) => {
            let v = v.to_vec();
            Ok(match ty {
                tonbo::record::DataType::String => {
                    nullable_value!(String::from_utf8(v).unwrap(), is_nullable)
                }
                tonbo::record::DataType::Bytes => nullable_value!(v, is_nullable),
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

pub(crate) fn set_result(ctx: &mut Context, col: &Value) -> rusqlite::Result<()> {
    match &col.datatype() {
        tonbo::record::DataType::UInt8 => {
            if col.is_nullable() {
                ctx.set_result(col.value.as_ref().downcast_ref::<Option<u8>>().unwrap())?;
            } else {
                ctx.set_result(col.value.as_ref().downcast_ref::<u8>().unwrap())?;
            }
        }
        tonbo::record::DataType::UInt16 => {
            if col.is_nullable() {
                ctx.set_result(col.value.as_ref().downcast_ref::<Option<u16>>().unwrap())?;
            } else {
                ctx.set_result(col.value.as_ref().downcast_ref::<u16>().unwrap())?;
            }
        }
        tonbo::record::DataType::UInt32 => {
            if col.is_nullable() {
                ctx.set_result(col.value.as_ref().downcast_ref::<Option<u32>>().unwrap())?;
            } else {
                ctx.set_result(col.value.as_ref().downcast_ref::<u32>().unwrap())?;
            }
        }
        tonbo::record::DataType::UInt64 => {
            if col.is_nullable() {
                ctx.set_result(col.value.as_ref().downcast_ref::<Option<u64>>().unwrap())?;
            } else {
                ctx.set_result(col.value.as_ref().downcast_ref::<u64>().unwrap())?;
            }
        }
        tonbo::record::DataType::Int8 => {
            if col.is_nullable() {
                ctx.set_result(col.value.as_ref().downcast_ref::<Option<i8>>().unwrap())?;
            } else {
                ctx.set_result(col.value.as_ref().downcast_ref::<i8>().unwrap())?;
            }
        }
        tonbo::record::DataType::Int16 => {
            if col.is_nullable() {
                ctx.set_result(col.value.as_ref().downcast_ref::<Option<i16>>().unwrap())?;
            } else {
                ctx.set_result(col.value.as_ref().downcast_ref::<i16>().unwrap())?;
            }
        }
        tonbo::record::DataType::Int32 => {
            if col.is_nullable() {
                ctx.set_result(col.value.as_ref().downcast_ref::<Option<i32>>().unwrap())?;
            } else {
                ctx.set_result(col.value.as_ref().downcast_ref::<i32>().unwrap())?;
            }
        }
        tonbo::record::DataType::Int64 => {
            if col.is_nullable() {
                ctx.set_result(col.value.as_ref().downcast_ref::<Option<i64>>().unwrap())?;
            } else {
                ctx.set_result(col.value.as_ref().downcast_ref::<i64>().unwrap())?;
            }
        }
        tonbo::record::DataType::String => {
            if col.is_nullable() {
                ctx.set_result(col.value.as_ref().downcast_ref::<Option<String>>().unwrap())?;
            } else {
                ctx.set_result(col.value.as_ref().downcast_ref::<String>().unwrap())?;
            }
        }
        tonbo::record::DataType::Boolean => {
            if col.is_nullable() {
                ctx.set_result(col.value.as_ref().downcast_ref::<Option<bool>>().unwrap())?;
            } else {
                ctx.set_result(col.value.as_ref().downcast_ref::<bool>().unwrap())?;
            }
        }
        tonbo::record::DataType::Bytes => {
            if col.is_nullable() {
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

pub(crate) fn type_trans(ty: &DataType) -> tonbo::record::DataType {
    match ty {
        DataType::Int8(_) => tonbo::record::DataType::Int8,
        DataType::Int16 | DataType::SmallInt(_) => tonbo::record::DataType::Int16,
        DataType::Int(_) | DataType::Int32 | DataType::Integer(_) => tonbo::record::DataType::Int32,
        DataType::Int64 | DataType::BigInt(_) => tonbo::record::DataType::Int64,
        DataType::UnsignedInt(_) | DataType::UInt32 | DataType::UnsignedInteger(_) => {
            tonbo::record::DataType::UInt32
        }
        DataType::UInt8 | DataType::UnsignedInt8(_) => tonbo::record::DataType::UInt8,
        DataType::UInt16 => tonbo::record::DataType::UInt16,
        DataType::UInt64 | DataType::UnsignedBigInt(_) => tonbo::record::DataType::UInt64,
        DataType::Bool | DataType::Boolean => tonbo::record::DataType::Boolean,
        DataType::Bytes(_) => tonbo::record::DataType::Bytes,
        DataType::Varchar(_) => tonbo::record::DataType::String,
        _ => todo!(),
    }
}

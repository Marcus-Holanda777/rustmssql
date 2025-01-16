use crate::MSchema;
use parquet::{
    basic::{LogicalType, Repetition, TimeUnit, Type as PhysicalType},
    format::{MicroSeconds, MilliSeconds},
    schema::types::Type,
};
use std::sync::Arc;

fn get_type(col: &str, types: PhysicalType, logical: Option<LogicalType>) -> Type {
    Type::primitive_type_builder(&col, types)
        .with_logical_type(logical)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap()
}

fn to_type_column(schema: MSchema) -> Type {
    let col = schema
        .column_name
        .unwrap()
        .trim()
        .to_lowercase()
        .split_whitespace()
        .map(|f| f.trim())
        .collect::<Vec<_>>()
        .join("_");

    // converter para o tipo Option<&str> e depos para &str
    let opt = schema.data_type.as_deref().unwrap();
    let scale = schema.numeric_scale.unwrap_or(0);
    let precision = schema.numeric_precision.unwrap_or(0) as i32;

    match opt {
        "int" | "smallint" | "tinyint" => get_type(&col, PhysicalType::INT32, None),
        "bigint" => get_type(&col, PhysicalType::INT64, None),
        "float" => get_type(&col, PhysicalType::DOUBLE, None),
        "real" => get_type(&col, PhysicalType::FLOAT, None),
        "decimal" | "numeric" => Type::primitive_type_builder(&col, PhysicalType::BYTE_ARRAY)
            .with_logical_type(Some(LogicalType::Decimal { scale, precision }))
            .with_precision(precision)
            .with_scale(scale)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap(),
        "bit" => get_type(&col, PhysicalType::BOOLEAN, None),
        "char" | "varchar" | "text" | "nchar" | "nvarchar" | "ntext" => {
            get_type(&col, PhysicalType::BYTE_ARRAY, Some(LogicalType::String))
        }
        "datetime" | "datetime2" | "smalldatetime" => get_type(
            &col,
            PhysicalType::INT64,
            Some(LogicalType::Timestamp {
                is_adjusted_to_u_t_c: false,
                unit: TimeUnit::MILLIS(MilliSeconds {}),
            }),
        ),
        "date" => get_type(&col, PhysicalType::INT32, Some(LogicalType::Date)),
        "time" => get_type(
            &col,
            PhysicalType::INT64,
            Some(LogicalType::Time {
                is_adjusted_to_u_t_c: false,
                unit: TimeUnit::MICROS(MicroSeconds {}),
            }),
        ),
        "binary" | "varbinary" | "image" => get_type(&col, PhysicalType::BYTE_ARRAY, None),
        _ => get_type(&col, PhysicalType::BYTE_ARRAY, Some(LogicalType::String)),
    }
}

pub fn create_schema_parquet(sql_types: Vec<MSchema>) -> Type {
    let mut fields = vec![];

    for mssql in sql_types {
        let data = to_type_column(mssql);
        let tp = Arc::new(data);

        fields.push(tp);
    }

    Type::group_type_builder("schema_mvsh")
        .with_fields(fields)
        .build()
        .unwrap()
}

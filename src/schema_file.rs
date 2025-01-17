use crate::MSchema;
use parquet::{
    basic::{LogicalType, Repetition, TimeUnit, Type as PhysicalType},
    data_type::Int32Type,
    format::{MicroSeconds, MilliSeconds},
    schema::types::Type,
};
use std::{
    collections::BTreeMap,
    sync::Arc,
};
use std::{fs, path::Path};
use tiberius::{QueryItem, QueryStream};
use tokio_stream::StreamExt;

use parquet::{file::writer::SerializedFileWriter, schema::parser::parse_message_type};

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

pub fn create_file_parquet() {
    let path = Path::new("sample.parquet");

    let message_type = "
        message schema {
            REQUIRED INT32 b;
        }
    ";

    let schema = Arc::new(parse_message_type(message_type).unwrap());
    let file = fs::File::create(&path).unwrap();
    let mut writer = SerializedFileWriter::new(file, schema, Default::default()).unwrap();

    let mut row_group_writer = writer.next_row_group().unwrap();
    while let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
        col_writer
            .typed::<Int32Type>()
            .write_batch(&[1, 2, 3, 4, 5], None, None)
            .unwrap();
        col_writer.close().unwrap()
    }

    row_group_writer.close().unwrap();
    writer.close().unwrap();
}

pub async fn write_parquet_from_stream(
    mut stream: QueryStream<'_>,
    schema: Arc<Type>,
    path: &str,
) -> anyhow::Result<()> {

    println!("{path} == {schema:?}");
    //let path_new = Path::new(path);
    //let file = fs::File::create(&path_new).unwrap();
    //let mut writer = SerializedFileWriter::new(file, schema, Default::default())?;
    //let mut row_group_writer = writer.next_row_group()?;

    // armazena os dados
    let mut datatable = BTreeMap::new();
    while let Some(row) = stream.try_next().await? {
        if let QueryItem::Row(r) = row {
            for (p, value) in r.into_iter().enumerate() {
                datatable.entry(p).or_insert_with(Vec::new).push(value);
            }
        };
    }

    //row_group_writer.close()?;
    //writer.close()?;
    for (k, valor) in datatable {
        println!("{k} == {valor:?}");
    }

    Ok(())
}

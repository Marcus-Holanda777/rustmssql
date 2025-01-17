use crate::MSchema;
use parquet::basic::Compression;
use parquet::file::{properties::WriterProperties, writer::SerializedFileWriter};
use parquet::{
    basic::{LogicalType, Repetition, TimeUnit, Type as PhysicalType},
    data_type::{ByteArray, ByteArrayType, Int32Type},
    format::{MicroSeconds, MilliSeconds},
    schema::types::Type,
};
use std::sync::Arc;
use std::{fs, path::Path};
use tiberius::{QueryItem, QueryStream};
use tokio_stream::StreamExt;

fn get_type(col: &str, types: PhysicalType, logical: Option<LogicalType>) -> Type {
    //! Retorna um tipo de dado para o parquet.

    Type::primitive_type_builder(&col, types)
        .with_logical_type(logical)
        .with_repetition(Repetition::REQUIRED)
        .build()
        .unwrap()
}

fn to_type_column(schema: &MSchema) -> Type {
    //! Converte um MSchema para um Type.
    //! Verifica o tipo de dado e retorna um Type.
    //! Se o tipo não for reconhecido, retorna um BYTE_ARRAY.

    let col = schema
        .column_name
        .as_ref()
        .unwrap()
        .trim()
        .to_lowercase()
        .split_whitespace()
        .map(|f| f.trim())
        .collect::<Vec<_>>()
        .join("_");

    // converter para o tipo Option<&str> e depos para &str
    let mut opt = schema.data_type.as_deref().unwrap();

    if let Some(indice) = opt.find("(") {
        opt = &opt[..indice];
    }

    let scale = schema.numeric_scale.unwrap_or(0) as i32;
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
            .with_repetition(Repetition::REQUIRED)
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

pub fn create_schema_parquet(sql_types: &Vec<MSchema>) -> Type {
    //! Cria um schema parquet a partir de um MSchema.
    //! Recebe um MSchema e retorna um Type.
    //! O Type é um schema parquet.

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

pub async fn write_parquet_from_stream(
    mut stream: QueryStream<'_>,
    schema: Arc<Type>,
    path: &str,
) -> anyhow::Result<()> {
    //! Escreve um arquivo parquet a partir de um QueryStream.
    //! Recebe um QueryStream, um Arc<Type> e um &str.
    //! O Arc<Type> é o schema parquet.
    //! O &str é o caminho do arquivo parquet.
    //! Retorna um Result<()>.

    let path_new = Path::new(path);
    let file = fs::File::create(&path_new).unwrap();

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build()
        .into();

    let mut writer = SerializedFileWriter::new(file, schema, props)?;

    let mut prme_cd_produtos = Vec::new();
    let mut nome_produto = Vec::new();

    // armazena os dados
    while let Some(row) = stream.try_next().await? {
        if let QueryItem::Row(r) = row {
            prme_cd_produtos.push(r.get::<i32, _>(0).unwrap_or(0));
            nome_produto.push(
                r.get::<&str, _>(1)
                    .map(|f| f.to_string())
                    .unwrap_or_default(),
            );
        }
    }

    // GRAVAR NO ARQUIVO PARQUET
    let mut row_group_writer = writer.next_row_group().unwrap();
    if let Some(mut col_writer) = row_group_writer.next_column()? {
        col_writer
            .typed::<Int32Type>()
            .write_batch(&prme_cd_produtos[..], None, None)?;
        col_writer.close()?;
    }

    if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
        let name_values: Vec<ByteArray> = nome_produto
            .clone()
            .into_iter()
            .map(|name| parquet::data_type::ByteArray::from(name.as_str()))
            .collect();

        col_writer
            .typed::<ByteArrayType>()
            .write_batch(&name_values[..], None, None)?;
        col_writer.close()?
    }

    row_group_writer.close()?;
    writer.close()?;

    Ok(())
}

use crate::MSchema;
use chrono::{NaiveDate, NaiveDateTime};
use parquet::basic::Compression;
use parquet::data_type::{
    BoolType, DoubleType, FixedLenByteArray, FixedLenByteArrayType, FloatType, Int64Type,
};
use parquet::file::{properties::WriterProperties, writer::SerializedFileWriter};
use parquet::format::NanoSeconds;
use parquet::{
    basic::{LogicalType, Repetition, TimeUnit, Type as PhysicalType},
    data_type::{ByteArray, ByteArrayType, Int32Type},
    format::{MicroSeconds, MilliSeconds},
    schema::types::Type,
};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::{fs, path::Path};
use tiberius::{ColumnData, QueryItem, QueryStream};
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

    // definir a precisao do tempo
    let datetime_precision = match schema.datetime_precision.unwrap_or(0) {
        0..=3 => TimeUnit::MILLIS(MilliSeconds {}),
        4..=6 => TimeUnit::MICROS(MicroSeconds {}),
        7.. => TimeUnit::NANOS(NanoSeconds {}),
    };

    let num_binary_digits = precision as f64 * 10f64.log2();
    // Plus one bit for the sign (+/-)
    let length_in_bits = num_binary_digits + 1.0;
    let length_in_bytes = (length_in_bits / 8.0).ceil() as usize;

    println!("{}, {}, {}, {}", opt, scale, precision, length_in_bytes);

    match opt {
        "int" | "smallint" | "tinyint" => get_type(&col, PhysicalType::INT32, None),
        "bigint" => get_type(&col, PhysicalType::INT64, None),
        "float" => get_type(&col, PhysicalType::DOUBLE, None),
        "real" => get_type(&col, PhysicalType::FLOAT, None),
        "decimal" | "numeric" => {
            Type::primitive_type_builder(&col, PhysicalType::FIXED_LEN_BYTE_ARRAY)
                .with_length(length_in_bytes.try_into().unwrap())
                .with_logical_type(Some(LogicalType::Decimal { scale, precision }))
                .with_precision(precision)
                .with_scale(scale)
                .with_repetition(Repetition::REQUIRED)
                .build()
                .unwrap()
        }
        "bit" => get_type(&col, PhysicalType::BOOLEAN, None),
        "char" | "varchar" | "text" | "nchar" | "nvarchar" | "ntext" => {
            get_type(&col, PhysicalType::BYTE_ARRAY, Some(LogicalType::String))
        }
        "datetime" | "datetime2" | "smalldatetime" => get_type(
            &col,
            PhysicalType::INT64,
            Some(LogicalType::Timestamp {
                is_adjusted_to_u_t_c: false,
                unit: datetime_precision,
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
    schema_sql: &Vec<MSchema>,
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
    let mut data: BTreeMap<usize, Vec<ColumnData>> = BTreeMap::new();

    // armazena os dados
    while let Some(row) = stream.try_next().await? {
        if let QueryItem::Row(r) = row {
            for (p, col_data) in r.into_iter().enumerate() {
                data.entry(p).or_insert_with(Vec::new).push(col_data);
            }
        }
    }

    // GRAVAR NO ARQUIVO PARQUET
    let mut row_group_writer = writer.next_row_group()?;

    let mut col_key: usize = 0;
    while let Some(mut col_write) = row_group_writer.next_column()? {
        let col_data = data.get(&col_key).unwrap();

        // para os tipos numeric e decimal
        let mssql = schema_sql.get(col_key).unwrap();
        let scale = mssql.numeric_scale.unwrap_or(0) as u32;
        let precision = mssql.numeric_precision.unwrap_or(0) as u32;

        let num_binary_digits = precision as f64 * 10f64.log2();
        let length_in_bits = num_binary_digits + 1.0;
        let length_in_bytes = (length_in_bits / 8.0).ceil() as usize;

        let zero_bytes = vec![0u8; length_in_bytes];

        // VERIFICAR O TIPO DE DADO -- POR COLUNA
        match &col_data[0] {
            ColumnData::I32(_) => {
                let lotes: Vec<i32> = col_data
                    .iter()
                    .map(|f| match f {
                        ColumnData::I32(v) => v.unwrap_or_default(),
                        _ => 0,
                    })
                    .collect();
                col_write
                    .typed::<Int32Type>()
                    .write_batch(&lotes[..], None, None)?;
            }
            ColumnData::String(_) => {
                let lotes: Vec<ByteArray> = col_data
                    .iter()
                    .map(|f| match f {
                        ColumnData::String(v) => ByteArray::from(
                            v.as_ref()
                                .map(|f| f.to_string())
                                .unwrap_or_default()
                                .as_str(),
                        ),
                        _ => ByteArray::from(""),
                    })
                    .collect();
                col_write
                    .typed::<ByteArrayType>()
                    .write_batch(&lotes[..], None, None)?;
            }
            ColumnData::U8(_) => {
                let lotes: Vec<i32> = col_data
                    .iter()
                    .map(|f| match f {
                        ColumnData::U8(v) => v.unwrap() as i32,
                        _ => 0,
                    })
                    .collect();
                col_write
                    .typed::<Int32Type>()
                    .write_batch(&lotes[..], None, None)?;
            }
            ColumnData::I16(_) => {
                let lotes: Vec<i32> = col_data
                    .iter()
                    .map(|f| match f {
                        ColumnData::I16(v) => v.unwrap_or_default() as i32,
                        _ => 0,
                    })
                    .collect();
                col_write
                    .typed::<Int32Type>()
                    .write_batch(&lotes[..], None, None)?;
            }
            ColumnData::I64(_) => {
                let lotes: Vec<i64> = col_data
                    .iter()
                    .map(|f| match f {
                        ColumnData::I64(v) => v.unwrap_or_default(),
                        _ => 0,
                    })
                    .collect();
                col_write
                    .typed::<Int64Type>()
                    .write_batch(&lotes[..], None, None)?;
            }
            ColumnData::F32(_) => {
                let lotes: Vec<f32> = col_data
                    .iter()
                    .map(|f| match f {
                        ColumnData::F32(v) => v.unwrap_or_default(),
                        _ => 0.0,
                    })
                    .collect();
                col_write
                    .typed::<FloatType>()
                    .write_batch(&lotes[..], None, None)?;
            }
            ColumnData::F64(_) => {
                let lotes: Vec<f64> = col_data
                    .iter()
                    .map(|f| match f {
                        ColumnData::F64(v) => v.unwrap_or_default(),
                        _ => 0.0,
                    })
                    .collect();
                col_write
                    .typed::<DoubleType>()
                    .write_batch(&lotes[..], None, None)?;
            }
            ColumnData::Numeric(_) => {
                let lotes: Vec<FixedLenByteArray> = col_data
                    .iter()
                    .map(|f| match f {
                        ColumnData::Numeric(Some(v)) => {
                            let bytes_array: String = v.to_string();
                            let bytes_decimal: Vec<u8> =
                                encode_decimal(&bytes_array, precision, scale, length_in_bytes);
                            FixedLenByteArray::from(ByteArray::from(bytes_decimal))
                        }
                        ColumnData::Numeric(None) => {
                            FixedLenByteArray::from(ByteArray::from(zero_bytes.clone()))
                        }
                        _ => FixedLenByteArray::from(ByteArray::from(zero_bytes.clone())),
                    })
                    .collect();
                col_write
                    .typed::<FixedLenByteArrayType>()
                    .write_batch(&lotes[..], None, None)?;
            }
            ColumnData::Bit(_) => {
                let lotes: Vec<bool> = col_data
                    .iter()
                    .map(|f| match f {
                        ColumnData::Bit(v) => v.unwrap_or_default(),
                        _ => false,
                    })
                    .collect();
                col_write
                    .typed::<BoolType>()
                    .write_batch(&lotes[..], None, None)?;
            }
            ColumnData::DateTime(_) => {
                let lotes: Vec<i64> = col_data
                    .iter()
                    .map(|f| match f {
                        ColumnData::DateTime(Some(dt)) => {
                            // Criar a data e hora a partir de `dt`
                            let datetime =
                                convert_to_naive_datetime(dt.days(), dt.seconds_fragments() as i32);
                            datetime.and_utc().timestamp_millis() // Retorna o timestamp diretamente como i64
                        }
                        ColumnData::DateTime(None) => {
                            // Valor padrão para datas nulas
                            let default_datetime = NaiveDateTime::new(
                                NaiveDate::from_ymd_opt(1900, 1, 1).unwrap(),
                                chrono::NaiveTime::from_hms_milli_opt(0, 0, 0, 0).unwrap(),
                            );
                            default_datetime.and_utc().timestamp_millis()
                        }
                        _ => {
                            // Caso o valor não seja `ColumnData::DateTime`
                            let default_datetime = NaiveDateTime::new(
                                NaiveDate::from_ymd_opt(1900, 1, 1).unwrap(),
                                chrono::NaiveTime::from_hms_milli_opt(0, 0, 0, 0).unwrap(),
                            );
                            default_datetime.and_utc().timestamp_millis()
                        }
                    })
                    .collect();
                col_write
                    .typed::<Int64Type>()
                    .write_batch(&lotes[..], None, None)?;
            }
            _ => unimplemented!(),
        };
        col_write.close()?;
        col_key += 1;
    }

    row_group_writer.close()?;
    writer.close()?;

    Ok(())
}

fn encode_decimal(value: &str, precision: u32, scale: u32, length_in_bytes: usize) -> Vec<u8> {
    // Converter a string para um número de ponto flutuante
    let float_value: f64 = value.parse().expect("Invalid decimal string");

    // Multiplicar pelo fator de escala (10^scale) usando i128 para evitar overflow
    let scale_factor = 10i128.pow(scale);
    let scaled_value = (float_value * scale_factor as f64).round() as i128;

    // Garantir que o valor escalado cabe dentro da precisão definida
    let max_value = 10i128.pow(precision) - 1;
    let min_value = -10i128.pow(precision);

    if scaled_value > max_value || scaled_value < min_value {
        panic!(
            "Valor escalado ({}) excede o intervalo permitido para a precisão {}",
            scaled_value, precision
        );
    }

    // Converter o valor escalado em um array de bytes no formato Big-Endian
    let mut bytes = vec![0u8; length_in_bytes];
    let scaled_bytes = &scaled_value.to_be_bytes();

    // Garantir que os índices são válidos
    let copy_start = if scaled_bytes.len() > length_in_bytes {
        scaled_bytes.len() - length_in_bytes
    } else {
        0
    };

    let copy_end = scaled_bytes.len();
    let dest_start = length_in_bytes.saturating_sub(scaled_bytes.len());

    bytes[dest_start..].copy_from_slice(&scaled_bytes[copy_start..copy_end]);

    bytes
}

fn convert_to_naive_datetime(days: i32, seconds_fragment: i32) -> NaiveDateTime {
    // Data base do SQL Server para DATETIME
    let base_date = NaiveDate::from_ymd_opt(1900, 1, 1).unwrap_or_default();

    // Adicionar os dias ao valor base
    let date = base_date + chrono::Duration::days(days.into());

    // Os "seconds_fragment" estão em 1/300 segundos. Precisamos convertê-los para segundos reais.
    let seconds = seconds_fragment as f64 / 300.0;

    // Separar a parte inteira (segundos completos) e a fração de segundo (nanossegundos)
    let whole_seconds = seconds.trunc() as i64;
    let fractional_nanoseconds = ((seconds - seconds.trunc()) * 1_000_000_000.0) as i64;

    // Adicionar os segundos e nanossegundos ao horário base (meia-noite)
    let datetime = date.and_hms_opt(0, 0, 0).unwrap_or_default()
        + chrono::Duration::seconds(whole_seconds)
        + chrono::Duration::nanoseconds(fractional_nanoseconds);

    datetime
}

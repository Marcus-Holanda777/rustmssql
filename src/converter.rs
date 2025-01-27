use crate::MSchema;
use anyhow::Ok;
use chrono::{NaiveDate, NaiveDateTime};
use parquet::data_type::{
    BoolType, ByteArray, ByteArrayType, DoubleType, FixedLenByteArray, FixedLenByteArrayType,
    FloatType, Int32Type, Int64Type,
};
use parquet::file::writer::SerializedColumnWriter;
use tiberius::ColumnData;

pub trait ColumnProcess<T> {
    fn process(&mut self) -> anyhow::Result<(), anyhow::Error> {
        Ok(())
    }
}

pub struct Converter<'a> {
    pub col_data: &'a Vec<ColumnData<'a>>,
    pub col_write: Option<SerializedColumnWriter<'a>>,
    pub mssql: Option<&'a MSchema>,
}

impl<'a> ColumnProcess<i32> for Converter<'a> {
    fn process(&mut self) -> anyhow::Result<(), anyhow::Error> {
        if let Some(mut col_write_t) = self.col_write.take() {
            let mut lotes: Vec<i32> = Vec::new();
            let mut levels: Vec<i16> = Vec::new();

            self.col_data.iter().for_each(|f| match f {
                ColumnData::I32(Some(valor)) => {
                    lotes.push(*valor as i32);
                    levels.push(1);
                }
                ColumnData::I32(None) => levels.push(0),
                ColumnData::U8(Some(valor)) => {
                    lotes.push(*valor as i32);
                    levels.push(1);
                }
                ColumnData::U8(None) => levels.push(0),
                ColumnData::I16(Some(valor)) => {
                    lotes.push(*valor as i32);
                    levels.push(1);
                }
                ColumnData::I16(None) => levels.push(0),
                ColumnData::Date(Some(dt)) => {
                    // Criar a data a partir de `dt`
                    let days = dt.days() as i32;
                    let base_date_parquet = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap_or_default();
                    let base_date_sql_server = NaiveDate::from_ymd_opt(1, 1, 1).unwrap_or_default();

                    let result_date = base_date_sql_server + chrono::Duration::days(days.into());
                    let duration = result_date
                        .signed_duration_since(base_date_parquet)
                        .num_days();

                    let row_add = duration.try_into().unwrap_or_default();
                    lotes.push(row_add);
                    levels.push(1);
                }
                ColumnData::Date(None) => levels.push(0),
                _ => levels.push(0),
            });

            col_write_t
                .typed::<Int32Type>()
                .write_batch(&lotes[..], Some(&levels[..]), None)?;

            col_write_t.close()?;
        }

        Ok(())
    }
}

impl<'a> ColumnProcess<f32> for Converter<'a> {
    fn process(&mut self) -> anyhow::Result<(), anyhow::Error> {
        if let Some(mut col_write_t) = self.col_write.take() {
            let mut lotes: Vec<f32> = Vec::new();
            let mut levels: Vec<i16> = Vec::new();

            self.col_data.iter().for_each(|f| match f {
                ColumnData::F32(Some(valor)) => {
                    lotes.push(*valor);
                    levels.push(1);
                }
                ColumnData::F32(None) => levels.push(0),
                _ => levels.push(0),
            });

            col_write_t
                .typed::<FloatType>()
                .write_batch(&lotes[..], Some(&levels[..]), None)?;

            col_write_t.close()?;
        }

        Ok(())
    }
}

impl<'a> ColumnProcess<f64> for Converter<'a> {
    fn process(&mut self) -> anyhow::Result<(), anyhow::Error> {
        if let Some(mut col_write_t) = self.col_write.take() {
            let mut lotes: Vec<f64> = Vec::new();
            let mut levels: Vec<i16> = Vec::new();

            self.col_data.iter().for_each(|f| match f {
                ColumnData::F64(Some(valor)) => {
                    lotes.push(*valor);
                    levels.push(1);
                }
                ColumnData::F64(None) => levels.push(0),
                _ => levels.push(0),
            });

            col_write_t
                .typed::<DoubleType>()
                .write_batch(&lotes[..], Some(&levels[..]), None)?;

            col_write_t.close()?;
        }

        Ok(())
    }
}

impl<'a> ColumnProcess<ByteArray> for Converter<'a> {
    fn process(&mut self) -> anyhow::Result<(), anyhow::Error> {
        if let Some(mut col_write_t) = self.col_write.take() {
            let mut lotes: Vec<ByteArray> = Vec::new();
            let mut levels: Vec<i16> = Vec::new();

            self.col_data.iter().for_each(|f| match f {
                ColumnData::String(Some(valor)) => {
                    lotes.push(ByteArray::from(valor.to_string().as_str()));
                    levels.push(1);
                }
                ColumnData::String(None) => levels.push(0),
                ColumnData::Xml(Some(valor)) => {
                    lotes.push(ByteArray::from(valor.to_string().as_str()));
                    levels.push(1);
                }
                ColumnData::Xml(None) => levels.push(0),
                _ => levels.push(0),
            });

            col_write_t.typed::<ByteArrayType>().write_batch(
                &lotes[..],
                Some(&levels[..]),
                None,
            )?;

            col_write_t.close()?;
        }

        Ok(())
    }
}

impl<'a> ColumnProcess<bool> for Converter<'a> {
    fn process(&mut self) -> anyhow::Result<(), anyhow::Error> {
        if let Some(mut col_write_t) = self.col_write.take() {
            let mut lotes: Vec<bool> = Vec::new();
            let mut levels: Vec<i16> = Vec::new();

            self.col_data.iter().for_each(|f| match f {
                ColumnData::Bit(Some(valor)) => {
                    lotes.push(*valor);
                    levels.push(1);
                }
                ColumnData::Bit(None) => levels.push(0),
                _ => levels.push(0),
            });

            col_write_t
                .typed::<BoolType>()
                .write_batch(&lotes[..], Some(&levels[..]), None)?;

            col_write_t.close()?;
        }

        Ok(())
    }
}

impl<'a> ColumnProcess<i64> for Converter<'a> {
    fn process(&mut self) -> anyhow::Result<(), anyhow::Error> {
        if let Some(mut col_write_t) = self.col_write.take() {
            let mut lotes: Vec<i64> = Vec::new();
            let mut levels: Vec<i16> = Vec::new();

            self.col_data.iter().for_each(|f| match f {
                ColumnData::I64(Some(valor)) => {
                    lotes.push(*valor);
                    levels.push(1);
                }
                ColumnData::I64(None) => levels.push(0),
                ColumnData::DateTime(Some(dt)) => {
                    // Criar a data e hora a partir de `dt`
                    let datetime =
                        convert_to_naive_datetime(dt.days(), dt.seconds_fragments() as i32);

                    let row_add = datetime.and_utc().timestamp_millis(); // Retorna o timestamp diretamente como i64
                    lotes.push(row_add);
                    levels.push(1);
                }
                ColumnData::DateTime(None) => levels.push(0),
                ColumnData::DateTime2(Some(dt)) => {
                    let days = dt.date().days();
                    let base_date_sql_server = NaiveDate::from_ymd_opt(1, 1, 1).unwrap_or_default();
                    let result_date = base_date_sql_server + chrono::Duration::days(days.into());

                    let increments = dt.time().increments() as i64;
                    let scale = dt.time().scale() as u32;

                    let nanos = increments * 10i64.pow(9 - scale);
                    let time_t = chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap_or_default()
                        + chrono::Duration::nanoseconds(nanos);

                    let datetime = NaiveDateTime::new(result_date, time_t);
                    let row_add = datetime.and_utc().timestamp_nanos_opt().unwrap_or_default();

                    lotes.push(row_add);
                    levels.push(1);
                }
                ColumnData::DateTime2(None) => levels.push(0),
                _ => levels.push(0),
            });

            col_write_t
                .typed::<Int64Type>()
                .write_batch(&lotes[..], Some(&levels[..]), None)?;

            col_write_t.close()?;
        }

        Ok(())
    }
}

impl<'a> ColumnProcess<FixedLenByteArray> for Converter<'a> {
    fn process(&mut self) -> anyhow::Result<(), anyhow::Error> {
        if let Some(mut col_write_t) = self.col_write.take() {
            let mut lotes: Vec<FixedLenByteArray> = Vec::new();
            let mut levels: Vec<i16> = Vec::new();

            let precision = self.mssql.as_ref().unwrap().numeric_precision.unwrap_or(0) as u32;
            let num_binary_digits = precision as f64 * 10f64.log2();
            let length_in_bits = num_binary_digits + 1.0;
            let length_in_bytes = (length_in_bits / 8.0).ceil() as usize;

            self.col_data.iter().for_each(|f| match f {
                ColumnData::Numeric(Some(v)) => {
                    let bytes_array = v.value();

                    let bytes_decimal: Vec<u8> =
                        encode_decimal(bytes_array, precision, length_in_bytes);

                    let row_add = FixedLenByteArray::from(ByteArray::from(bytes_decimal));
                    lotes.push(row_add);
                    levels.push(1);
                }
                ColumnData::Numeric(None) => {
                    levels.push(0);
                }
                _ => levels.push(0),
            });

            col_write_t.typed::<FixedLenByteArrayType>().write_batch(
                &lotes[..],
                Some(&levels[..]),
                None,
            )?;

            col_write_t.close()?;
        }

        Ok(())
    }
}

pub fn parse_rows<'a, T>(conv: Option<Converter<'a>>) -> anyhow::Result<(), anyhow::Error>
where
    Converter<'a>: ColumnProcess<T>,
{
    if let Some(mut conv_t) = conv {
        conv_t.process()?;
    }
    Ok(())
}

fn encode_decimal(scaled_value: i128, precision: u32, length_in_bytes: usize) -> Vec<u8> {
    // Converter a string para um número de ponto flutuante
    //!let float_value: f64 = value.parse().expect("Invalid decimal string");

    // Multiplicar pelo fator de escala (10^scale) usando i128 para evitar overflow
    //!let scale_factor = 10i128.pow(scale);
    //!let scaled_value = (float_value * scale_factor as f64).round() as i128;

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

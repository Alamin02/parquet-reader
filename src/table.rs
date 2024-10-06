use arrow_schema::{Field, Schema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::sync::Arc;

#[derive(Debug)]
pub enum DataVariant {
    String(String),
    Int(i32),
    Float(f64),
    Bool(bool),
}

impl ToString for DataVariant {
    fn to_string(&self) -> String {
        match self {
            DataVariant::String(s) => s.to_string(),
            DataVariant::Int(i) => i.to_string(),
            DataVariant::Float(f) => f.to_string(),
            DataVariant::Bool(b) => b.to_string(),
        }
    }
}

#[derive(Debug)]
pub struct DataTable {
    pub columns: Vec<Arc<Field>>,
    pub rows: Vec<Vec<DataVariant>>, // We only need string representation to view the data
}

impl DataTable {
    pub fn add_record_batch(self: &mut Self, record_batch: arrow::record_batch::RecordBatch) {
        let mut rows: Vec<Vec<DataVariant>> = vec![];

        for column in record_batch.columns() {
            match column.data_type() {
                // TODO: Reduce clutter by using a macro
                arrow_schema::DataType::Utf8 => {
                    let data: Vec<_> = column
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                        .unwrap()
                        .iter()
                        .collect();

                    for i in 0..data.len() {
                        if rows.len() <= i {
                            rows.push(vec![DataVariant::String(
                                data.get(i).unwrap().unwrap().to_string(),
                            )]);
                        } else {
                            rows.get_mut(i).unwrap().push(DataVariant::String(
                                data.get(i).unwrap().unwrap().to_string(),
                            ));
                        }
                    }
                }

                arrow_schema::DataType::Int32 => {
                    let data: Vec<_> = column
                        .as_any()
                        .downcast_ref::<arrow::array::Int32Array>()
                        .unwrap()
                        .iter()
                        .collect();

                    for i in 0..data.len() {
                        if rows.len() <= i {
                            rows.push(vec![DataVariant::Int(data.get(i).unwrap().unwrap())]);
                        } else {
                            rows.get_mut(i)
                                .unwrap()
                                .push(DataVariant::Int(data.get(i).unwrap().unwrap()));
                        }
                    }
                }

                arrow_schema::DataType::Float64 => {
                    let data: Vec<_> = column
                        .as_any()
                        .downcast_ref::<arrow::array::Float64Array>()
                        .unwrap()
                        .iter()
                        .collect();

                    for i in 0..data.len() {
                        if rows.len() <= i {
                            rows.push(vec![DataVariant::Float(data.get(i).unwrap().unwrap())]);
                        } else {
                            rows.get_mut(i)
                                .unwrap()
                                .push(DataVariant::Float(data.get(i).unwrap().unwrap()));
                        }
                    }
                }

                arrow_schema::DataType::Boolean => {
                    let data: Vec<_> = column
                        .as_any()
                        .downcast_ref::<arrow::array::BooleanArray>()
                        .unwrap()
                        .iter()
                        .collect();

                    for i in 0..data.len() {
                        if rows.len() <= i {
                            rows.push(vec![DataVariant::Bool(data.get(i).unwrap().unwrap())]);
                        } else {
                            rows.get_mut(i)
                                .unwrap()
                                .push(DataVariant::Bool(data.get(i).unwrap().unwrap()));
                        }
                    }
                }

                _ => {
                    println!("Unsupported data type: {:?}", column.data_type());
                }
            }
        }

        self.rows.extend(rows);
    }

    pub fn new(schema: &Arc<Schema>) -> DataTable {
        DataTable {
            columns: schema.fields().to_vec(),
            rows: vec![],
        }
    }

    pub fn from_parquet_file(file_path: &String) -> DataTable {
        let file = std::fs::File::open(file_path).unwrap();
        let reader_builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();

        let schema = reader_builder.schema();
        let mut table: DataTable = DataTable::new(&schema);

        let reader = reader_builder.with_batch_size(10).build();

        if let Ok(reader) = reader {
            for record_batch in reader {
                let record_batch = record_batch.unwrap();
                table.add_record_batch(record_batch);
            }
        }

        table
    }
}

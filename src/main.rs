use parquet_reader::ParquetReaderApp;

fn main() {
    let app_name = "Parquet Reader";
    let native_options = eframe::NativeOptions::default();

    let _ = eframe::run_native(
        app_name,
        native_options,
        Box::new(|_| Ok(Box::new(ParquetReaderApp::new()))),
    );
}

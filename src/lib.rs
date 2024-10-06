mod table;

use eframe::egui::{self};
use eframe::egui::{Context, ScrollArea};
use table::DataTable;

#[derive(Default)]
pub struct ParquetReaderApp {
    file_path: Option<String>,
    table: Option<table::DataTable>,
}

impl ParquetReaderApp {
    fn header(&mut self, ui: &mut eframe::egui::Ui) {
        let mut path: Option<String> = None;

        ui.visuals_mut().widgets.inactive.bg_fill = egui::Color32::BLACK; // Button background

        eframe::egui::containers::Sides::new().height(20.).show(
            ui,
            |ui| {
                ui.label(format!(
                    "File path: {}",
                    self.file_path
                        .as_ref()
                        .map(|s| s.as_str())
                        .unwrap_or("file not selected")
                ));
            },
            |ui| {
                if ui.button("Open file").clicked() {
                    if let Some(file_path) = rfd::FileDialog::new().pick_file() {
                        path = file_path.to_str().map(|s| s.to_string());
                    }
                };
            },
        );

        if let Some(path) = path {
            self.file_path = Some(path);
            self.table = Some(DataTable::from_parquet_file(
                self.file_path.as_ref().unwrap(),
            ));
        }
    }

    fn table(&mut self, ui: &mut eframe::egui::Ui) {
        if let Some(_) = &self.table {
            egui::Grid::new("parquet_data_table")
                .striped(true)
                .show(ui, |ui| {
                    // show table column names in first row
                    for column in &self.table.as_ref().unwrap().columns {
                        ui.label(
                            egui::RichText::new(column.name())
                                .strong()
                                .text_style(egui::TextStyle::Heading),
                        );
                    }
                    ui.end_row();

                    // show table data in subsequent rows
                    for row in &self.table.as_ref().unwrap().rows {
                        for data in row {
                            ui.label(data.to_string());
                        }
                        ui.end_row();
                    }
                });
        }
    }
}

impl eframe::App for ParquetReaderApp {
    fn update(&mut self, ctx: &Context, _frame: &mut eframe::Frame) {
        egui::TopBottomPanel::top("topbar").show(ctx, |ui| {
            self.header(ui);
        });

        egui::CentralPanel::default().show(ctx, |ui| {
            let available_width = ui.available_width();

            if let Some(_) = &self.file_path {
                ScrollArea::both().show(ui, |ui| {
                    ui.set_width(available_width);
                    self.table(ui);
                });
            }
        });
    }
}

impl ParquetReaderApp {
    pub fn new() -> Self {
        Self::default()
    }
}

// Copyright 2022 The Engula Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub mod store;
pub mod client;

use prometheus::{self, Encoder, TextEncoder};
use warp::Filter;

pub fn export_metrics() -> Vec<u8> {
    let mut buffer = vec![];
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    buffer
}

pub async fn run_metrics_service(port: u16) {
    let exporter = warp::path("metrics").map(|| export_metrics());
    warp::serve(exporter).run(([0, 0, 0, 0], port)).await;
}

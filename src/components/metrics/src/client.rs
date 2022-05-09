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

use lazy_static::lazy_static;
use prometheus::*;

lazy_static! {
    pub static ref CLIENT_STORE_RPC_LATENCY: HistogramVec = register_histogram_vec!(
        "client_store_rpc_latency_us",
        "The latency of RPC issued to store",
        &["op"],
    )
    .unwrap();
    pub static ref CLIENT_STORE_READ_RPC_LATENCY: Histogram = CLIENT_STORE_RPC_LATENCY
        .get_metric_with_label_values(&["read"])
        .unwrap();
    pub static ref CLIENT_STORE_WRITE_RPC_LATENCY: Histogram = CLIENT_STORE_RPC_LATENCY
        .get_metric_with_label_values(&["write"])
        .unwrap();
    pub static ref CLIENT_STORE_SEAL_RPC_LATENCY: Histogram = CLIENT_STORE_RPC_LATENCY
        .get_metric_with_label_values(&["seal"])
        .unwrap();
}

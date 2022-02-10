# Copyright 2022 The Engula Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/bin/bash
#
# install deps:
# 1. rustup component add llvm-tools-preview
# 2. cargo install cargo-binutils

mkdir -p target/cov/
rm -rf target/cov/*.profraw
filename="target/cov/sdcons"

RUSTFLAGS="-Z instrument-coverage" \
    LLVM_PROFILE_FILE="${filename}-%m.profraw" \
    cargo test --tests --verbose

rustc --version | grep nightly >/dev/null
if [[ $? == "0" ]]; then
    echo "try report coverage"
    cargo profdata -- merge \
        -sparse ${filename}-*.profraw \
        -o ${filename}.profdata
    cargo cov -- report \
        --use-color \
        --ignore-filename-regex='/rustc/' \
        --ignore-filename-regex='/.cargo/registry' \
        --ignore-filename-regex="target/debug/" \
        --ignore-filename-regex="target/release/" \
        $(
            for file in \
                $(
                    RUSTFLAGS="-Z instrument-coverage" \
                        cargo test --tests --no-run --message-format=json |
                        jq -r "select(.profile.test == true) | .filenames[]" |
                        grep -v dSYM -
                ); do
                printf "%s %s " -object $file
            done
        ) \
        --instr-profile=${filename}.profdata --summary-only
fi

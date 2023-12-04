#!/bin/bash

set -e

./scripts/download-pocket-ic.sh

./scripts/build-test-canister.sh

export POCKET_IC_BIN="$(pwd)/bin/pocket-ic"
export TEST_CANISTER_WASM_PATH="$(pwd)/bin/test_canister.wasm"

cd tests/ic-websocket-cdk-rs

cargo test --package ic-websocket-cdk --lib -- tests::integration_tests --test-threads 1

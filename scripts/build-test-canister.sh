#!/bin/bash

cd tests/test_canister

# install Motoko dependencies
mops install

dfx build --check

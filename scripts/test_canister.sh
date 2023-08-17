#!/bin/bash

set -e

cd tests

# install Motoko dependencies
mops install

# install Node.js dependencies
npm install

# prepare environment for integration tests
dfx start --clean --background

npm run deploy:tests

npm run generate

# get the canister id of the test canister and save it to .env
echo "CANISTER_ID_TEST_CANISTER='$(npx json -f .dfx/local/canister_ids.json test_canister.local)'" > .env

# run integration tests
npm run test:integration

dfx stop

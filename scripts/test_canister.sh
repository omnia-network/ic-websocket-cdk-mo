#!/bin/bash

set -e

cd tests

npm install

# integration tests
dfx start --clean --background

npm run deploy:tests

npm run generate

npm run test:integration

dfx stop

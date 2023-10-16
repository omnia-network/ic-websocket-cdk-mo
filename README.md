# ic-websocket-cdk-mo


This repository contains the Motoko implementation of IC WebSocket CDK. For more information about IC WebSockets, see [IC WebSocket Gateway](https://github.com/omnia-network/ic-websocket-gateway).

> ⚠️ This library is still in development and is not ready for production use. Expect breaking changes.

## Installation

You can install the library using [mops](https://mops.one) following these steps:

- first, install the dependencies (this step won't be necessary when the **ic-websocket-cdk** will be available on [mops](https://mops.one)):
  ```bash
  mops add base@0.9.1
  mops add ic-certification@0.1.1
  mops add sha2@0.0.2
  mops add cbor@0.1.3
  ```
- install the **ic-websocket-cdk**:
  ```bash
  mops add https://github.com/omnia-network/ic-websocket-cdk-mo#<last-commit-hash-on-this-repo>
  ```

For example, a valid installation script is:

```bash
mops add base@0.9.1
mops add ic-certification@0.1.1
mops add sha2@0.0.2
mops add cbor@0.1.3
mops add candid = "1.0.2"
mops add https://github.com/omnia-network/ic-websocket-cdk-mo#a229d4a6d95987b4b1d46e9c5e7fde6d896fa118
```

The **ic-websocket-cdk** package will be available on **mops** soon.

## Usage

TODO: Add usage instructions

### Candid interface
In order for the frontend clients and the Gateway to work properly, the canister must expose some specific methods in its Candid interface, between the custom methods that you've implemented for your logic. A valid Candid interface for the canister is the following:

```
import "./ws_types.did";

service : {
  "ws_register" : (CanisterWsRegisterArguments) -> (CanisterWsRegisterResult);
  "ws_open" : (CanisterWsOpenArguments) -> (CanisterWsOpenResult);
  "ws_close" : (CanisterWsCloseArguments) -> (CanisterWsCloseResult);
  "ws_message" : (CanisterWsMessageArguments) -> (CanisterWsMessageResult);
  "ws_get_messages" : (CanisterWsGetMessagesArguments) -> (CanisterWsGetMessagesResult) query;
};
```
This snipped is copied from the [service.example.did](./did/service.example.did) file and the types imported are defined in the [ws_types.did](./did/ws_types.did) file.

**Note**: `dfx` should already generate the Candid interface for you, so you don't need to write it yourself.

## Development

The **ic-websocket-cdk** library implementation can be found in the [src](./src/) folder.

### Testing

There are integration tests available: for these tests a local IC replica is set up and the CDK is deployed to a [test canister](./tests/src/test_canister/main.mo). Tests are written in Node.js and are available in the [tests](./tests/integration/) folder.

There's a script that runs the integration tests, taking care of installing the Node.js dependencies, setting up the replica and deploying the canister. To run the script, execute the following command:

```bash
./scripts/test_canister.sh
```

## License

TODO: Add a license

## Contributing

Feel free to open issues and pull requests.

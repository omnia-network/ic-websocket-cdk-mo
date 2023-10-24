# ic-websocket-cdk-mo

[![mops](https://oknww-riaaa-aaaam-qaf6a-cai.raw.ic0.app/badge/mops/ic-websocket-cdk)](https://mops.one/ic-websocket-cdk)

This repository contains the Motoko implementation of IC WebSocket CDK. For more information about IC WebSockets, see [IC WebSocket Gateway](https://github.com/omnia-network/ic-websocket-gateway).

## Installation

You can install the library using [mops](https://mops.one):

```bash
mops add ic-websocket-cdk
```

## Usage

Refer to the [ic-websockets-pingpong-mo](https://github.com/iamenochchirima/ic-websockets-pingpong-mo) repository for an example of how to use this library.

### Candid interface
In order for the frontend clients and the Gateway to work properly, the canister must expose some specific methods in its Candid interface, between the custom methods that you've implemented for your logic. A valid Candid interface for the canister is the following:

```
import "./ws_types.did";

// define here your message type
type MyMessageType = {
  some_field : text;
};

service : {
  "ws_open" : (CanisterWsOpenArguments) -> (CanisterWsOpenResult);
  "ws_close" : (CanisterWsCloseArguments) -> (CanisterWsCloseResult);
  "ws_message" : (CanisterWsMessageArguments, opt MyMessageType) -> (CanisterWsMessageResult);
  "ws_get_messages" : (CanisterWsGetMessagesArguments) -> (CanisterWsGetMessagesResult) query;
};
```
This snipped is copied from the [service.example.did](./did/service.example.did) file and the types imported are defined in the [ws_types.did](./did/ws_types.did) file.

To define your message type, you can use the [Candid reference docs](https://internetcomputer.org/docs/current/references/candid-ref). We suggest you to define your message type using a [variant](https://internetcomputer.org/docs/current/references/candid-ref#type-variant--n--t--), so that you can support different messages over the same websocket instance and make it safe for future updates.

**Note**: `dfx` should already generate the Candid interface for you, so you don't need to write any `.did` file yourself.

## Development

The **ic-websocket-cdk** library implementation can be found in the [src](./src/) folder.

### Testing

There are integration tests available: for these tests a local IC replica is set up and the CDK is deployed to a [test canister](./tests/src/test_canister/main.mo). Tests are written in Node.js and are available in the [tests](./tests/integration/) folder.

There's a script that runs the integration tests, taking care of installing the Node.js dependencies, setting up the replica and deploying the canister. To run the script, execute the following command:

```bash
./scripts/test_canister.sh
```

## License

MIT License. See [LICENSE](./LICENSE).

## Contributing

Feel free to open issues and pull requests.

import Debug "mo:base/Debug";
import IcWebSocketCdk "mo:ic-websocket-cdk";

actor class TestCanister(gateway_principal : Text) {

  let ws_state = IcWebSocketCdk.IcWebSocketState(gateway_principal);

  func on_open(args : IcWebSocketCdk.OnOpenCallbackArgs) : async () {
    Debug.print("Opened websocket: " # debug_show (args.client_principal));
  };

  func on_message(args : IcWebSocketCdk.OnMessageCallbackArgs) : async () {
    Debug.print("Received message: " # debug_show (args.client_principal));
  };

  func on_close(args : IcWebSocketCdk.OnCloseCallbackArgs) : async () {
    Debug.print("Client " # debug_show (args.client_principal) # " disconnected");
  };

  let handlers = IcWebSocketCdk.WsHandlers(
    ?on_open,
    ?on_message,
    ?on_close,
  );

  let params = IcWebSocketCdk.WsInitParams(
    ws_state,
    handlers,
  );

  let ws = IcWebSocketCdk.IcWebSocket(params);

  // method called by the WS Gateway after receiving FirstMessage from the client
  public shared ({ caller }) func ws_open(args : IcWebSocketCdk.CanisterWsOpenArguments) : async IcWebSocketCdk.CanisterWsOpenResult {
    await ws.ws_open(caller, args);
  };

  // method called by the Ws Gateway when closing the IcWebSocket connection
  public shared ({ caller }) func ws_close(args : IcWebSocketCdk.CanisterWsCloseArguments) : async IcWebSocketCdk.CanisterWsCloseResult {
    await ws.ws_close(caller, args);
  };

  // method called by the WS Gateway to send a message of type GatewayMessage to the canister
  public shared ({ caller }) func ws_message(args : IcWebSocketCdk.CanisterWsMessageArguments) : async IcWebSocketCdk.CanisterWsMessageResult {
    await ws.ws_message(caller, args);
  };

  // method called by the WS Gateway to get messages for all the clients it serves
  public shared query ({ caller }) func ws_get_messages(args : IcWebSocketCdk.CanisterWsGetMessagesArguments) : async IcWebSocketCdk.CanisterWsGetMessagesResult {
    ws.ws_get_messages(caller, args);
  };

  //// Debug/tests methods
  // wipe all websocket data in the canister
  public shared func ws_wipe() : async () {
    await ws.wipe();
  };

  // send a message to the client, usually called by the canister itself
  public shared func ws_send(client_principal : IcWebSocketCdk.ClientPrincipal, msg_bytes : Blob) : async IcWebSocketCdk.CanisterWsSendResult {
    await IcWebSocketCdk.ws_send(ws_state, client_principal, msg_bytes);
    // or, same would be:
    // await ws.ws_send(client_key, msg_bytes);
  };
};

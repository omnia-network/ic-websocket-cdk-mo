import Debug "mo:base/Debug";
import IcWebSocketCdk "mo:ic-websocket-cdk";

actor class TestCanister(gateway_principal : Text) {
  func on_open(args : IcWebSocketCdk.OnOpenCallbackArgs) : async () {
    Debug.print("Opened websocket: " # debug_show (args.client_key));
  };

  func on_message(args : IcWebSocketCdk.OnMessageCallbackArgs) : async () {
    Debug.print("Received message: " # debug_show (args.client_key));
  };

  func on_close(args : IcWebSocketCdk.OnCloseCallbackArgs) : async () {
    Debug.print("Client " # debug_show (args.client_key) # " disconnected");
  };

  let handlers = IcWebSocketCdk.WsHandlers(
    ?on_open,
    ?on_message,
    ?on_close,
  );

  var ws = IcWebSocketCdk.IcWebSocket(handlers, gateway_principal);

  system func postupgrade() {
    ws := IcWebSocketCdk.IcWebSocket(handlers, gateway_principal);
  };

  // method called by the client SDK when instantiating a new IcWebSocket
  public shared ({ caller }) func ws_register(args : IcWebSocketCdk.CanisterWsRegisterArguments) : async IcWebSocketCdk.CanisterWsRegisterResult {
    await ws.ws_register(caller, args);
  };

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
  public shared func ws_send(client_key : IcWebSocketCdk.ClientPublicKey, msg_bytes : Blob) : async IcWebSocketCdk.CanisterWsSendResult {
    await ws.ws_send(client_key, msg_bytes);
  };
};

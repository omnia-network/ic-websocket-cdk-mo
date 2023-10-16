import Array "mo:base/Array";
import Debug "mo:base/Debug";
import IcWebSocketCdk "mo:ic-websocket-cdk";

actor class TestCanister(
  init_gateway_principal : Text,
  init_max_number_of_returned_messages : Nat,
  init_send_ack_interval_ms : Nat64,
  init_keep_alive_timeout_ms : Nat64,
) {

  var ws_state = IcWebSocketCdk.IcWebSocketState(init_gateway_principal);

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
    handlers,
    ?init_max_number_of_returned_messages,
    ?init_send_ack_interval_ms,
    ?init_keep_alive_timeout_ms,
  );

  var ws = IcWebSocketCdk.IcWebSocket(ws_state, params);

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
  public shared func ws_send(client_principal : IcWebSocketCdk.ClientPrincipal, messages : [Blob]) : async IcWebSocketCdk.CanisterWsSendResult {
    var res : IcWebSocketCdk.CanisterWsSendResult = #Ok;

    label f for (msg_bytes in Array.vals(messages)) {
      switch (await IcWebSocketCdk.ws_send(ws_state, client_principal, msg_bytes)) {
        case (#Ok(value)) {
          // Do nothing
        };
        case (#Err(error)) {
          res := #Err(error);
          break f;
        };
      };
    };

    return res;
  };

  // initialize the CDK again
  public shared func initialize(
    gateway_principal : Text,
    max_number_of_returned_messages : Nat,
    send_ack_interval_ms : Nat64,
    keep_alive_timeout_ms : Nat64,
  ) : () {
    ws_state := IcWebSocketCdk.IcWebSocketState(gateway_principal);

    let params = IcWebSocketCdk.WsInitParams(
      handlers,
      ?max_number_of_returned_messages,
      ?send_ack_interval_ms,
      ?keep_alive_timeout_ms,
    );

    ws := IcWebSocketCdk.IcWebSocket(ws_state, params);
  };
};

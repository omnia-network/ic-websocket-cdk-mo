import Array "mo:base/Array";
import Debug "mo:base/Debug";
import Nat64 "mo:base/Nat64";
import IcWebSocketCdk "mo:ic-websocket-cdk";
import IcWebSocketCdkTypes "mo:ic-websocket-cdk/Types";
import IcWebSocketCdkState "mo:ic-websocket-cdk/State";

actor class TestCanister(
  init_max_number_of_returned_messages : Nat64,
  init_send_ack_interval_ms : Nat64,
  init_keep_alive_timeout_ms : Nat64,
) {

  type AppMessage = {
    text : Text;
  };

  let params = IcWebSocketCdkTypes.WsInitParams(
    ?Nat64.toNat(init_max_number_of_returned_messages),
    ?init_send_ack_interval_ms,
    ?init_keep_alive_timeout_ms,
  );

  var ws_state = IcWebSocketCdkState.IcWebSocketState(params);

  func on_open(args : IcWebSocketCdk.OnOpenCallbackArgs) : async () {
    Debug.print("Opened websocket: " # debug_show (args.client_principal));
  };

  func on_message(args : IcWebSocketCdk.OnMessageCallbackArgs) : async () {
    Debug.print("Received message: " # debug_show (args.client_principal));
  };

  func on_close(args : IcWebSocketCdk.OnCloseCallbackArgs) : async () {
    Debug.print("Client " # debug_show (args.client_principal) # " disconnected");
  };

  let handlers = IcWebSocketCdkTypes.WsHandlers(
    ?on_open,
    ?on_message,
    ?on_close,
  );

  var ws = IcWebSocketCdk.IcWebSocket(ws_state, params, handlers);

  // method called by the WS Gateway after receiving FirstMessage from the client
  public shared ({ caller }) func ws_open(args : IcWebSocketCdk.CanisterWsOpenArguments) : async IcWebSocketCdk.CanisterWsOpenResult {
    await ws.ws_open(caller, args);
  };

  // method called by the Ws Gateway when closing the IcWebSocket connection
  public shared ({ caller }) func ws_close(args : IcWebSocketCdk.CanisterWsCloseArguments) : async IcWebSocketCdk.CanisterWsCloseResult {
    await ws.ws_close(caller, args);
  };

  // method called by the WS Gateway to send a message of type GatewayMessage to the canister
  public shared ({ caller }) func ws_message(args : IcWebSocketCdk.CanisterWsMessageArguments, msg_type : ?AppMessage) : async IcWebSocketCdk.CanisterWsMessageResult {
    await ws.ws_message(caller, args, msg_type);
  };

  // method called by the WS Gateway to get messages for all the clients it serves
  public shared query ({ caller }) func ws_get_messages(args : IcWebSocketCdk.CanisterWsGetMessagesArguments) : async IcWebSocketCdk.CanisterWsGetMessagesResult {
    ws.ws_get_messages(caller, args);
  };

  //// Debug/tests methods
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
};

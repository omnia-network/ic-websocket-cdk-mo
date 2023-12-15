import Hash "mo:base/Hash";
import Principal "mo:base/Principal";
import Text "mo:base/Text";
import Nat64 "mo:base/Nat64";
import Bool "mo:base/Bool";
import List "mo:base/List";
import Blob "mo:base/Blob";
import Array "mo:base/Array";
import Prelude "mo:base/Prelude";
import Iter "mo:base/Iter";
import Error "mo:base/Error";
import CborDecoder "mo:cbor/Decoder";
import CborEncoder "mo:cbor/Encoder";
import CborValue "mo:cbor/Value";

import Constants "Constants";
import Utils "Utils";

module {
  /// Just to be compatible with the Rust version.
  public type Result<Ok, Err> = { #Ok : Ok; #Err : Err };

  public type ClientPrincipal = Principal;

  public type ClientKey = {
    client_principal : ClientPrincipal;
    client_nonce : Nat64;
  };
  // functions needed for ClientKey
  public func areClientKeysEqual(k1 : ClientKey, k2 : ClientKey) : Bool {
    Principal.equal(k1.client_principal, k2.client_principal) and Nat64.equal(k1.client_nonce, k2.client_nonce);
  };
  public func clientKeyToText(k : ClientKey) : Text {
    Principal.toText(k.client_principal) # "_" # Nat64.toText(k.client_nonce);
  };
  public func hashClientKey(k : ClientKey) : Hash.Hash {
    Text.hash(clientKeyToText(k));
  };

  /// The result of [ws_open].
  public type CanisterWsOpenResult = Result<(), Text>;
  /// The result of [ws_close].
  public type CanisterWsCloseResult = Result<(), Text>;
  // The result of [ws_message].
  public type CanisterWsMessageResult = Result<(), Text>;
  /// The result of [ws_get_messages].
  public type CanisterWsGetMessagesResult = Result<CanisterOutputCertifiedMessages, Text>;
  /// The result of [send].
  public type CanisterSendResult = Result<(), Text>;
  /// @deprecated Use [`CanisterSendResult`] instead.
  public type CanisterWsSendResult = Result<(), Text>;
  /// The result of [close].
  public type CanisterCloseResult = Result<(), Text>;

  /// The arguments for [ws_open].
  public type CanisterWsOpenArguments = {
    client_nonce : Nat64;
    gateway_principal : GatewayPrincipal;
  };

  /// The arguments for [ws_close].
  public type CanisterWsCloseArguments = {
    client_key : ClientKey;
  };

  /// The arguments for [ws_message].
  public type CanisterWsMessageArguments = {
    msg : WebsocketMessage;
  };

  /// The arguments for [ws_get_messages].
  public type CanisterWsGetMessagesArguments = {
    nonce : Nat64;
  };

  /// Messages exchanged through the WebSocket.
  public type WebsocketMessage = {
    client_key : ClientKey; // The client that the gateway will forward the message to or that sent the message.
    sequence_num : Nat64; // Both ways, messages should arrive with sequence numbers 0, 1, 2...
    timestamp : Nat64; // Timestamp of when the message was made for the recipient to inspect.
    is_service_message : Bool; // Whether the message is a service message sent by the CDK to the client or vice versa.
    content : Blob; // Application message encoded in binary.
  };
  /// Encodes the `WebsocketMessage` into a CBOR blob.
  public func encode_websocket_message(websocket_message : WebsocketMessage) : Result<Blob, Text> {
    let principal_blob = Blob.toArray(Principal.toBlob(websocket_message.client_key.client_principal));
    let cbor_value : CborValue.Value = #majorType5([
      (#majorType3("client_key"), #majorType5([(#majorType3("client_principal"), #majorType2(principal_blob)), (#majorType3("client_nonce"), #majorType0(websocket_message.client_key.client_nonce))])),
      (#majorType3("sequence_num"), #majorType0(websocket_message.sequence_num)),
      (#majorType3("timestamp"), #majorType0(websocket_message.timestamp)),
      (#majorType3("is_service_message"), #majorType7(#bool(websocket_message.is_service_message))),
      (#majorType3("content"), #majorType2(Blob.toArray(websocket_message.content))),
    ]);

    switch (CborEncoder.encode(cbor_value)) {
      case (#err(#invalidValue(err))) {
        return #Err(err);
      };
      case (#ok(data)) {
        #Ok(Blob.fromArray(data));
      };
    };
  };

  /// Decodes the CBOR blob into a `WebsocketMessage`.
  func decode_websocket_message(bytes : Blob) : Result<WebsocketMessage, Text> {
    switch (CborDecoder.decode(bytes)) {
      case (#err(err)) {
        #Err("deserialization failed");
      };
      case (#ok(c)) {
        switch (c) {
          case (#majorType6({ tag; value })) {
            switch (value) {
              case (#majorType5(raw_content)) {
                #Ok({
                  client_key = do {
                    let client_key_key_value = Array.find(raw_content, func((key, _) : (CborValue.Value, CborValue.Value)) : Bool = key == #majorType3("client_key"));
                    switch (client_key_key_value) {
                      case (?(_, #majorType5(raw_client_key))) {
                        let client_principal_value = Array.find(raw_client_key, func((key, _) : (CborValue.Value, CborValue.Value)) : Bool = key == #majorType3("client_principal"));
                        let client_principal = switch (client_principal_value) {
                          case (?(_, #majorType2(client_principal_blob))) {
                            Principal.fromBlob(
                              Blob.fromArray(client_principal_blob)
                            );
                          };
                          case (_) {
                            return #Err("missing field `client_key.client_principal`");
                          };
                        };
                        let client_nonce_value = Array.find(raw_client_key, func((key, _) : (CborValue.Value, CborValue.Value)) : Bool = key == #majorType3("client_nonce"));
                        let client_nonce = switch (client_nonce_value) {
                          case (?(_, #majorType0(client_nonce))) {
                            client_nonce;
                          };
                          case (_) {
                            return #Err("missing field `client_key.client_nonce`");
                          };
                        };

                        {
                          client_principal;
                          client_nonce;
                        };
                      };
                      case (_) {
                        return #Err("missing field `client_key`");
                      };
                    };
                  };
                  sequence_num = do {
                    let sequence_num_key_value = Array.find(raw_content, func((key, _) : (CborValue.Value, CborValue.Value)) : Bool = key == #majorType3("sequence_num"));
                    switch (sequence_num_key_value) {
                      case (?(_, #majorType0(sequence_num))) {
                        sequence_num;
                      };
                      case (_) {
                        return #Err("missing field `sequence_num`");
                      };
                    };
                  };
                  timestamp = do {
                    let timestamp_key_value = Array.find(raw_content, func((key, _) : (CborValue.Value, CborValue.Value)) : Bool = key == #majorType3("timestamp"));
                    switch (timestamp_key_value) {
                      case (?(_, #majorType0(timestamp))) {
                        timestamp;
                      };
                      case (_) {
                        return #Err("missing field `timestamp`");
                      };
                    };
                  };
                  is_service_message = do {
                    let is_service_message_key_value = Array.find(raw_content, func((key, _) : (CborValue.Value, CborValue.Value)) : Bool = key == #majorType3("is_service_message"));
                    switch (is_service_message_key_value) {
                      case (?(_, #majorType7(#bool(is_service_message)))) {
                        is_service_message;
                      };
                      case (_) {
                        return #Err("missing field `is_service_message`");
                      };
                    };
                  };
                  content = do {
                    let content_key_value = Array.find(raw_content, func((key, _) : (CborValue.Value, CborValue.Value)) : Bool = key == #majorType3("message"));
                    switch (content_key_value) {
                      case (?(_, #majorType2(content_blob))) {
                        Blob.fromArray(content_blob);
                      };
                      case (_) {
                        return #Err("missing field `content`");
                      };
                    };
                  };
                });
              };
              case (_) {
                #Err("invalid CBOR message content");
              };
            };
          };
          case (_) {
            #Err("invalid CBOR message content");
          };
        };
      };
    };
  };

  // Element of the list of messages returned to the WS Gateway after polling.
  public type CanisterOutputMessage = {
    client_key : ClientKey; // The client that the gateway will forward the message to.
    key : Text; // Key for certificate verification.
    content : Blob; // The message to be relayed, that contains the application message.
  };

  /// List of messages returned to the WS Gateway after polling.
  public type CanisterOutputCertifiedMessages = {
    messages : [CanisterOutputMessage]; // List of messages.
    cert : Blob; // cert+tree constitute the certificate for all returned messages.
    tree : Blob; // cert+tree constitute the certificate for all returned messages.
    is_end_of_queue : Bool; // Whether the end of the queue has been reached.
  };

  public type MessagesForGatewayRange = {
    start_index : Nat;
    end_index : Nat;
    is_end_of_queue : Bool;
  };

  public type TimestampNs = Nat64;

  type MessageToDelete = {
    timestamp : TimestampNs;
  };

  public type GatewayPrincipal = Principal;

  /// Contains data about the registered WS Gateway.
  public class RegisteredGateway() {
    /// The queue of the messages that the gateway can poll.
    public var messages_queue : List.List<CanisterOutputMessage> = List.nil();
    /// The queue of messages' keys to delete.
    public var messages_to_delete : List.List<MessageToDelete> = List.nil();
    /// Keeps track of the nonce which:
    /// - the WS Gateway uses to specify the first index of the certified messages to be returned when polling
    /// - the client uses as part of the path in the Merkle tree in order to verify the certificate of the messages relayed by the WS Gateway
    public var outgoing_message_nonce : Nat64 = Constants.INITIAL_OUTGOING_MESSAGE_NONCE;
    /// The number of clients connected to this gateway.
    public var connected_clients_count : Nat64 = 0;

    /// Increments the outgoing message nonce by 1.
    public func increment_nonce() {
      outgoing_message_nonce += 1;
    };

    /// Increments the connected clients count by 1.
    public func increment_clients_count() {
      connected_clients_count += 1;
    };

    /// Decrements the connected clients count by 1, returning the new value.
    public func decrement_clients_count() : Nat64 {
      if (connected_clients_count > 0) {
        connected_clients_count -= 1;
      };
      connected_clients_count;
    };

    /// Adds the message to the queue and its metadata to the `messages_to_delete` queue.
    public func add_message_to_queue(message : CanisterOutputMessage, message_timestamp : TimestampNs) {
      messages_queue := List.append(
        messages_queue,
        List.fromArray([message]),
      );
      messages_to_delete := List.append(
        messages_to_delete,
        List.fromArray([{
          timestamp = message_timestamp;
        }]),
      );
    };

    /// Deletes the oldest `n` messages that are older than `message_max_age_ms` from the queue.
    ///
    /// Returns the deleted messages keys.
    public func delete_old_messages(n : Nat, message_max_age_ms : Nat64) : List.List<Text> {
      let time = Utils.get_current_time();
      var deleted_keys : List.List<Text> = List.nil();

      label f for (_ in Iter.range(0, n - 1)) {
        switch (List.get(messages_to_delete, 0)) {
          case (?message_to_delete) {
            if ((time - message_to_delete.timestamp) > (message_max_age_ms * 1_000_000)) {
              let deleted_message = do {
                let (m, l) = List.pop(messages_queue);
                messages_queue := l;
                m;
              };
              switch (deleted_message) {
                case (?deleted_message) {
                  deleted_keys := List.append(
                    deleted_keys,
                    List.fromArray([deleted_message.key]),
                  );
                };
                case (null) {
                  // there is no case in which the messages_to_delete queue is populated
                  // while the messages_queue is empty
                  Prelude.unreachable();
                };
              };
              let (_, l) = List.pop(messages_to_delete);
              messages_to_delete := l;
            } else {
              // In this case, no messages can be deleted because
              // they're all not older than `message_max_age_ms`.
              break f;
            };
          };
          case (null) {
            // There are no messages in the queue. Shouldn't happen.
            break f;
          };
        };
      };

      deleted_keys;
    };
  };

  /// The metadata about a registered client.
  public class RegisteredClient(gw_principal : GatewayPrincipal) {
    public var last_keep_alive_timestamp : TimestampNs = Utils.get_current_time();
    public let gateway_principal : GatewayPrincipal = gw_principal;

    /// Gets the last keep alive timestamp.
    public func get_last_keep_alive_timestamp() : TimestampNs {
      last_keep_alive_timestamp;
    };

    /// Set the last keep alive timestamp to the current time.
    public func update_last_keep_alive_timestamp() {
      last_keep_alive_timestamp := Utils.get_current_time();
    };
  };

  public type CanisterOpenMessageContent = {
    client_key : ClientKey;
  };

  public type CanisterAckMessageContent = {
    last_incoming_sequence_num : Nat64;
  };

  public type ClientKeepAliveMessageContent = {
    last_incoming_sequence_num : Nat64;
  };

  public type CloseMessageReason = {
    /// When the canister receives a wrong sequence number from the client.
    #WrongSequenceNumber;
    /// When the canister receives an invalid service message from the client.
    #InvalidServiceMessage;
    /// When the canister doesn't receive the keep alive message from the client in time.
    #KeepAliveTimeout;
    /// When the developer calls the `close` function.
    #ClosedByApplication;
  };

  public type CanisterCloseMessageContent = {
    reason : CloseMessageReason;
  };

  /// A service message sent by the CDK to the client or vice versa.
  public type WebsocketServiceMessageContent = {
    /// Message sent by the **canister** when a client opens a connection.
    #OpenMessage : CanisterOpenMessageContent;
    /// Message sent _periodically_ by the **canister** to the client to acknowledge the messages received.
    #AckMessage : CanisterAckMessageContent;
    /// Message sent by the **client** in response to an acknowledgement message from the canister.
    #KeepAliveMessage : ClientKeepAliveMessageContent;
    /// Message sent by the **canister** when it wants to close the connection.
    #CloseMessage : CanisterCloseMessageContent;
  };
  public func encode_websocket_service_message_content(content : WebsocketServiceMessageContent) : Blob {
    to_candid (content);
  };
  public func decode_websocket_service_message_content(bytes : Blob) : Result<WebsocketServiceMessageContent, Text> {
    let decoded : ?WebsocketServiceMessageContent = from_candid (bytes); // traps if the bytes are not a valid candid message
    return switch (decoded) {
      case (?value) { #Ok(value) };
      case (null) { #Err("Error decoding service message content: unknown") };
    };
  };

  /// Arguments passed to the `on_open` handler.
  public type OnOpenCallbackArgs = {
    client_principal : ClientPrincipal;
  };
  /// Handler initialized by the canister and triggered by the CDK once the IC WebSocket connection
  /// is established.
  public type OnOpenCallback = (OnOpenCallbackArgs) -> async ();

  /// Arguments passed to the `on_message` handler.
  /// The `message` argument is the message received from the client, serialized in Candid.
  /// Use [`from_candid`] to deserialize the message.
  ///
  /// # Example
  /// This example is the deserialize equivalent of the [`send`]'s serialize one.
  /// ```motoko
  /// import IcWebSocketCdk "mo:ic-websocket-cdk";
  ///
  /// actor MyCanister {
  ///   // ...
  ///
  ///   type MyMessage = {
  ///     some_field: Text;
  ///   };
  ///
  ///   // initialize the CDK
  ///
  ///   func on_message(args : IcWebSocketCdk.OnMessageCallbackArgs) : async () {
  ///     let received_message: ?MyMessage = from_candid(args.message);
  ///     switch (received_message) {
  ///       case (?received_message) {
  ///         Debug.print("Received message: some_field: " # received_message.some_field);
  ///       };
  ///       case (invalid_arg) {
  ///         return #Err("invalid argument: " # debug_show (invalid_arg));
  ///       };
  ///     };
  ///   };
  ///
  ///   // ...
  /// }
  /// ```
  public type OnMessageCallbackArgs = {
    /// The principal of the client sending the message to the canister.
    client_principal : ClientPrincipal;
    /// The message received from the client, serialized in Candid. See [OnMessageCallbackArgs] for an example on how to deserialize the message.
    message : Blob;
  };
  /// Handler initialized by the canister and triggered by the CDK once a message is received by
  /// the CDK.
  public type OnMessageCallback = (OnMessageCallbackArgs) -> async ();

  /// Arguments passed to the `on_close` handler.
  public type OnCloseCallbackArgs = {
    client_principal : ClientPrincipal;
  };
  /// Handler initialized by the canister
  /// and triggered by the CDK once the WS Gateway closes the IC WebSocket connection
  /// for that client.
  ///
  /// Make sure you **don't** call the [close](crate::close) function in this callback.
  public type OnCloseCallback = (OnCloseCallbackArgs) -> async ();

  /// Handlers initialized by the canister and triggered by the CDK.
  ///
  /// **Note**: if the callbacks that you define here trap for some reason,
  /// the CDK will disconnect the client with principal `args.client_principal`.
  /// However, the client **won't** be notified
  /// until at least the next time it will try to send a message to the canister.
  public class WsHandlers(
    init_on_open : ?OnOpenCallback,
    init_on_message : ?OnMessageCallback,
    init_on_close : ?OnCloseCallback,
  ) {
    var on_open : ?OnOpenCallback = init_on_open;
    var on_message : ?OnMessageCallback = init_on_message;
    var on_close : ?OnCloseCallback = init_on_close;

    public func call_on_open(args : OnOpenCallbackArgs) : async () {
      switch (on_open) {
        case (?callback) {
          // we don't have to recover from errors here,
          // we just let the canister trap
          await callback(args);
        };
        case (null) {
          // Do nothing.
        };
      };
    };

    public func call_on_message(args : OnMessageCallbackArgs) : async () {
      switch (on_message) {
        case (?callback) {
          // see call_on_open
          await callback(args);
        };
        case (null) {
          // Do nothing.
        };
      };
    };

    public func call_on_close(args : OnCloseCallbackArgs) : async () {
      switch (on_close) {
        case (?callback) {
          // see call_on_open
          await callback(args);
        };
        case (null) {
          // Do nothing.
        };
      };
    };
  };

  /// Parameters for the IC WebSocket CDK initialization.
  ///
  /// Arguments:
  ///
  /// - `init_max_number_of_returned_messages`: Maximum number of returned messages. Defaults to `50` if null.
  /// - `init_send_ack_interval_ms`: Send ack interval in milliseconds. Defaults to `300_000` (5 minutes) if null.
  public class WsInitParams(
    init_max_number_of_returned_messages : ?Nat,
    init_send_ack_interval_ms : ?Nat64,
  ) = self {
    /// The maximum number of messages to be returned in a polling iteration.
    ///
    /// Defaults to `50`.
    public var max_number_of_returned_messages : Nat = switch (init_max_number_of_returned_messages) {
      case (?value) { value };
      case (null) { Constants.DEFAULT_MAX_NUMBER_OF_RETURNED_MESSAGES };
    };
    /// The interval at which to send an acknowledgement message to the client,
    /// so that the client knows that all the messages it sent have been received by the canister (in milliseconds).
    ///
    /// Must be greater than [`CLIENT_KEEP_ALIVE_TIMEOUT_MS`] (1 minute).
    ///
    /// Defaults to `300_000` (5 minutes).
    public var send_ack_interval_ms : Nat64 = switch (init_send_ack_interval_ms) {
      case (?value) { value };
      case (null) { Constants.DEFAULT_SEND_ACK_INTERVAL_MS };
    };

    /// Checks the validity of the timer parameters.
    /// `send_ack_interval_ms` must be greater than [`CLIENT_KEEP_ALIVE_TIMEOUT_MS`].
    ///
    /// # Traps
    /// If `send_ack_interval_ms` <= [`CLIENT_KEEP_ALIVE_TIMEOUT_MS`].
    public func check_validity() {
      if (send_ack_interval_ms <= Constants.Computed().CLIENT_KEEP_ALIVE_TIMEOUT_MS) {
        Utils.custom_trap("send_ack_interval_ms must be greater than CLIENT_KEEP_ALIVE_TIMEOUT_MS");
      };
    };

    public func with_max_number_of_returned_messages(
      n : Nat
    ) : WsInitParams {
      max_number_of_returned_messages := n;
      self;
    };

    /// Sets the interval (in milliseconds) at which to send an acknowledgement message
    /// to the connected clients.
    ///
    /// Must be greater than [`CLIENT_KEEP_ALIVE_TIMEOUT_MS`] (1 minute).
    ///
    /// # Traps
    /// If `send_ack_interval_ms` <= [`CLIENT_KEEP_ALIVE_TIMEOUT_MS`]. See [WsInitParams.check_validity].
    public func with_send_ack_interval_ms(
      ms : Nat64
    ) : WsInitParams {
      send_ack_interval_ms := ms;
      check_validity();
      self;
    };
  };
};

import HashMap "mo:base/HashMap";
import TrieSet "mo:base/TrieSet";
import Timer "mo:base/Timer";
import List "mo:base/List";
import Iter "mo:base/Iter";
import Principal "mo:base/Principal";
import Prelude "mo:base/Prelude";
import Option "mo:base/Option";
import Nat64 "mo:base/Nat64";
import Text "mo:base/Text";
import Blob "mo:base/Blob";
import CertifiedData "mo:base/CertifiedData";
import CertTree "mo:ic-certification/CertTree";
import Sha256 "mo:sha2/Sha256";

import Constants "Constants";
import Errors "Errors";
import Types "Types";
import Utils "Utils";

module {
  type CanisterOutputMessage = Types.CanisterOutputMessage;
  type CanisterWsGetMessagesResult = Types.CanisterWsGetMessagesResult;
  type CanisterWsSendResult = Types.CanisterWsSendResult;
  type ClientKey = Types.ClientKey;
  type ClientPrincipal = Types.ClientPrincipal;
  type GatewayPrincipal = Types.GatewayPrincipal;
  type RegisteredClient = Types.RegisteredClient;
  type RegisteredGateway = Types.RegisteredGateway;
  type Result<Ok, Err> = Types.Result<Ok, Err>;
  type WsInitParams = Types.WsInitParams;
  type WsHandlers = Types.WsHandlers;

  /// IC WebSocket class that holds the internal state of the IC WebSocket.
  ///
  /// Arguments:
  ///
  /// - `init_params`: `WsInitParams`.
  ///
  /// **Note**: you should only pass an instance of this class to the IcWebSocket class constructor, without using the methods or accessing the fields directly.
  ///
  /// # Traps
  /// If the parameters are invalid. See [`WsInitParams.check_validity`] for more details.
  public class IcWebSocketState(init_params : WsInitParams) = self {
    //// STATE ////
    /// Maps the client's key to the client metadata.
    public var REGISTERED_CLIENTS = HashMap.HashMap<ClientKey, RegisteredClient>(0, Types.areClientKeysEqual, Types.hashClientKey);
    /// Maps the client's principal to the current client key.
    var CURRENT_CLIENT_KEY_MAP = HashMap.HashMap<ClientPrincipal, ClientKey>(0, Principal.equal, Principal.hash);
    /// Keeps track of all the clients for which we're waiting for a keep alive message.
    public var CLIENTS_WAITING_FOR_KEEP_ALIVE : TrieSet.Set<ClientKey> = TrieSet.empty();
    /// Maps the client's public key to the sequence number to use for the next outgoing message (to that client).
    var OUTGOING_MESSAGE_TO_CLIENT_NUM_MAP = HashMap.HashMap<ClientKey, Nat64>(0, Types.areClientKeysEqual, Types.hashClientKey);
    /// Maps the client's public key to the expected sequence number of the next incoming message (from that client).
    var INCOMING_MESSAGE_FROM_CLIENT_NUM_MAP = HashMap.HashMap<ClientKey, Nat64>(0, Types.areClientKeysEqual, Types.hashClientKey);
    /// Keeps track of the Merkle tree used for certified queries.
    var CERT_TREE_STORE : CertTree.Store = CertTree.newStore();
    var CERT_TREE = CertTree.Ops(CERT_TREE_STORE);
    /// Keeps track of the principals of the WS Gateways that poll the canister.
    var REGISTERED_GATEWAYS = HashMap.HashMap<GatewayPrincipal, RegisteredGateway>(0, Principal.equal, Principal.hash);
    /// The acknowledgement active timer.
    public var ACK_TIMER : ?Timer.TimerId = null;
    /// The keep alive active timer.
    public var KEEP_ALIVE_TIMER : ?Timer.TimerId = null;

    //// FUNCTIONS ////
    /// Resets all state to the initial state.
    public func reset_internal_state(handlers : WsHandlers) : async () {
      // for each client, call the on_close handler before clearing the map
      for (client_key in REGISTERED_CLIENTS.keys()) {
        await remove_client(client_key, handlers);
      };

      // make sure all the maps are cleared
      CURRENT_CLIENT_KEY_MAP := HashMap.HashMap<ClientPrincipal, ClientKey>(0, Principal.equal, Principal.hash);
      CLIENTS_WAITING_FOR_KEEP_ALIVE := TrieSet.empty<ClientKey>();
      OUTGOING_MESSAGE_TO_CLIENT_NUM_MAP := HashMap.HashMap<ClientKey, Nat64>(0, Types.areClientKeysEqual, Types.hashClientKey);
      INCOMING_MESSAGE_FROM_CLIENT_NUM_MAP := HashMap.HashMap<ClientKey, Nat64>(0, Types.areClientKeysEqual, Types.hashClientKey);
      CERT_TREE_STORE := CertTree.newStore();
      CERT_TREE := CertTree.Ops(CERT_TREE_STORE);
      REGISTERED_GATEWAYS := HashMap.HashMap<GatewayPrincipal, RegisteredGateway>(0, Principal.equal, Principal.hash);
    };

    /// Increments the clients connected count for the given gateway.
    /// If the gateway is not registered, a new entry is created with a clients connected count of 1.
    func increment_gateway_clients_count(gateway_principal : GatewayPrincipal) {
      switch (REGISTERED_GATEWAYS.get(gateway_principal)) {
        case (?registered_gateway) {
          registered_gateway.increment_clients_count();
        };
        case (null) {
          let new_gw = Types.RegisteredGateway();
          new_gw.increment_clients_count();
          REGISTERED_GATEWAYS.put(gateway_principal, new_gw);
        };
      };
    };

    /// Decrements the clients connected count for the given gateway.
    /// If there are no more clients connected, the gateway is removed from the list of registered gateways.
    func decrement_gateway_clients_count(gateway_principal : GatewayPrincipal) {
      switch (REGISTERED_GATEWAYS.get(gateway_principal)) {
        case (?registered_gateway) {
          let clients_count = registered_gateway.decrement_clients_count();
          if (clients_count == 0) {
            REGISTERED_GATEWAYS.delete(gateway_principal);
          };
        };
        case (null) {
          Prelude.unreachable(); // gateway must be registered at this point
        };
      };
    };

    func get_registered_gateway(gateway_principal : GatewayPrincipal) : Result<RegisteredGateway, Text> {
      switch (REGISTERED_GATEWAYS.get(gateway_principal)) {
        case (?registered_gateway) { #Ok(registered_gateway) };
        case (null) {
          #Err(Errors.to_string(#GatewayNotRegistered({ gateway_principal })));
        };
      };
    };

    public func check_is_gateway_registered(gateway_principal : GatewayPrincipal) : Result<(), Text> {
      switch (get_registered_gateway(gateway_principal)) {
        case (#Ok(_)) { #Ok };
        case (#Err(err)) { #Err(err) };
      };
    };

    public func is_registered_gateway(gateway_principal : GatewayPrincipal) : Bool {
      switch (get_registered_gateway(gateway_principal)) {
        case (#Ok(_)) { true };
        case (#Err(err)) { false };
      };
    };

    public func get_outgoing_message_nonce(gateway_principal : GatewayPrincipal) : Result<Nat64, Text> {
      switch (get_registered_gateway(gateway_principal)) {
        case (#Ok(registered_gateway)) {
          #Ok(registered_gateway.outgoing_message_nonce);
        };
        case (#Err(err)) { #Err(err) };
      };
    };

    public func increment_outgoing_message_nonce(gateway_principal : GatewayPrincipal) : Result<(), Text> {
      switch (get_registered_gateway(gateway_principal)) {
        case (#Ok(registered_gateway)) {
          registered_gateway.increment_nonce();
          #Ok;
        };
        case (#Err(err)) { #Err(err) };
      };
    };

    func insert_client(client_key : ClientKey, new_client : RegisteredClient) {
      CURRENT_CLIENT_KEY_MAP.put(client_key.client_principal, client_key);
      REGISTERED_CLIENTS.put(client_key, new_client);
    };

    func get_registered_client(client_key : ClientKey) : Result<RegisteredClient, Text> {
      switch (REGISTERED_CLIENTS.get(client_key)) {
        case (?registered_client) { #Ok(registered_client) };
        case (null) {
          #Err(Errors.to_string(#ClientKeyNotConnected({ client_key })));
        };
      };
    };

    public func get_client_key_from_principal(client_principal : ClientPrincipal) : Result<ClientKey, Text> {
      switch (CURRENT_CLIENT_KEY_MAP.get(client_principal)) {
        case (?client_key) #Ok(client_key);
        case (null) #Err(Errors.to_string(#ClientPrincipalNotConnected({ client_principal })));
      };
    };

    public func check_registered_client_exists(client_key : ClientKey) : Result<(), Text> {
      switch (get_registered_client(client_key)) {
        case (#Ok(_)) { #Ok };
        case (#Err(err)) { #Err(err) };
      };
    };

    public func check_client_registered_to_gateway(client_key : ClientKey, gateway_principal : GatewayPrincipal) : Result<(), Text> {
      switch (get_registered_client(client_key)) {
        case (#Ok(registered_client)) {
          if (Principal.equal(registered_client.gateway_principal, gateway_principal)) {
            #Ok;
          } else {
            #Err(Errors.to_string(#ClientNotRegisteredToGateway({ client_key; gateway_principal })));
          };
        };
        case (#Err(err)) { #Err(err) };
      };
    };

    public func add_client_to_wait_for_keep_alive(client_key : ClientKey) {
      CLIENTS_WAITING_FOR_KEEP_ALIVE := TrieSet.put<ClientKey>(CLIENTS_WAITING_FOR_KEEP_ALIVE, client_key, Types.hashClientKey(client_key), Types.areClientKeysEqual);
    };

    func init_outgoing_message_to_client_num(client_key : ClientKey) {
      OUTGOING_MESSAGE_TO_CLIENT_NUM_MAP.put(client_key, Constants.INITIAL_CANISTER_SEQUENCE_NUM);
    };

    public func get_outgoing_message_to_client_num(client_key : ClientKey) : Result<Nat64, Text> {
      switch (OUTGOING_MESSAGE_TO_CLIENT_NUM_MAP.get(client_key)) {
        case (?num) #Ok(num);
        case (null) #Err(Errors.to_string(#OutgoingMessageToClientNumNotInitialized({ client_key })));
      };
    };

    public func increment_outgoing_message_to_client_num(client_key : ClientKey) : Result<(), Text> {
      switch (get_outgoing_message_to_client_num(client_key)) {
        case (#Ok(num)) {
          OUTGOING_MESSAGE_TO_CLIENT_NUM_MAP.put(client_key, num + 1);
          #Ok;
        };
        case (#Err(error)) #Err(error);
      };
    };

    func init_expected_incoming_message_from_client_num(client_key : ClientKey) {
      INCOMING_MESSAGE_FROM_CLIENT_NUM_MAP.put(client_key, Constants.INITIAL_CLIENT_SEQUENCE_NUM);
    };

    public func get_expected_incoming_message_from_client_num(client_key : ClientKey) : Result<Nat64, Text> {
      switch (INCOMING_MESSAGE_FROM_CLIENT_NUM_MAP.get(client_key)) {
        case (?num) #Ok(num);
        case (null) #Err(Errors.to_string(#ExpectedIncomingMessageToClientNumNotInitialized({ client_key })));
      };
    };

    public func increment_expected_incoming_message_from_client_num(client_key : ClientKey) : Result<(), Text> {
      switch (get_expected_incoming_message_from_client_num(client_key)) {
        case (#Ok(num)) {
          INCOMING_MESSAGE_FROM_CLIENT_NUM_MAP.put(client_key, num + 1);
          #Ok;
        };
        case (#Err(error)) #Err(error);
      };
    };

    public func add_client(client_key : ClientKey, new_client : RegisteredClient) {
      // insert the client in the map
      insert_client(client_key, new_client);
      // initialize incoming client's message sequence number to 1
      init_expected_incoming_message_from_client_num(client_key);
      // initialize outgoing message sequence number to 0
      init_outgoing_message_to_client_num(client_key);

      increment_gateway_clients_count(new_client.gateway_principal);
    };

    public func remove_client(client_key : ClientKey, handlers : WsHandlers) : async () {
      CLIENTS_WAITING_FOR_KEEP_ALIVE := TrieSet.delete(CLIENTS_WAITING_FOR_KEEP_ALIVE, client_key, Types.hashClientKey(client_key), Types.areClientKeysEqual);
      CURRENT_CLIENT_KEY_MAP.delete(client_key.client_principal);
      OUTGOING_MESSAGE_TO_CLIENT_NUM_MAP.delete(client_key);
      INCOMING_MESSAGE_FROM_CLIENT_NUM_MAP.delete(client_key);

      let registered_client = REGISTERED_CLIENTS.remove(client_key);
      switch (registered_client) {
        case (?registered_client) {
          decrement_gateway_clients_count(registered_client.gateway_principal);
        };
        case (null) {
          Prelude.unreachable();
        };
      };

      await handlers.call_on_close({
        client_principal = client_key.client_principal;
      });
    };

    public func format_message_for_gateway_key(gateway_principal : Principal, nonce : Nat64) : Text {
      let nonce_to_text = do {
        // prints the nonce with 20 padding zeros
        var nonce_str = Nat64.toText(nonce);
        let padding : Nat = 20 - Text.size(nonce_str);
        if (padding > 0) {
          for (i in Iter.range(0, padding - 1)) {
            nonce_str := "0" # nonce_str;
          };
        };

        nonce_str;
      };
      Principal.toText(gateway_principal) # "_" # nonce_to_text;
    };

    func get_gateway_messages_queue(gateway_principal : Principal) : List.List<CanisterOutputMessage> {
      switch (get_registered_gateway(gateway_principal)) {
        case (#Ok(registered_gateway)) {
          registered_gateway.messages_queue;
        };
        case (#Err(error)) {
          // the value exists because we just checked that the gateway is registered
          Prelude.unreachable();
        };
      };
    };

    func get_messages_for_gateway_range(gateway_principal : Principal, nonce : Nat64, max_number_of_returned_messages : Nat) : Types.MessagesForGatewayRange {
      let messages_queue = get_gateway_messages_queue(gateway_principal);

      let queue_len = List.size(messages_queue);

      // smallest key used to determine the first message from the queue which has to be returned to the WS Gateway
      let smallest_key = format_message_for_gateway_key(gateway_principal, nonce);
      // partition the queue at the message which has the key with the nonce specified as argument to get_cert_messages
      let start_index = do {
        let partitions = List.partition(
          messages_queue,
          func(el : CanisterOutputMessage) : Bool {
            Text.less(el.key, smallest_key);
          },
        );
        List.size(partitions.0);
      };
      let (end_index, is_end_of_queue) = if ((queue_len - start_index) : Nat > max_number_of_returned_messages) {
        (start_index + max_number_of_returned_messages, false);
      } else { (queue_len, true) };

      {
        start_index;
        end_index;
        is_end_of_queue;
      };
    };

    func get_messages_for_gateway(gateway_principal : Principal, start_index : Nat, end_index : Nat) : List.List<CanisterOutputMessage> {
      let messages_queue = get_gateway_messages_queue(gateway_principal);

      var messages : List.List<CanisterOutputMessage> = List.nil();
      for (i in Iter.range(start_index, end_index - 1)) {
        let message = List.get(messages_queue, i);
        switch (message) {
          case (?message) {
            messages := List.push(message, messages);
          };
          case (null) {
            Prelude.unreachable(); // the value exists because this function is called only after partitioning the queue
          };
        };
      };

      List.reverse(messages);
    };

    /// Gets the messages in [MESSAGES_FOR_GATEWAYS] starting from the one with the specified nonce
    public func get_cert_messages(gateway_principal : Principal, nonce : Nat64, max_number_of_returned_messages : Nat) : CanisterWsGetMessagesResult {
      let { start_index; end_index; is_end_of_queue } = get_messages_for_gateway_range(gateway_principal, nonce, max_number_of_returned_messages);
      let messages = get_messages_for_gateway(gateway_principal, start_index, end_index);

      if (List.isNil(messages)) {
        return get_cert_messages_empty();
      };

      let keys = List.map(
        messages,
        func(message : CanisterOutputMessage) : CertTree.Path {
          [Text.encodeUtf8(message.key)];
        },
      );
      let (cert, tree) = get_cert_for_range(List.toIter(keys));

      #Ok({
        messages = List.toArray(messages);
        cert = cert;
        tree = tree;
        is_end_of_queue = is_end_of_queue;
      });
    };

    public func get_cert_messages_empty() : CanisterWsGetMessagesResult {
      #Ok({
        messages = [];
        cert = Blob.fromArray([]);
        tree = Blob.fromArray([]);
        is_end_of_queue = true;
      });
    };

    func labeledHash(l : Blob, content : CertTree.Hash) : CertTree.Hash {
      let d = Sha256.Digest(#sha256);
      d.writeBlob("\13ic-hashtree-labeled");
      d.writeBlob(l);
      d.writeBlob(content);
      d.sum();
    };

    public func put_cert_for_message(key : Text, value : Blob) {
      let root_hash = do {
        CERT_TREE.put([Text.encodeUtf8(key)], Sha256.fromBlob(#sha256, value));
        labeledHash(Constants.LABEL_WEBSOCKET, CERT_TREE.treeHash());
      };

      CertifiedData.set(root_hash);
    };

    /// Adds the message to the gateway queue.
    func push_message_in_gateway_queue(gateway_principal : Principal, message : CanisterOutputMessage, message_timestamp : Nat64) : Result<(), Text> {
      switch (get_registered_gateway(gateway_principal)) {
        case (#Ok(registered_gateway)) {
          // messages in the queue are inserted with contiguous and increasing nonces
          // (from beginning to end of the queue) as ws_send is called sequentially, the nonce
          // is incremented by one in each call, and the message is pushed at the end of the queue
          registered_gateway.add_message_to_queue(message, message_timestamp);
          #Ok;
        };
        case (#Err(err)) { #Err(err) };
      };
    };

    /// Deletes the an amount of [MESSAGES_TO_DELETE] messages from the queue
    /// that are older than the ack interval.
    func delete_old_messages_for_gateway(gateway_principal : GatewayPrincipal) : Result<(), Text> {
      let ack_interval_ms = init_params.send_ack_interval_ms;
      let deleted_messages_keys = switch (get_registered_gateway(gateway_principal)) {
        case (#Ok(registered_gateway)) {
          registered_gateway.delete_old_messages(Constants.MESSAGES_TO_DELETE_COUNT, ack_interval_ms);
        };
        case (#Err(err)) { return #Err(err) };
      };

      for (key in Iter.fromList(deleted_messages_keys)) {
        CERT_TREE.delete([Text.encodeUtf8(key)]);
      };

      #Ok;
    };

    func get_cert_for_range(keys : Iter.Iter<CertTree.Path>) : (Blob, Blob) {
      let witness = CERT_TREE.reveals(keys);
      let tree : CertTree.Witness = #labeled(Constants.LABEL_WEBSOCKET, witness);

      switch (CertifiedData.getCertificate()) {
        case (?cert) {
          let tree_blob = CERT_TREE.encodeWitness(tree);
          (cert, tree_blob);
        };
        case (null) Prelude.unreachable();
      };
    };

    func handle_keep_alive_client_message(client_key : ClientKey, _keep_alive_message : Types.ClientKeepAliveMessageContent) {
      // update the last keep alive timestamp for the client
      switch (REGISTERED_CLIENTS.get(client_key)) {
        case (?client_metadata) {
          client_metadata.update_last_keep_alive_timestamp();
        };
        case (null) {
          // Do nothing.
        };
      };
    };

    public func handle_received_service_message(client_key : ClientKey, content : Blob) : async Result<(), Text> {
      let decoded = switch (Types.decode_websocket_service_message_content(content)) {
        case (#Err(err)) {
          return #Err(err);
        };
        case (#Ok(message_content)) {
          message_content;
        };
      };

      switch (decoded) {
        case (#KeepAliveMessage(keep_alive_message)) {
          handle_keep_alive_client_message(client_key, keep_alive_message);
          #Ok;
        };
        case (_) {
          return #Err(Errors.to_string(#InvalidServiceMessage));
        };
      };
    };

    public func send_service_message_to_client(client_key : ClientKey, message : Types.WebsocketServiceMessageContent) : Result<(), Text> {
      let message_bytes = Types.encode_websocket_service_message_content(message);
      _ws_send(client_key, message_bytes, true);
    };

    /// Internal function used to put the messages in the outgoing messages queue and certify them.
    public func _ws_send(client_key : ClientKey, msg_bytes : Blob, is_service_message : Bool) : CanisterWsSendResult {
      // get the registered client if it exists
      let registered_client = switch (get_registered_client(client_key)) {
        case (#Err(err)) {
          return #Err(err);
        };
        case (#Ok(registered_client)) {
          registered_client;
        };
      };

      // the nonce in key is used by the WS Gateway to determine the message to start in the polling iteration
      // the key is also passed to the client in order to validate the body of the certified message
      let outgoing_message_nonce = switch (get_outgoing_message_nonce(registered_client.gateway_principal)) {
        case (#Err(err)) {
          return #Err(err);
        };
        case (#Ok(nonce)) {
          nonce;
        };
      };
      let message_key = format_message_for_gateway_key(registered_client.gateway_principal, outgoing_message_nonce);

      // increment the nonce for the next message
      switch (increment_outgoing_message_nonce(registered_client.gateway_principal)) {
        case (#Err(err)) {
          return #Err(err);
        };
        case (_) {
          // do nothing
        };
      };

      // increment the sequence number for the next message to the client
      switch (increment_outgoing_message_to_client_num(client_key)) {
        case (#Err(err)) {
          return #Err(err);
        };
        case (_) {
          // do nothing
        };
      };

      let message_timestamp = Utils.get_current_time();

      let websocket_message : Types.WebsocketMessage = {
        client_key;
        sequence_num = switch (get_outgoing_message_to_client_num(client_key)) {
          case (#Err(err)) {
            return #Err(err);
          };
          case (#Ok(sequence_num)) {
            sequence_num;
          };
        };
        timestamp = message_timestamp;
        is_service_message;
        content = msg_bytes;
      };

      // CBOR serialize message of type WebsocketMessage
      let message_content = switch (Types.encode_websocket_message(websocket_message)) {
        case (#Err(err)) {
          return #Err(err);
        };
        case (#Ok(content)) {
          content;
        };
      };

      // delete old messages from the gateway queue
      switch (delete_old_messages_for_gateway(registered_client.gateway_principal)) {
        case (#Err(err)) {
          return #Err(err);
        };
        case (_) {
          // do nothing
        };
      };

      // certify data
      put_cert_for_message(message_key, message_content);

      push_message_in_gateway_queue(
        registered_client.gateway_principal,
        {
          client_key;
          content = message_content;
          key = message_key;
        },
        message_timestamp,
      );
    };

    public func _ws_send_to_client_principal(client_principal : ClientPrincipal, msg_bytes : Blob) : CanisterWsSendResult {
      let client_key = switch (get_client_key_from_principal(client_principal)) {
        case (#Err(err)) {
          return #Err(err);
        };
        case (#Ok(client_key)) {
          client_key;
        };
      };
      _ws_send(client_key, msg_bytes, false);
    };
  };
};

import Array "mo:base/Array";
import Blob "mo:base/Blob";
import CertifiedData "mo:base/CertifiedData";
import Deque "mo:base/Deque";
import HashMap "mo:base/HashMap";
import Iter "mo:base/Iter";
import List "mo:base/List";
import Nat64 "mo:base/Nat64";
import Option "mo:base/Option";
import Prelude "mo:base/Prelude";
import Principal "mo:base/Principal";
import Text "mo:base/Text";
import Time "mo:base/Time";
import Timer "mo:base/Timer";
import CborValue "mo:cbor/Value";
import CborDecoder "mo:cbor/Decoder";
import CborEncoder "mo:cbor/Encoder";
import CertTree "mo:ic-certification/CertTree";
import Sha256 "mo:sha2/Sha256";

import Logger "Logger";

module {
	//// CONSTANTS ////
	/// The label used when constructing the certification tree.
	let LABEL_WEBSOCKET : Blob = "websocket";
	/// The maximum number of messages returned by [ws_get_messages] at each poll.
	let MAX_NUMBER_OF_RETURNED_MESSAGES : Nat = 10;
	/// The delay between two consecutive checks if the registered gateway is still alive.
	/// TODO: set it at build time from the environment, so that it can be changed (e.g. for integration tests).
	///       Rust reference: https://github.com/omnia-network/ic-websocket-cdk-rs/blob/48e784e6c5a0fbf5ff6ec8024b74e6d358a1231a/src/ic-websocket-cdk/src/lib.rs#L231-L247
	let CHECK_REGISTERED_GATEWAY_DELAY_NS : Nat = 60_000_000_000; // 60 seconds

	//// TYPES ////
	/// Just to be compatible with the Rust version.
	type Result<Ok, Err> = { #Ok : Ok; #Err : Err };

	public type ClientPublicKey = Blob;

	/// The result of [ws_register].
	public type CanisterWsRegisterResult = Result<(), Text>;
	/// The result of [ws_open].
	public type CanisterWsOpenResult = Result<CanisterWsOpenResultValue, Text>;
	// The result of [ws_message].
	public type CanisterWsMessageResult = Result<(), Text>;
	/// The result of [ws_get_messages].
	public type CanisterWsGetMessagesResult = Result<CanisterOutputCertifiedMessages, Text>;
	/// The result of [ws_send].
	public type CanisterWsSendResult = Result<(), Text>;
	/// The result of [ws_close].
	public type CanisterWsCloseResult = Result<(), Text>;

	public type CanisterWsOpenResultValue = {
		client_key : ClientPublicKey;
		canister_id : Principal;
		nonce : Nat64;
	};

	/// The arguments for [ws_register].
	public type CanisterWsRegisterArguments = {
		client_key : ClientPublicKey;
	};

	/// The arguments for [ws_open].
	public type CanisterWsOpenArguments = {
		content : Blob;
		sig : Blob;
	};

	/// The arguments for [ws_message].
	public type CanisterWsMessageArguments = {
		msg : CanisterIncomingMessage;
	};

	/// The arguments for [ws_get_messages].
	public type CanisterWsGetMessagesArguments = {
		nonce : Nat64;
	};

	/// The arguments for [ws_close].
	public type CanisterWsCloseArguments = {
		client_key : ClientPublicKey;
	};

	/// The first message received by the canister in [ws_open].
	type CanisterOpenMessageContent = {
		client_key : ClientPublicKey;
		canister_id : Principal;
	};

	/// Message + signature from client, **relayed** by the WS Gateway.
	public type RelayedClientMessage = {
		content : Blob;
		sig : Blob;
	};

	/// Message coming **directly** from client, not relayed by the WS Gateway.
	public type DirectClientMessage = {
		message : Blob;
		client_key : ClientPublicKey;
	};

	/// Heartbeat message sent from the WS Gateway to the canister, so that the canister can
	/// verify that the WS Gateway is still alive.
	public type GatewayStatusMessage = {
		status_index : Nat64;
	};

	/// The variants of the possible messages received by the canister in [ws_message].
	/// - **IcWebSocketEstablished**: message sent from WS Gateway to the canister to notify it about the
	///                               establishment of the IcWebSocketConnection
	/// - **IcWebSocketGatewayStatus**:      message sent from WS Gateway to the canister to notify it about the
	///                               status of the IcWebSocketConnection
	/// - **RelayedByGateway**:       message sent from the client to the WS Gateway (via WebSocket) and
	///                               relayed to the canister by the WS Gateway
	/// - **DirectlyFromClient**:     message sent from directly client so that it is not necessary to
	///                               verify the signature
	public type CanisterIncomingMessage = {
		#DirectlyFromClient : DirectClientMessage;
		#RelayedByGateway : RelayedClientMessage;
		#IcWebSocketEstablished : ClientPublicKey;
		#IcWebSocketGatewayStatus : GatewayStatusMessage;
	};

	/// Messages exchanged through the WebSocket.
	type WebsocketMessage = {
		client_key : ClientPublicKey; // The client that the gateway will forward the message to or that sent the message.
		sequence_num : Nat64; // Both ways, messages should arrive with sequence numbers 0, 1, 2...
		timestamp : Nat64; // Timestamp of when the message was made for the recipient to inspect.
		message : Blob; // Application message encoded in binary.
	};

	/// Element of the list of messages returned to the WS Gateway after polling.
	public type CanisterOutputMessage = {
		client_key : ClientPublicKey; // The client that the gateway will forward the message to.
		content : Blob; // The message to be relayed, that contains the application message.
		key : Text; // Key for certificate verification.
	};

	/// List of messages returned to the WS Gateway after polling.
	public type CanisterOutputCertifiedMessages = {
		messages : [CanisterOutputMessage]; // List of messages.
		cert : Blob; // cert+tree constitute the certificate for all returned messages.
		tree : Blob; // cert+tree constitute the certificate for all returned messages.
	};

	/// Arguments passed to the `on_open` handler.
	public type OnOpenCallbackArgs = {
		client_key : ClientPublicKey;
	};
	/// Handler initialized by the canister and triggered by the CDK once the IC WebSocket connection
	/// is established.
	public type OnOpenCallback = (OnOpenCallbackArgs) -> async ();

	/// Arguments passed to the `on_message` handler.
	public type OnMessageCallbackArgs = {
		client_key : ClientPublicKey;
		message : Blob;
	};
	/// Handler initialized by the canister and triggered by the CDK once a message is received by
	/// the CDK.
	public type OnMessageCallback = (OnMessageCallbackArgs) -> async ();

	/// Arguments passed to the `on_close` handler.
	public type OnCloseCallbackArgs = {
		client_key : ClientPublicKey;
	};
	/// Handler initialized by the canister and triggered by the CDK once the WS Gateway closes the
	/// IC WebSocket connection.
	public type OnCloseCallback = (OnCloseCallbackArgs) -> async ();

	//// FUNCTIONS ////
	func get_current_time() : Time.Time {
		Time.now();
	};

	/// Contains data about the registered WS Gateway.
	class RegisteredGateway(gw_principal : Principal) {
		/// The principal of the gateway.
		public var gateway_principal : Principal = gw_principal;
		/// The last time the gateway sent a heartbeat message.
		public var last_heartbeat : ?Time.Time = null;
		/// The last status index received from the gateway.
		public var last_status_index : Nat64 = 0;

		/// Updates the registered gateway's status index with the given one.
		/// Sets the last heartbeat to the current time.
		public func update_status_index(status_index : Nat64) : Result<(), Text> {
			if (status_index <= last_status_index) {
				if (status_index == 0) {
					Logger.custom_print("Gateway status index set to 0");
				} else {
					return #Err("Gateway status index is equal to or behind the current one");
				};
			};
			last_status_index := status_index;
			last_heartbeat := ?get_current_time();
			#Ok;
		};

		/// Resets the registered gateway to the initial state.
		public func reset() {
			last_heartbeat := null;
			last_status_index := 0;

			Logger.custom_print("Gateway has been reset");
		};
	};

	/// Returns the delay in nanoseconds between two consecutive checks if the registered gateway is still alive.
	func get_check_registered_gateway_delay_ns() : Nat {
		// TODO: check if this is an integration test
		CHECK_REGISTERED_GATEWAY_DELAY_NS;
	};

	/// Handlers initialized by the canister and triggered by the CDK.
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
					await callback(args);
				};
				case (null) {
					// Do nothing.
				};
			};
		};
	};

	/// Decodes the CBOR blob into a `CanisterOpenMessageContent`.
	func decode_canister_open_message_content(bytes : Blob) : Result<CanisterOpenMessageContent, Text> {
		switch (CborDecoder.decode(bytes)) {
			case (#err(err)) {
				#Err("deserialization failed");
			};
			case (#ok(c)) {
				switch (c) {
					case (#majorType6({ tag; value })) {
						switch (value) {
							case (#majorType5(content)) {
								#Ok({
									client_key = do {
										let client_key_key_value = Array.find(content, func((key, _) : (CborValue.Value, CborValue.Value)) : Bool = key == #majorType3("client_key"));
										switch (client_key_key_value) {
											case (?(_, #majorType2(client_key_blob))) {
												Blob.fromArray(client_key_blob);
											};
											case (_) {
												return #Err("missing field `client_key`");
											};
										};
									};
									canister_id = do {
										let canister_id_key_value = Array.find(content, func((key, _) : (CborValue.Value, CborValue.Value)) : Bool = key == #majorType3("canister_id"));
										switch (canister_id_key_value) {
											case (?(_, #majorType2(canister_id_blob))) {
												Principal.fromBlob(Blob.fromArray(canister_id_blob));
											};
											case (_) {
												return #Err("missing field `canister_id`");
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

	/// Encodes the `WebsocketMessage` into a CBOR blob.
	func encode_websocket_message(websocket_message : WebsocketMessage) : Result<Blob, Text> {
		let cbor_value : CborValue.Value = #majorType5([
			(#majorType3("client_key"), #majorType2(Blob.toArray(websocket_message.client_key))),
			(#majorType3("sequence_num"), #majorType0(websocket_message.sequence_num)),
			(#majorType3("timestamp"), #majorType0(websocket_message.timestamp)),
			(#majorType3("message"), #majorType2(Blob.toArray(websocket_message.message))),
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
							case (#majorType5(content)) {
								#Ok({
									client_key = do {
										let client_key_key_value = Array.find(content, func((key, _) : (CborValue.Value, CborValue.Value)) : Bool = key == #majorType3("client_key"));
										switch (client_key_key_value) {
											case (?(_, #majorType2(client_key_blob))) {
												Blob.fromArray(client_key_blob);
											};
											case (_) {
												return #Err("missing field `client_key`");
											};
										};
									};
									sequence_num = do {
										let sequence_num_key_value = Array.find(content, func((key, _) : (CborValue.Value, CborValue.Value)) : Bool = key == #majorType3("sequence_num"));
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
										let timestamp_key_value = Array.find(content, func((key, _) : (CborValue.Value, CborValue.Value)) : Bool = key == #majorType3("timestamp"));
										switch (timestamp_key_value) {
											case (?(_, #majorType0(timestamp))) {
												timestamp;
											};
											case (_) {
												return #Err("missing field `timestamp`");
											};
										};
									};
									message = do {
										let message_key_value = Array.find(content, func((key, _) : (CborValue.Value, CborValue.Value)) : Bool = key == #majorType3("message"));
										switch (message_key_value) {
											case (?(_, #majorType2(message_blob))) {
												Blob.fromArray(message_blob);
											};
											case (_) {
												return #Err("missing field `message`");
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

	class TmpClients() {
		public var second_last_index_clients = List.nil<ClientPublicKey>();
		public var last_index_clients = List.nil<ClientPublicKey>();

		public func shift() {
			second_last_index_clients := last_index_clients;
			last_index_clients := List.nil<ClientPublicKey>();
		};

		public func clear() {
			second_last_index_clients := List.nil<ClientPublicKey>();
			last_index_clients := List.nil<ClientPublicKey>();
		};

		public func insert(client_key : ClientPublicKey) {
			last_index_clients := List.push(client_key, last_index_clients);
		};

		public func contain_client(client_key : ClientPublicKey) : Bool {
			switch (List.find(last_index_clients, func(c : ClientPublicKey) : Bool = Blob.equal(c, client_key))) {
				case (?_) true;
				case (_) {
					switch (List.find(second_last_index_clients, func(c : ClientPublicKey) : Bool = Blob.equal(c, client_key))) {
						case (?_) true;
						case (_) false;
					};
				};
			};
		};

		public func remove(client_key : ClientPublicKey) {
			second_last_index_clients := List.filter(second_last_index_clients, func(c : ClientPublicKey) : Bool = not Blob.equal(c, client_key));
			last_index_clients := List.filter(last_index_clients, func(c : ClientPublicKey) : Bool = not Blob.equal(c, client_key));
		};
	};

	/// IC WebSocket class that holds the internal state of the IC WebSocket.
	///
	/// **Note**: you should only pass an instance of this class to the IcWebSocket class constructor, without using the methods or accessing the fields directly.
	public class IcWebSocketState(gateway_principal : Text) {
		//// STATE ////
		/// Maps the client's public key to the client's identity (anonymous if not authenticated).
		public var CLIENT_CALLER_MAP = HashMap.HashMap<ClientPublicKey, Principal>(0, Blob.equal, Blob.hash);
		/// Maps the clients that still don't have a connection opem, based on the gateway stats index at which they were registered.
		public let TMP_CLIENTS : TmpClients = TmpClients();
		/// Maps the client's public key to the sequence number to use for the next outgoing message (to that client).
		public var OUTGOING_MESSAGE_TO_CLIENT_NUM_MAP = HashMap.HashMap<ClientPublicKey, Nat64>(0, Blob.equal, Blob.hash);
		/// Maps the client's public key to the expected sequence number of the next incoming message (from that client).
		public var INCOMING_MESSAGE_FROM_CLIENT_NUM_MAP = HashMap.HashMap<ClientPublicKey, Nat64>(0, Blob.equal, Blob.hash);
		/// Keeps track of the Merkle tree used for certified queries
		public var CERT_TREE_STORE : CertTree.Store = CertTree.newStore();
		public var CERT_TREE = CertTree.Ops(CERT_TREE_STORE);
		/// Keeps track of the principal of the WS Gateway which polls the canister
		public var REGISTERED_GATEWAY : RegisteredGateway = RegisteredGateway(Principal.fromText(gateway_principal));
		/// Keeps track of the messages that have to be sent to the WS Gateway
		public var MESSAGES_FOR_GATEWAY : List.List<CanisterOutputMessage> = List.nil();
		/// Keeps track of the nonce which:
		/// - the WS Gateway uses to specify the first index of the certified messages to be returned when polling
		/// - the client uses as part of the path in the Merkle tree in order to verify the certificate of the messages relayed by the WS Gateway
		public var OUTGOING_MESSAGE_NONCE : Nat64 = 0;

		//// FUNCTIONS ////
		/// Resets all RefCells to their initial state.
		/// If there is a registered gateway, resets its state as well.
		public func reset_internal_state(handlers : WsHandlers) : async () {
			// for each client, call the on_close handler before clearing the map
			for (client_key in CLIENT_CALLER_MAP.keys()) {
				// If a client registers while the gateway crashes and restarts, we have to keep the client in the map,
				// so that the ws_open invoked by the gateway doesn't fail.
				// To be sure that we retain the latest unregistered clients,
				// we keep all the clients that have registered after the last two times the gateway updated the status index
				if (not is_client_in_tmp_clients(client_key)) {
					await handlers.call_on_close({
						client_key;
					});

					CLIENT_CALLER_MAP.delete(client_key);
				};
			};

			OUTGOING_MESSAGE_TO_CLIENT_NUM_MAP := HashMap.HashMap<ClientPublicKey, Nat64>(0, Blob.equal, Blob.hash);
			INCOMING_MESSAGE_FROM_CLIENT_NUM_MAP := HashMap.HashMap<ClientPublicKey, Nat64>(0, Blob.equal, Blob.hash);
			CERT_TREE_STORE := CertTree.newStore();
			CERT_TREE := CertTree.Ops(CERT_TREE_STORE);
			MESSAGES_FOR_GATEWAY := List.nil<CanisterOutputMessage>();
			OUTGOING_MESSAGE_NONCE := 0;
		};

		public func get_outgoing_message_nonce() : Nat64 {
			OUTGOING_MESSAGE_NONCE;
		};

		public func increment_outgoing_message_nonce() {
			OUTGOING_MESSAGE_NONCE += 1;
		};

		public func put_client_caller(client_key : ClientPublicKey, caller : Principal) {
			CLIENT_CALLER_MAP.put(client_key, caller);

			// add the client to the temporary clients
			insert_in_tmp_clients(client_key);
		};

		public func get_client_caller(client_key : ClientPublicKey) : ?Principal {
			CLIENT_CALLER_MAP.get(client_key);
		};

		public func get_registered_gateway_principal() : Principal {
			REGISTERED_GATEWAY.gateway_principal;
		};

		func insert_in_tmp_clients(client_key : ClientPublicKey) {
			TMP_CLIENTS.insert(client_key);
		};

		func shift_tmp_clients() {
			TMP_CLIENTS.shift();
		};

		func is_client_in_tmp_clients(client_key : ClientPublicKey) : Bool {
			TMP_CLIENTS.contain_client(client_key);
		};

		func remove_client_from_tmp_clients(client_key : ClientPublicKey) {
			TMP_CLIENTS.remove(client_key);
		};

		/// Updates the registered gateway with the new status index.
		/// If the status index is not greater than the current one, the function returns an error.
		public func update_registered_gateway_status_index(handlers : WsHandlers, status_index : Nat64) : async Result<(), Text> {
			// if the current status index is > 0 and the new status index is 0, it means that the gateway has been restarted
			// in this case, we reset the internal state because all clients are not connected to the gateway anymore
			if (REGISTERED_GATEWAY.last_status_index > 0 and status_index == 0) {
				await reset_internal_state(handlers);

				REGISTERED_GATEWAY.reset();

				#Ok;
			} else {
				// update the temporary clients, shifting the last index clients to the second last index clients
				shift_tmp_clients();

				REGISTERED_GATEWAY.update_status_index(status_index);
			};
		};

		public func check_registered_client_key(client_key : ClientPublicKey) : Result<(), Text> {
			if (Option.isNull(CLIENT_CALLER_MAP.get(client_key))) {
				return #Err("client's public key has not been previously registered by client");
			};

			#Ok;
		};

		func init_outgoing_message_to_client_num(client_key : ClientPublicKey) {
			OUTGOING_MESSAGE_TO_CLIENT_NUM_MAP.put(client_key, 0);
		};

		public func get_outgoing_message_to_client_num(client_key : ClientPublicKey) : Result<Nat64, Text> {
			switch (OUTGOING_MESSAGE_TO_CLIENT_NUM_MAP.get(client_key)) {
				case (?num) #Ok(num);
				case (null) #Err("outgoing message to client num not initialized for client");
			};
		};

		public func increment_outgoing_message_to_client_num(client_key : ClientPublicKey) : Result<(), Text> {
			let num = get_outgoing_message_to_client_num(client_key);
			switch (num) {
				case (#Ok(num)) {
					OUTGOING_MESSAGE_TO_CLIENT_NUM_MAP.put(client_key, num + 1);
					#Ok;
				};
				case (#Err(error)) #Err(error);
			};
		};

		func init_expected_incoming_message_from_client_num(client_key : ClientPublicKey) {
			INCOMING_MESSAGE_FROM_CLIENT_NUM_MAP.put(client_key, 0);
		};

		public func get_expected_incoming_message_from_client_num(client_key : ClientPublicKey) : Result<Nat64, Text> {
			switch (INCOMING_MESSAGE_FROM_CLIENT_NUM_MAP.get(client_key)) {
				case (?num) #Ok(num);
				case (null) #Err("expected incoming message num not initialized for client");
			};
		};

		public func increment_expected_incoming_message_from_client_num(client_key : ClientPublicKey) : Result<(), Text> {
			let num = get_expected_incoming_message_from_client_num(client_key);
			switch (num) {
				case (#Ok(num)) {
					INCOMING_MESSAGE_FROM_CLIENT_NUM_MAP.put(client_key, num + 1);
					#Ok;
				};
				case (#Err(error)) #Err(error);
			};
		};

		public func add_client(client_key : ClientPublicKey) {
			// initialize incoming client's message sequence number to 0
			init_expected_incoming_message_from_client_num(client_key);
			// initialize outgoing message sequence number to 0
			init_outgoing_message_to_client_num(client_key);

			// now that the client is registered, remove it from the temporary clients
			remove_client_from_tmp_clients(client_key);
		};

		public func remove_client(client_key : ClientPublicKey) {
			CLIENT_CALLER_MAP.delete(client_key);
			OUTGOING_MESSAGE_TO_CLIENT_NUM_MAP.delete(client_key);
			INCOMING_MESSAGE_FROM_CLIENT_NUM_MAP.delete(client_key);
		};

		public func get_message_for_gateway_key(gateway_principal : Principal, nonce : Nat64) : Text {
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

		func get_messages_for_gateway_range(gateway_principal : Principal, nonce : Nat64) : (Nat, Nat) {
			// smallest key used to determine the first message from the queue which has to be returned to the WS Gateway
			let smallest_key = get_message_for_gateway_key(gateway_principal, nonce);
			// partition the queue at the message which has the key with the nonce specified as argument to get_cert_messages
			let start_index = do {
				let partitions = List.partition(
					MESSAGES_FOR_GATEWAY,
					func(el : CanisterOutputMessage) : Bool {
						Text.less(el.key, smallest_key);
					},
				);
				List.size(partitions.0);
			};
			var end_index = List.size(MESSAGES_FOR_GATEWAY);
			if (((end_index - start_index) : Nat) > MAX_NUMBER_OF_RETURNED_MESSAGES) {
				end_index := start_index + MAX_NUMBER_OF_RETURNED_MESSAGES;
			};

			(start_index, end_index);
		};

		func get_messages_for_gateway(start_index : Nat, end_index : Nat) : List.List<CanisterOutputMessage> {
			var messages : List.List<CanisterOutputMessage> = List.nil();
			for (i in Iter.range(start_index, end_index - 1)) {
				let message = List.get(MESSAGES_FOR_GATEWAY, i);
				switch (message) {
					case (?message) {
						messages := List.push(message, messages);
					};
					case (null) {
						// Do nothing
					};
				};
			};

			List.reverse(messages);
		};

		public func get_cert_messages(gateway_principal : Principal, nonce : Nat64) : CanisterWsGetMessagesResult {
			let (start_index, end_index) = get_messages_for_gateway_range(gateway_principal, nonce);
			let messages = get_messages_for_gateway(start_index, end_index);

			if (List.isNil(messages)) {
				return #Ok({
					messages = [];
					cert = Blob.fromArray([]);
					tree = Blob.fromArray([]);
				});
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
			});
		};

		/// Checks if the caller of the method is the same as the one that was registered during the initialization of the CDK
		public func check_is_registered_gateway(input_principal : Principal) : Result<(), Text> {
			let gateway_principal = get_registered_gateway_principal();
			// check if the caller is the same as the one that was registered during the initialization of the CDK
			if (Principal.notEqual(gateway_principal, input_principal)) {
				return #Err("caller is not the gateway that has been registered during CDK initialization");
			};

			#Ok;
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
				labeledHash(LABEL_WEBSOCKET, CERT_TREE.treeHash());
			};

			CertifiedData.set(root_hash);
		};

		func get_cert_for_range(keys : Iter.Iter<CertTree.Path>) : (Blob, Blob) {
			let witness = CERT_TREE.reveals(keys);
			let tree : CertTree.Witness = #labeled(LABEL_WEBSOCKET, witness);

			switch (CertifiedData.getCertificate()) {
				case (?cert) {
					let tree_blob = CERT_TREE.encodeWitness(tree);
					(cert, tree_blob);
				};
				case (null) Prelude.unreachable();
			};
		};
	};

	/// Sends a message to the client.
	///
	/// Under the hood, the message is certified, and then it is added to the queue of messages
	/// that the WS Gateway will poll in the next iteration.
	/// **Note**: you have to serialize the message to a `Blob` before calling this method.
	/// Use the `to_candid` function or any other serialization method of your choice.
	public func ws_send(ws_state : IcWebSocketState, client_key : ClientPublicKey, msg_bytes : Blob) : async CanisterWsSendResult {
		// check if the client is registered
		switch (ws_state.check_registered_client_key(client_key)) {
			case (#Err(err)) {
				return #Err(err);
			};
			case (_) {
				// do nothing
			};
		};

		// get the principal of the gateway that is polling the canister
		let gateway_principal = ws_state.get_registered_gateway_principal();

		// the nonce in key is used by the WS Gateway to determine the message to start in the polling iteration
		// the key is also passed to the client in order to validate the body of the certified message
		let outgoing_message_nonce = ws_state.get_outgoing_message_nonce();
		let key = ws_state.get_message_for_gateway_key(gateway_principal, outgoing_message_nonce);

		// increment the nonce for the next message
		ws_state.increment_outgoing_message_nonce();

		// increment the sequence number for the next message to the client
		switch (ws_state.increment_outgoing_message_to_client_num(client_key)) {
			case (#Err(err)) {
				return #Err(err);
			};
			case (_) {
				// do nothing
			};
		};

		switch (ws_state.get_outgoing_message_to_client_num(client_key)) {
			case (#Err(err)) {
				#Err(err);
			};
			case (#Ok(sequence_num)) {
				let websocket_message : WebsocketMessage = {
					client_key;
					sequence_num;
					timestamp = Nat64.fromIntWrap(get_current_time());
					message = msg_bytes;
				};

				// CBOR serialize message of type WebsocketMessage
				switch (encode_websocket_message(websocket_message)) {
					case (#Err(err)) {
						#Err(err);
					};
					case (#Ok(content)) {
						// certify data
						ws_state.put_cert_for_message(key, content);

						ws_state.MESSAGES_FOR_GATEWAY := List.append(
							ws_state.MESSAGES_FOR_GATEWAY,
							List.fromArray([{ client_key; content; key }]),
						);

						#Ok;
					};
				};
			};
		};
	};

	public class IcWebSocket(init_handlers : WsHandlers, init_ws_state : IcWebSocketState) {
		/// The state of the IC WebSocket.
		private var WS_STATE : IcWebSocketState = init_ws_state;
		/// The callback handlers for the WebSocket.
		private var HANDLERS : WsHandlers = init_handlers;

		/// Resets the internal state of the IC WebSocket CDK.
		///
		/// **Note:** You should only call this function in tests.
		public func wipe() : async () {
			await WS_STATE.reset_internal_state(HANDLERS);

			// if there's a registered gateway, reset its state
			WS_STATE.REGISTERED_GATEWAY.reset();

			// remove all clients from the map
			WS_STATE.CLIENT_CALLER_MAP := HashMap.HashMap<ClientPublicKey, Principal>(0, Blob.equal, Blob.hash);

			// clear the temporary clients
			WS_STATE.TMP_CLIENTS.clear();

			Logger.custom_print("Internal state has been wiped!");
		};

		/// Schedules a timer to check if the registered gateway has sent a heartbeat recently.
		///
		/// The timer delay is given by the [get_check_registered_gateway_delay_ms] function.
		///
		/// The timer callback is [check_registered_gateway_timer_callback].
		func schedule_registered_gateway_check() {
			ignore Timer.setTimer(
				#nanoseconds(get_check_registered_gateway_delay_ns()),
				check_registered_gateway_timer_callback,
			);
		};

		/// Checks if the registered gateway has sent a heartbeat recently.
		/// If not, this means that the gateway has been restarted and all clients registered have been disconnected.
		/// In this case, all internal IC WebSocket CDK state is reset.
		///
		/// At the end, a new timer is scheduled to check again if the registered gateway has sent a heartbeat recently.
		func check_registered_gateway_timer_callback() : async () {
			switch (WS_STATE.REGISTERED_GATEWAY.last_heartbeat) {
				case (?last_heartbeat) {
					if (get_current_time() - last_heartbeat > get_check_registered_gateway_delay_ns()) {
						Logger.custom_print("[timer-cb]: Registered gateway has not sent a heartbeat for more than" # debug_show (get_check_registered_gateway_delay_ns() / 1_000_000_000) # "seconds, resetting all internal state");

						await WS_STATE.reset_internal_state(HANDLERS);

						WS_STATE.REGISTERED_GATEWAY.reset();
					};
				};
				case (null) {
					Logger.custom_print("[timer-cb]: Registered gateway has not sent a heartbeat yet");
				};
			};

			schedule_registered_gateway_check();
		};

		/// Initialize the CDK by setting the callback handlers and the **principal** of the WS Gateway that
		/// will be polling the canister
		do {
			// schedule a timer that will check if the registered gateway is still alive
			schedule_registered_gateway_check();
		};

		/// Handles the register event received from the client.
		///
		/// Registers the public key that the client SDK has generated to initialize an IcWebSocket connection.
		public func ws_register(caller : Principal, args : CanisterWsRegisterArguments) : async CanisterWsRegisterResult {
			// TODO: check who is the caller, which can be a client or the anonymous principal

			// associate the identity of the client to its public key received as input
			WS_STATE.put_client_caller(args.client_key, caller);
			#Ok;
		};

		/// Handles the WS connection open event received from the WS Gateway
		///
		/// WS Gateway relays the first message sent by the client together with its signature
		/// to prove that the first message is actually coming from the same client that registered its public key
		/// beforehand by calling the [ws_register] method.
		public func ws_open(caller : Principal, args : CanisterWsOpenArguments) : async CanisterWsOpenResult {
			// the caller must be the gateway that was registered during CDK initialization
			switch (WS_STATE.check_is_registered_gateway(caller)) {
				case (#Err(err)) {
					return #Err(err);
				};
				case (_) {
					// do nothing
				};
			};

			// decode the first message sent by the client
			let { canister_id; client_key } = switch (decode_canister_open_message_content(args.content)) {
				case (#Err(err)) {
					return #Err(err);
				};
				case (#Ok(c)) {
					c;
				};
			};

			switch (WS_STATE.check_registered_client_key(client_key)) {
				case (#Err(err)) {
					return #Err(err);
				};
				case (_) {
					// do nothing
				};
			};

			// TODO: parse public key and verify signature
			// Rust CDK reference: https://github.com/omnia-network/ic-websocket-cdk-rs/blob/48e784e6c5a0fbf5ff6ec8024b74e6d358a1231a/src/ic-websocket-cdk/src/lib.rs#L689-L700

			WS_STATE.add_client(client_key);

			#Ok({
				client_key;
				canister_id;
				nonce = WS_STATE.get_outgoing_message_nonce();
			});
		};

		/// Handles the WS connection close event received from the WS Gateway.
		public func ws_close(caller : Principal, args : CanisterWsCloseArguments) : async CanisterWsCloseResult {
			switch (WS_STATE.check_is_registered_gateway(caller)) {
				case (#Err(err)) {
					#Err(err);
				};
				case (_) {
					switch (WS_STATE.check_registered_client_key(args.client_key)) {
						case (#Err(err)) {
							#Err(err);
						};
						case (_) {
							WS_STATE.remove_client(args.client_key);

							await HANDLERS.call_on_close({
								client_key = args.client_key;
							});

							#Ok;
						};
					};
				};
			};
		};

		/// Handles the WS messages received either directly from the client or relayed by the WS Gateway.
		public func ws_message(caller : Principal, args : CanisterWsMessageArguments) : async CanisterWsMessageResult {
			switch (args.msg) {
				// message sent directly from client
				case (#DirectlyFromClient(received_message)) {
					// check if the identity of the caller corresponds to the one registered for the given public key
					switch (WS_STATE.get_client_caller(received_message.client_key)) {
						case (null) {
							#Err("client is not registered, call ws_register first");
						};
						case (?expected_caller) {
							if (caller != expected_caller) {
								return #Err("caller is not the same that registered the public key");
							};

							await HANDLERS.call_on_message({
								client_key = received_message.client_key;
								message = received_message.message;
							});

							#Ok;
						};
					};
				};
				// WS Gateway relays a message from the client
				case (#RelayedByGateway(received_message)) {
					// this message can come only from the registered gateway
					switch (WS_STATE.check_is_registered_gateway(caller)) {
						case (#Err(err)) {
							return #Err(err);
						};
						case (_) {
							// do nothing
						};
					};

					let { client_key; sequence_num; timestamp; message } = switch (decode_websocket_message(received_message.content)) {
						case (#Err(err)) {
							return #Err(err);
						};
						case (#Ok(c)) {
							c;
						};
					};

					switch (WS_STATE.check_registered_client_key(client_key)) {
						case (#Err(err)) {
							return #Err(err);
						};
						case (_) {
							// do nothing
						};
					};

					// TODO: parse public key and verify signature
					// Rust CDK reference: https://github.com/omnia-network/ic-websocket-cdk-rs/blob/48e784e6c5a0fbf5ff6ec8024b74e6d358a1231a/src/ic-websocket-cdk/src/lib.rs#L774-L782

					let expected_sequence_num = WS_STATE.get_expected_incoming_message_from_client_num(client_key);
					switch (expected_sequence_num) {
						case (#Err(err)) {
							#Err(err);
						};
						case (#Ok(expected_sequence_num)) {
							// check if the incoming message has the expected sequence number
							if (sequence_num == expected_sequence_num) {
								// increase the expected sequence number by 1
								switch (WS_STATE.increment_expected_incoming_message_from_client_num(client_key)) {
									case (#Err(err)) {
										return #Err(err);
									};
									case (_) {
										// do nothing
									};
								};

								// trigger the on_message handler initialized by canister
								// create message to send to client
								await HANDLERS.call_on_message({
									client_key;
									message;
								});

								#Ok;
							} else {
								#Err("incoming client's message relayed from WS Gateway does not have the expected sequence number");
							};
						};
					};
				};
				// WS Gateway notifies the canister of the established IC WebSocket connection
				case (#IcWebSocketEstablished(client_key)) {
					// this message can come only from the registered gateway
					switch (WS_STATE.check_is_registered_gateway(caller)) {
						case (#Err(err)) {
							#Err(err);
						};
						case (_) {
							// check if client registered its public key by calling ws_register
							switch (WS_STATE.check_registered_client_key(client_key)) {
								case (#Err(err)) {
									#Err(err);
								};
								case (_) {
									Logger.custom_print("Can start notifying client with key: " # debug_show (client_key));

									// call the on_open handler
									await HANDLERS.call_on_open({
										client_key;
									});

									#Ok;
								};
							};
						};
					};
				};
				// WS Gateway notifies the canister that it is up and running
				case (#IcWebSocketGatewayStatus(gateway_status)) {
					// this message can come only from the registered gateway
					switch (WS_STATE.check_is_registered_gateway(caller)) {
						case (#Err(err)) {
							#Err(err);
						};
						case (_) {
							await WS_STATE.update_registered_gateway_status_index(HANDLERS, gateway_status.status_index);
						};
					};
				};
			};
		};

		/// Returns messages to the WS Gateway in response of a polling iteration.
		public func ws_get_messages(caller : Principal, args : CanisterWsGetMessagesArguments) : CanisterWsGetMessagesResult {
			// check if the caller of this method is the WS Gateway that has been set during the initialization of the SDK
			switch (WS_STATE.check_is_registered_gateway(caller)) {
				case (#Err(err)) {
					#Err(err);
				};
				case (_) {
					WS_STATE.get_cert_messages(caller, args.nonce);
				};
			};
		};

		/// Sends a message to the client. See [ws_send] function for reference.
		public func send(client_key : ClientPublicKey, msg_bytes : Blob) : async CanisterWsSendResult {
			await ws_send(WS_STATE, client_key, msg_bytes);
		};
	};
};

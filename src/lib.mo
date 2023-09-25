import Array "mo:base/Array";
import Blob "mo:base/Blob";
import CertifiedData "mo:base/CertifiedData";
import Deque "mo:base/Deque";
import HashMap "mo:base/HashMap";
import Hash "mo:base/Hash";
import Iter "mo:base/Iter";
import List "mo:base/List";
import Nat64 "mo:base/Nat64";
import Option "mo:base/Option";
import Prelude "mo:base/Prelude";
import Principal "mo:base/Principal";
import Text "mo:base/Text";
import Time "mo:base/Time";
import Timer "mo:base/Timer";
import Bool "mo:base/Bool";
import Error "mo:base/Error";
import CborValue "mo:cbor/Value";
import CborDecoder "mo:cbor/Decoder";
import CborEncoder "mo:cbor/Encoder";
import CertTree "mo:ic-certification/CertTree";
import Sha256 "mo:sha2/Sha256";
import Arg "mo:candid/Arg";
import Decoder "mo:candid/Decoder";
import Encoder "mo:candid/Encoder";
import Tag "mo:candid/Tag";
import Type "mo:candid/Type";
import Value "mo:candid/Value";

import Logger "Logger";

module {
	//// CONSTANTS ////
	/// The label used when constructing the certification tree.
	let LABEL_WEBSOCKET : Blob = "websocket";
	/// The maximum number of messages returned by [ws_get_messages] at each poll.
	let MAX_NUMBER_OF_RETURNED_MESSAGES : Nat = 10;
	/// The default delay between two consecutive acknowledgements sent to the client.
	let DEFAULT_SEND_ACK_DELAY_MS : Nat64 = 60_000; // 60 seconds
	/// The default delay to wait for the client to send a keep alive after receiving an acknowledgement.
	let DEFAULT_CLIENT_KEEP_ALIVE_DELAY_MS : Nat64 = 10_000; // 10 seconds

	//// TYPES ////
	type CandidType = Type.Type;
	type CandidValue = Value.Value;
	type CandidTag = Tag.Tag;
	/// Just to be compatible with the Rust version.
	type Result<Ok, Err> = { #Ok : Ok; #Err : Err };

	public type ClientPrincipal = Principal;

	public type ClientKey = {
		client_principal : ClientPrincipal;
		client_nonce : Nat64;
	};
	// functions needed for ClientKey
	func areClientKeysEqual(k1 : ClientKey, k2 : ClientKey) : Bool {
		Principal.equal(k1.client_principal, k2.client_principal) and Nat64.equal(k1.client_nonce, k2.client_nonce);
	};
	func clientKeyToText(k : ClientKey) : Text {
		Principal.toText(k.client_principal) # "_" # Nat64.toText(k.client_nonce);
	};
	func hashClientKey(k : ClientKey) : Hash.Hash {
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
	/// The result of [ws_send].
	public type CanisterWsSendResult = Result<(), Text>;

	/// The arguments for [ws_open].
	public type CanisterWsOpenArguments = {
		client_nonce : Nat64;
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
	type WebsocketMessage = {
		client_key : ClientKey; // The client that the gateway will forward the message to or that sent the message.
		sequence_num : Nat64; // Both ways, messages should arrive with sequence numbers 0, 1, 2...
		timestamp : Nat64; // Timestamp of when the message was made for the recipient to inspect.
		is_service_message : Bool; // Whether the message is a service message sent by the CDK to the client or vice versa.
		content : Blob; // Application message encoded in binary.
	};

	/// Element of the list of messages returned to the WS Gateway after polling.
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
	};

	/// Contains data about the registered WS Gateway.
	class RegisteredGateway(gw_principal : Principal) {
		/// The principal of the gateway.
		public var gateway_principal : Principal = gw_principal;
	};

	/// The metadata about a registered client.
	class RegisteredClient() {
		public var last_keep_alive_timestamp : Nat64 = Nat64.fromIntWrap(get_current_time());
	};

	type CanisterOpenMessageContent = {
		client_key : ClientKey;
	};
	let CanisterOpenMessageContentIdl : CandidType = #record([{
		tag = #name("client_key");
		type_ = #record([
			{
				tag = #name("client_principal");
				type_ = #principal;
			},
			{
				tag = #name("client_nonce");
				type_ = #nat64;
			},
		]);
	}]);

	type CanisterAckMessageContent = {
		last_incoming_sequence_num : Nat64;
	};
	let CanisterAckMessageContentIdl : CandidType = #record([{
		tag = #name("last_incoming_sequence_num");
		type_ = #nat64;
	}]);

	type ClientKeepAliveMessageContent = {
		last_incoming_sequence_num : Nat64;
	};
	let ClientKeepAliveMessageContentIdl : CandidType = #record([{
		tag = #name("last_incoming_sequence_num");
		type_ = #nat64;
	}]);

	type WebsocketServiceMessageContent = {
		#OpenMessage : CanisterOpenMessageContent;
		#AckMessage : CanisterAckMessageContent;
		#KeepAliveMessage : ClientKeepAliveMessageContent;
	};

	let WebsocketServiceMessageIdl : CandidType = #variant([
		{
			tag = #name("OpenMessage");
			type_ = CanisterOpenMessageContentIdl;
		},
		{
			tag = #name("AckMessage");
			type_ = CanisterAckMessageContentIdl;
		},
		{
			tag = #name("KeepAliveMessage");
			type_ = ClientKeepAliveMessageContentIdl;
		},
	]);

	func encode_websocket_service_message_content(content : WebsocketServiceMessageContent) : Blob {
		let value : CandidValue = switch (content) {
			case (#OpenMessage(open_content)) {
				#variant({
					tag = #name("OpenMessage");
					value = #record([{
						tag = #name("client_key");
						value = #record([
							{
								tag = #name("client_principal");
								value = #principal(open_content.client_key.client_principal);
							},
							{
								tag = #name("client_nonce");
								value = #nat64(open_content.client_key.client_nonce);
							},
						]);
					}]);
				});
			};
			case (#AckMessage(ack_content)) {
				#variant({
					tag = #name("AckMessage");
					value = #record([{
						tag = #name("last_incoming_sequence_num");
						value = #nat64(ack_content.last_incoming_sequence_num);
					}]);
				});
			};
			case (#KeepAliveMessage(keep_alive_content)) {
				#variant({
					tag = #name("KeepAliveMessage");
					value = #record([{
						tag = #name("last_incoming_sequence_num");
						value = #nat64(keep_alive_content.last_incoming_sequence_num);
					}]);
				});
			};
		};
		let args : [Arg.Arg] = [{
			type_ = WebsocketServiceMessageIdl;
			value;
		}];

		Encoder.encode(args);
	};

	func decode_websocket_service_message_content(bytes : Blob) : Result<WebsocketServiceMessageContent, Text> {
		let args : [Arg.Arg] = switch (Decoder.decode(bytes)) {
			case (null) {
				return #Err("deserialization failed");
			};
			case (?args) {
				args;
			};
		};

		if (Array.size(args) != 1) {
			return #Err("invalid number of arguments");
		};

		let arg = args[0];
		switch (arg.value) {
			case (#variant(content)) {
				switch ((content.tag, content.value)) {
					case ((#name("OpenMessage"), #record(open_message))) {
						let open_message_content : WebsocketServiceMessageContent = #OpenMessage({
							client_key = do {
								let client_key_record = Array.find(
									open_message,
									func(rec : Value.RecordFieldValue) : Bool = switch (rec.tag) {
										case (#name(n)) { n == "client_key" };
										case (_) { false };
									},
								);
								switch (client_key_record) {
									case (?client_key_record) {
										switch (client_key_record.value) {
											case (#record(client_key)) {
												let client_principal_field = Array.find(
													client_key,
													func(rec : Value.RecordFieldValue) : Bool = switch (rec.tag) {
														case (#name(n)) { n == "client_principal" };
														case (_) { false };
													},
												);
												let client_nonce_field = Array.find(
													client_key,
													func(rec : Value.RecordFieldValue) : Bool = switch (rec.tag) {
														case (#name(n)) { n == "client_nonce" };
														case (_) { false };
													},
												);
												switch ((client_principal_field, client_nonce_field)) {
													case ((?client_principal, ?client_nonce)) {
														switch ((client_principal.value, client_nonce.value)) {
															case ((#principal(client_principal), #nat64(client_nonce))) {
																{
																	client_principal;
																	client_nonce;
																};
															};
															case (_) {
																return #Err("invalid argument");
															};
														};
													};
													case (_) {
														return #Err("invalid argument");
													};
												};
											};
											case (_) {
												return #Err("invalid argument");
											};
										};
									};
									case (_) {
										return #Err("missing field `client_key`");
									};
								};
							};
						});

						#Ok(open_message_content);
					};
					case ((#name("AckMessage"), #record(ack_message))) {
						let ack_message_content : WebsocketServiceMessageContent = #AckMessage({
							last_incoming_sequence_num = do {
								let last_incoming_record = Array.find(
									ack_message,
									func(rec : Value.RecordFieldValue) : Bool = switch (rec.tag) {
										case ((#name(n))) { n == "last_incoming_sequence_num" };
										case (_) { false };
									},
								);
								switch (last_incoming_record) {
									case (?last_incoming_record) {
										switch (last_incoming_record.value) {
											case (#nat64(last_incoming_sequence_num)) {
												last_incoming_sequence_num;
											};
											case (_) {
												return #Err("invalid argument");
											};
										};
									};
									case (_) {
										return #Err("missing field `last_incoming_sequence_num`");
									};
								};
							};
						});

						#Ok(ack_message_content);
					};
					case ((#name("KeepAliveMessage"), #record(keep_alive_message))) {
						let keep_alive_message_content : WebsocketServiceMessageContent = #KeepAliveMessage({
							last_incoming_sequence_num = do {
								let last_incoming_record = Array.find(
									keep_alive_message,
									func(rec : Value.RecordFieldValue) : Bool = switch (rec.tag) {
										case ((#name(n))) { n == "last_incoming_sequence_num" };
										case (_) { false };
									},
								);
								switch (last_incoming_record) {
									case (?last_incoming_record) {
										switch (last_incoming_record.value) {
											case (#nat64(last_incoming_sequence_num)) {
												last_incoming_sequence_num;
											};
											case (_) {
												return #Err("invalid argument");
											};
										};
									};
									case (_) {
										return #Err("missing field `last_incoming_sequence_num`");
									};
								};
							};
						});

						#Ok(keep_alive_message_content);
					};
					case (_) {
						return #Err("invalid argument");
					};
				};
			};
			case (_) {
				return #Err("invalid argument");
			};
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
	public type OnMessageCallbackArgs = {
		client_principal : ClientPrincipal;
		message : Blob;
	};
	/// Handler initialized by the canister and triggered by the CDK once a message is received by
	/// the CDK.
	public type OnMessageCallback = (OnMessageCallbackArgs) -> async ();

	/// Arguments passed to the `on_close` handler.
	public type OnCloseCallbackArgs = {
		client_principal : ClientPrincipal;
	};
	/// Handler initialized by the canister and triggered by the CDK once the WS Gateway closes the
	/// IC WebSocket connection.
	public type OnCloseCallback = (OnCloseCallbackArgs) -> async ();

	//// FUNCTIONS ////
	func get_current_time() : Time.Time {
		Time.now();
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
					try {
						await callback(args);
					} catch (err) {
						Logger.custom_print("Error calling on_open handler: " # Error.message(err));
					};
				};
				case (null) {
					// Do nothing.
				};
			};
		};

		public func call_on_message(args : OnMessageCallbackArgs) : async () {
			switch (on_message) {
				case (?callback) {
					try {
						await callback(args);
					} catch (err) {
						Logger.custom_print("Error calling on_message handler: " # Error.message(err));
					};
				};
				case (null) {
					// Do nothing.
				};
			};
		};

		public func call_on_close(args : OnCloseCallbackArgs) : async () {
			switch (on_close) {
				case (?callback) {
					try {
						await callback(args);
					} catch (err) {
						Logger.custom_print("Error calling on_close handler: " # Error.message(err));
					};
				};
				case (null) {
					// Do nothing.
				};
			};
		};
	};

	/// Encodes the `WebsocketMessage` into a CBOR blob.
	func encode_websocket_message(websocket_message : WebsocketMessage) : Result<Blob, Text> {
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

	/// IC WebSocket class that holds the internal state of the IC WebSocket.
	///
	/// **Note**: you should only pass an instance of this class to the IcWebSocket class constructor, without using the methods or accessing the fields directly.
	public class IcWebSocketState(gateway_principal : Text) {
		//// STATE ////
		/// Maps the client's key to the client metadata
		public var REGISTERED_CLIENTS = HashMap.HashMap<ClientKey, RegisteredClient>(0, areClientKeysEqual, hashClientKey);
		/// Maps the client's principal to the current client key
		public var CURRENT_CLIENT_KEY_MAP = HashMap.HashMap<ClientPrincipal, ClientKey>(0, Principal.equal, Principal.hash);
		/// Maps the client's public key to the sequence number to use for the next outgoing message (to that client).
		public var OUTGOING_MESSAGE_TO_CLIENT_NUM_MAP = HashMap.HashMap<ClientKey, Nat64>(0, areClientKeysEqual, hashClientKey);
		/// Maps the client's public key to the expected sequence number of the next incoming message (from that client).
		public var INCOMING_MESSAGE_FROM_CLIENT_NUM_MAP = HashMap.HashMap<ClientKey, Nat64>(0, areClientKeysEqual, hashClientKey);
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
		/// Resets all state to the initial state.
		public func reset_internal_state(handlers : WsHandlers) : async () {
			// for each client, call the on_close handler before clearing the map
			for (client_key in REGISTERED_CLIENTS.keys()) {
				await remove_client(client_key, handlers);
			};

			CURRENT_CLIENT_KEY_MAP := HashMap.HashMap<ClientPrincipal, ClientKey>(0, Principal.equal, Principal.hash);
			OUTGOING_MESSAGE_TO_CLIENT_NUM_MAP := HashMap.HashMap<ClientKey, Nat64>(0, areClientKeysEqual, hashClientKey);
			INCOMING_MESSAGE_FROM_CLIENT_NUM_MAP := HashMap.HashMap<ClientKey, Nat64>(0, areClientKeysEqual, hashClientKey);
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

		public func insert_client(client_key : ClientKey, new_client : RegisteredClient) {
			CURRENT_CLIENT_KEY_MAP.put(client_key.client_principal, client_key);
			REGISTERED_CLIENTS.put(client_key, new_client);
		};

		public func is_client_registered(client_key : ClientKey) : Bool {
			Option.isSome(REGISTERED_CLIENTS.get(client_key));
		};

		public func get_client_key_from_principal(client_principal : ClientPrincipal) : Result<ClientKey, Text> {
			switch (CURRENT_CLIENT_KEY_MAP.get(client_principal)) {
				case (?client_key) #Ok(client_key);
				case (null) #Err("client with principal" # Principal.toText(client_principal) # "doesn't have an open connection");
			};
		};

		public func check_registered_client(client_key : ClientKey) : Result<(), Text> {
			if (not is_client_registered(client_key)) {
				return #Err("client with key" # clientKeyToText(client_key) # "doesn't have an open connection");
			};

			#Ok;
		};

		public func update_last_keep_alive_timestamp_for_client(client_key : ClientKey) {
			let client = REGISTERED_CLIENTS.get(client_key);
			switch (client) {
				case (?client_metadata) {
					client_metadata.last_keep_alive_timestamp := Nat64.fromIntWrap(get_current_time());
					REGISTERED_CLIENTS.put(client_key, client_metadata);
				};
				case (null) {
					// Do nothing.
				};
			};
		};

		public func get_registered_gateway_principal() : Principal {
			REGISTERED_GATEWAY.gateway_principal;
		};

		func init_outgoing_message_to_client_num(client_key : ClientKey) {
			OUTGOING_MESSAGE_TO_CLIENT_NUM_MAP.put(client_key, 0);
		};

		public func get_outgoing_message_to_client_num(client_key : ClientKey) : Result<Nat64, Text> {
			switch (OUTGOING_MESSAGE_TO_CLIENT_NUM_MAP.get(client_key)) {
				case (?num) #Ok(num);
				case (null) #Err("outgoing message to client num not initialized for client");
			};
		};

		public func increment_outgoing_message_to_client_num(client_key : ClientKey) : Result<(), Text> {
			let num = get_outgoing_message_to_client_num(client_key);
			switch (num) {
				case (#Ok(num)) {
					OUTGOING_MESSAGE_TO_CLIENT_NUM_MAP.put(client_key, num + 1);
					#Ok;
				};
				case (#Err(error)) #Err(error);
			};
		};

		func init_expected_incoming_message_from_client_num(client_key : ClientKey) {
			INCOMING_MESSAGE_FROM_CLIENT_NUM_MAP.put(client_key, 1);
		};

		public func get_expected_incoming_message_from_client_num(client_key : ClientKey) : Result<Nat64, Text> {
			switch (INCOMING_MESSAGE_FROM_CLIENT_NUM_MAP.get(client_key)) {
				case (?num) #Ok(num);
				case (null) #Err("expected incoming message num not initialized for client");
			};
		};

		public func increment_expected_incoming_message_from_client_num(client_key : ClientKey) : Result<(), Text> {
			let num = get_expected_incoming_message_from_client_num(client_key);
			switch (num) {
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
		};

		public func remove_client(client_key : ClientKey, handlers : WsHandlers) : async () {
			CURRENT_CLIENT_KEY_MAP.delete(client_key.client_principal);
			REGISTERED_CLIENTS.delete(client_key);
			OUTGOING_MESSAGE_TO_CLIENT_NUM_MAP.delete(client_key);
			INCOMING_MESSAGE_FROM_CLIENT_NUM_MAP.delete(client_key);

			await handlers.call_on_close({
				client_principal = client_key.client_principal;
			});
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
			let queue_len = List.size(MESSAGES_FOR_GATEWAY);

			if (nonce == 0 and queue_len > 0) {
				// this is the case in which the poller on the gateway restarted
				// the range to return is end:last index and start: max(end - MAX_NUMBER_OF_RETURNED_MESSAGES, 0)
				let start_index = if (queue_len > MAX_NUMBER_OF_RETURNED_MESSAGES) {
					(queue_len - MAX_NUMBER_OF_RETURNED_MESSAGES) : Nat;
				} else {
					0;
				};

				return (start_index, queue_len);
			};

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
			var end_index = queue_len;
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

		public func is_registered_gateway(principal : Principal) : Bool {
			Principal.equal(principal, REGISTERED_GATEWAY.gateway_principal);
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

	/// Internal function used to put the messages in the outgoing messages queue and certify them.
	func _ws_send(ws_state : IcWebSocketState, client_principal : ClientPrincipal, msg_bytes : Blob, is_service_message : Bool) : CanisterWsSendResult {
		let client_key = switch (ws_state.get_client_key_from_principal(client_principal)) {
			case (#Err(err)) {
				return #Err(err);
			};
			case (#Ok(client_key)) {
				client_key;
			};
		};

		// check if the client is registered
		switch (ws_state.check_registered_client(client_key)) {
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

		let sequence_num = switch (ws_state.get_outgoing_message_to_client_num(client_key)) {
			case (#Err(err)) {
				return #Err(err);
			};
			case (#Ok(sequence_num)) {
				sequence_num;
			};
		};

		let websocket_message : WebsocketMessage = {
			client_key;
			sequence_num;
			timestamp = Nat64.fromIntWrap(get_current_time());
			is_service_message;
			content = msg_bytes;
		};

		// CBOR serialize message of type WebsocketMessage
		let content = switch (encode_websocket_message(websocket_message)) {
			case (#Err(err)) {
				return #Err(err);
			};
			case (#Ok(content)) {
				content;
			};
		};

		// certify data
		ws_state.put_cert_for_message(key, content);

		ws_state.MESSAGES_FOR_GATEWAY := List.append(
			ws_state.MESSAGES_FOR_GATEWAY,
			List.fromArray([{ client_key; content; key }]),
		);

		#Ok;
	};

	/// Sends a message to the client.
	///
	/// Under the hood, the message is certified, and then it is added to the queue of messages
	/// that the WS Gateway will poll in the next iteration.
	/// **Note**: you have to serialize the message to a `Blob` before calling this method.
	/// Use the `to_candid` function.
	public func ws_send(ws_state : IcWebSocketState, client_principal : ClientPrincipal, msg_bytes : Blob) : async CanisterWsSendResult {
		_ws_send(ws_state, client_principal, msg_bytes, false);
	};

	func send_service_message_to_client(ws_state : IcWebSocketState, client_key : ClientKey, message : WebsocketServiceMessageContent) : Result<(), Text> {
		let message_bytes = encode_websocket_service_message_content(message);
		_ws_send(ws_state, client_key.client_principal, message_bytes, true);
	};

	/// Parameters for the IC WebSocket CDK initialization.
	public class WsInitParams(init_ws_state : IcWebSocketState, init_handlers : WsHandlers) {
		public var state : IcWebSocketState = init_ws_state;
		/// The callback handlers for the WebSocket.
		public var handlers : WsHandlers = init_handlers;
		/// The interval at which to send an acknowledgement message to the client,
		/// so that the client knows that all the messages it sent have been received by the canister (in milliseconds).
		/// Defaults to `60_000` (60 seconds).
		public var send_ack_interval_ms : Nat64 = DEFAULT_SEND_ACK_DELAY_MS;
		/// The delay to wait for the client to send a keep alive after receiving an acknowledgement (in milliseconds).
		/// Defaults to `10_000` (10 seconds).
		public var keep_alive_interval_ms : Nat64 = DEFAULT_CLIENT_KEEP_ALIVE_DELAY_MS;
	};

	public class IcWebSocket(params : WsInitParams) {
		/// The state of the IC WebSocket.
		private var WS_STATE : IcWebSocketState = params.state;
		/// The callback handlers for the WebSocket.
		private var HANDLERS : WsHandlers = params.handlers;

		/// Resets the internal state of the IC WebSocket CDK.
		///
		/// **Note:** You should only call this function in tests.
		public func wipe() : async () {
			await WS_STATE.reset_internal_state(HANDLERS);

			Logger.custom_print("Internal state has been wiped!");
		};

		/// Handles the WS connection open event received from the WS Gateway
		///
		/// WS Gateway relays the first message sent by the client together with its signature
		/// to prove that the first message is actually coming from the same client that registered its public key
		/// beforehand by calling the [ws_register] method.
		public func ws_open(caller : Principal, args : CanisterWsOpenArguments) : async CanisterWsOpenResult {
			// anonymous clients cannot open a connection
			if (Principal.isAnonymous(caller)) {
				return #Err("anonymous clients cannot open a connection");
			};

			// avoid gateway opening a connection for its own principal
			if (WS_STATE.is_registered_gateway(caller)) {
				return #Err("caller is the registered gateway which can't open a connection for itself");
			};

			let client_key : ClientKey = {
				client_principal = caller;
				client_nonce = args.client_nonce;
			};
			// check if client is not registered yet
			if (WS_STATE.is_client_registered(client_key)) {
				return #Err("client with key" # clientKeyToText(client_key) # "already has an open connection");
			};

			// initialize client maps
			let new_client = RegisteredClient();
			WS_STATE.add_client(client_key, new_client);

			let open_message : CanisterOpenMessageContent = {
				client_key;
			};
			let message : WebsocketServiceMessageContent = #OpenMessage(open_message);
			switch (send_service_message_to_client(WS_STATE, client_key, message)) {
				case (#Err(err)) {
					return #Err(err);
				};
				case (#Ok(_)) {
					// do nothing
				};
			};

			await HANDLERS.call_on_open({
				client_principal = client_key.client_principal;
			});

			#Ok;
		};

		/// Handles the WS connection close event received from the WS Gateway.
		public func ws_close(caller : Principal, args : CanisterWsCloseArguments) : async CanisterWsCloseResult {
			switch (WS_STATE.check_is_registered_gateway(caller)) {
				case (#Err(err)) {
					return #Err(err);
				};
				case (_) {
					// do nothing
				};
			};

			switch (WS_STATE.check_registered_client(args.client_key)) {
				case (#Err(err)) {
					return #Err(err);
				};
				case (_) {
					// do nothing
				};
			};

			await WS_STATE.remove_client(args.client_key, HANDLERS);

			#Ok;
		};

		/// Handles the WS messages received either directly from the client or relayed by the WS Gateway.
		public func ws_message(caller : Principal, args : CanisterWsMessageArguments) : async CanisterWsMessageResult {
			// check if client registered its principal by calling ws_open
			let registered_client_key = switch (WS_STATE.get_client_key_from_principal(caller)) {
				case (#Err(err)) {
					return #Err(err);
				};
				case (#Ok(client_key)) {
					client_key;
				};
			};

			let {
				client_key;
				sequence_num;
				timestamp;
				is_service_message;
				content;
			} = args.msg;

			// check if the client key is correct
			if (not areClientKeysEqual(registered_client_key, client_key)) {
				return #Err("client with principal" #Principal.toText(caller) # "has a different key than the one used in the message");
			};

			let expected_sequence_num = switch (WS_STATE.get_expected_incoming_message_from_client_num(client_key)) {
				case (#Err(err)) {
					return #Err(err);
				};
				case (#Ok(sequence_num)) {
					sequence_num;
				};
			};

			// check if the incoming message has the expected sequence number
			if (sequence_num != expected_sequence_num) {
				await WS_STATE.remove_client(client_key, HANDLERS);
				return #Err(
					"incoming client's message does not have the expected sequence number. Expected: " #
					Nat64.toText(expected_sequence_num)
					# ", actual: " #
					Nat64.toText(sequence_num)
					# ". Client removed."
				);
			};
			// increase the expected sequence number by 1
			switch (WS_STATE.increment_expected_incoming_message_from_client_num(client_key)) {
				case (#Err(err)) {
					return #Err(err);
				};
				case (_) {
					// do nothing
				};
			};

			if (is_service_message) {
				return await handle_keep_alive_client_message(client_key, content);
			};

			await HANDLERS.call_on_message({
				client_principal = client_key.client_principal;
				message = content;
			});

			#Ok;
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
		public func send(client_principal : ClientPrincipal, msg_bytes : Blob) : async CanisterWsSendResult {
			await ws_send(WS_STATE, client_principal, msg_bytes);
		};

		func handle_keep_alive_client_message(client_key : ClientKey, content : Blob) : async Result<(), Text> {
			let message_content : WebsocketServiceMessageContent = switch (decode_websocket_service_message_content(content)) {
				case (#Err(err)) {
					return #Err(err);
				};
				case (#Ok(message_content)) {
					message_content;
				};
			};

			switch (message_content) {
				case (#KeepAliveMessage(_keep_alive_message)) {
					WS_STATE.update_last_keep_alive_timestamp_for_client(client_key);
					#Ok;
				};
				case (_) {
					return #Err("invalid keep alive message content");
				};
			};
		};
	};
};

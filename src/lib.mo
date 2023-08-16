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
import Result "mo:base/Result";
import Text "mo:base/Text";
import Time "mo:base/Time";
import Timer "mo:base/Timer";
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
	let CHECK_REGISTERED_GATEWAY_DELAY_NS : Nat = 60_000_000_000; // 60 seconds

	//// TYPES ////
	public type ClientPublicKey = Blob;

	/// The result of [ws_register].
	public type CanisterWsRegisterResult = Result.Result<(), Text>;
	/// The result of [ws_open].
	public type CanisterWsOpenResult = Result.Result<CanisterWsOpenResultValue, Text>;
	// The result of [ws_message].
	public type CanisterWsMessageResult = Result.Result<(), Text>;
	/// The result of [ws_get_messages].
	public type CanisterWsGetMessagesResult = Result.Result<CanisterOutputCertifiedMessages, Text>;
	/// The result of [ws_send].
	public type CanisterWsSendResult = Result.Result<(), Text>;
	/// The result of [ws_close].
	public type CanisterWsCloseResult = Result.Result<(), Text>;

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
		msg : Blob;
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
	type CanisterFirstMessageContent = {
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
	public type WebsocketMessage = {
		client_key : ClientPublicKey; // To or from client key.
		sequence_num : Nat64; // Both ways, messages should arrive with sequence numbers 0, 1, 2...
		timestamp : Time.Time; // Timestamp of when the message was made for the recipient to inspect.
		message : Blob; // Application message encoded in binary.
	};

	/// Element of the list of messages returned to the WS Gateway after polling.
	public type CanisterOutputMessage = {
		client_key : ClientPublicKey; // The client that the gateway will forward the message to.
		key : Text; // Key for certificate verification.
		val : Blob; // Encoded WebsocketMessage.
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
		public func update_status_index(status_index : Nat64) : Result.Result<(), Text> {
			if (status_index <= last_status_index) {
				if (status_index == 0) {
					Logger.custom_print("Gateway status index set to 0");
				} else {
					return #err("Gateway status index is equal to or behind the current one");
				};
			};
			last_status_index := status_index;
			last_heartbeat := ?get_current_time();
			#ok;
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

	shared actor class IcWebSocket(init_handlers : WsHandlers) {
		//// STATE ////
		/// Maps the client's public key to the client's identity (anonymous if not authenticated).
		private var CLIENT_CALLER_MAP = HashMap.HashMap<ClientPublicKey, Principal>(0, Blob.equal, Blob.hash);
		/// Maps the client's public key to the sequence number to use for the next outgoing message (to that client).
		private var OUTGOING_MESSAGE_TO_CLIENT_NUM_MAP = HashMap.HashMap<ClientPublicKey, Nat64>(0, Blob.equal, Blob.hash);
		/// Maps the client's public key to the expected sequence number of the next incoming message (from that client).
		private var INCOMING_MESSAGE_FROM_CLIENT_NUM_MAP = HashMap.HashMap<ClientPublicKey, Nat64>(0, Blob.equal, Blob.hash);
		/// Keeps track of the Merkle tree used for certified queries
		private var CERT_TREE_STORE : CertTree.Store = CertTree.newStore();
		private var CERT_TREE = CertTree.Ops(CERT_TREE_STORE);
		/// Keeps track of the principal of the WS Gateway which polls the canister
		private var REGISTERED_GATEWAY : ?RegisteredGateway = null;
		/// Keeps track of the messages that have to be sent to the WS Gateway
		private var MESSAGES_FOR_GATEWAY : List.List<CanisterOutputMessage> = List.nil();
		/// Keeps track of the nonce which:
		/// - the WS Gateway uses to specify the first index of the certified messages to be returned when polling
		/// - the client uses as part of the path in the Merkle tree in order to verify the certificate of the messages relayed by the WS Gateway
		private var OUTGOING_MESSAGE_NONCE : Nat64 = 0;
		/// The callback handlers for the WebSocket.
		private var HANDLERS : WsHandlers = WsHandlers(null, null, null);

		//// FUNCTIONS ////
		/// Resets all RefCells to their initial state.
		/// If there is a registered gateway, resets its state as well.
		func reset_internal_state() : async () {
			// for each client, call the on_close handler before clearing the map
			for (client_key in CLIENT_CALLER_MAP.keys()) {
				await HANDLERS.call_on_close({
					client_key;
				});
			};
			CLIENT_CALLER_MAP := HashMap.HashMap<ClientPublicKey, Principal>(0, Blob.equal, Blob.hash);

			OUTGOING_MESSAGE_TO_CLIENT_NUM_MAP := HashMap.HashMap<ClientPublicKey, Nat64>(0, Blob.equal, Blob.hash);
			INCOMING_MESSAGE_FROM_CLIENT_NUM_MAP := HashMap.HashMap<ClientPublicKey, Nat64>(0, Blob.equal, Blob.hash);
			CERT_TREE_STORE := CertTree.newStore();
			CERT_TREE := CertTree.Ops(CERT_TREE_STORE);
			REGISTERED_GATEWAY := null;
			MESSAGES_FOR_GATEWAY := List.nil<CanisterOutputMessage>();
			OUTGOING_MESSAGE_NONCE := 0;
		};

		/// Resets the internal state of the IC WebSocket CDK.
		///
		/// **Note:** You should only call this function in tests.
		public func wipe() : async () {
			await reset_internal_state();

			// if there's a registered gateway, reset its state
			switch (REGISTERED_GATEWAY) {
				case (?registered_gateway) {
					registered_gateway.reset();
				};
				case (null) {
					// Do nothing.
				};
			};

			Logger.custom_print("Internal state has been wiped!");
		};

		func get_outgoing_message_nonce() : Nat64 {
			OUTGOING_MESSAGE_NONCE;
		};

		func increment_outgoing_message_nonce() {
			OUTGOING_MESSAGE_NONCE += 1;
		};

		func put_client_caller(client_key : ClientPublicKey, caller : Principal) {
			CLIENT_CALLER_MAP.put(client_key, caller);
		};

		func get_client_caller(client_key : ClientPublicKey) : ?Principal {
			CLIENT_CALLER_MAP.get(client_key);
		};

		func initialize_registered_gateway(gw_principal : Text) {
			let gateway_principal = Principal.fromText(gw_principal);
			REGISTERED_GATEWAY := Option.make(RegisteredGateway(gateway_principal));
		};

		func get_registered_gateway_principal() : ?Principal {
			switch (REGISTERED_GATEWAY) {
				case (?registered_gateway) {
					Option.make(registered_gateway.gateway_principal);
				};
				case (null) null;
			};
		};

		/// Updates the registered gateway with the new status index.
		/// If the status index is not greater than the current one, the function returns an error.
		func update_registered_gateway_status_index(status_index : Nat64) : async Result.Result<(), Text> {
			switch (REGISTERED_GATEWAY) {
				case (?registered_gateway) {
					// if the current status index is > 0 and the new status index is 0, it means that the gateway has been restarted
					// in this case, we reset the internal state because all clients are not connected to the gateway anymore
					if (registered_gateway.last_status_index > 0 and status_index == 0) {
						await reset_internal_state();

						registered_gateway.reset();

						#ok;
					} else {
						registered_gateway.update_status_index(status_index);
					};
				};
				case (null) #err("no gateway registered");
			};
		};

		func check_registered_client_key(client_key : ClientPublicKey) : Result.Result<(), Text> {
			if (Option.isNull(CLIENT_CALLER_MAP.get(client_key))) {
				return #err("client's public key has not been previously registered by client");
			};

			#ok;
		};

		func init_outgoing_message_to_client_num(client_key : ClientPublicKey) {
			OUTGOING_MESSAGE_TO_CLIENT_NUM_MAP.put(client_key, 0);
		};

		func get_outgoing_message_to_client_num(client_key : ClientPublicKey) : Result.Result<Nat64, Text> {
			switch (OUTGOING_MESSAGE_TO_CLIENT_NUM_MAP.get(client_key)) {
				case (?num) #ok(num);
				case (null) #err("outgoing message to client num not initialized for client");
			};
		};

		func increment_outgoing_message_to_client_num(client_key : ClientPublicKey) : Result.Result<(), Text> {
			let num = get_outgoing_message_to_client_num(client_key);
			switch (num) {
				case (#ok(num)) {
					OUTGOING_MESSAGE_TO_CLIENT_NUM_MAP.put(client_key, num + 1);
					#ok;
				};
				case (#err(error)) #err(error);
			};
		};

		func init_expected_incoming_message_from_client_num(client_key : ClientPublicKey) {
			INCOMING_MESSAGE_FROM_CLIENT_NUM_MAP.put(client_key, 0);
		};

		func get_expected_incoming_message_from_client_num(client_key : ClientPublicKey) : Result.Result<Nat64, Text> {
			switch (INCOMING_MESSAGE_FROM_CLIENT_NUM_MAP.get(client_key)) {
				case (?num) #ok(num);
				case (null) #err("expected incoming message from client num not initialized for client");
			};
		};

		func increment_expected_incoming_message_from_client_num(client_key : ClientPublicKey) : Result.Result<(), Text> {
			let num = get_expected_incoming_message_from_client_num(client_key);
			switch (num) {
				case (#ok(num)) {
					INCOMING_MESSAGE_FROM_CLIENT_NUM_MAP.put(client_key, num + 1);
					#ok;
				};
				case (#err(error)) #err(error);
			};
		};

		func add_client(client_key : ClientPublicKey) {
			// initialize incoming client's message sequence number to 0
			init_expected_incoming_message_from_client_num(client_key);
			// initialize outgoing message sequence number to 0
			init_outgoing_message_to_client_num(client_key);
		};

		func remove_client(client_key : ClientPublicKey) {
			CLIENT_CALLER_MAP.delete(client_key);
			OUTGOING_MESSAGE_TO_CLIENT_NUM_MAP.delete(client_key);
			INCOMING_MESSAGE_FROM_CLIENT_NUM_MAP.delete(client_key);
		};

		func get_message_for_gateway_key(gateway_principal : Principal, nonce : Nat64) : Text {
			let nonce_to_text = do {
				// prints the nonce with 20 padding zeros
				var nonce_str = Nat64.toText(nonce);
				let padding : Nat = 20 - Text.size(nonce_str);
				if (padding > 0) {
					for (i in Iter.range(0, padding)) {
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
				let partitions = List.partition(MESSAGES_FOR_GATEWAY, func(el : CanisterOutputMessage) : Bool { Text.less(el.key, smallest_key) });
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
			for (i in Iter.range(start_index, end_index)) {
				let message = List.get(MESSAGES_FOR_GATEWAY, i);
				switch (message) {
					case (?message) {
						messages := List.push(message, messages);
					};
					case (null) Prelude.unreachable();
				};
			};

			messages;
		};

		func get_cert_messages(gateway_principal : Principal, nonce : Nat64) : CanisterWsGetMessagesResult {
			let (start_index, end_index) = get_messages_for_gateway_range(gateway_principal, nonce);
			let messages = get_messages_for_gateway(start_index, end_index);

			if (List.isNil(messages)) {
				return #ok({
					messages = [];
					cert = Blob.fromArray([]);
					tree = Blob.fromArray([]);
				});
			};

			let first_key = switch (List.get(messages, 0)) {
				case (?message) message.key;
				case (null) "";
			};
			let last_key = switch (List.last(messages)) {
				case (?message) message.key;
				case (null) "";
			};
			// let (cert, tree) = get_cert_for_range(first_key, last_key);
			let (cert, tree) = (Blob.fromArray([]), Blob.fromArray([]));

			#ok({
				messages = List.toArray(messages);
				cert = cert;
				tree = tree;
			});
		};

		/// Checks if the caller of the method is the same as the one that was registered during the initialization of the CDK
		func check_is_registered_gateway(input_principal : Principal) : Result.Result<(), Text> {
			let gateway_principal = get_registered_gateway_principal();
			// check if the caller is the same as the one that was registered during the initialization of the CDK
			switch gateway_principal {
				case (?gateway_principal) {
					if (Principal.notEqual(gateway_principal, input_principal)) {
						return #err("caller is not the gateway that has been registered during CDK initialization");
					};

					#ok;
				};
				case (null) {
					#err("no gateway registered");
				};
			};
		};

		func labeledHash(l : Blob, content : CertTree.Hash) : CertTree.Hash {
			let d = Sha256.Digest(#sha256);
			d.writeBlob("\13ic-hashtree-labeled");
			d.writeBlob(l);
			d.writeBlob(content);
			d.sum();
		};

		func put_cert_for_message(key : Text, value : Blob) {
			let root_hash = do {
				CERT_TREE.put([Text.encodeUtf8(key)], Sha256.fromBlob(#sha256, value));
				labeledHash(LABEL_WEBSOCKET, CERT_TREE.treeHash());
			};

			CertifiedData.set(root_hash);
		};

		func get_cert_for_range(first : Text, last : Text) : (Blob, Blob) {
			let witness = CERT_TREE.reveals(Iter.fromArray([[Text.encodeUtf8(first)], [Text.encodeUtf8(last)]]));
			let tree : CertTree.Witness = #labeled(LABEL_WEBSOCKET, witness);

			switch (CertifiedData.getCertificate()) {
				case (?cert) {
					let tree_blob = CERT_TREE.encodeWitness(tree);
					(cert, tree_blob);
				};
				case (null) Prelude.unreachable();
			};
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
			switch (REGISTERED_GATEWAY) {
				case (?registered_gateway) {
					switch (registered_gateway.last_heartbeat) {
						case (?last_heartbeat) {
							if (get_current_time() - last_heartbeat > get_check_registered_gateway_delay_ns()) {
								Logger.custom_print("[timer-cb]: Registered gateway has not sent a heartbeat for more than" # debug_show (get_check_registered_gateway_delay_ns() / 1_000_000_000) # "seconds, resetting all internal state");

								await reset_internal_state();

								registered_gateway.reset();
							};
						};
						case (null) {
							Logger.custom_print("[timer-cb]: Registered gateway has not sent a heartbeat yet");
						};
					};
				};
				case (null) {
					Logger.custom_print("[timer-cb]: No registered gateway");
				};
			};

			schedule_registered_gateway_check();
		};

		func initialize_handlers(handlers : WsHandlers) {
			HANDLERS := handlers;
		};

		/// Initialize the CDK by setting the callback handlers and the **principal** of the WS Gateway that
		/// will be polling the canister
		public func init(gateway_principal : Text) : async () {
			// set the handlers specified by the canister that the CDK uses to manage the IC WebSocket connection
			initialize_handlers(init_handlers);

			// set the principal of the (only) WS Gateway that will be polling the canister
			initialize_registered_gateway(gateway_principal);

			// schedule a timer that will check if the registered gateway is still alive
			schedule_registered_gateway_check();
		};

		/// Handles the register event received from the client.
		///
		/// Registers the public key that the client SDK has generated to initialize an IcWebSocket connection.
		public func ws_register(caller : Principal, args : CanisterWsRegisterArguments) : async CanisterWsRegisterResult {
			// TODO: check who is the caller, which can be a client or the anonymous principal

			// associate the identity of the client to its public key received as input
			put_client_caller(args.client_key, caller);
			#ok;
		};

		/// Handles the WS connection open event received from the WS Gateway
		///
		/// WS Gateway relays the first message sent by the client together with its signature
		/// to prove that the first message is actually coming from the same client that registered its public key
		/// beforehand by calling the [ws_register] method.
		public func ws_open(caller : Principal, args : CanisterWsOpenArguments) : async CanisterWsOpenResult {
			// the caller must be the gateway that was registered during CDK initialization
			switch (check_is_registered_gateway(caller)) {
				case (#err(err)) {
					#err(err);
				};
				case (_) {
					// decode the first message sent by the client
					let canister_first_message_content : ?CanisterFirstMessageContent = from_candid (args.msg);
					switch (canister_first_message_content) {
						case (null) {
							#err("invalid first message");
						};
						case (?{ client_key; canister_id }) {
							// TODO: parse public key and verify signature

							switch (check_registered_client_key(client_key)) {
								case (#err(err)) {
									#err(err);
								};
								case (_) {
									add_client(client_key);

									#ok({
										client_key;
										canister_id;
										nonce = get_outgoing_message_nonce();
									});
								};
							};
						};
					};
				};
			};
		};

		/// Handles the WS connection close event received from the WS Gateway.
		public func ws_close(caller : Principal, args : CanisterWsCloseArguments) : async CanisterWsCloseResult {
			switch (check_is_registered_gateway(caller)) {
				case (#err(err)) {
					#err(err);
				};
				case (_) {
					switch (check_registered_client_key(args.client_key)) {
						case (#err(err)) {
							#err(err);
						};
						case (_) {
							remove_client(args.client_key);

							await HANDLERS.call_on_close({
								client_key = args.client_key;
							});

							#ok;
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
					switch (get_client_caller(received_message.client_key)) {
						case (null) {
							#err("client is not registered, call ws_register first");
						};
						case (?expected_caller) {
							if (caller != expected_caller) {
								return #err("caller is not the same that registered the public key");
							};

							await HANDLERS.call_on_message({
								client_key = received_message.client_key;
								message = received_message.message;
							});

							#ok;
						};
					};
				};
				// WS Gateway relays a message from the client
				case (#RelayedByGateway(received_message)) {
					// this message can come only from the registered gateway
					switch (check_is_registered_gateway(caller)) {
						case (#err(err)) {
							#err(err);
						};
						case (_) {
							let websocket_message : ?WebsocketMessage = from_candid (received_message.content);
							switch (websocket_message) {
								case (null) {
									#err("deserialization failed");
								};
								case (?{ client_key; sequence_num; message }) {
									switch (check_registered_client_key(client_key)) {
										case (#err(err)) {
											#err(err);
										};
										case (_) {
											// TODO: parse public key and verify signature

											let expected_sequence_num = get_expected_incoming_message_from_client_num(client_key);
											switch (expected_sequence_num) {
												case (#err(err)) {
													#err(err);
												};
												case (#ok(expected_sequence_num)) {
													if (sequence_num == expected_sequence_num) {
														// increase the expected sequence number by 1
														switch (increment_expected_incoming_message_from_client_num(client_key)) {
															case (#err(err)) {
																#err(err);
															};
															case (_) {
																// trigger the on_message handler initialized by canister
																// create message to send to client
																await HANDLERS.call_on_message({
																	client_key = client_key;
																	message = message;
																});

																#ok;
															};
														};
													} else {
														#err("incoming client's message relayed from WS Gateway does not have the expected sequence number");
													};
												};
											};
										};
									};
								};
							};
						};
					};
				};
				// WS Gateway notifies the canister of the established IC WebSocket connection
				case (#IcWebSocketEstablished(client_key)) {
					// this message can come only from the registered gateway
					switch (check_is_registered_gateway(caller)) {
						case (#err(err)) {
							#err(err);
						};
						case (_) {
							// check if client registered its public key by calling ws_register
							switch (check_registered_client_key(client_key)) {
								case (#err(err)) {
									#err(err);
								};
								case (_) {
									Logger.custom_print("Can start notifying client with key: " # debug_show (client_key));

									// call the on_open handler
									await HANDLERS.call_on_open({
										client_key;
									});

									#ok;
								};
							};
						};
					};
				};
				// WS Gateway notifies the canister that it is up and running
				case (#IcWebSocketGatewayStatus(gateway_status)) {
					// this message can come only from the registered gateway
					switch (check_is_registered_gateway(caller)) {
						case (#err(err)) {
							#err(err);
						};
						case (_) {
							await update_registered_gateway_status_index(gateway_status.status_index);
						};
					};
				};
			};
		};

		/// Returns messages to the WS Gateway in response of a polling iteration.
		public func ws_get_messages(caller : Principal, args : CanisterWsGetMessagesArguments) : async CanisterWsGetMessagesResult {
			// check if the caller of this method is the WS Gateway that has been set during the initialization of the SDK
			switch (check_is_registered_gateway(caller)) {
				case (#err(err)) {
					#err(err);
				};
				case (_) {
					get_cert_messages(caller, args.nonce);
				};
			};
		};

		/// Sends a message to the client.
		///
		/// Under the hood, the message is serialized and certified, and then it is added to the queue of messages
		/// that the WS Gateway will poll in the next iteration.
		/// **Note**: you have to serialize the message to a `Blob` before calling this method. Use the `to_candid` function.
		public func ws_send(client_key : ClientPublicKey, msg_cbor : Blob) : async CanisterWsSendResult {
			// check if the client is registered
			switch (check_registered_client_key(client_key)) {
				case (#err(err)) {
					#err(err);
				};
				case (_) {
					// get the principal of the gateway that is polling the canister
					switch (get_registered_gateway_principal()) {
						case (null) {
							#err("No gateway registered");
						};
						case (?gateway_principal) {
							// the nonce in key is used by the WS Gateway to determine the message to start in the polling iteration
							// the key is also passed to the client in order to validate the body of the certified message
							let outgoing_message_nonce = get_outgoing_message_nonce();
							let key = get_message_for_gateway_key(gateway_principal, outgoing_message_nonce);

							// increment the nonce for the next message
							increment_outgoing_message_nonce();

							// increment the sequence number for the next message to the client
							switch (increment_outgoing_message_to_client_num(client_key)) {
								case (#err(err)) {
									#err(err);
								};
								case (_) {
									switch (get_outgoing_message_to_client_num(client_key)) {
										case (#err(err)) {
											#err(err);
										};
										case (#ok(sequence_num)) {
											let input : WebsocketMessage = {
												client_key;
												sequence_num;
												timestamp = get_current_time();
												message = msg_cbor;
											};

											// serialize the message of type WebsocketMessage
											let data = to_candid (input);

											// certify data
											put_cert_for_message(key, data);

											MESSAGES_FOR_GATEWAY := List.append(MESSAGES_FOR_GATEWAY, List.fromArray([{ client_key; key; val = data }]));

											#ok;
										};
									};
								};
							};
						};
					};
				};
			};
		};
	};
};

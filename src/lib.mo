/// IC WebSocket CDK Motoko Library

import Array "mo:base/Array";
import Blob "mo:base/Blob";
import CertifiedData "mo:base/CertifiedData";
import Debug "mo:base/Debug";
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
import TrieSet "mo:base/TrieSet";
import Result "mo:base/Result";
import CborValue "mo:cbor/Value";
import CborDecoder "mo:cbor/Decoder";
import CborEncoder "mo:cbor/Encoder";
import CertTree "mo:ic-certification/CertTree";
import Sha256 "mo:sha2/Sha256";

import State "State";
import Types "Types";
import Utils "Utils";
import Timers "Timers";
import Errors "Errors";

module {
	//// TYPES ////
	// re-export types
	public type CanisterCloseResult = Types.CanisterCloseResult;
	public type CanisterSendResult = Types.CanisterSendResult;
	public type CanisterWsCloseArguments = Types.CanisterWsCloseArguments;
	public type CanisterWsCloseResult = Types.CanisterWsCloseResult;
	public type CanisterWsGetMessagesArguments = Types.CanisterWsGetMessagesArguments;
	public type CanisterWsGetMessagesResult = Types.CanisterWsGetMessagesResult;
	public type CanisterWsMessageArguments = Types.CanisterWsMessageArguments;
	public type CanisterWsMessageResult = Types.CanisterWsMessageResult;
	public type CanisterWsOpenArguments = Types.CanisterWsOpenArguments;
	public type CanisterWsOpenResult = Types.CanisterWsOpenResult;
	/// @deprecated Use [`CanisterSendResult`] instead.
	public type CanisterWsSendResult = Types.CanisterWsSendResult;
	public type ClientPrincipal = Types.ClientPrincipal;
	public type OnCloseCallbackArgs = Types.OnCloseCallbackArgs;
	public type OnMessageCallbackArgs = Types.OnMessageCallbackArgs;
	public type OnOpenCallbackArgs = Types.OnOpenCallbackArgs;

	// these classes cannot be re-exported
	type WsHandlers = Types.WsHandlers;
	type WsInitParams = Types.WsInitParams;
	type IcWebSocketState = State.IcWebSocketState;

	/// The IC WebSocket instance.
	///
	/// **Note**: Restarts the acknowledgement timers under the hood.
	///
	/// # Traps
	/// If the parameters are invalid. See [`WsInitParams::check_validity`] for more details.
	public class IcWebSocket(init_ws_state : IcWebSocketState, params : WsInitParams, handlers : WsHandlers) {
		/// The state of the IC WebSocket.
		private var WS_STATE : IcWebSocketState = init_ws_state;

		// the equivalent of the [init] function for the Rust CDK
		do {
			// check if the parameters are valid
			params.check_validity();

			// cancel initial timers
			Timers.cancel_timers(WS_STATE);

			// schedule a timer that will send an acknowledgement message to clients
			Timers.schedule_send_ack_to_clients(WS_STATE, params.send_ack_interval_ms, handlers);
		};

		/// Handles the WS connection open event sent by the client and relayed by the Gateway.
		public func ws_open(caller : Principal, args : CanisterWsOpenArguments) : async CanisterWsOpenResult {
			// anonymous clients cannot open a connection
			if (Principal.isAnonymous(caller)) {
				return #Err(Errors.to_string(#AnonymousPrincipalNotAllowed));
			};

			let client_key : Types.ClientKey = {
				client_principal = caller;
				client_nonce = args.client_nonce;
			};
			// check if client is not registered yet
			// by swapping the result of the check_registered_client_exists function
			switch (WS_STATE.check_registered_client_exists(client_key)) {
				case (#Err(err)) {
					// do nothing
				};
				case (#Ok(_)) {
					return #Err(Errors.to_string(#ClientKeyAlreadyConnected({ client_key })));
				};
			};

			// check if there's a client already registered with the same principal
			// and remove it if there is
			switch (WS_STATE.get_client_key_from_principal(client_key.client_principal)) {
				case (#Err(err)) {
					// Do nothing
				};
				case (#Ok(old_client_key)) {
					await WS_STATE.remove_client(old_client_key, ?handlers, null);
				};
			};

			// initialize client maps
			let new_client = Types.RegisteredClient(args.gateway_principal);
			WS_STATE.add_client(client_key, new_client);

			let open_message : Types.CanisterOpenMessageContent = {
				client_key;
			};
			let message : Types.WebsocketServiceMessageContent = #OpenMessage(open_message);
			switch (WS_STATE.send_service_message_to_client(client_key, message)) {
				case (#Err(err)) {
					return #Err(err);
				};
				case (#Ok(_)) {
					// do nothing
				};
			};

			await handlers.call_on_open({
				client_principal = client_key.client_principal;
			});

			#Ok;
		};

		/// Handles the WS connection close event received from the WS Gateway.
		///
		/// If you want to close the connection with the client in your logic,
		/// use the [close] function instead.
		public func ws_close(caller : Principal, args : CanisterWsCloseArguments) : async CanisterWsCloseResult {
			let gateway_principal = caller;

			// check if the gateway is registered
			switch (WS_STATE.check_is_gateway_registered(gateway_principal)) {
				case (#Err(err)) {
					return #Err(err);
				};
				case (_) {
					// do nothing
				};
			};

			// check if client registered itself by calling ws_open
			switch (WS_STATE.check_registered_client_exists(args.client_key)) {
				case (#Err(err)) {
					return #Err(err);
				};
				case (_) {
					// do nothing
				};
			};

			// check if the client is registered to the gateway that is closing the connection
			switch (WS_STATE.check_client_registered_to_gateway(args.client_key, gateway_principal)) {
				case (#Err(err)) {
					return #Err(err);
				};
				case (_) {
					// do nothing
				};
			};

			await WS_STATE.remove_client(args.client_key, ?handlers, null);

			#Ok;
		};

		/// Handles the WS messages received either directly from the client or relayed by the WS Gateway.
		///
		/// The second argument is only needed to expose the type of the message on the canister Candid interface and get automatic types generation on the client side.
		/// This way, on the client you have the same types and you don't have to care about serializing and deserializing the messages sent through IC WebSocket.
		///
		/// # Example
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
		///   // declare also the other methods: ws_open, ws_close, ws_get_messages
		///
		///   public shared ({ caller }) func ws_message(args : IcWebSocketCdk.CanisterWsMessageArguments, msg_type : ?MyMessage) : async IcWebSocketCdk.CanisterWsMessageResult {
		///     await ws.ws_message(caller, args, msg_type);
		///   };
		///
		///   // ...
		/// }
		/// ```
		public func ws_message(caller : Principal, args : CanisterWsMessageArguments, _msg_type : ?Any) : async CanisterWsMessageResult {
			let client_principal = caller;
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
			if (not Types.areClientKeysEqual(registered_client_key, client_key)) {
				return #Err(Errors.to_string(#ClientKeyMessageMismatch({ client_key })));
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
				await WS_STATE.remove_client(client_key, ?handlers, ? #WrongSequenceNumber);
				return #Err(Errors.to_string(#IncomingSequenceNumberWrong({ expected_sequence_num; actual_sequence_num = sequence_num })));
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
				return await WS_STATE.handle_received_service_message(client_key, content);
			};

			await handlers.call_on_message({
				client_principal = client_key.client_principal;
				message = content;
			});
			#Ok;
		};

		/// Returns messages to the WS Gateway in response of a polling iteration.
		public func ws_get_messages(caller : Principal, args : CanisterWsGetMessagesArguments) : CanisterWsGetMessagesResult {
			let gateway_principal = caller;
			if (not WS_STATE.is_registered_gateway(gateway_principal)) {
				return WS_STATE.get_cert_messages_empty();
			};

			WS_STATE.get_cert_messages(caller, args.nonce, params.max_number_of_returned_messages);
		};

		/// Sends a message to the client. See [IcWebSocketCdk.send] function for reference.
		public func send(client_principal : ClientPrincipal, msg_bytes : Blob) : async CanisterWsSendResult {
			WS_STATE._ws_send_to_client_principal(client_principal, msg_bytes);
		};

		/// Closes the connection with the client.
		///
		/// This function **must not** be called in the `on_close` callback.
		public func close(client_principal : ClientPrincipal) : async CanisterCloseResult {
			await WS_STATE._close_for_client_principal(client_principal, ?handlers);
		};

		/// Resets the internal state of the IC WebSocket CDK.
		///
		/// **Note:** You should only call this function in tests.
		public func wipe() : async () {
			await WS_STATE.reset_internal_state(handlers);

			Utils.custom_print("Internal state has been wiped!");
		};
	};

	/// Sends a message to the client. The message must already be serialized **using Candid**.
	/// Use [`to_candid`] to serialize the message.
	///
	/// Under the hood, the message is certified and added to the queue of messages
	/// that the WS Gateway will poll in the next iteration.
	///
	/// # Example
	/// This example is the serialize equivalent of the [`OnMessageCallbackArgs`]'s deserialize one.
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
	///   // at some point in your code
	///   let msg : MyMessage = {
	///     some_field: "Hello, World!";
	///   };
	///
	///   IcWebSocketCdk.send(ws_state, client_principal, to_candid(msg));
	/// }
	/// ```
	public func send(ws_state : IcWebSocketState, client_principal : ClientPrincipal, msg_bytes : Blob) : async CanisterSendResult {
		ws_state._ws_send_to_client_principal(client_principal, msg_bytes);
	};

	/// @deprecated Use [`send`] instead.
	public func ws_send(ws_state : IcWebSocketState, client_principal : ClientPrincipal, msg_bytes : Blob) : async CanisterWsSendResult {
		await send(ws_state, client_principal, msg_bytes);
	};

	/// Closes the connection with the client.
	/// It can be used in a similar way as the [`IcWebSocketCdk.send`] function.
	///
	/// This function **must not** be called in the `on_close` callback
	/// and **doesn't** trigger the `on_close` callback.
	public func close(ws_state : IcWebSocketState, client_principal : ClientPrincipal) : async CanisterCloseResult {
		await ws_state._close_for_client_principal(client_principal, null);
	};
};

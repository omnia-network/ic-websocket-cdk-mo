import Timer "mo:base/Timer";
import Nat64 "mo:base/Nat64";
import Array "mo:base/Array";
import TrieSet "mo:base/TrieSet";

import Constants "Constants";
import Types "Types";
import State "State";
import Utils "Utils";

module {
  func put_ack_timer_id(ws_state : State.IcWebSocketState, timer_id : Timer.TimerId) {
    ws_state.ACK_TIMER := ?timer_id;
  };

  func cancel_ack_timer(ws_state : State.IcWebSocketState) {
    switch (ws_state.ACK_TIMER) {
      case (?t_id) {
        Timer.cancelTimer(t_id);
        ws_state.ACK_TIMER := null;
      };
      case (null) {
        // Do nothing
      };
    };
  };

  func put_keep_alive_timer_id(ws_state : State.IcWebSocketState, timer_id : Timer.TimerId) {
    ws_state.KEEP_ALIVE_TIMER := ?timer_id;
  };

  func cancel_keep_alive_timer(ws_state : State.IcWebSocketState) {
    switch (ws_state.KEEP_ALIVE_TIMER) {
      case (?t_id) {
        Timer.cancelTimer(t_id);
        ws_state.KEEP_ALIVE_TIMER := null;
      };
      case (null) {
        // Do nothing
      };
    };
  };

  public func cancel_timers(ws_state : State.IcWebSocketState) {
    cancel_ack_timer(ws_state);
    cancel_keep_alive_timer(ws_state);
  };

  /// Start an interval to send an acknowledgement messages to the clients.
  ///
  /// The interval callback is [send_ack_to_clients_timer_callback]. After the callback is executed,
  /// a timer is scheduled to check if the registered clients have sent a keep alive message.
  public func schedule_send_ack_to_clients(ws_state : State.IcWebSocketState, ack_interval_ms : Nat64, handlers : Types.WsHandlers) {
    let timer_id = Timer.recurringTimer(
      #nanoseconds(Nat64.toNat(ack_interval_ms * 1_000_000)),
      func() : async () {
        send_ack_to_clients_timer_callback(ws_state, ack_interval_ms);

        schedule_check_keep_alive(ws_state, handlers);
      },
    );

    put_ack_timer_id(ws_state, timer_id);
  };

  /// Schedules a timer to check if the clients (only those to which an ack message was sent) have sent a keep alive message
  /// after receiving an acknowledgement message.
  ///
  /// The timer callback is [check_keep_alive_timer_callback].
  func schedule_check_keep_alive(ws_state : State.IcWebSocketState, handlers : Types.WsHandlers) {
    let timer_id = Timer.setTimer(
      #nanoseconds(Nat64.toNat(Constants.Computed().CLIENT_KEEP_ALIVE_TIMEOUT_NS)),
      func() : async () {
        await check_keep_alive_timer_callback(ws_state, handlers);
      },
    );

    put_keep_alive_timer_id(ws_state, timer_id);
  };

  /// Sends an acknowledgement message to the client.
  /// The message contains the current incoming message sequence number for that client,
  /// so that the client knows that all the messages it sent have been received by the canister.
  func send_ack_to_clients_timer_callback(ws_state : State.IcWebSocketState, ack_interval_ms : Nat64) {
    for (client_key in ws_state.REGISTERED_CLIENTS.keys()) {
      // ignore the error, which shouldn't happen since the client is registered and the sequence number is initialized
      switch (ws_state.get_expected_incoming_message_from_client_num(client_key)) {
        case (#Ok(expected_incoming_sequence_num)) {
          let ack_message : Types.CanisterAckMessageContent = {
            // the expected sequence number is 1 more because it's incremented when a message is received
            last_incoming_sequence_num = expected_incoming_sequence_num - 1;
          };
          let message : Types.WebsocketServiceMessageContent = #AckMessage(ack_message);
          switch (ws_state.send_service_message_to_client(client_key, message)) {
            case (#Err(err)) {
              // TODO: decide what to do when sending the message fails

              Utils.custom_print("[ack-to-clients-timer-cb]: Error sending ack message to client" # Types.clientKeyToText(client_key) # ": " # err);
            };
            case (#Ok(_)) {
              ws_state.add_client_to_wait_for_keep_alive(client_key);
            };
          };
        };
        case (#Err(err)) {
          // TODO: decide what to do when getting the expected incoming sequence number fails (shouldn't happen)
          Utils.custom_print("[ack-to-clients-timer-cb]: Error getting expected incoming sequence number for client" # Types.clientKeyToText(client_key) # ": " # err);
        };
      };
    };

    Utils.custom_print("[ack-to-clients-timer-cb]: Sent ack messages to all clients");
  };

  /// Checks if the clients for which we are waiting for keep alive have sent a keep alive message.
  /// If a client has not sent a keep alive message, it is removed from the connected clients.
  ///
  /// Before checking the clients, it removes all the empty expired gateways from the list of registered gateways.
  func check_keep_alive_timer_callback(ws_state : State.IcWebSocketState, handlers : Types.WsHandlers) : async () {
    ws_state.remove_empty_expired_gateways();

    for (client_key in Array.vals(TrieSet.toArray(ws_state.CLIENTS_WAITING_FOR_KEEP_ALIVE))) {
      // get the last keep alive timestamp for the client and check if it has exceeded the timeout
      switch (ws_state.REGISTERED_CLIENTS.get(client_key)) {
        case (?client_metadata) {
          let last_keep_alive = client_metadata.get_last_keep_alive_timestamp();

          if (Utils.get_current_time() - last_keep_alive > Constants.Computed().CLIENT_KEEP_ALIVE_TIMEOUT_NS) {
            await ws_state.remove_client(client_key, ?handlers, ? #KeepAliveTimeout);

            Utils.custom_print("[check-keep-alive-timer-cb]: Client " # Types.clientKeyToText(client_key) # " has not sent a keep alive message in the last " # debug_show (Constants.Computed().CLIENT_KEEP_ALIVE_TIMEOUT_MS) # " ms and has been removed");
          };
        };
        case (null) {
          // Do nothing
        };
      };
    };

    Utils.custom_print("[check-keep-alive-timer-cb]: Checked keep alive messages for all clients");
  };
};

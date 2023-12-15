module {
  /// The label used when constructing the certification tree.
  public let LABEL_WEBSOCKET : Blob = "websocket";
  /// The default maximum number of messages returned by [ws_get_messages] at each poll.
  public let DEFAULT_MAX_NUMBER_OF_RETURNED_MESSAGES : Nat = 50;
  /// The default interval at which to send acknowledgements to the client.
  public let DEFAULT_SEND_ACK_INTERVAL_MS : Nat64 = 300_000; // 5 minutes
  /// The maximum communication latency allowed between the client and the canister.
  public let COMMUNICATION_LATENCY_BOUND_MS : Nat64 = 30_000; // 30 seconds
  public class Computed() {
    /// The default timeout to wait for the client to send a keep alive after receiving an acknowledgement.
    public let CLIENT_KEEP_ALIVE_TIMEOUT_MS : Nat64 = 2 * COMMUNICATION_LATENCY_BOUND_MS;
    /// Same as [CLIENT_KEEP_ALIVE_TIMEOUT_MS], but in nanoseconds.
    public let CLIENT_KEEP_ALIVE_TIMEOUT_NS : Nat64 = CLIENT_KEEP_ALIVE_TIMEOUT_MS * 1_000_000;
  };

  /// The initial nonce for outgoing messages.
  public let INITIAL_OUTGOING_MESSAGE_NONCE : Nat64 = 0;
  /// The initial sequence number to expect from messages coming from clients.
  /// The first message coming from the client will have sequence number `1` because on the client the sequence number is incremented before sending the message.
  public let INITIAL_CLIENT_SEQUENCE_NUM : Nat64 = 1;
  /// The initial sequence number for outgoing messages.
  public let INITIAL_CANISTER_SEQUENCE_NUM : Nat64 = 0;

  /// The number of messages to delete from the outgoing messages queue every time a new message is added.
  public let MESSAGES_TO_DELETE_COUNT : Nat = 5;
};

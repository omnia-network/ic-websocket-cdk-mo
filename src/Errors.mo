import Principal "mo:base/Principal";
import Nat64 "mo:base/Nat64";

import Types "Types";

module {
  public type WsError = {
    #AnonymousPrincipalNotAllowed;
    #ClientKeyAlreadyConnected : {
      client_key : Types.ClientKey;
    };
    #ClientKeyMessageMismatch : {
      client_key : Types.ClientKey;
    };
    #ClientKeyNotConnected : {
      client_key : Types.ClientKey;
    };
    #ClientNotRegisteredToGateway : {
      client_key : Types.ClientKey;
      gateway_principal : Types.GatewayPrincipal;
    };
    #ClientPrincipalNotConnected : {
      client_principal : Types.ClientPrincipal;
    };
    #DecodeServiceMessageContent : {
      err : Text;
    };
    #ExpectedIncomingMessageToClientNumNotInitialized : {
      client_key : Types.ClientKey;
    };
    #GatewayNotRegistered : {
      gateway_principal : Types.GatewayPrincipal;
    };
    #InvalidServiceMessage;
    #IncomingSequenceNumberWrong : {
      expected_sequence_num : Nat64;
      actual_sequence_num : Nat64;
    };
    #OutgoingMessageToClientNumNotInitialized : {
      client_key : Types.ClientKey;
    };
  };

  public func to_string(err : WsError) : Text {
    switch (err) {
      case (#AnonymousPrincipalNotAllowed) {
        "Anonymous principal is not allowed";
      };
      case (#ClientKeyAlreadyConnected({ client_key })) {
        "Client with key " # Types.clientKeyToText(client_key) # " already has an open connection";
      };
      case (#ClientKeyMessageMismatch({ client_key })) {
        "Client with principal " # Principal.toText(client_key.client_principal) # " has a different key than the one used in the message";
      };
      case (#ClientKeyNotConnected({ client_key })) {
        "Client with key " # Types.clientKeyToText(client_key) # " doesn't have an open connection";
      };
      case (#ClientNotRegisteredToGateway({ client_key; gateway_principal })) {
        "Client with key " # Types.clientKeyToText(client_key) # " was not registered to gateway " # Principal.toText(gateway_principal);
      };
      case (#ClientPrincipalNotConnected({ client_principal })) {
        "Client with principal " # Principal.toText(client_principal) # " doesn't have an open connection";
      };
      case (#DecodeServiceMessageContent({ err })) {
        "Error decoding service message content: " # err;
      };
      case (#ExpectedIncomingMessageToClientNumNotInitialized({ client_key })) {
        "Expected incoming message to client num not initialized for client key " # Types.clientKeyToText(client_key);
      };
      case (#GatewayNotRegistered({ gateway_principal })) {
        "Gateway with principal " # Principal.toText(gateway_principal) # " is not registered";
      };
      case (#InvalidServiceMessage) {
        "Invalid service message";
      };
      case (#IncomingSequenceNumberWrong({ expected_sequence_num; actual_sequence_num })) {
        "Expected incoming sequence number " # Nat64.toText(expected_sequence_num) # " but got " # Nat64.toText(actual_sequence_num);
      };
      case (#OutgoingMessageToClientNumNotInitialized({ client_key })) {
        "Outgoing message to client num not initialized for client key " # Types.clientKeyToText(client_key);
      };
    };
  };

  public func to_string_result(err : WsError) : Types.Result<(), Text> {
    #Err(to_string(err));
  };
};

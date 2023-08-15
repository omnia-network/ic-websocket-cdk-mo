import IcWebsocketCdk "../src";
import Option "mo:base/Option";

func on_open(args : IcWebsocketCdk.OnOpenCallbackArgs) : async () {

};

func on_message(args : IcWebsocketCdk.OnMessageCallbackArgs) : async () {

};

func on_close(args : IcWebsocketCdk.OnCloseCallbackArgs) : async () {

};

let handlers = IcWebsocketCdk.WsHandlers(
  Option.make(on_open),
  Option.make(on_message),
  Option.make(on_close),
);

let ws = IcWebsocketCdk.IcWebSocket(handlers);
assert (42 == 42);

import Text "mo:base/Text";
import Debug "mo:base/Debug";
import Nat64 "mo:base/Nat64";
import Time "mo:base/Time";

module {
  public func custom_print(s : Text) {
    Debug.print(Text.concat("[IC-WEBSOCKET-CDK]: ", s));
  };

  public func custom_trap(s : Text) {
    Debug.trap(s);
  };

  public func get_current_time() : Nat64 {
    Nat64.fromIntWrap(Time.now());
  };
};

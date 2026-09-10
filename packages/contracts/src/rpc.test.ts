import { describe, expect, it } from "vite-plus/test";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";

import { WsSubscribeServerConfigRpc } from "./rpc.ts";

/**
 * Clients send stream opt-ins, including to servers built
 * before the field existed, whose payload schema was an empty struct. What
 * makes that safe is that such a schema accepts the request rather than
 * rejecting it -- an error here would take down the config subscription.
 */
describe.each(["environmentThemes", "environmentLabels"])(
  "subscribeServerConfig %s compatibility",
  (field) => {
    it("is accepted by a server whose schema predates the field", () => {
      const oldServerPayload = Schema.Struct({});
      const decoded = Schema.decodeUnknownExit(oldServerPayload)({ [field]: true });
      expect(Exit.isSuccess(decoded)).toBe(true);
    });

    it("is carried by a server that declares it", () => {
      const decoded = Schema.decodeUnknownSync(WsSubscribeServerConfigRpc.payloadSchema)({
        [field]: true,
      });
      expect(decoded).toEqual({ [field]: true });
    });

    it("stays optional, so a client that never sends it still subscribes", () => {
      const decoded = Schema.decodeUnknownSync(WsSubscribeServerConfigRpc.payloadSchema)({});
      expect(decoded).toEqual({});
    });
  },
);

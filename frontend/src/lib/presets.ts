import { MAX_PAYLOAD_BYTES, newEventId, type Draft } from "./validate";

export interface Preset {
  label: string;
  // What the API should answer, shown on the button so a viewer knows what
  // the demo is about to prove before it is sent.
  expects: 202 | 422;
  description: string;
  build: () => Draft;
}

const pretty = (value: unknown) => JSON.stringify(value, null, 2);
const now = () => new Date().toISOString();
const pick = <T,>(items: T[]) => items[Math.floor(Math.random() * items.length)];
const ref = (prefix: string) => `${prefix}-${Math.floor(1000 + Math.random() * 9000)}`;

// The e-commerce domain from the README, with values varied per click so a
// run of demo sends looks like traffic rather than one event repeated.
export const VALID_PRESETS: Preset[] = [
  {
    label: "Order created",
    expects: 202,
    description: "The README's example event",
    build: () => ({
      event_id: newEventId(),
      event_type: "order.created",
      source: "order-service",
      payloadText: pretty({
        order_id: ref("ORD"),
        user_id: ref("USR"),
        amount: Number((10 + Math.random() * 240).toFixed(2)),
        currency: pick(["USD", "EUR", "PKR"]),
      }),
    }),
  },
  {
    label: "User signed up",
    expects: 202,
    description: "User activity",
    build: () => ({
      event_id: newEventId(),
      event_type: "user.signed_up",
      source: "auth-service",
      payloadText: pretty({ user_id: ref("USR"), plan: pick(["free", "pro"]), referrer: pick(["search", "ads", null]) }),
    }),
  },
  {
    label: "Cart item added",
    expects: 202,
    description: "Nested payload",
    build: () => ({
      event_id: newEventId(),
      event_type: "cart.item_added",
      source: "web-frontend",
      payloadText: pretty({
        cart_id: ref("CART"),
        item: { sku: ref("SKU"), name: pick(["Desk lamp", "Notebook", "Headphones"]), quantity: 1 + Math.floor(Math.random() * 3) },
        at: now(),
      }),
    }),
  },
  {
    label: "Payment failed",
    expects: 202,
    description: "A business failure is still a valid event",
    build: () => ({
      event_id: newEventId(),
      event_type: "payment.failed",
      source: "payment-service",
      payloadText: pretty({ order_id: ref("ORD"), reason: pick(["card_declined", "insufficient_funds"]), retryable: false }),
    }),
  },
];

// Each one breaks exactly one rule in EventCreate, so the 422 that comes back
// points at a single field. They only reach the server with browser checks off.
export const INVALID_PRESETS: Preset[] = [
  {
    label: "Undotted type",
    expects: 422,
    description: "event_type must be <domain>.<action>",
    build: () => ({ ...VALID_PRESETS[0].build(), event_type: "ordercreated" }),
  },
  {
    label: "Bad event ID",
    expects: 422,
    description: "event_id must be a UUID",
    build: () => ({ ...VALID_PRESETS[0].build(), event_id: "order-42" }),
  },
  {
    label: "Payload is a list",
    expects: 422,
    description: "payload must be a JSON object",
    build: () => ({ ...VALID_PRESETS[0].build(), payloadText: pretty(["ORD-001", 99.99]) }),
  },
  {
    label: "Oversized payload",
    expects: 422,
    description: `over the ${MAX_PAYLOAD_BYTES.toLocaleString()} byte guard`,
    build: () => ({
      ...VALID_PRESETS[0].build(),
      payloadText: pretty({ note: "a client embedding a file by mistake", blob: "x".repeat(MAX_PAYLOAD_BYTES) }),
    }),
  },
];

import { useEffect, useRef, useState } from "react";
import { api } from "../api/client";
import type { StoredEvent } from "../api/types";

export type StreamState = "connecting" | "open" | "paused" | "retrying";

export interface EventStream {
  events: StoredEvent[];
  state: StreamState;
  received: number;
  lastId: number | null;
  pause: () => void;
  resume: () => void;
  clear: () => void;
}

const RETRY_MS = 3000;

/**
 * Subscribes to GET /events/stream and keeps the newest `keep` events.
 *
 * The browser's own EventSource retry is not used. The server sends no SSE
 * `id:` fields, so an automatic reconnect would carry no Last-Event-ID and
 * resume from the newest row, silently skipping whatever arrived during the
 * gap. Instead every reconnect, after a drop or a pause, is opened by hand
 * with `?after=<last id seen>`, which is what that parameter exists for.
 *
 * A refused connection (the server caps open streams and answers 503) looks
 * the same as a dropped one from here, since EventSource hides the status, so
 * both are retried on a delay rather than in a tight loop.
 */
export function useEventStream(keep = 200): EventStream {
  const [events, setEvents] = useState<StoredEvent[]>([]);
  const [state, setState] = useState<StreamState>("connecting");
  const [received, setReceived] = useState(0);
  const [paused, setPaused] = useState(false);

  const lastIdRef = useRef<number | null>(null);
  const [lastId, setLastId] = useState<number | null>(null);

  useEffect(() => {
    if (paused) {
      setState("paused");
      return;
    }

    let source: EventSource | null = null;
    let retryTimer: number | undefined;

    const open = () => {
      setState("connecting");
      source = new EventSource(api.streamUrl(lastIdRef.current ?? undefined));

      source.onopen = () => setState("open");

      source.onmessage = (message) => {
        const event = JSON.parse(message.data) as StoredEvent;
        lastIdRef.current = event.id;
        setLastId(event.id);
        setReceived((n) => n + 1);
        setEvents((current) => [event, ...current].slice(0, keep));
      };

      source.onerror = () => {
        source?.close();
        setState("retrying");
        retryTimer = window.setTimeout(open, RETRY_MS);
      };
    };

    open();
    return () => {
      window.clearTimeout(retryTimer);
      source?.close();
    };
  }, [paused, keep]);

  return {
    events,
    state,
    received,
    lastId,
    pause: () => setPaused(true),
    resume: () => setPaused(false),
    clear: () => setEvents([]),
  };
}

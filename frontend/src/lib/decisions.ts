import type { StageId } from "./pipeline";

export interface Decision {
  n: number;
  title: string;
  question: string;
  chose: string;
  why: string;
  cost: string;
  stages: StageId[];
}

// Condensed from "Architecture & Design Decisions" in the README, which has
// the full reasoning and the options that were rejected.
export const DECISIONS: Decision[] = [
  {
    n: 1,
    title: "Kafka between the API and the database",
    question: "Insert straight into Postgres on every request, buffer in memory, or put Kafka in between?",
    chose: "Kafka as the event backbone.",
    why: "The API answers as fast as Kafka acknowledges, whatever the database is doing. An in-memory queue would lose everything on a crash.",
    cost: "More to run (Kafka, ZooKeeper); events are queryable a moment later; at-least-once delivery means duplicates must be handled.",
    stages: ["api", "kafka"],
  },
  {
    n: 2,
    title: "Three partitions on events.raw",
    question: "One partition for strict ordering, or more for parallel consumers?",
    chose: "3 partitions locally, created explicitly by a kafka-init job.",
    why: "Partitions cap how many consumers can work in parallel. Auto created topics default to 1, which would silently cap it, so auto creation is off.",
    cost: "Ordering holds per partition, not globally; the count can be raised later but never lowered.",
    stages: ["kafka"],
  },
  {
    n: 3,
    title: "Wait for the broker, acks=1",
    question: "Fire and forget, wait for the leader (acks=1), or wait for every replica (acks=all)?",
    chose: "acks=1, configurable through KAFKA_PRODUCER_ACKS.",
    why: "A 202 then means the event really reached Kafka. acks=all is right for payments; fire and forget is never right for an ingestion API.",
    cost: "A small window of loss if the leader fails before replicating; slightly slower than fire and forget.",
    stages: ["api"],
  },
  {
    n: 4,
    title: "Deduplicate on event_id in Postgres",
    question: "Kafka may deliver twice. Ignore it, dedupe in Redis, or let the database enforce it?",
    chose: "A UNIQUE constraint on event_id with ON CONFLICT DO NOTHING, plus an optional best effort 409 pre-check in the API.",
    why: "Atomic, needs no extra service, and correct under concurrency. The pre-check only gives faster feedback; the constraint is the guarantee.",
    cost: "Clients must generate event_id; the pre-check puts one database read back on the request path, so it can be switched off.",
    stages: ["postgres", "api", "client", "consumer"],
  },
  {
    n: 5,
    title: "JSONB payload, indexed by access pattern",
    question: "How should a time series of differently shaped events be stored?",
    chose: "Fixed columns for what is queried, JSONB for the payload, indexes on event_type, source and created_at DESC.",
    why: "Event shapes can change without migrations, and the indexes match the filters actually used, newest first.",
    cost: "Querying inside payloads is less efficient than real columns would be.",
    stages: ["postgres"],
  },
  {
    n: 6,
    title: "A dead letter queue in two places",
    question: "What happens to an event the consumer cannot process, even after retries?",
    chose: "Record it in a dlq_events table and on an events.dlq topic, with the raw payload and the reason.",
    why: "The table makes failures inspectable, the topic lets something alert on them, and the raw payload makes replay possible once the cause is fixed.",
    cost: "Two writes per failure, and replay is a deliberate manual step rather than automatic.",
    stages: ["dlq", "consumer"],
  },
];

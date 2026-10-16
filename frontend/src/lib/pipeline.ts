// The architecture as data: the stages an event passes through and the edges
// between them. The diagram, the stage details and the guided tour all read
// from here, so the picture and the explanations cannot drift apart.

export type StageId = "client" | "ingest" | "api" | "kafka" | "consumer" | "postgres" | "dlq";

export interface Stage {
  id: StageId;
  title: string;
  subtitle: string;
  x: number;
  y: number;
}

export const NODE_W = 170;
export const NODE_H = 64;

export const STAGES: Stage[] = [
  { id: "ingest", title: "Ingest poller", subtitle: "USGS, Open-Meteo", x: 20, y: 40 },
  { id: "client", title: "Clients", subtitle: "this console, services", x: 20, y: 230 },
  { id: "api", title: "FastAPI", subtitle: "POST /events", x: 270, y: 135 },
  { id: "kafka", title: "Kafka", subtitle: "events.raw · 3 partitions", x: 520, y: 135 },
  { id: "consumer", title: "Consumer", subtitle: "group event-processors", x: 770, y: 135 },
  { id: "postgres", title: "PostgreSQL", subtitle: "events table", x: 770, y: 290 },
  { id: "dlq", title: "Dead letter queue", subtitle: "dlq_events + events.dlq", x: 520, y: 290 },
];

export type EdgeId =
  | "ingest-api"
  | "client-api"
  | "api-kafka"
  | "kafka-consumer"
  | "consumer-postgres"
  | "consumer-dlq"
  | "postgres-client";

export interface Edge {
  id: EdgeId;
  from: StageId;
  to: StageId;
  label: string;
  // SVG path in the diagram's coordinate space.
  path: string;
  // Where the live count sits.
  labelAt: [number, number];
}

const right = (s: Stage): [number, number] => [s.x + NODE_W, s.y + NODE_H / 2];
const left = (s: Stage): [number, number] => [s.x, s.y + NODE_H / 2];
const top = (s: Stage): [number, number] => [s.x + NODE_W / 2, s.y];
const bottom = (s: Stage): [number, number] => [s.x + NODE_W / 2, s.y + NODE_H];
const stage = (id: StageId) => STAGES.find((s) => s.id === id)!;

function line([x1, y1]: [number, number], [x2, y2]: [number, number]): string {
  return `M${x1},${y1} L${x2},${y2}`;
}

// Elbow from one node's right side into another's left side.
function elbow([x1, y1]: [number, number], [x2, y2]: [number, number]): string {
  const mid = (x1 + x2) / 2;
  return `M${x1},${y1} L${mid},${y1} L${mid},${y2} L${x2},${y2}`;
}

const [ingest, client, api, kafka, consumer, postgres, dlq] = [
  "ingest",
  "client",
  "api",
  "kafka",
  "consumer",
  "postgres",
  "dlq",
].map((id) => stage(id as StageId));

// Under everything, so the read path back to clients crosses nothing.
const SSE_Y = 385;
// Between the main row and the lower row.
const DLQ_LANE_Y = 245;

export const EDGES: Edge[] = [
  { id: "ingest-api", from: "ingest", to: "api", label: "HTTP", path: elbow(right(ingest), left(api)), labelAt: [230, 66] },
  { id: "client-api", from: "client", to: "api", label: "HTTP", path: elbow(right(client), left(api)), labelAt: [230, 256] },
  { id: "api-kafka", from: "api", to: "kafka", label: "publish", path: line(right(api), left(kafka)), labelAt: [480, 156] },
  {
    id: "kafka-consumer",
    from: "kafka",
    to: "consumer",
    label: "consume",
    path: line(right(kafka), left(consumer)),
    labelAt: [730, 156],
  },
  {
    id: "consumer-postgres",
    from: "consumer",
    to: "postgres",
    label: "insert",
    path: line(bottom(consumer), top(postgres)),
    labelAt: [862, 250],
  },
  {
    id: "consumer-dlq",
    from: "consumer",
    to: "dlq",
    label: "on failure",
    // Leaves the consumer's lower left and drops into the top of the DLQ,
    // passing above PostgreSQL rather than through it.
    path: `M${consumer.x + 20},${consumer.y + NODE_H} L${consumer.x + 20},${DLQ_LANE_Y} L${top(dlq)[0]},${DLQ_LANE_Y} L${top(dlq)[0]},${dlq.y}`,
    labelAt: [700, 235],
  },
  {
    id: "postgres-client",
    from: "postgres",
    to: "client",
    label: "SSE /events/stream",
    path: `M${bottom(postgres)[0]},${bottom(postgres)[1]} L${bottom(postgres)[0]},${SSE_Y} L${bottom(client)[0]},${SSE_Y} L${bottom(client)[0]},${bottom(client)[1]}`,
    labelAt: [480, SSE_Y + 16],
  },
];

export const VIEW_W = 960;
export const VIEW_H = 410;

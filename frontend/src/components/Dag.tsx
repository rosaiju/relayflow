import type { Task } from "../api";

const NODE_W = 196;
const NODE_H = 62;
const COL_GAP = 64;
const ROW_GAP = 22;
const PAD = 16;

interface Placed {
  task: Task;
  x: number;
  y: number;
}

/** Column = length of the longest dependency path from a root, so edges always point right. */
function layout(tasks: Task[]): { nodes: Placed[]; width: number; height: number } {
  const byKey = new Map(tasks.map((t) => [t.task_key, t]));
  const depth = new Map<string, number>();
  const visit = (key: string, seen: Set<string>): number => {
    const known = depth.get(key);
    if (known !== undefined) return known;
    if (seen.has(key)) return 0; // defensive: definitions are validated as acyclic
    seen.add(key);
    const deps = byKey.get(key)?.depends_on ?? [];
    const d = deps.length ? Math.max(...deps.map((dep) => visit(dep, seen) + 1)) : 0;
    depth.set(key, d);
    return d;
  };
  tasks.forEach((t) => visit(t.task_key, new Set()));

  const columns = new Map<number, Task[]>();
  for (const t of tasks) {
    const d = depth.get(t.task_key) ?? 0;
    columns.set(d, [...(columns.get(d) ?? []), t]);
  }
  const tallest = Math.max(...[...columns.values()].map((c) => c.length));
  const height = PAD * 2 + tallest * NODE_H + (tallest - 1) * ROW_GAP;
  const nodes: Placed[] = [];
  for (const [d, col] of columns) {
    const colHeight = col.length * NODE_H + (col.length - 1) * ROW_GAP;
    const top = (height - colHeight) / 2;
    col
      .sort((a, b) => a.task_key.localeCompare(b.task_key))
      .forEach((task, i) => nodes.push({ task, x: PAD + d * (NODE_W + COL_GAP), y: top + i * (NODE_H + ROW_GAP) }));
  }
  const width = PAD * 2 + columns.size * NODE_W + (columns.size - 1) * COL_GAP;
  return { nodes, width, height };
}

export function Dag({
  tasks,
  selected,
  onSelect,
}: {
  tasks: Task[];
  selected: string | null;
  onSelect: (key: string) => void;
}) {
  const { nodes, width, height } = layout(tasks);
  const at = new Map(nodes.map((n) => [n.task.task_key, n]));
  return (
    <div className="dag-scroll">
      <svg
        className="dag"
        viewBox={`0 0 ${width} ${height}`}
        width={width}
        height={height}
        role="img"
        aria-label="Task dependency graph"
      >
        <defs>
          <marker id="arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto">
            <path d="M0,0 L8,4 L0,8 z" className="dag-arrow" />
          </marker>
        </defs>
        {nodes.flatMap(({ task, x, y }) =>
          task.depends_on.map((dep) => {
            const from = at.get(dep);
            if (!from) return null;
            const x1 = from.x + NODE_W;
            const y1 = from.y + NODE_H / 2;
            const x2 = x - 2;
            const y2 = y + NODE_H / 2;
            const mid = (x1 + x2) / 2;
            const done = from.task.status === "succeeded";
            return (
              <path
                key={`${dep}->${task.task_key}`}
                d={`M${x1},${y1} C${mid},${y1} ${mid},${y2} ${x2},${y2}`}
                className={`dag-edge ${done ? "dag-edge-done" : ""}`}
                markerEnd="url(#arrow)"
              />
            );
          }),
        )}
        {nodes.map(({ task, x, y }) => {
          const recovered = task.attempts.some((a) => a.status === "lease_expired");
          return (
            <g
              key={task.task_key}
              transform={`translate(${x},${y})`}
              className={`dag-node dag-${task.status} ${selected === task.task_key ? "dag-selected" : ""}`}
              onClick={() => onSelect(task.task_key)}
              onKeyDown={(e) => (e.key === "Enter" || e.key === " ") && onSelect(task.task_key)}
              tabIndex={0}
              role="button"
              aria-label={`${task.task_key}: ${task.status}`}
              data-testid={`dag-node-${task.task_key}`}
              data-status={task.status}
            >
              <rect width={NODE_W} height={NODE_H} rx={10} />
              <text x={12} y={22} className="dag-key">
                {task.task_key}
              </text>
              <text x={12} y={40} className="dag-type">
                {task.task_type}
              </text>
              <text x={12} y={55} className="dag-meta">
                {task.status.replace("_", " ")} · {task.attempt_count} attempt{task.attempt_count === 1 ? "" : "s"}
                {recovered ? " · recovered" : ""}
              </text>
              {task.status === "running" && <circle cx={NODE_W - 14} cy={14} r={5} className="dag-pulse" />}
            </g>
          );
        })}
      </svg>
    </div>
  );
}

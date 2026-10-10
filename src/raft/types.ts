export type Role = "follower" | "candidate" | "leader";

export type Op = { kind: "put"; key: string; value: number } | { kind: "get"; key: string };

export interface Command {
  client: number;
  seq: number;
  op: Op;
}

export interface Entry {
  term: number;
  cmd: Command;
}

export interface Durable {
  term: number;
  votedFor: number | null;
  log: Entry[];
}

export type RaftMsg =
  | { type: "vote"; term: number; lastIndex: number; lastTerm: number }
  | { type: "voteReply"; term: number; granted: boolean }
  | { type: "append"; term: number; prevIndex: number; prevTerm: number; entries: Entry[]; leaderCommit: number }
  | { type: "appendReply"; term: number; success: boolean; match: number; conflict: number };

export type ClientMsg =
  | { type: "request"; seq: number; op: Op }
  | { type: "response"; seq: number; value: number | null }
  | { type: "redirect"; seq: number; leader: number | null };

export type Msg = RaftMsg | ClientMsg;

/** Things a node reports about itself, for the checkers and the trace. */
export type Observation =
  | { type: "role"; role: Role; term: number }
  | { type: "apply"; index: number; term: number; cmd: Command }
  | { type: "commit"; index: number }
  | { type: "log"; length: number };

/**
 * The only door between a node and the world. A node never reads a clock,
 * calls Math.random or touches a socket; the simulator hands it all of that.
 */
export interface Env {
  readonly id: number;
  readonly peers: readonly number[];
  now(): number;
  random(): number;
  send(to: number, msg: Msg): void;
  setTimer(kind: string, delay: number): void;
  observe(o: Observation): void;
}

// The bug zoo: real mistakes people make implementing Raft, each behind a flag.
// The correct implementation runs with no flags set. A bug only counts as
// "found" when the checkers flag it without being told which one is on.
export type BugId =
  | "stale-read"
  | "volatile-vote"
  | "vote-ignores-log"
  | "commit-old-term"
  | "truncate-on-append";

export interface BugInfo {
  id: BugId;
  title: string;
  /** What the buggy code does differently. */
  change: string;
  /** Why it is wrong, in one or two sentences. */
  why: string;
  /** Where in the Raft paper the rule lives. */
  ref: string;
}

export const BUGS: readonly BugInfo[] = [
  {
    id: "stale-read",
    title: "Reads served from the leader's local state",
    change: "A leader answers GET from its own key-value map instead of going through the log.",
    why: "A leader cut off by a partition still believes it leads. It keeps answering reads while a new leader on the majority side accepts writes, so clients read values that were already overwritten.",
    ref: "Raft §8 (read-only operations)",
  },
  {
    id: "volatile-vote",
    title: "votedFor kept only in memory",
    change: "currentTerm and the log are persisted, votedFor is not.",
    why: "A node that votes, crashes and restarts in the same term can vote again for a different candidate. Two candidates can then each collect a majority, which gives two leaders in one term.",
    ref: "Raft Figure 2 (persistent state)",
  },
  {
    id: "vote-ignores-log",
    title: "Votes granted without the up-to-date check",
    change: "RequestVote skips the comparison of the candidate's last log term and index.",
    why: "A candidate missing committed entries can win an election and then overwrite those entries on its followers. A committed write disappears.",
    ref: "Raft §5.4.1 (election restriction)",
  },
  {
    id: "commit-old-term",
    title: "Committing entries from earlier terms by counting replicas",
    change: "The leader advances commitIndex to any index stored on a majority, whatever that entry's term.",
    why: "An entry from an earlier term can sit on a majority and still be overwritten by a later leader. This is the scenario in the paper's Figure 8: the leader may only count replicas for entries from its own term.",
    ref: "Raft §5.4.2 / Figure 8",
  },
  {
    id: "truncate-on-append",
    title: "Follower truncates its log on every AppendEntries",
    change: "The follower deletes everything after prevLogIndex before appending, even when the entries already match.",
    why: "A delayed or reordered AppendEntries erases entries the follower has already acknowledged. The leader counted that acknowledgement towards a commit, so a committed entry can lose its majority.",
    ref: "Raft Figure 2 (AppendEntries receiver, step 3)",
  },
];

export type Bugs = Partial<Record<BugId, boolean>>;

export function bugInfo(id: BugId): BugInfo {
  return BUGS.find((b) => b.id === id)!;
}

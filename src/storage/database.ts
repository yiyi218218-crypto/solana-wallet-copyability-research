import { DatabaseSync } from "node:sqlite";
import { nowIso } from "../audit/provenance.js";
import { applyMigrations } from "./migrations.js";

export class ResearchDatabase {
  readonly sqlite: DatabaseSync;

  constructor(filename = ":memory:") {
    this.sqlite = new DatabaseSync(filename);
    applyMigrations(this.sqlite, nowIso());
  }

  close(): void {
    this.sqlite.close();
  }

  status(): { journalMode: string; migrations: number; observations: number; trades: number } {
    const journal = this.sqlite.prepare("PRAGMA journal_mode").get() as { journal_mode: string };
    const count = (table: string): number => (this.sqlite.prepare(`SELECT COUNT(*) AS count FROM ${table}`).get() as { count: number }).count;
    return { journalMode: journal.journal_mode, migrations: count("schema_migrations"), observations: count("source_observations"), trades: count("wallet_trades") };
  }
}

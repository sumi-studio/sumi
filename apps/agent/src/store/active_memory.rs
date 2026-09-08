//! Select the current memory graph without traversing archived lineage.
//!
//! SQLite is private runtime state. This selection reads metadata only; Store
//! authenticates the selected plaintext when it materializes the graph.
use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, anyhow};
use sqlx::{Row, SqliteConnection};

pub(super) struct ActiveMemorySelection {
    pub batch_ids: HashSet<String>,
    pub live_l0_batch_ids: HashSet<String>,
    pub job_ids: HashSet<String>,
    pub next_l0_batch_seq: u64,
    pub job_sequence_max: HashMap<String, u64>,
}

impl ActiveMemorySelection {
    pub async fn load(connection: &mut SqliteConnection) -> Result<Self> {
        // The two seed sets describe current state. Producer jobs provide one
        // immediate witness edge for visible summaries. Do not recursively
        // include the producers of dropped sources: L2 reintegration would
        // otherwise walk the individual's entire lifetime at every restart.
        // Unary + on the source id removes SQLite's TEXT column affinity so
        // the JSON expression equality can use its reverse-source index.
        let rows = sqlx::query(
            "WITH live_l0 AS MATERIALIZED (
                 SELECT id, state FROM memory_batches
                 WHERE layer = 0 AND state IN ('open', 'sealed', 'compacting', 'compact_failed', 'compacted')
             ), active_jobs AS MATERIALIZED (
                 SELECT j.id, j.kind, j.batch_seq, j.source_ids FROM memory_jobs j
                 WHERE j.status IN ('pending', 'running', 'completed')
                 UNION
                 SELECT j.id, j.kind, j.batch_seq, j.source_ids
                 FROM live_l0 l CROSS JOIN memory_jobs j
                 WHERE j.kind = 'compact_l0'
                   AND json_extract(j.source_ids, '$[0]') = +l.id
                   AND ((j.status = 'failed' AND l.state = 'compact_failed')
                     OR (j.status = 'unchanged' AND l.state = 'sealed'))
             ), roots AS (
                 SELECT id FROM live_l0
                 UNION SELECT id FROM memory_batches WHERE state = 'promoted'
                 UNION
                 SELECT s.value FROM active_jobs j, json_each(j.source_ids) s
                 UNION
                 SELECT b.id FROM active_jobs j JOIN memory_batches b
                   ON b.batch_seq = j.batch_seq
                  AND b.layer = CASE j.kind WHEN 'compact_l0' THEN 1 ELSE 2 END
             ), selected_jobs AS (
                 SELECT id FROM active_jobs
                 UNION
                 SELECT j.id FROM roots r
                 CROSS JOIN memory_batches b ON b.id = r.id
                 CROSS JOIN memory_jobs j
                 WHERE j.batch_seq = b.batch_seq AND j.status = 'applied'
                   AND j.kind = CASE b.layer WHEN 1 THEN 'compact_l0' WHEN 2 THEN 'compact_l1' END
                 UNION
                 SELECT j.id FROM roots r
                 CROSS JOIN memory_batches b ON b.id = r.id
                 CROSS JOIN memory_jobs j
                 WHERE b.layer = 2 AND j.kind = 'consolidate_l2'
                   AND j.batch_seq = b.batch_seq AND j.status = 'applied'
             ), selected_batches AS (
                 SELECT id FROM roots
                 UNION
                 SELECT s.value FROM selected_jobs x CROSS JOIN memory_jobs j ON x.id = j.id,
                     json_each(j.source_ids) s
                 UNION
                 SELECT b.id FROM selected_jobs x CROSS JOIN memory_jobs j ON x.id = j.id
                 CROSS JOIN memory_batches b ON b.batch_seq = j.batch_seq
                   AND b.layer = CASE j.kind WHEN 'compact_l0' THEN 1 ELSE 2 END
             )
             SELECT 'batch' AS record_type, id FROM selected_batches
             UNION ALL SELECT 'job', id FROM selected_jobs",
        )
        .fetch_all(&mut *connection)
        .await
        .context("failed to select active memory graph")?;
        let mut batch_ids = HashSet::new();
        let mut job_ids = HashSet::new();
        for row in rows {
            let id: String = row.try_get("id")?;
            match row.try_get::<&str, _>("record_type")? {
                "batch" => {
                    batch_ids.insert(id);
                }
                "job" => {
                    job_ids.insert(id);
                }
                _ => unreachable!("selection emits only batch and job rows"),
            }
        }
        let live_l0_batch_ids = sqlx::query_scalar::<_, String>(
            "SELECT id FROM memory_batches WHERE layer = 0 AND state IN ('open', 'sealed', 'compacting', 'compact_failed', 'compacted')",
        )
        .fetch_all(&mut *connection)
        .await?
        .into_iter()
        .collect();
        // UNIQUE(layer, batch_seq) / UNIQUE(kind, batch_seq) support indexed
        // high-water reads. Live selection must never reset durable counters.
        let highest: Option<i64> = sqlx::query_scalar(
            "SELECT batch_seq FROM memory_batches WHERE layer = 0 ORDER BY batch_seq DESC LIMIT 1",
        )
        .fetch_optional(&mut *connection)
        .await?;
        let next_l0_batch_seq = highest
            .map(|value| {
                u64::try_from(value)
                    .ok()
                    .and_then(|value| value.checked_add(1))
                    .ok_or_else(|| anyhow!("durable L0 batch sequence overflow"))
            })
            .transpose()?
            .unwrap_or(0);
        let mut job_sequence_max = HashMap::new();
        for kind in ["compact_l0", "compact_l1", "consolidate_l2"] {
            if let Some(value) = sqlx::query_scalar::<_, i64>(
                "SELECT batch_seq FROM memory_jobs WHERE kind = ? ORDER BY batch_seq DESC LIMIT 1",
            )
            .bind(kind)
            .fetch_optional(&mut *connection)
            .await?
            {
                job_sequence_max.insert(kind.to_owned(), u64::try_from(value)?);
            }
        }
        Ok(Self {
            batch_ids,
            live_l0_batch_ids,
            job_ids,
            next_l0_batch_seq,
            job_sequence_max,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::Connection;

    #[tokio::test]
    async fn selection_stops_at_immediate_lineage_and_keeps_global_counters() {
        let mut db = SqliteConnection::connect("sqlite::memory:").await.unwrap();
        sqlx::raw_sql(
            "CREATE TABLE memory_batches(id TEXT PRIMARY KEY, layer INTEGER, batch_seq INTEGER, state TEXT,
                 UNIQUE(layer,batch_seq));
             CREATE TABLE memory_jobs(id TEXT PRIMARY KEY, kind TEXT, batch_seq INTEGER,
                 source_ids TEXT, status TEXT, UNIQUE(kind,batch_seq));
             CREATE INDEX batches_state_layer ON memory_batches(state, layer);
             CREATE INDEX jobs_status_kind ON memory_jobs(status, kind);
             CREATE INDEX jobs_source ON memory_jobs(kind, json_extract(source_ids, '$[0]'), status);",
        ).execute(&mut db).await.unwrap();
        // An arbitrarily long reintegration chain must not become a recursive
        // dependency of the current L2 body. No body column even exists here.
        for seq in 1..=1_001 {
            sqlx::query("INSERT INTO memory_batches VALUES (?,2,?,?)")
                .bind(format!("l2-{seq}"))
                .bind(seq)
                .bind(if seq == 1_001 { "promoted" } else { "dropped" })
                .execute(&mut db)
                .await
                .unwrap();
            if seq > 1 {
                sqlx::query("INSERT INTO memory_jobs VALUES (?,'consolidate_l2',?,?,'applied')")
                    .bind(format!("job-{seq}"))
                    .bind(seq)
                    .bind(serde_json::to_string(&[format!("l2-{}", seq - 1)]).unwrap())
                    .execute(&mut db)
                    .await
                    .unwrap();
            }
        }
        sqlx::raw_sql(
            "INSERT INTO memory_batches VALUES ('archived-l0',0,7000,'dropped');
             INSERT INTO memory_batches VALUES ('live-l0',0,7001,'compacted');
             INSERT INTO memory_batches VALUES ('candidate',1,9000,'compacted');
             INSERT INTO memory_jobs VALUES ('shelf','compact_l0',9000,'[\"live-l0\"]','completed');
             INSERT INTO memory_jobs VALUES ('old-failure','compact_l0',8999,'[\"archived-l0\"]','failed');
             INSERT INTO memory_jobs VALUES ('old-discard','compact_l0',8998,'[\"live-l0\"]','discarded');
             INSERT INTO memory_batches VALUES ('retained-l0',0,7002,'sealed');
             INSERT INTO memory_batches VALUES ('unchanged-target',1,9001,'dropped');
             INSERT INTO memory_jobs VALUES ('retained-original','compact_l0',9001,'[\"retained-l0\"]','unchanged');
             INSERT INTO memory_batches VALUES ('failed-higher-target',2,1002,'dropped');
             INSERT INTO memory_jobs VALUES ('old-higher-failure','consolidate_l2',1002,'[\"l2-1001\"]','failed');",
        ).execute(&mut db).await.unwrap();
        let selected = ActiveMemorySelection::load(&mut db).await.unwrap();
        assert_eq!(
            selected.batch_ids,
            HashSet::from([
                "l2-1000".to_owned(),
                "l2-1001".to_owned(),
                "live-l0".to_owned(),
                "candidate".to_owned(),
                "retained-l0".to_owned(),
                "unchanged-target".to_owned(),
            ])
        );
        assert_eq!(
            selected.job_ids,
            HashSet::from([
                "job-1001".to_owned(),
                "shelf".to_owned(),
                "retained-original".to_owned()
            ])
        );
        assert_eq!(
            selected.live_l0_batch_ids,
            HashSet::from(["live-l0".to_owned(), "retained-l0".to_owned()])
        );
        assert_eq!(selected.next_l0_batch_seq, 7003);
        assert_eq!(selected.job_sequence_max["compact_l0"], 9001);
        assert_eq!(selected.job_sequence_max["consolidate_l2"], 1002);
    }
}

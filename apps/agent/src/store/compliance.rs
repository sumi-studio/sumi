//! Compliance store for deletion tombstones and data-access audit.
//!
//! The canonical Cloud placement is a control-plane database that outlives the
//! agent volume.  This module exposes a typed `TombstoneRepository` boundary
//! and a SQLite implementation.  An in-memory implementation is supplied for
//! focused unit tests.
#![allow(dead_code)]

use std::{
    collections::{HashMap, VecDeque},
    fmt,
    sync::{Mutex, MutexGuard},
};

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use chrono::Utc;

use sqlx::{
    Row, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
};
#[cfg(test)]
use std::str::FromStr;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TombstoneScope {
    Conversation,
    Agent,
}

impl TombstoneScope {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Conversation => "conversation",
            Self::Agent => "agent",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "conversation" => Ok(Self::Conversation),
            "agent" => Ok(Self::Agent),
            _ => bail!("invalid tombstone scope {value}"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TombstoneStatus {
    Requested,
    Fenced,
    LivePurged,
    BackupExpired,
}

impl TombstoneStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Requested => "requested",
            Self::Fenced => "fenced",
            Self::LivePurged => "live_purged",
            Self::BackupExpired => "backup_expired",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "requested" => Ok(Self::Requested),
            "fenced" => Ok(Self::Fenced),
            "live_purged" => Ok(Self::LivePurged),
            "backup_expired" => Ok(Self::BackupExpired),
            _ => bail!("invalid tombstone status {value}"),
        }
    }

    fn next(self) -> Option<Self> {
        match self {
            Self::Requested => Some(Self::Fenced),
            Self::Fenced => Some(Self::LivePurged),
            Self::LivePurged => Some(Self::BackupExpired),
            Self::BackupExpired => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Tombstone {
    pub id: String,
    pub tenant_id: String,
    pub agent_id: String,
    pub conversation_id: Option<String>,
    pub scope: TombstoneScope,
    pub status: TombstoneStatus,
    pub requested_at: String,
    pub purge_after: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuditRecord {
    pub id: String,
    pub actor_id: String,
    pub tenant_id: String,
    pub action: String,
    pub scope: String,
    pub result_count: i64,
    pub created_at: String,
}

/// Monotonic state-machine for deletion tombstones plus audit append.
#[async_trait]
pub trait TombstoneRepository: Send + Sync {
    /// Create a new tombstone in `Requested`.  `conversation_id` is required
    /// for `Conversation` scope and must be `None` for `Agent` scope.
    async fn request(
        &self,
        tenant_id: &str,
        agent_id: &str,
        conversation_id: Option<&str>,
        scope: TombstoneScope,
        purge_after: &str,
    ) -> Result<String>;

    /// CAS-advance a tombstone to `next`.  Same-state is an idempotent no-op.
    /// Reverse or skipped transitions fail closed.
    async fn advance(
        &self,
        id: &str,
        expected: TombstoneStatus,
        next: TombstoneStatus,
    ) -> Result<()>;

    /// Load one tombstone by id.
    async fn get(&self, id: &str) -> Result<Tombstone>;

    /// List tombstones for one agent.
    async fn list_for_agent(&self, tenant_id: &str, agent_id: &str) -> Result<Vec<Tombstone>>;

    /// Return any tombstone that blocks access to a conversation or agent.
    async fn blocking_tombstone(
        &self,
        tenant_id: &str,
        agent_id: &str,
        conversation_id: Option<&str>,
    ) -> Result<Option<Tombstone>>;

    /// Append one data-access audit record.
    async fn log_access(
        &self,
        actor_id: &str,
        tenant_id: &str,
        action: &str,
        scope: &str,
        result_count: i64,
    ) -> Result<()>;

    /// List audit records for one tenant, newest first.
    async fn list_audit(
        &self,
        tenant_id: &str,
        actor_id: Option<&str>,
        action: Option<&str>,
    ) -> Result<Vec<AuditRecord>>;
}

fn validate_scope_identity(scope: TombstoneScope, conversation_id: Option<&str>) -> Result<()> {
    match (scope, conversation_id) {
        (TombstoneScope::Conversation, Some(_)) | (TombstoneScope::Agent, None) => Ok(()),
        (TombstoneScope::Conversation, None) => {
            bail!("conversation scope requires a conversation_id")
        }
        (TombstoneScope::Agent, Some(_)) => bail!("agent scope must not have a conversation_id"),
    }
}

fn validate_transition(expected: TombstoneStatus, next: TombstoneStatus) -> Result<()> {
    if expected == next {
        return Ok(());
    }
    let allowed_next = expected
        .next()
        .ok_or_else(|| anyhow!("tombstone status {expected:?} is terminal; cannot advance"))?;
    if allowed_next != next {
        bail!("illegal tombstone transition {expected:?} -> {next:?}");
    }
    Ok(())
}

fn status_to_i64(status: TombstoneStatus) -> i64 {
    match status {
        TombstoneStatus::Requested => 0,
        TombstoneStatus::Fenced => 1,
        TombstoneStatus::LivePurged => 2,
        TombstoneStatus::BackupExpired => 3,
    }
}

/// In-memory tombstone/audit store for unit tests.
pub struct InMemoryTombstoneRepository {
    tombstones: Mutex<HashMap<String, Tombstone>>,
    audit: Mutex<VecDeque<AuditRecord>>,
}

impl Default for InMemoryTombstoneRepository {
    fn default() -> Self {
        Self {
            tombstones: Mutex::new(HashMap::new()),
            audit: Mutex::new(VecDeque::new()),
        }
    }
}

impl InMemoryTombstoneRepository {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock_tombstones(&self) -> Result<MutexGuard<'_, HashMap<String, Tombstone>>> {
        self.tombstones
            .lock()
            .map_err(|_| anyhow!("tombstone lock poisoned"))
    }

    fn lock_audit(&self) -> Result<MutexGuard<'_, VecDeque<AuditRecord>>> {
        self.audit
            .lock()
            .map_err(|_| anyhow!("audit lock poisoned"))
    }
}

#[async_trait]
impl TombstoneRepository for InMemoryTombstoneRepository {
    async fn request(
        &self,
        tenant_id: &str,
        agent_id: &str,
        conversation_id: Option<&str>,
        scope: TombstoneScope,
        purge_after: &str,
    ) -> Result<String> {
        validate_scope_identity(scope, conversation_id)?;
        let id = Uuid::now_v7().to_string();
        let tombstone = Tombstone {
            id: id.clone(),
            tenant_id: tenant_id.to_owned(),
            agent_id: agent_id.to_owned(),
            conversation_id: conversation_id.map(|s| s.to_owned()),
            scope,
            status: TombstoneStatus::Requested,
            requested_at: Utc::now().to_rfc3339(),
            purge_after: purge_after.to_owned(),
        };
        self.lock_tombstones()?.insert(id.clone(), tombstone);
        Ok(id)
    }

    async fn advance(
        &self,
        id: &str,
        expected: TombstoneStatus,
        next: TombstoneStatus,
    ) -> Result<()> {
        validate_transition(expected, next)?;
        let mut tombstones = self.lock_tombstones()?;
        let tombstone = tombstones
            .get_mut(id)
            .ok_or_else(|| anyhow!("tombstone {id} not found"))?;
        if tombstone.status != expected {
            bail!(
                "tombstone CAS mismatch for {id}: expected {expected:?}, found {:?}",
                tombstone.status
            );
        }
        tombstone.status = next;
        Ok(())
    }

    async fn get(&self, id: &str) -> Result<Tombstone> {
        self.lock_tombstones()?
            .get(id)
            .cloned()
            .ok_or_else(|| anyhow!("tombstone {id} not found"))
    }

    async fn list_for_agent(&self, tenant_id: &str, agent_id: &str) -> Result<Vec<Tombstone>> {
        let tombstones = self.lock_tombstones()?;
        Ok(tombstones
            .values()
            .filter(|t| t.tenant_id == tenant_id && t.agent_id == agent_id)
            .cloned()
            .collect())
    }

    async fn blocking_tombstone(
        &self,
        _tenant_id: &str,
        _agent_id: &str,
        conversation_id: Option<&str>,
    ) -> Result<Option<Tombstone>> {
        let tombstones = self.lock_tombstones()?;
        Ok(tombstones
            .values()
            .filter(|t| {
                t.status != TombstoneStatus::BackupExpired
                    && (conversation_id.is_none()
                        || t.conversation_id.as_deref() == conversation_id
                        || t.scope == TombstoneScope::Agent)
            })
            .min_by_key(|t| status_to_i64(t.status))
            .cloned())
    }

    async fn log_access(
        &self,
        actor_id: &str,
        tenant_id: &str,
        action: &str,
        scope: &str,
        result_count: i64,
    ) -> Result<()> {
        let record = AuditRecord {
            id: Uuid::now_v7().to_string(),
            actor_id: actor_id.to_owned(),
            tenant_id: tenant_id.to_owned(),
            action: action.to_owned(),
            scope: scope.to_owned(),
            result_count,
            created_at: Utc::now().to_rfc3339(),
        };
        self.lock_audit()?.push_front(record);
        Ok(())
    }

    async fn list_audit(
        &self,
        tenant_id: &str,
        actor_id: Option<&str>,
        action: Option<&str>,
    ) -> Result<Vec<AuditRecord>> {
        let audit = self.lock_audit()?;
        Ok(audit
            .iter()
            .filter(|r| {
                r.tenant_id == tenant_id
                    && actor_id.is_none_or(|a| r.actor_id == a)
                    && action.is_none_or(|a| r.action == a)
            })
            .cloned()
            .collect())
    }
}

/// Persistent SQLite compliance store.  Tombstones survive agent volume deletion
/// because this database is opened at a path outside the agent DB.
pub struct SqliteTombstoneRepository {
    pool: SqlitePool,
}

impl fmt::Debug for SqliteTombstoneRepository {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqliteTombstoneRepository")
            .finish_non_exhaustive()
    }
}

impl SqliteTombstoneRepository {
    pub async fn open(path: &std::path::Path) -> Result<Self> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .with_context(|| format!("failed to open compliance database {}", path.display()))?;
        sqlx::query(CREATE_SCHEMA)
            .execute(&pool)
            .await
            .context("failed to apply compliance schema")?;
        Ok(Self { pool })
    }

    #[cfg(test)]
    pub async fn open_in_memory() -> Result<Self> {
        let options = SqliteConnectOptions::from_str("sqlite::memory:")?
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Memory);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;
        sqlx::query(CREATE_SCHEMA)
            .execute(&pool)
            .await
            .context("failed to apply compliance schema")?;
        Ok(Self { pool })
    }
}

const CREATE_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS deletion_tombstones (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  conversation_id TEXT,
  scope TEXT NOT NULL,
  status TEXT NOT NULL,
  requested_at TEXT NOT NULL,
  purge_after TEXT NOT NULL,
  CHECK (scope IN ('conversation', 'agent')),
  CHECK (status IN ('requested', 'fenced', 'live_purged', 'backup_expired')),
  CHECK (
    (scope = 'conversation' AND conversation_id IS NOT NULL)
    OR
    (scope = 'agent' AND conversation_id IS NULL)
  )
);

CREATE TABLE IF NOT EXISTS data_access_audit (
  id TEXT PRIMARY KEY,
  actor_id TEXT NOT NULL,
  tenant_id TEXT NOT NULL,
  action TEXT NOT NULL,
  scope TEXT NOT NULL,
  result_count INTEGER,
  created_at TEXT NOT NULL
);
"#;

#[async_trait]
impl TombstoneRepository for SqliteTombstoneRepository {
    async fn request(
        &self,
        tenant_id: &str,
        agent_id: &str,
        conversation_id: Option<&str>,
        scope: TombstoneScope,
        purge_after: &str,
    ) -> Result<String> {
        validate_scope_identity(scope, conversation_id)?;
        let id = Uuid::now_v7().to_string();
        let requested_at = Utc::now().to_rfc3339();
        sqlx::query(
            "INSERT INTO deletion_tombstones(
                id, tenant_id, agent_id, conversation_id, scope, status,
                requested_at, purge_after
             ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&id)
        .bind(tenant_id)
        .bind(agent_id)
        .bind(conversation_id)
        .bind(scope.as_str())
        .bind(TombstoneStatus::Requested.as_str())
        .bind(&requested_at)
        .bind(purge_after)
        .execute(&self.pool)
        .await
        .context("failed to insert tombstone")?;
        Ok(id)
    }

    async fn advance(
        &self,
        id: &str,
        expected: TombstoneStatus,
        next: TombstoneStatus,
    ) -> Result<()> {
        validate_transition(expected, next)?;
        if expected == next {
            return Ok(());
        }
        let updated = sqlx::query(
            "UPDATE deletion_tombstones
             SET status = ?
             WHERE id = ? AND status = ?",
        )
        .bind(next.as_str())
        .bind(id)
        .bind(expected.as_str())
        .execute(&self.pool)
        .await
        .context("failed to advance tombstone")?;
        if updated.rows_affected() != 1 {
            let current: Option<String> =
                sqlx::query_scalar("SELECT status FROM deletion_tombstones WHERE id = ?")
                    .bind(id)
                    .fetch_optional(&self.pool)
                    .await
                    .context("failed to read tombstone status for CAS failure")?;
            match current {
                Some(status) if status == next.as_str() => Ok(()),
                Some(status) => {
                    bail!("tombstone CAS mismatch for {id}: expected {expected:?}, found {status}")
                }
                None => bail!("tombstone {id} not found during CAS advance"),
            }
        } else {
            Ok(())
        }
    }

    async fn get(&self, id: &str) -> Result<Tombstone> {
        let row = sqlx::query(
            "SELECT tenant_id, agent_id, conversation_id, scope, status,
                    requested_at, purge_after
             FROM deletion_tombstones WHERE id = ?",
        )
        .bind(id)
        .fetch_one(&self.pool)
        .await
        .context("failed to read tombstone")?;
        Ok(Tombstone {
            id: id.to_owned(),
            tenant_id: row.try_get("tenant_id")?,
            agent_id: row.try_get("agent_id")?,
            conversation_id: row.try_get("conversation_id")?,
            scope: TombstoneScope::parse(row.try_get::<String, _>("scope")?.as_str())?,
            status: TombstoneStatus::parse(row.try_get::<String, _>("status")?.as_str())?,
            requested_at: row.try_get("requested_at")?,
            purge_after: row.try_get("purge_after")?,
        })
    }

    async fn list_for_agent(&self, tenant_id: &str, agent_id: &str) -> Result<Vec<Tombstone>> {
        let rows = sqlx::query(
            "SELECT id, conversation_id, scope, status, requested_at, purge_after
             FROM deletion_tombstones
             WHERE tenant_id = ? AND agent_id = ?
             ORDER BY requested_at",
        )
        .bind(tenant_id)
        .bind(agent_id)
        .fetch_all(&self.pool)
        .await
        .context("failed to list tombstones")?;
        rows.into_iter()
            .map(|row| {
                Ok(Tombstone {
                    id: row.try_get("id")?,
                    tenant_id: tenant_id.to_owned(),
                    agent_id: agent_id.to_owned(),
                    conversation_id: row.try_get("conversation_id")?,
                    scope: TombstoneScope::parse(row.try_get::<String, _>("scope")?.as_str())?,
                    status: TombstoneStatus::parse(row.try_get::<String, _>("status")?.as_str())?,
                    requested_at: row.try_get("requested_at")?,
                    purge_after: row.try_get("purge_after")?,
                })
            })
            .collect()
    }

    async fn blocking_tombstone(
        &self,
        tenant_id: &str,
        agent_id: &str,
        conversation_id: Option<&str>,
    ) -> Result<Option<Tombstone>> {
        let mut query = String::from(
            "SELECT id, conversation_id, scope, status, requested_at, purge_after
             FROM deletion_tombstones
             WHERE tenant_id = ? AND agent_id = ? AND status <> 'backup_expired'",
        );
        if conversation_id.is_some() {
            query.push_str(" AND (conversation_id = ? OR scope = 'agent') ORDER BY status LIMIT 1");
        } else {
            query.push_str(" AND scope = 'agent' ORDER BY status LIMIT 1");
        }
        let mut q = sqlx::query(&query).bind(tenant_id).bind(agent_id);
        if let Some(cid) = conversation_id {
            q = q.bind(cid);
        }
        let row = q
            .fetch_optional(&self.pool)
            .await
            .context("failed to probe blocking tombstone")?;
        let Some(row) = row else {
            return Ok(None);
        };
        Ok(Some(Tombstone {
            id: row.try_get("id")?,
            tenant_id: tenant_id.to_owned(),
            agent_id: agent_id.to_owned(),
            conversation_id: row.try_get("conversation_id")?,
            scope: TombstoneScope::parse(row.try_get::<String, _>("scope")?.as_str())?,
            status: TombstoneStatus::parse(row.try_get::<String, _>("status")?.as_str())?,
            requested_at: row.try_get("requested_at")?,
            purge_after: row.try_get("purge_after")?,
        }))
    }

    async fn log_access(
        &self,
        actor_id: &str,
        tenant_id: &str,
        action: &str,
        scope: &str,
        result_count: i64,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO data_access_audit(
                id, actor_id, tenant_id, action, scope, result_count, created_at
             ) VALUES(?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(Uuid::now_v7().to_string())
        .bind(actor_id)
        .bind(tenant_id)
        .bind(action)
        .bind(scope)
        .bind(result_count)
        .bind(Utc::now().to_rfc3339())
        .execute(&self.pool)
        .await
        .context("failed to insert audit record")?;
        Ok(())
    }

    async fn list_audit(
        &self,
        tenant_id: &str,
        actor_id: Option<&str>,
        action: Option<&str>,
    ) -> Result<Vec<AuditRecord>> {
        let mut sql = String::from(
            "SELECT id, actor_id, action, scope, result_count, created_at
             FROM data_access_audit WHERE tenant_id = ?",
        );
        if actor_id.is_some() {
            sql.push_str(" AND actor_id = ?");
        }
        if action.is_some() {
            sql.push_str(" AND action = ?");
        }
        sql.push_str(" ORDER BY created_at DESC");
        let mut q = sqlx::query(&sql).bind(tenant_id);
        if let Some(a) = actor_id {
            q = q.bind(a);
        }
        if let Some(a) = action {
            q = q.bind(a);
        }
        let rows = q
            .fetch_all(&self.pool)
            .await
            .context("failed to list audit")?;
        rows.into_iter()
            .map(|row| {
                Ok(AuditRecord {
                    id: row.try_get("id")?,
                    actor_id: row.try_get("actor_id")?,
                    tenant_id: tenant_id.to_owned(),
                    action: row.try_get("action")?,
                    scope: row.try_get("scope")?,
                    result_count: row.try_get::<Option<i64>, _>("result_count")?.unwrap_or(0),
                    created_at: row.try_get("created_at")?,
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn sqlite_tombstone_state_machine_enforces_order_and_idempotence() {
        let repo = SqliteTombstoneRepository::open_in_memory().await.unwrap();
        let id = repo
            .request(
                "t",
                "a",
                Some("c1"),
                TombstoneScope::Conversation,
                "2027-01-01T00:00:00Z",
            )
            .await
            .unwrap();

        assert_eq!(
            repo.get(&id).await.unwrap().status,
            TombstoneStatus::Requested
        );

        // Same-state is a no-op.
        repo.advance(&id, TombstoneStatus::Requested, TombstoneStatus::Requested)
            .await
            .unwrap();

        repo.advance(&id, TombstoneStatus::Requested, TombstoneStatus::Fenced)
            .await
            .unwrap();

        // Reverse must fail.
        assert!(
            repo.advance(&id, TombstoneStatus::Fenced, TombstoneStatus::Requested)
                .await
                .is_err()
        );

        // Skip must fail.
        assert!(
            repo.advance(&id, TombstoneStatus::Fenced, TombstoneStatus::BackupExpired)
                .await
                .is_err()
        );

        repo.advance(&id, TombstoneStatus::Fenced, TombstoneStatus::LivePurged)
            .await
            .unwrap();
        repo.advance(
            &id,
            TombstoneStatus::LivePurged,
            TombstoneStatus::BackupExpired,
        )
        .await
        .unwrap();

        // Terminal advance must fail.
        assert!(
            repo.advance(
                &id,
                TombstoneStatus::BackupExpired,
                TombstoneStatus::LivePurged
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn sqlite_rejects_scope_identity_mismatch() {
        let repo = SqliteTombstoneRepository::open_in_memory().await.unwrap();
        assert!(
            repo.request(
                "t",
                "a",
                None,
                TombstoneScope::Conversation,
                "2027-01-01T00:00:00Z"
            )
            .await
            .is_err()
        );
        assert!(
            repo.request(
                "t",
                "a",
                Some("c1"),
                TombstoneScope::Agent,
                "2027-01-01T00:00:00Z"
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn sqlite_audit_round_trip() {
        let repo = SqliteTombstoneRepository::open_in_memory().await.unwrap();
        repo.log_access("actor-1", "t", "search", "conversation", 3)
            .await
            .unwrap();
        repo.log_access("actor-2", "t", "export", "agent", 1)
            .await
            .unwrap();
        let all = repo.list_audit("t", None, None).await.unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(
            repo.list_audit("t", Some("actor-1"), None)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            repo.list_audit("t", None, Some("export"))
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn sqlite_tombstone_survives_repo_reopen_for_backup_replay() {
        let dir = std::env::temp_dir().join(format!("sumi-compliance-test-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("compliance.db");

        let id = {
            let repo = SqliteTombstoneRepository::open(&path).await.unwrap();
            let id = repo
                .request(
                    "t",
                    "a",
                    Some("c"),
                    TombstoneScope::Conversation,
                    "2027-01-01T00:00:00Z",
                )
                .await
                .unwrap();
            repo.advance(&id, TombstoneStatus::Requested, TombstoneStatus::Fenced)
                .await
                .unwrap();
            repo.advance(&id, TombstoneStatus::Fenced, TombstoneStatus::LivePurged)
                .await
                .unwrap();
            id
        };

        // Simulate a backup replay: a new process opens the same compliance file.
        let repo = SqliteTombstoneRepository::open(&path).await.unwrap();
        let tombstone = repo.get(&id).await.unwrap();
        assert_eq!(tombstone.status, TombstoneStatus::LivePurged);
    }
}

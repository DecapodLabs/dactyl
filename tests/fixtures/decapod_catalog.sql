CREATE TABLE agent_prompts (
        id TEXT PRIMARY KEY,
        context TEXT NOT NULL,
        prompt_text TEXT NOT NULL,
        priority INTEGER DEFAULT 100,
        active INTEGER DEFAULT 1,
        usage_count INTEGER DEFAULT 0,
        last_shown_at TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT
    );
CREATE TABLE agents (
    agent_id TEXT PRIMARY KEY,
    trust_level TEXT NOT NULL DEFAULT 'basic',
    trust_granted_at TEXT,
    trust_granted_by TEXT NOT NULL DEFAULT 'system',
    last_seen TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    expertise_json TEXT NOT NULL DEFAULT '[]',
    category_claims_json TEXT NOT NULL DEFAULT '[]',
    updated_at TEXT NOT NULL
);
CREATE TABLE approvals (
        approval_id TEXT PRIMARY KEY,
        action_fingerprint TEXT NOT NULL,
        actor TEXT NOT NULL,
        ts TEXT NOT NULL,
        scope TEXT NOT NULL,
        expires_at TEXT
    );
CREATE TABLE archives (
        id TEXT PRIMARY KEY,
        path TEXT NOT NULL,
        content_hash TEXT NOT NULL,
        summary_hash TEXT NOT NULL,
        created_at TEXT NOT NULL
    );
CREATE TABLE categories (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        description TEXT DEFAULT '',
        keywords TEXT DEFAULT '',
        created_at TEXT NOT NULL
    );
CREATE TABLE claims (
        id TEXT PRIMARY KEY,
        subject TEXT NOT NULL,
        kind TEXT NOT NULL,
        provenance TEXT,
        created_at TEXT NOT NULL
    );
CREATE TABLE cron_jobs (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        description TEXT DEFAULT '',
        schedule TEXT NOT NULL,
        command TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'active',
        last_run TEXT,
        next_run TEXT,
        tags TEXT DEFAULT '',
        created_at TEXT NOT NULL,
        updated_at TEXT,
        dir_path TEXT NOT NULL,
        scope TEXT NOT NULL
    );
CREATE TABLE sessions (
        id TEXT PRIMARY KEY,
        tree_id TEXT NOT NULL,
        title TEXT NOT NULL,
        description TEXT DEFAULT '',
        status TEXT NOT NULL DEFAULT 'active',
        federation_node_id TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        completed_at TEXT,
        dir_path TEXT NOT NULL,
        scope TEXT NOT NULL DEFAULT 'repo',
        actor TEXT NOT NULL DEFAULT 'decapod'
    );
CREATE TABLE decisions (
        id TEXT PRIMARY KEY,
        session_id TEXT NOT NULL,
        question_id TEXT NOT NULL,
        tree_id TEXT NOT NULL,
        question_text TEXT NOT NULL,
        chosen_value TEXT NOT NULL,
        chosen_label TEXT NOT NULL,
        rationale TEXT DEFAULT '',
        user_note TEXT DEFAULT '',
        federation_node_id TEXT,
        created_at TEXT NOT NULL,
        actor TEXT NOT NULL DEFAULT 'decapod',
        FOREIGN KEY(session_id) REFERENCES sessions(id)
    );
CREATE TABLE events (
    event_id TEXT PRIMARY KEY,
    ts TEXT NOT NULL,
    seq INTEGER NOT NULL,
    stream TEXT NOT NULL,
    subject_kind TEXT,
    subject_id TEXT,
    event_type TEXT NOT NULL DEFAULT '',
    payload TEXT NOT NULL,
    actor TEXT NOT NULL DEFAULT 'decapod'
);
CREATE TABLE feedback (
        id TEXT PRIMARY KEY,
        source TEXT NOT NULL,
        text TEXT NOT NULL,
        links TEXT,
        created_at TEXT NOT NULL
    );
CREATE TABLE health_cache (
        claim_id TEXT PRIMARY KEY,
        computed_state TEXT NOT NULL,
        reason TEXT,
        updated_at TEXT NOT NULL,
        FOREIGN KEY(claim_id) REFERENCES claims(id)
    );
CREATE TABLE knowledge (
        id TEXT PRIMARY KEY,
        title TEXT NOT NULL,
        content TEXT NOT NULL,
        provenance TEXT NOT NULL,
        claim_id TEXT,
        tags TEXT DEFAULT '',
        created_at TEXT NOT NULL,
        updated_at TEXT,
        dir_path TEXT NOT NULL,
        scope TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'active',
        merge_key TEXT DEFAULT '',
        supersedes_id TEXT,
        ttl_policy TEXT NOT NULL DEFAULT 'persistent',
        expires_ts TEXT
    );
CREATE TABLE legacy_event_imports (
             filename TEXT PRIMARY KEY,
             content_hash TEXT NOT NULL,
             record_count INTEGER NOT NULL,
             imported_at TEXT NOT NULL
         );
CREATE TABLE meta (
        namespace TEXT NOT NULL,
        key TEXT NOT NULL,
        value TEXT NOT NULL,
        PRIMARY KEY(namespace, key)
    );
CREATE TABLE node_edges (
    id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL,
    target_id TEXT NOT NULL,
    edge_type TEXT NOT NULL,
    metadata TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    actor TEXT NOT NULL DEFAULT 'decapod'
);
CREATE TABLE nodes (
        id TEXT PRIMARY KEY,
        node_type TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'active',
        priority TEXT NOT NULL DEFAULT 'notable',
        confidence TEXT NOT NULL DEFAULT 'agent_inferred',
        title TEXT NOT NULL,
        body TEXT NOT NULL DEFAULT '',
        scope TEXT NOT NULL DEFAULT 'repo',
        tags TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        effective_from TEXT,
        effective_to TEXT,
        dir_path TEXT NOT NULL,
        actor TEXT NOT NULL DEFAULT 'decapod'
    );
CREATE TABLE obligations (
        id TEXT PRIMARY KEY,
        intent_ref TEXT NOT NULL,
        risk_tier TEXT NOT NULL,
        required_proofs TEXT NOT NULL, -- JSON array of claim IDs or proof labels
        state_commit_root TEXT,
        status TEXT NOT NULL DEFAULT 'open', -- open, met, failed
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        metadata TEXT -- JSON blob for extra info
    );
CREATE TABLE obligation_edges (
        edge_id TEXT PRIMARY KEY,
        from_id TEXT NOT NULL,
        to_id TEXT NOT NULL,
        kind TEXT NOT NULL DEFAULT 'depends_on',
        created_at TEXT NOT NULL,
        UNIQUE(from_id, to_id),
        FOREIGN KEY(from_id) REFERENCES obligations(id) ON DELETE CASCADE,
        FOREIGN KEY(to_id) REFERENCES obligations(id) ON DELETE CASCADE
    );
CREATE TABLE originals_index (
        content_hash TEXT PRIMARY KEY,
        event_id TEXT NOT NULL,
        ts TEXT NOT NULL,
        actor TEXT NOT NULL,
        kind TEXT NOT NULL,
        byte_size INTEGER NOT NULL,
        session_id TEXT
    );
CREATE TABLE preferences (
        id TEXT PRIMARY KEY,
        category TEXT NOT NULL,
        key TEXT NOT NULL,
        value TEXT NOT NULL,
        context TEXT,
        source TEXT NOT NULL,
        confidence INTEGER DEFAULT 100,
        created_at TEXT NOT NULL,
        updated_at TEXT,
        last_accessed_at TEXT,
        access_count INTEGER DEFAULT 0,
        UNIQUE(category, key)
    );
CREATE TABLE proof_events (
        event_id TEXT PRIMARY KEY,
        claim_id TEXT NOT NULL,
        ts TEXT NOT NULL,
        surface TEXT NOT NULL,
        result TEXT NOT NULL,
        sla_seconds INTEGER NOT NULL,
        FOREIGN KEY(claim_id) REFERENCES claims(id)
    );
CREATE TABLE reflexes (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        description TEXT DEFAULT '',
        trigger_type TEXT NOT NULL,
        trigger_config TEXT DEFAULT '{}',
        action_type TEXT NOT NULL,
        action_config TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'active',
        tags TEXT DEFAULT '',
        created_at TEXT NOT NULL,
        updated_at TEXT,
        dir_path TEXT NOT NULL,
        scope TEXT NOT NULL
    );
CREATE TABLE request_dedupe(
            request_id TEXT PRIMARY KEY,
            payload_hash TEXT NOT NULL,
            status TEXT NOT NULL,
            commit_marker TEXT,
            result_envelope TEXT NOT NULL,
            retry_after_ms_hint INTEGER,
            created_at TEXT NOT NULL
        );
CREATE TABLE risk_zones (
        id TEXT PRIMARY KEY,
        zone_name TEXT NOT NULL UNIQUE,
        description TEXT DEFAULT '',
        required_trust_level TEXT NOT NULL DEFAULT 'basic',
        requires_approval BOOLEAN NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL
    );
CREATE TABLE summaries (
        summary_hash TEXT PRIMARY KEY,
        ts TEXT NOT NULL,
        scope TEXT NOT NULL,
        original_hashes TEXT NOT NULL,
        summary_text TEXT NOT NULL,
        token_estimate INTEGER NOT NULL
    );
CREATE TABLE tasks (
        id TEXT PRIMARY KEY,
        hash TEXT NOT NULL,
        title TEXT NOT NULL,
        description TEXT DEFAULT '',
        tags TEXT DEFAULT '',
        owner TEXT DEFAULT '',
        due TEXT,
        ref TEXT DEFAULT '',
        status TEXT NOT NULL DEFAULT 'open',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        completed_at TEXT,
        closed_at TEXT,
        dir_path TEXT NOT NULL,
        scope TEXT NOT NULL,
        parent_task_id TEXT,
        priority TEXT DEFAULT 'medium',
        depends_on TEXT DEFAULT '',
        blocks TEXT DEFAULT '',
        category TEXT DEFAULT '',
        component TEXT DEFAULT '',
        assigned_to TEXT DEFAULT '',
        assigned_at TEXT,
        one_shot INTEGER DEFAULT 0
    , lease_expires_at TEXT, lease_generation INTEGER DEFAULT 0, lease_lifecycle TEXT DEFAULT '', intent_anchor TEXT DEFAULT '', revision INTEGER NOT NULL DEFAULT 0);
CREATE TABLE task_dependencies (
        id TEXT PRIMARY KEY,
        task_id TEXT NOT NULL,
        depends_on_task_id TEXT NOT NULL,
        created_at TEXT NOT NULL,
        UNIQUE(task_id, depends_on_task_id),
        FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE CASCADE,
        FOREIGN KEY(depends_on_task_id) REFERENCES tasks(id) ON DELETE CASCADE
    );
CREATE TABLE task_owners (
        id TEXT PRIMARY KEY,
        task_id TEXT NOT NULL,
        agent_id TEXT NOT NULL,
        claimed_at TEXT NOT NULL,
        claim_type TEXT NOT NULL DEFAULT 'primary',
        FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE CASCADE
    );
CREATE TABLE task_tags (
    task_id TEXT NOT NULL,
    tag TEXT NOT NULL,
    PRIMARY KEY(task_id, tag),
    FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE CASCADE
);
CREATE TABLE task_verification (
        todo_id TEXT PRIMARY KEY,
        proof_plan TEXT NOT NULL DEFAULT '[]',
        verification_artifacts TEXT,
        last_verified_at TEXT,
        last_verified_status TEXT,
        last_verified_notes TEXT,
        verification_policy_days INTEGER NOT NULL DEFAULT 90,
        updated_at TEXT NOT NULL,
        FOREIGN KEY(todo_id) REFERENCES tasks(id) ON DELETE CASCADE
    );
CREATE UNIQUE INDEX idx_events_stream_seq_unique ON events(stream, seq);
CREATE INDEX idx_events_stream_seq ON events(stream, seq);
CREATE INDEX idx_events_subject ON events(subject_kind, subject_id);
CREATE INDEX idx_tasks_status ON tasks(status);
CREATE INDEX idx_tasks_scope ON tasks(scope);
CREATE INDEX idx_task_owners_task ON task_owners(task_id);
CREATE UNIQUE INDEX idx_task_owners_task_agent ON task_owners(task_id, agent_id);
CREATE INDEX idx_task_tags_tag ON task_tags(tag);

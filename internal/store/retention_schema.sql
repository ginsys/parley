-- Migration 6: all hold/provenance and future ingestion/recovery DDL.
-- PR 4b consumes these tables without a third migration.
CREATE TABLE migration_incidents (
 incident_id TEXT PRIMARY KEY NOT NULL CHECK(length(incident_id)=36),
 from_schema_version INTEGER NOT NULL CHECK(from_schema_version=5),
 migration_version INTEGER NOT NULL CHECK(migration_version=6)
);
CREATE TABLE work_provenance (
 work_kind TEXT NOT NULL CHECK(work_kind IN ('envelope','pending','join')),
 work_id TEXT NOT NULL,
 envelope_id TEXT UNIQUE REFERENCES envelopes(id),
 provenance TEXT NOT NULL CHECK(provenance IN ('authenticated','legacy')),
 binding_id TEXT,
 credential_version INTEGER,
 migration_incident_id TEXT REFERENCES migration_incidents(incident_id),
 original_conversation TEXT NOT NULL,
 original_from_peer TEXT NOT NULL,
 original_to_peer TEXT,
 original_grant_version INTEGER,
 PRIMARY KEY(work_kind,work_id),
 FOREIGN KEY(binding_id,credential_version) REFERENCES credentials(binding_id,credential_version),
 CHECK((work_kind='envelope' AND envelope_id IS NOT NULL AND envelope_id=work_id AND original_to_peer IS NOT NULL AND typeof(original_grant_version)='integer') OR
       (work_kind IN ('pending','join') AND envelope_id IS NULL AND original_grant_version IS NULL)),
 CHECK((provenance='authenticated' AND binding_id IS NOT NULL AND credential_version IS NOT NULL AND migration_incident_id IS NULL) OR
       (provenance='legacy' AND work_kind='envelope' AND binding_id IS NULL AND credential_version IS NULL AND migration_incident_id IS NOT NULL))
);
CREATE INDEX idx_work_author ON work_provenance(binding_id,work_kind,work_id);
CREATE INDEX idx_legacy_author ON work_provenance(original_from_peer,work_kind,work_id) WHERE provenance='legacy';
CREATE TABLE migration_quarantine (
 quarantine_id TEXT PRIMARY KEY NOT NULL CHECK(length(quarantine_id)=36),
 incident_id TEXT NOT NULL REFERENCES migration_incidents(incident_id),
 work_kind TEXT NOT NULL DEFAULT 'envelope' CHECK(work_kind='envelope'),
 work_id TEXT NOT NULL,
 quarantine_version INTEGER NOT NULL DEFAULT 1 CHECK(typeof(quarantine_version)='integer' AND quarantine_version>0),
 status TEXT NOT NULL DEFAULT 'held' CHECK(status IN ('held','released','cancelled')),
 UNIQUE(work_kind,work_id),
 FOREIGN KEY(work_kind,work_id) REFERENCES work_provenance(work_kind,work_id)
);
CREATE INDEX idx_quarantine_work ON migration_quarantine(work_kind,work_id,status);
CREATE TABLE revocation_incidents (
 incident_id TEXT PRIMARY KEY NOT NULL CHECK(length(incident_id)=36),
 binding_id TEXT NOT NULL REFERENCES bindings(binding_id),
 credential_version INTEGER NOT NULL,
 binding_version INTEGER NOT NULL CHECK(typeof(binding_version)='integer' AND binding_version>0),
 kind TEXT NOT NULL CHECK(kind IN ('revoked','retired')),
 created_at_ns INTEGER NOT NULL CHECK(typeof(created_at_ns)='integer'),
 FOREIGN KEY(binding_id,credential_version) REFERENCES credentials(binding_id,credential_version)
);
CREATE INDEX idx_revocation_binding ON revocation_incidents(binding_id,incident_id);
CREATE TABLE security_holds (
 hold_id TEXT PRIMARY KEY NOT NULL CHECK(length(hold_id)=36),
 work_kind TEXT NOT NULL CHECK(work_kind IN ('envelope','pending','join')),
 work_id TEXT NOT NULL,
 incident_id TEXT NOT NULL CHECK(length(incident_id)=36),
 revocation_incident_id TEXT REFERENCES revocation_incidents(incident_id),
 recovery_incident_id TEXT REFERENCES recovery_incidents(incident_id),
 hold_version INTEGER NOT NULL DEFAULT 1 CHECK(typeof(hold_version)='integer' AND hold_version>0),
 status TEXT NOT NULL DEFAULT 'held' CHECK(status IN ('held','released','cancelled')),
 CHECK((revocation_incident_id IS NOT NULL AND revocation_incident_id=incident_id AND recovery_incident_id IS NULL) OR
       (recovery_incident_id IS NOT NULL AND recovery_incident_id=incident_id AND revocation_incident_id IS NULL)),
 UNIQUE(work_kind,work_id,incident_id),
 FOREIGN KEY(work_kind,work_id) REFERENCES work_provenance(work_kind,work_id)
);
CREATE INDEX idx_hold_work ON security_holds(work_kind,work_id,status);
CREATE TABLE work_dispositions (
 disposition_id TEXT PRIMARY KEY NOT NULL CHECK(length(disposition_id)=36),
 hold_id TEXT REFERENCES security_holds(hold_id),
 quarantine_id TEXT REFERENCES migration_quarantine(quarantine_id),
 previous_version INTEGER NOT NULL CHECK(typeof(previous_version)='integer' AND previous_version>0),
 action TEXT NOT NULL CHECK(action IN ('cancel','release')),
 reason_code TEXT NOT NULL CHECK(reason_code IN ('owner_reviewed','compromise','retirement','restore_reconciled','cancelled')),
 reason_note TEXT CHECK(reason_note IS NULL OR length(CAST(reason_note AS BLOB))<=512),
 evidence_ref TEXT CHECK(evidence_ref IS NULL OR length(evidence_ref)=36),
 audit_principal_id TEXT NOT NULL CHECK(length(audit_principal_id)=36),
 audit_operation_id TEXT NOT NULL CHECK(length(audit_operation_id)=36),
 created_at_ns INTEGER NOT NULL CHECK(typeof(created_at_ns)='integer'),
 CHECK((hold_id IS NOT NULL AND quarantine_id IS NULL) OR (hold_id IS NULL AND quarantine_id IS NOT NULL)),
 FOREIGN KEY(audit_principal_id,audit_operation_id) REFERENCES command_audit(principal_id,operation_id) DEFERRABLE INITIALLY DEFERRED,
 UNIQUE(hold_id,previous_version),
 UNIQUE(quarantine_id,previous_version)
);

-- A cursor is an opaque host-produced value; the trusted source verifies order.
-- An event records both ends so advancement cannot bypass an unresolved predecessor.
CREATE TABLE ingestion_cursors (
 binding_id TEXT PRIMARY KEY NOT NULL REFERENCES bindings(binding_id),
 source_id TEXT NOT NULL CHECK(length(source_id)=36),
 cursor TEXT NOT NULL CHECK(length(CAST(cursor AS BLOB))<=4096),
 cursor_version INTEGER NOT NULL DEFAULT 1 CHECK(typeof(cursor_version)='integer' AND cursor_version>0)
);
CREATE TABLE ingestion_evidence (
 binding_id TEXT NOT NULL REFERENCES bindings(binding_id),
 native_event_id TEXT NOT NULL CHECK(length(CAST(native_event_id AS BLOB)) BETWEEN 1 AND 4096),
 source_id TEXT NOT NULL CHECK(length(source_id)=36),
 source_revision TEXT NOT NULL CHECK(length(CAST(source_revision AS BLOB)) BETWEEN 1 AND 4096),
 source_digest BLOB NOT NULL CHECK(length(source_digest)=32),
 cursor_before TEXT NOT NULL CHECK(length(CAST(cursor_before AS BLOB))<=4096),
 cursor_after TEXT NOT NULL CHECK(length(CAST(cursor_after AS BLOB))<=4096),
 classification TEXT NOT NULL CHECK(classification IN ('pending','accepted','no_marker','malformed','rejected','held')),
 result_json TEXT CHECK(result_json IS NULL OR json_valid(result_json)),
 accepted_credential_version INTEGER,
 PRIMARY KEY(binding_id,native_event_id),
 FOREIGN KEY(binding_id,accepted_credential_version) REFERENCES credentials(binding_id,credential_version),
 CHECK((classification='pending' AND result_json IS NULL AND accepted_credential_version IS NULL) OR
       (classification='accepted' AND result_json IS NOT NULL AND accepted_credential_version IS NOT NULL) OR
       (classification IN ('no_marker','malformed','rejected','held') AND result_json IS NOT NULL AND accepted_credential_version IS NULL))
);
CREATE INDEX idx_ingestion_pending ON ingestion_evidence(binding_id,source_id,cursor_before,native_event_id) WHERE classification='pending';
CREATE TABLE ingestion_barriers (
 binding_id TEXT PRIMARY KEY NOT NULL REFERENCES bindings(binding_id),
 barrier_version INTEGER NOT NULL DEFAULT 1 CHECK(typeof(barrier_version)='integer' AND barrier_version>0),
 status TEXT NOT NULL CHECK(status IN ('held','resolved')),
 paused_cursor TEXT CHECK(paused_cursor IS NULL OR length(CAST(paused_cursor AS BLOB))<=4096),
 disposition_ref TEXT CHECK(disposition_ref IS NULL OR length(disposition_ref)=36),
 CHECK(status='held' OR disposition_ref IS NOT NULL)
);
CREATE TABLE ingestion_barrier_incidents (
 binding_id TEXT NOT NULL REFERENCES ingestion_barriers(binding_id),
 incident_id TEXT NOT NULL UNIQUE REFERENCES revocation_incidents(incident_id),
 barrier_version INTEGER NOT NULL CHECK(typeof(barrier_version)='integer' AND barrier_version>0),
 PRIMARY KEY(binding_id,incident_id)
);
CREATE TABLE ingestion_dispositions (
 disposition_id TEXT PRIMARY KEY NOT NULL CHECK(length(disposition_id)=36),
 binding_id TEXT NOT NULL REFERENCES bindings(binding_id),
 barrier_version INTEGER NOT NULL CHECK(typeof(barrier_version)='integer' AND barrier_version>0),
 source_id TEXT NOT NULL CHECK(length(source_id)=36),
 cursor_before TEXT NOT NULL,
 cursor_after TEXT NOT NULL,
 evidence_ref TEXT NOT NULL CHECK(length(evidence_ref)=36),
 audit_principal_id TEXT NOT NULL CHECK(length(audit_principal_id)=36),
 audit_operation_id TEXT NOT NULL CHECK(length(audit_operation_id)=36),
 FOREIGN KEY(audit_principal_id,audit_operation_id) REFERENCES command_audit(principal_id,operation_id) DEFERRABLE INITIALLY DEFERRED,
 UNIQUE(binding_id,barrier_version)
);

CREATE TABLE clock_checkpoint (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
	checkpoint_version INTEGER NOT NULL DEFAULT 1 CHECK(typeof(checkpoint_version)='integer' AND checkpoint_version>0),
 last_trusted_ns INTEGER CHECK(last_trusted_ns IS NULL OR typeof(last_trusted_ns)='integer')
);
INSERT INTO clock_checkpoint(singleton,last_trusted_ns) VALUES(1,NULL);
CREATE TABLE recovery_incidents (
 incident_id TEXT PRIMARY KEY NOT NULL CHECK(length(incident_id)=36),
 server_id TEXT NOT NULL REFERENCES installation(server_id),
 kind TEXT NOT NULL CHECK(kind IN ('clock','restore')),
 recovery_version INTEGER NOT NULL DEFAULT 1 CHECK(typeof(recovery_version)='integer' AND recovery_version>0),
 status TEXT NOT NULL DEFAULT 'held' CHECK(status IN ('held','reconciled','cleared')),
 last_trusted_ns INTEGER,
 observed_ns INTEGER,
 evidence_ref TEXT CHECK(evidence_ref IS NULL OR length(evidence_ref)=36),
 CHECK((kind='clock' AND typeof(last_trusted_ns)='integer' AND typeof(observed_ns)='integer') OR
       (kind='restore' AND last_trusted_ns IS NULL AND observed_ns IS NULL)),
 CHECK(status='held' OR evidence_ref IS NOT NULL)
);
CREATE INDEX idx_recovery_held ON recovery_incidents(status,incident_id);
CREATE TABLE retired_namespaces (
 principal_id TEXT PRIMARY KEY NOT NULL CHECK(length(principal_id)=36),
 binding_id TEXT UNIQUE REFERENCES bindings(binding_id),
 incident_id TEXT NOT NULL REFERENCES recovery_incidents(incident_id),
 retired_at_ns INTEGER NOT NULL CHECK(typeof(retired_at_ns)='integer'),
 CHECK(binding_id IS NULL OR binding_id=principal_id)
);

-- Broader reviewed restore recovery can record an unusable historical clock floor
-- disposition after retiring affected namespaces. Ordinary reconciliation cannot.
CREATE TABLE recovery_clock_dispositions (
 incident_id TEXT PRIMARY KEY NOT NULL REFERENCES recovery_incidents(incident_id),
	checkpoint_version INTEGER NOT NULL UNIQUE CHECK(typeof(checkpoint_version)='integer' AND checkpoint_version>0),
 previous_floor_ns INTEGER NOT NULL CHECK(typeof(previous_floor_ns)='integer'),
 reviewed_floor_ns INTEGER NOT NULL CHECK(typeof(reviewed_floor_ns)='integer'),
 audit_principal_id TEXT NOT NULL CHECK(length(audit_principal_id)=36),
 audit_operation_id TEXT NOT NULL CHECK(length(audit_operation_id)=36),
 FOREIGN KEY(audit_principal_id,audit_operation_id) REFERENCES command_audit(principal_id,operation_id) DEFERRABLE INITIALLY DEFERRED
);

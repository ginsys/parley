-- Migration 5. Historical adoption schemas remain frozen in their own files.
-- Epoch and view revision are process-local; schema version is PRAGMA user_version.
CREATE TABLE installation (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 server_id TEXT NOT NULL UNIQUE CHECK(length(server_id)=36),
 audit_sequence INTEGER NOT NULL DEFAULT 0 CHECK(typeof(audit_sequence)='integer' AND audit_sequence>=0)
);
CREATE TRIGGER installation_identity_immutable BEFORE UPDATE OF singleton,server_id ON installation
 BEGIN SELECT RAISE(ABORT,'immutable installation identity'); END;
CREATE TRIGGER installation_retained BEFORE DELETE ON installation
 BEGIN SELECT RAISE(ABORT,'installation retained'); END;

CREATE TABLE bindings (
 binding_id TEXT PRIMARY KEY NOT NULL CHECK(length(binding_id)=36),
 peer_id TEXT NOT NULL UNIQUE CHECK(length(CAST(peer_id AS BLOB)) BETWEEN 1 AND 256 AND length(trim(peer_id,' '))>0 AND peer_id NOT GLOB '*[^ -~]*' AND instr(peer_id,char(0))=0),
 host_kind TEXT NOT NULL CHECK(host_kind IN ('claude_code','codex_cli')),
 host_namespace_id TEXT NOT NULL CHECK(length(CAST(host_namespace_id AS BLOB)) BETWEEN 1 AND 4096),
 host_session_id TEXT NOT NULL CHECK(length(CAST(host_session_id AS BLOB)) BETWEEN 1 AND 4096),
 connector_uid INTEGER NOT NULL CHECK(typeof(connector_uid)='integer' AND connector_uid BETWEEN 0 AND 4294967295),
 status TEXT NOT NULL CHECK(status IN ('enabled','revoked','retired')),
 binding_version INTEGER NOT NULL CHECK(typeof(binding_version)='integer' AND binding_version>0),
 connection_generation INTEGER NOT NULL DEFAULT 0 CHECK(typeof(connection_generation)='integer' AND connection_generation>=0),
 UNIQUE(host_kind,host_namespace_id,host_session_id)
);
CREATE TRIGGER binding_identity_immutable BEFORE UPDATE OF binding_id,peer_id,host_kind,host_namespace_id,host_session_id,connector_uid ON bindings
 BEGIN SELECT RAISE(ABORT,'immutable binding identity'); END;
CREATE TRIGGER binding_retirement_terminal BEFORE UPDATE ON bindings WHEN OLD.status='retired'
 BEGIN SELECT RAISE(ABORT,'retired binding'); END;
CREATE TRIGGER binding_retained BEFORE DELETE ON bindings
 BEGIN SELECT RAISE(ABORT,'binding retained'); END;

CREATE TABLE credentials (
 binding_id TEXT NOT NULL REFERENCES bindings(binding_id),
 credential_version INTEGER NOT NULL CHECK(typeof(credential_version)='integer' AND credential_version>0),
 credential_id TEXT NOT NULL UNIQUE CHECK(length(credential_id)=36),
 verifier BLOB NOT NULL CHECK(typeof(verifier)='blob' AND length(verifier)=32),
 expires_at_ns INTEGER NOT NULL CHECK(typeof(expires_at_ns)='integer'),
 status TEXT NOT NULL CHECK(status IN ('current','superseded','expired','revoked')),
 PRIMARY KEY(binding_id,credential_version)
);
CREATE UNIQUE INDEX credentials_one_current ON credentials(binding_id) WHERE status='current';
CREATE TRIGGER credential_identity_immutable BEFORE UPDATE OF binding_id,credential_version,credential_id,verifier,expires_at_ns ON credentials
 BEGIN SELECT RAISE(ABORT,'immutable credential version'); END;
CREATE TRIGGER credential_terminal BEFORE UPDATE OF status ON credentials WHEN OLD.status!='current' AND NEW.status!=OLD.status
 BEGIN SELECT RAISE(ABORT,'terminal credential status'); END;
CREATE TRIGGER credential_retained BEFORE DELETE ON credentials
 BEGIN SELECT RAISE(ABORT,'credential retained'); END;

-- Enrollment commits pending evidence. The trusted publisher records a terminal
-- observation in a separate audited transaction after file I/O; replay never
-- republishes a credential, and unknown requires a new human rotation/revocation.
CREATE TABLE credential_publications (
 credential_id TEXT PRIMARY KEY NOT NULL REFERENCES credentials(credential_id),
 status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','published','unknown')),
 observed_at_ns INTEGER,
 CHECK((status='pending' AND observed_at_ns IS NULL) OR
       (status!='pending' AND typeof(observed_at_ns)='integer'))
);
CREATE TRIGGER publication_identity_immutable BEFORE UPDATE OF credential_id ON credential_publications
 BEGIN SELECT RAISE(ABORT,'immutable publication identity'); END;
CREATE TRIGGER publication_terminal BEFORE UPDATE ON credential_publications WHEN OLD.status!='pending'
 BEGIN SELECT RAISE(ABORT,'terminal publication evidence'); END;
CREATE TRIGGER publication_retained BEFORE DELETE ON credential_publications
 BEGIN SELECT RAISE(ABORT,'publication evidence retained'); END;

CREATE TABLE operation_results (
 principal_id TEXT NOT NULL CHECK(length(principal_id)=36),
 operation_id TEXT NOT NULL CHECK(length(operation_id)=36),
 operation_kind TEXT NOT NULL,
 request_digest BLOB NOT NULL CHECK(typeof(request_digest)='blob' AND length(request_digest)=32),
 result_json TEXT NOT NULL CHECK(json_valid(result_json)),
 PRIMARY KEY(principal_id,operation_id)
);
CREATE TRIGGER operation_result_immutable BEFORE UPDATE ON operation_results
 BEGIN SELECT RAISE(ABORT,'immutable operation result'); END;
CREATE TRIGGER operation_result_retained BEFORE DELETE ON operation_results
 BEGIN SELECT RAISE(ABORT,'operation result retained'); END;

CREATE TABLE command_audit (
 audit_id TEXT PRIMARY KEY NOT NULL CHECK(length(audit_id)=36),
 audit_sequence INTEGER NOT NULL UNIQUE CHECK(typeof(audit_sequence)='integer' AND audit_sequence>0),
 principal_id TEXT NOT NULL,
 connector_uid INTEGER NOT NULL CHECK(typeof(connector_uid)='integer' AND connector_uid BETWEEN 0 AND 4294967295),
 operation_id TEXT NOT NULL,
 operation_kind TEXT NOT NULL,
 request_digest BLOB NOT NULL CHECK(typeof(request_digest)='blob' AND length(request_digest)=32),
 server_time_ns INTEGER NOT NULL CHECK(typeof(server_time_ns)='integer'),
 result_json TEXT NOT NULL CHECK(json_valid(result_json)),
 commit_epoch TEXT NOT NULL CHECK(length(commit_epoch)=36),
 commit_revision INTEGER NOT NULL CHECK(typeof(commit_revision)='integer' AND commit_revision>0),
 UNIQUE(principal_id,operation_id),
 FOREIGN KEY(principal_id,operation_id) REFERENCES operation_results(principal_id,operation_id)
);
CREATE TRIGGER command_audit_immutable BEFORE UPDATE ON command_audit
 BEGIN SELECT RAISE(ABORT,'immutable audit'); END;
CREATE TRIGGER command_audit_retained BEFORE DELETE ON command_audit
 BEGIN SELECT RAISE(ABORT,'audit retained'); END;

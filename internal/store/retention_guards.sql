-- These guards install after migration-only legacy insertion.
CREATE TRIGGER authenticated_work_owner BEFORE INSERT ON work_provenance WHEN NEW.provenance='authenticated'
BEGIN
 SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM bindings WHERE binding_id=NEW.binding_id AND peer_id=NEW.original_from_peer)
 THEN RAISE(ABORT,'provenance owner mismatch') END;
 SELECT CASE WHEN NEW.work_kind='envelope' AND NOT EXISTS(
  SELECT 1 FROM envelopes WHERE id=NEW.envelope_id AND conversation=NEW.original_conversation
   AND from_peer=NEW.original_from_peer AND to_peer=NEW.original_to_peer AND grant_version=NEW.original_grant_version)
 THEN RAISE(ABORT,'provenance envelope mismatch') END;
END;
CREATE TRIGGER quarantine_migration_only BEFORE INSERT ON migration_quarantine
 BEGIN SELECT RAISE(ABORT,'quarantine is migration-only'); END;
CREATE TRIGGER quarantine_identity BEFORE UPDATE OF quarantine_id,incident_id,work_kind,work_id ON migration_quarantine
 BEGIN SELECT RAISE(ABORT,'immutable quarantine identity'); END;
CREATE TRIGGER quarantine_transition BEFORE UPDATE ON migration_quarantine
 WHEN OLD.status='cancelled' OR (OLD.status='released' AND NEW.status='held') OR
 OLD.quarantine_version=9223372036854775807 OR NEW.quarantine_version!=OLD.quarantine_version+1
 BEGIN SELECT RAISE(ABORT,'invalid quarantine transition'); END;
CREATE TRIGGER hold_identity BEFORE UPDATE OF hold_id,work_kind,work_id,incident_id,revocation_incident_id,recovery_incident_id ON security_holds
 BEGIN SELECT RAISE(ABORT,'immutable hold identity'); END;
CREATE TRIGGER hold_transition BEFORE UPDATE ON security_holds
 WHEN OLD.status='cancelled' OR (OLD.status='released' AND NEW.status='held') OR
 OLD.hold_version=9223372036854775807 OR NEW.hold_version!=OLD.hold_version+1
 BEGIN SELECT RAISE(ABORT,'invalid hold transition'); END;
CREATE TRIGGER hold_attribution BEFORE INSERT ON security_holds
BEGIN
 SELECT CASE WHEN NEW.revocation_incident_id IS NOT NULL AND NOT EXISTS(
  SELECT 1 FROM work_provenance p JOIN revocation_incidents r ON r.incident_id=NEW.revocation_incident_id
  JOIN bindings b ON b.binding_id=r.binding_id WHERE p.work_kind=NEW.work_kind AND p.work_id=NEW.work_id
   AND ((p.provenance='authenticated' AND p.binding_id=r.binding_id) OR
        (p.provenance='legacy' AND p.original_from_peer=b.peer_id)))
 THEN RAISE(ABORT,'revocation work attribution mismatch') END;
 SELECT CASE WHEN NEW.recovery_incident_id IS NOT NULL AND NOT EXISTS(
  SELECT 1 FROM recovery_incidents WHERE incident_id=NEW.recovery_incident_id AND kind='restore')
 THEN RAISE(ABORT,'recovery work attribution mismatch') END;
END;
CREATE TRIGGER cursor_identity BEFORE UPDATE OF binding_id,source_id ON ingestion_cursors
 BEGIN SELECT RAISE(ABORT,'immutable source identity'); END;
CREATE TRIGGER cursor_version BEFORE UPDATE ON ingestion_cursors
 WHEN OLD.cursor_version=9223372036854775807 OR NEW.cursor_version!=OLD.cursor_version+1
 BEGIN SELECT RAISE(ABORT,'invalid cursor version'); END;
CREATE TRIGGER event_identity BEFORE UPDATE OF binding_id,native_event_id,source_id,source_revision,source_digest,cursor_before,cursor_after ON ingestion_evidence
 BEGIN SELECT RAISE(ABORT,'immutable event identity'); END;
CREATE TRIGGER event_terminal BEFORE UPDATE ON ingestion_evidence WHEN OLD.classification!='pending' OR NEW.classification='pending'
 BEGIN SELECT RAISE(ABORT,'terminal event evidence'); END;
CREATE TRIGGER barrier_identity BEFORE UPDATE OF binding_id ON ingestion_barriers
 BEGIN SELECT RAISE(ABORT,'immutable barrier identity'); END;
CREATE TRIGGER barrier_transition BEFORE UPDATE ON ingestion_barriers
 WHEN OLD.barrier_version=9223372036854775807 OR NEW.barrier_version!=OLD.barrier_version+1 OR
 (OLD.status='held' AND NEW.status='held' AND NEW.paused_cursor IS NOT OLD.paused_cursor)
 BEGIN SELECT RAISE(ABORT,'invalid barrier transition'); END;
CREATE TRIGGER recovery_identity BEFORE UPDATE OF incident_id,server_id,kind,last_trusted_ns,observed_ns ON recovery_incidents
 BEGIN SELECT RAISE(ABORT,'immutable recovery identity'); END;
CREATE TRIGGER recovery_transition BEFORE UPDATE ON recovery_incidents
 WHEN OLD.status='cleared' OR (OLD.status='reconciled' AND NEW.status!='cleared') OR
 (OLD.status='held' AND NEW.status!='reconciled') OR
 OLD.recovery_version=9223372036854775807 OR NEW.recovery_version!=OLD.recovery_version+1 OR
 (OLD.evidence_ref IS NOT NULL AND NEW.evidence_ref IS NOT OLD.evidence_ref)
 BEGIN SELECT RAISE(ABORT,'invalid recovery transition'); END;
CREATE TRIGGER checkpoint_transition BEFORE UPDATE ON clock_checkpoint
 WHEN NEW.singleton!=OLD.singleton OR OLD.checkpoint_version=9223372036854775807 OR
 NEW.checkpoint_version!=OLD.checkpoint_version+1 OR NEW.last_trusted_ns IS NULL OR
 (NEW.last_trusted_ns<OLD.last_trusted_ns AND NOT EXISTS(
  SELECT 1 FROM recovery_clock_dispositions d JOIN recovery_incidents r ON r.incident_id=d.incident_id
  WHERE d.checkpoint_version=OLD.checkpoint_version AND d.previous_floor_ns=OLD.last_trusted_ns
   AND d.reviewed_floor_ns=NEW.last_trusted_ns AND r.kind='restore' AND r.status='held'))
 BEGIN SELECT RAISE(ABORT,'invalid clock checkpoint transition'); END;

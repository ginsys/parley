package store

// CredentialExpiryObserved denies the exact immutable credential while an
// observed terminal expiry is waiting for durable storage, even after rollback.
func (d *DB) CredentialExpiryObserved(id string) bool {
	_, denied := d.coordinator.credentialExpiries.Load(id)
	return denied
}

// RememberCredentialExpiry retains denial evidence before a fallible write.
// It contains no bearer material and never denies a newly rotated credential.
func (d *DB) RememberCredentialExpiry(c CredentialRecord) {
	d.coordinator.credentialExpiries.Store(c.ID, c.ExpiresAtNS)
}

// ForgetCredentialExpiry is only called after the observed identity is durable
// terminal state. Forgetting an observation never changes credential storage.
func (d *DB) ForgetCredentialExpiry(id string) { d.coordinator.credentialExpiries.Delete(id) }

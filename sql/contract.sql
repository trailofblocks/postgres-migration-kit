ALTER TABLE payments ALTER COLUMN state SET NOT NULL;
DROP TRIGGER payments_sync_state ON payments;
DROP FUNCTION sync_payment_state();
ALTER TABLE payments DROP COLUMN status, DROP CONSTRAINT payments_state_present;

UPDATE migration_progress SET stage='contracted' WHERE singleton;

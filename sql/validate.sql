ALTER TABLE payments
    VALIDATE CONSTRAINT payments_state_values,
    VALIDATE CONSTRAINT payments_state_present;

UPDATE migration_progress SET stage='validated' WHERE singleton;

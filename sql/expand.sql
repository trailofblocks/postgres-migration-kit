ALTER TABLE payments ADD COLUMN state text;
ALTER TABLE payments
    ADD CONSTRAINT payments_state_values CHECK (state IN ('pending','captured','refunded')) NOT VALID,
    ADD CONSTRAINT payments_state_present CHECK (state IS NOT NULL) NOT VALID;

CREATE FUNCTION sync_payment_state() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        IF NEW.status IS DISTINCT FROM OLD.status AND NEW.state IS NOT DISTINCT FROM OLD.state THEN
            NEW.state := lower(NEW.status);
        ELSIF NEW.state IS DISTINCT FROM OLD.state AND NEW.status IS NOT DISTINCT FROM OLD.status THEN
            NEW.status := upper(NEW.state);
        END IF;
    END IF;

    NEW.state := coalesce(NEW.state, lower(NEW.status));
    NEW.status := coalesce(NEW.status, upper(NEW.state));
    IF NEW.state IS DISTINCT FROM lower(NEW.status) THEN
        RAISE EXCEPTION 'payment state and legacy status disagree' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER payments_sync_state BEFORE INSERT OR UPDATE ON payments
FOR EACH ROW EXECUTE FUNCTION sync_payment_state();

UPDATE migration_progress SET stage='expanded', cutoff_id=(SELECT coalesce(max(id),0) FROM payments)
WHERE singleton;

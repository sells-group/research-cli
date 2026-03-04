-- schema/public/triggers.sql
-- Trigger functions and triggers

CREATE OR REPLACE FUNCTION public.trg_address_geom()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    IF NEW.latitude IS NOT NULL AND NEW.longitude IS NOT NULL THEN
        NEW.geom := ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326);
    ELSE
        NEW.geom := NULL;
    END IF;
    RETURN NEW;
END;
$function$;

CREATE TRIGGER address_geom_sync
    BEFORE INSERT OR UPDATE OF latitude, longitude
    ON public.company_addresses
    FOR EACH ROW
    EXECUTE FUNCTION trg_address_geom();

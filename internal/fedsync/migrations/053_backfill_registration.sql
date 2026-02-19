-- Backfill sec_registered for historical rows with no registration flag.
-- Every firm in adv_firms IS registered (they come from SEC FOIA or SEC compilation feed).
-- Rows with all three registration flags false were inserted before the derivation logic was added.
UPDATE fed_data.adv_filings f
SET sec_registered = true
WHERE NOT sec_registered
  AND NOT exempt_reporting
  AND NOT state_registered
  AND EXISTS (
    SELECT 1 FROM fed_data.adv_firms af
    WHERE af.crd_number = f.crd_number
  );

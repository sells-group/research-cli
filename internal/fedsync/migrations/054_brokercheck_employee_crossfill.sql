-- Cross-fill num_employees from BrokerCheck registered reps.
-- BrokerCheck num_registered_reps is a lower-bound proxy for employee count.
-- Fills rows where the source CSV lacked Item 5 (mainly ERA historical).
-- Future syncs overwrite with actual values from SEC filings.
UPDATE fed_data.adv_filings f
SET num_employees = bc.num_registered_reps,
    total_employees = bc.num_registered_reps
FROM fed_data.brokercheck bc
WHERE bc.crd_number = f.crd_number
  AND f.num_employees = 0
  AND bc.num_registered_reps > 0;

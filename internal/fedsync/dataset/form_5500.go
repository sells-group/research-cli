package dataset

import (
	"archive/zip"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rotisserie/eris"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/sells-group/research-cli/internal/db"
	"github.com/sells-group/research-cli/internal/fetcher"
)

const (
	form5500StartYear = 2020
	form5500BatchSize = 10000
)

// Form5500 implements the DOL Form 5500 (ERISA) retirement plan dataset.
// Data source: DOL EFAST2 bulk FOIA download — 4 separate ZIPs per year:
//   - F_5500 (main form): sponsor info, EIN, participant counts, plan metadata
//   - F_5500_SF (short form): small plan data with inline financials, 401(k) compliance
//   - F_SCH_H (Schedule H): full balance sheet, income statement, fee breakdowns
//   - F_SCH_C_PART1_ITEM1 (Schedule C): service provider directory
//
// Each ZIP maps to its own table; columns are stored 1:1 with DOL CSV headers.
type Form5500 struct{}

// Name implements Dataset.
func (d *Form5500) Name() string { return "form_5500" }

// Table implements Dataset.
func (d *Form5500) Table() string { return "fed_data.form_5500" }

// Phase implements Dataset.
func (d *Form5500) Phase() Phase { return Phase1 }

// Cadence implements Dataset.
func (d *Form5500) Cadence() Cadence { return Annual }

// ShouldRun implements Dataset.
func (d *Form5500) ShouldRun(now time.Time, lastSync *time.Time) bool {
	return AnnualAfter(now, lastSync, time.July)
}

// form5500ZipType identifies the different FOIA ZIP file types.
type form5500ZipType int

const (
	zipMainForm form5500ZipType = iota
	zipShortForm
	zipScheduleH
	zipScheduleC
)

// form5500Download describes a ZIP download.
type form5500Download struct {
	urlPattern string // format string with %d for year (used twice)
	zipType    form5500ZipType
	label      string
}

var form5500Downloads = []form5500Download{
	{
		urlPattern: "https://askebsa.dol.gov/FOIA%%20Files/%d/Latest/F_5500_%d_Latest.zip",
		zipType:    zipMainForm,
		label:      "main_form",
	},
	{
		urlPattern: "https://askebsa.dol.gov/FOIA%%20Files/%d/Latest/F_5500_SF_%d_Latest.zip",
		zipType:    zipShortForm,
		label:      "short_form",
	},
	{
		urlPattern: "https://askebsa.dol.gov/FOIA%%20Files/%d/Latest/F_SCH_H_%d_Latest.zip",
		zipType:    zipScheduleH,
		label:      "schedule_h",
	},
	{
		urlPattern: "https://askebsa.dol.gov/FOIA%%20Files/%d/Latest/F_SCH_C_PART1_ITEM1_%d_Latest.zip",
		zipType:    zipScheduleC,
		label:      "schedule_c",
	},
}

// ---------------------------------------------------------------------------
// Column type system for dynamic CSV parsing
// ---------------------------------------------------------------------------

// colType describes how to parse a CSV column value for the database.
type colType int

const (
	colText    colType = iota // sanitizeUTF8(trimQuotes(val)) → TEXT
	colNumeric                // nil for empty, parseFloat64 otherwise → NUMERIC
	colInt                    // nil for empty, parseInt otherwise → INTEGER
)

// colTypeFor determines the DB column type from a DOL column name.
// Naming conventions: _amt → NUMERIC, _cnt → INTEGER, everything else → TEXT.
// Special cases: row_order → INTEGER, *_cnt_boy → INTEGER.
func colTypeFor(name string) colType {
	switch {
	case strings.HasSuffix(name, "_amt"):
		return colNumeric
	case strings.HasSuffix(name, "_cnt"):
		return colInt
	case strings.HasSuffix(name, "_cnt_boy"):
		return colInt
	case name == "row_order":
		return colInt
	default:
		return colText
	}
}

// parseValue converts a raw CSV string to the appropriate Go type for CopyFrom.
func parseValue(val string, ct colType) any {
	val = trimQuotes(val)
	switch ct {
	case colNumeric:
		val = strings.TrimSpace(val)
		if val == "" {
			return nil
		}
		f, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return nil
		}
		return f
	case colInt:
		val = strings.TrimSpace(val)
		if val == "" {
			return nil
		}
		v, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return nil
		}
		return v
	default:
		return sanitizeUTF8(val)
	}
}

// ---------------------------------------------------------------------------
// Table specifications: valid columns for each target table
// ---------------------------------------------------------------------------

// form5500TableSpec maps zip type → (table name, conflict keys, valid column set).
type form5500TableSpec struct {
	table        string
	conflictKeys []string
	validCols    map[string]bool
	// requireACKID is the column name to check for non-empty ack_id.
	requireACKID string
	// requireEIN is the optional EIN column that must be non-empty (empty string = no check).
	requireEIN string
}

func newColSet(cols ...string) map[string]bool {
	m := make(map[string]bool, len(cols))
	for _, c := range cols {
		m[c] = true
	}
	return m
}

var form5500Specs = map[form5500ZipType]*form5500TableSpec{
	zipMainForm: {
		table:        "fed_data.form_5500",
		conflictKeys: []string{"ack_id"},
		requireACKID: "ack_id",
		requireEIN:   "spons_dfe_ein",
		validCols: newColSet(
			"ack_id", "form_plan_year_begin_date", "form_tax_prd",
			"type_plan_entity_cd", "type_dfe_plan_entity_cd",
			"initial_filing_ind", "amended_ind", "final_filing_ind",
			"short_plan_yr_ind", "collective_bargain_ind",
			"f5558_application_filed_ind", "ext_automatic_ind",
			"dfvc_program_ind", "ext_special_ind", "ext_special_text",
			"plan_name", "spons_dfe_pn", "plan_eff_date",
			"sponsor_dfe_name", "spons_dfe_dba_name", "spons_dfe_care_of_name",
			"spons_dfe_mail_us_address1", "spons_dfe_mail_us_address2",
			"spons_dfe_mail_us_city", "spons_dfe_mail_us_state", "spons_dfe_mail_us_zip",
			"spons_dfe_mail_foreign_addr1", "spons_dfe_mail_foreign_addr2",
			"spons_dfe_mail_foreign_city", "spons_dfe_mail_forgn_prov_st",
			"spons_dfe_mail_foreign_cntry", "spons_dfe_mail_forgn_postal_cd",
			"spons_dfe_loc_us_address1", "spons_dfe_loc_us_address2",
			"spons_dfe_loc_us_city", "spons_dfe_loc_us_state", "spons_dfe_loc_us_zip",
			"spons_dfe_loc_foreign_address1", "spons_dfe_loc_foreign_address2",
			"spons_dfe_loc_foreign_city", "spons_dfe_loc_forgn_prov_st",
			"spons_dfe_loc_foreign_cntry", "spons_dfe_loc_forgn_postal_cd",
			"spons_dfe_ein", "spons_dfe_phone_num", "business_code",
			"admin_name", "admin_care_of_name",
			"admin_us_address1", "admin_us_address2",
			"admin_us_city", "admin_us_state", "admin_us_zip",
			"admin_foreign_address1", "admin_foreign_address2",
			"admin_foreign_city", "admin_foreign_prov_state",
			"admin_foreign_cntry", "admin_foreign_postal_cd",
			"admin_ein", "admin_phone_num",
			"last_rpt_spons_name", "last_rpt_spons_ein", "last_rpt_plan_num",
			"admin_signed_date", "admin_signed_name",
			"spons_signed_date", "spons_signed_name",
			"dfe_signed_date", "dfe_signed_name",
			"tot_partcp_boy_cnt", "tot_active_partcp_cnt",
			"rtd_sep_partcp_rcvg_cnt", "rtd_sep_partcp_fut_cnt",
			"subtl_act_rtd_sep_cnt", "benef_rcvg_bnft_cnt",
			"tot_act_rtd_sep_benef_cnt", "partcp_account_bal_cnt",
			"sep_partcp_partl_vstd_cnt", "contrib_emplrs_cnt",
			"type_pension_bnft_code", "type_welfare_bnft_code",
			"funding_insurance_ind", "funding_sec412_ind",
			"funding_trust_ind", "funding_gen_asset_ind",
			"benefit_insurance_ind", "benefit_sec412_ind",
			"benefit_trust_ind", "benefit_gen_asset_ind",
			"sch_r_attached_ind", "sch_mb_attached_ind",
			"sch_sb_attached_ind", "sch_h_attached_ind",
			"sch_i_attached_ind", "sch_a_attached_ind",
			"num_sch_a_attached_cnt", "sch_c_attached_ind",
			"sch_d_attached_ind", "sch_g_attached_ind",
			"filing_status", "date_received",
			"valid_admin_signature", "valid_dfe_signature", "valid_sponsor_signature",
			"admin_phone_num_foreign", "spons_dfe_phone_num_foreign",
			"admin_name_same_spon_ind", "admin_address_same_spon_ind",
			"preparer_name", "preparer_firm_name",
			"preparer_us_address1", "preparer_us_address2",
			"preparer_us_city", "preparer_us_state", "preparer_us_zip",
			"preparer_foreign_address1", "preparer_foreign_address2",
			"preparer_foreign_city", "preparer_foreign_prov_state",
			"preparer_foreign_cntry", "preparer_foreign_postal_cd",
			"preparer_phone_num", "preparer_phone_num_foreign",
			"tot_act_partcp_boy_cnt",
			"subj_m1_filing_req_ind", "compliance_m1_filing_req_ind",
			"m1_receipt_confirmation_code",
			"admin_manual_signed_date", "admin_manual_signed_name",
			"last_rpt_plan_name",
			"spons_manual_signed_date", "spons_manual_signed_name",
			"dfe_manual_signed_date", "dfe_manual_signed_name",
			"adopted_plan_perm_sec_act", "partcp_account_bal_cnt_boy",
			"sch_dcg_attached_ind", "num_sch_dcg_attached_cnt",
			"sch_mep_attached_ind",
		),
	},
	zipShortForm: {
		table:        "fed_data.form_5500_sf",
		conflictKeys: []string{"ack_id"},
		requireACKID: "ack_id",
		requireEIN:   "sf_spons_ein",
		validCols: newColSet(
			"ack_id", "sf_plan_year_begin_date", "sf_tax_prd",
			"sf_plan_entity_cd", "sf_initial_filing_ind",
			"sf_amended_ind", "sf_final_filing_ind", "sf_short_plan_yr_ind",
			"sf_5558_application_filed_ind", "sf_ext_automatic_ind",
			"sf_dfvc_program_ind", "sf_ext_special_ind", "sf_ext_special_text",
			"sf_plan_name", "sf_plan_num", "sf_plan_eff_date",
			"sf_sponsor_name", "sf_sponsor_dfe_dba_name",
			"sf_spons_us_address1", "sf_spons_us_address2",
			"sf_spons_us_city", "sf_spons_us_state", "sf_spons_us_zip",
			"sf_spons_foreign_address1", "sf_spons_foreign_address2",
			"sf_spons_foreign_city", "sf_spons_foreign_prov_state",
			"sf_spons_foreign_cntry", "sf_spons_foreign_postal_cd",
			"sf_spons_ein", "sf_spons_phone_num", "sf_business_code",
			"sf_admin_name", "sf_admin_care_of_name",
			"sf_admin_us_address1", "sf_admin_us_address2",
			"sf_admin_us_city", "sf_admin_us_state", "sf_admin_us_zip",
			"sf_admin_foreign_address1", "sf_admin_foreign_address2",
			"sf_admin_foreign_city", "sf_admin_foreign_prov_state",
			"sf_admin_foreign_cntry", "sf_admin_foreign_postal_cd",
			"sf_admin_ein", "sf_admin_phone_num",
			"sf_last_rpt_spons_name", "sf_last_rpt_spons_ein", "sf_last_rpt_plan_num",
			"sf_tot_partcp_boy_cnt", "sf_tot_act_rtd_sep_benef_cnt",
			"sf_partcp_account_bal_cnt",
			"sf_eligible_assets_ind", "sf_iqpa_waiver_ind",
			"sf_tot_assets_boy_amt", "sf_tot_liabilities_boy_amt", "sf_net_assets_boy_amt",
			"sf_tot_assets_eoy_amt", "sf_tot_liabilities_eoy_amt", "sf_net_assets_eoy_amt",
			"sf_emplr_contrib_income_amt", "sf_particip_contrib_income_amt",
			"sf_oth_contrib_rcvd_amt", "sf_other_income_amt", "sf_tot_income_amt",
			"sf_tot_distrib_bnft_amt", "sf_corrective_deemed_distr_amt",
			"sf_admin_srvc_providers_amt", "sf_oth_expenses_amt",
			"sf_tot_expenses_amt", "sf_net_income_amt", "sf_tot_plan_transfers_amt",
			"sf_type_pension_bnft_code", "sf_type_welfare_bnft_code",
			"sf_fail_transmit_contrib_ind", "sf_fail_transmit_contrib_amt",
			"sf_party_in_int_not_rptd_ind", "sf_party_in_int_not_rptd_amt",
			"sf_plan_ins_fdlty_bond_ind", "sf_plan_ins_fdlty_bond_amt",
			"sf_loss_discv_dur_year_ind", "sf_loss_discv_dur_year_amt",
			"sf_broker_fees_paid_ind", "sf_broker_fees_paid_amt",
			"sf_fail_provide_benef_due_ind", "sf_fail_provide_benef_due_amt",
			"sf_partcp_loans_ind", "sf_partcp_loans_eoy_amt",
			"sf_plan_blackout_period_ind", "sf_comply_blackout_notice_ind",
			"sf_db_plan_funding_reqd_ind", "sf_dc_plan_funding_reqd_ind",
			"sf_ruling_letter_grant_date",
			"sf_sec_412_req_contrib_amt", "sf_emplr_contrib_paid_amt",
			"sf_funding_deficiency_amt", "sf_funding_deadline_ind",
			"sf_res_term_plan_adpt_ind", "sf_res_term_plan_adpt_amt",
			"sf_all_plan_ast_distrib_ind",
			"sf_admin_signed_date", "sf_admin_signed_name",
			"sf_spons_signed_date", "sf_spons_signed_name",
			"filing_status", "date_received",
			"valid_admin_signature", "valid_sponsor_signature",
			"sf_admin_phone_num_foreign", "sf_spons_care_of_name",
			"sf_spons_loc_foreign_address1", "sf_spons_loc_foreign_address2",
			"sf_spons_loc_foreign_city", "sf_spons_loc_foreign_cntry",
			"sf_spons_loc_foreign_postal_cd", "sf_spons_loc_foreign_prov_stat",
			"sf_spons_loc_us_address1", "sf_spons_loc_us_address2",
			"sf_spons_loc_us_city", "sf_spons_loc_us_state", "sf_spons_loc_us_zip",
			"sf_spons_phone_num_foreign",
			"sf_admin_name_same_spon_ind", "sf_admin_addrss_same_spon_ind",
			"sf_preparer_name", "sf_preparer_firm_name",
			"sf_preparer_us_address1", "sf_preparer_us_address2",
			"sf_preparer_us_city", "sf_preparer_us_state", "sf_preparer_us_zip",
			"sf_preparer_foreign_address1", "sf_preparer_foreign_address2",
			"sf_preparer_foreign_city", "sf_preparer_foreign_prov_state",
			"sf_preparer_foreign_cntry", "sf_preparer_foreign_postal_cd",
			"sf_preparer_phone_num", "sf_preparer_phone_num_foreign",
			"sf_fdcry_trust_name", "sf_fdcry_trust_ein",
			"sf_unp_min_cont_cur_yrtot_amt",
			"sf_covered_pbgc_insurance_ind",
			"sf_tot_act_partcp_boy_cnt", "sf_tot_act_partcp_eoy_cnt",
			"sf_sep_partcp_partl_vstd_cnt",
			"sf_trus_inc_unrel_tax_inc_ind", "sf_trus_inc_unrel_tax_inc_amt",
			"sf_fdcry_truste_cust_name", "sf_fdcry_truste_cust_phone_num",
			"sf_fdcry_trus_cus_phon_numfore",
			"sf_401k_plan_ind", "sf_401k_satisfy_rqmts_ind",
			"sf_adp_acp_test_ind", "sf_mthd_used_satisfy_rqmts_ind",
			"sf_plan_satisfy_tests_ind", "sf_plan_timely_amended_ind",
			"sf_last_plan_amendment_date", "sf_tax_code",
			"sf_last_opin_advi_date", "sf_last_opin_advi_serial_num",
			"sf_fav_determ_ltr_date", "sf_plan_maintain_us_terri_ind",
			"sf_in_service_distrib_ind", "sf_in_service_distrib_amt",
			"sf_min_req_distrib_ind",
			"sf_admin_manual_sign_date", "sf_admin_manual_signed_name",
			"sf_401k_design_based_safe_ind",
			"sf_401k_prior_year_adp_ind", "sf_401k_current_year_adp_ind",
			"sf_401k_na_ind",
			"sf_mthd_ratio_prcnt_test_ind", "sf_mthd_avg_bnft_test_ind",
			"sf_mthd_na_ind", "sf_distrib_made_employe_62_ind",
			"sf_last_rpt_plan_name", "sf_premium_filing_confirm_no",
			"sf_spons_manual_signed_date", "sf_spons_manual_signed_name",
			"sf_pbgc_notified_cd", "sf_pbgc_notified_explan_text",
			"sf_adopted_plan_perm_sec_act", "collectively_bargained",
			"sf_partcp_account_bal_cnt_boy",
			"sf_401k_design_based_safe_harbor_ind",
			"sf_401k_prior_year_adp_test_ind",
			"sf_401k_current_year_adp_test_ind",
			"sf_opin_letter_date", "sf_opin_letter_serial_num",
		),
	},
	zipScheduleH: {
		table:        "fed_data.form_5500_schedule_h",
		conflictKeys: []string{"ack_id"},
		requireACKID: "ack_id",
		validCols: newColSet(
			"ack_id", "sch_h_plan_year_begin_date", "sch_h_tax_prd",
			"sch_h_pn", "sch_h_ein",
			"non_int_bear_cash_boy_amt", "emplr_contrib_boy_amt",
			"partcp_contrib_boy_amt", "other_receivables_boy_amt",
			"int_bear_cash_boy_amt", "govt_sec_boy_amt",
			"corp_debt_preferred_boy_amt", "corp_debt_other_boy_amt",
			"pref_stock_boy_amt", "common_stock_boy_amt",
			"joint_venture_boy_amt", "real_estate_boy_amt",
			"other_loans_boy_amt", "partcp_loans_boy_amt",
			"int_common_tr_boy_amt", "int_pool_sep_acct_boy_amt",
			"int_master_tr_boy_amt", "int_103_12_invst_boy_amt",
			"int_reg_invst_co_boy_amt", "ins_co_gen_acct_boy_amt",
			"oth_invst_boy_amt", "emplr_sec_boy_amt",
			"emplr_prop_boy_amt", "bldgs_used_boy_amt",
			"tot_assets_boy_amt", "bnfts_payable_boy_amt",
			"oprtng_payable_boy_amt", "acquis_indbt_boy_amt",
			"other_liab_boy_amt", "tot_liabilities_boy_amt",
			"net_assets_boy_amt",
			"non_int_bear_cash_eoy_amt", "emplr_contrib_eoy_amt",
			"partcp_contrib_eoy_amt", "other_receivables_eoy_amt",
			"int_bear_cash_eoy_amt", "govt_sec_eoy_amt",
			"corp_debt_preferred_eoy_amt", "corp_debt_other_eoy_amt",
			"pref_stock_eoy_amt", "common_stock_eoy_amt",
			"joint_venture_eoy_amt", "real_estate_eoy_amt",
			"other_loans_eoy_amt", "partcp_loans_eoy_amt",
			"int_common_tr_eoy_amt", "int_pool_sep_acct_eoy_amt",
			"int_master_tr_eoy_amt", "int_103_12_invst_eoy_amt",
			"int_reg_invst_co_eoy_amt", "ins_co_gen_acct_eoy_amt",
			"oth_invst_eoy_amt", "emplr_sec_eoy_amt",
			"emplr_prop_eoy_amt", "bldgs_used_eoy_amt",
			"tot_assets_eoy_amt", "bnfts_payable_eoy_amt",
			"oprtng_payable_eoy_amt", "acquis_indbt_eoy_amt",
			"other_liab_eoy_amt", "tot_liabilities_eoy_amt",
			"net_assets_eoy_amt",
			"emplr_contrib_income_amt", "participant_contrib_amt",
			"oth_contrib_rcvd_amt", "non_cash_contrib_bs_amt",
			"tot_contrib_amt",
			"int_bear_cash_amt", "int_on_govt_sec_amt",
			"int_on_corp_debt_amt", "int_on_oth_loans_amt",
			"int_on_partcp_loans_amt", "int_on_oth_invst_amt",
			"total_interest_amt",
			"divnd_pref_stock_amt", "divnd_common_stock_amt",
			"registered_invst_amt", "total_dividends_amt",
			"total_rents_amt",
			"aggregate_proceeds_amt", "aggregate_costs_amt",
			"tot_gain_loss_sale_ast_amt",
			"unrealzd_apprctn_re_amt", "unrealzd_apprctn_oth_amt",
			"tot_unrealzd_apprctn_amt",
			"gain_loss_com_trust_amt", "gain_loss_pool_sep_amt",
			"gain_loss_master_tr_amt", "gain_loss_103_12_invst_amt",
			"gain_loss_reg_invst_amt",
			"other_income_amt", "tot_income_amt",
			"distrib_drt_partcp_amt", "ins_carrier_bnfts_amt",
			"oth_bnft_payment_amt", "tot_distrib_bnft_amt",
			"tot_corrective_distrib_amt", "tot_deemed_distr_part_lns_amt",
			"tot_int_expense_amt",
			"professional_fees_amt", "contract_admin_fees_amt",
			"invst_mgmt_fees_amt", "other_admin_fees_amt",
			"tot_admin_expenses_amt", "tot_expenses_amt",
			"net_income_amt",
			"tot_transfers_to_amt", "tot_transfers_from_amt",
			"acctnt_opinion_type_cd", "acct_performed_ltd_audit_ind",
			"accountant_firm_name", "accountant_firm_ein",
			"acct_opin_not_on_file_ind",
			"fail_transmit_contrib_ind", "fail_transmit_contrib_amt",
			"loans_in_default_ind", "loans_in_default_amt",
			"leases_in_default_ind", "leases_in_default_amt",
			"party_in_int_not_rptd_ind", "party_in_int_not_rptd_amt",
			"plan_ins_fdlty_bond_ind", "plan_ins_fdlty_bond_amt",
			"loss_discv_dur_year_ind", "loss_discv_dur_year_amt",
			"asset_undeterm_val_ind", "asset_undeterm_val_amt",
			"non_cash_contrib_ind", "non_cash_contrib_amt",
			"ast_held_invst_ind", "five_prcnt_trans_ind",
			"all_plan_ast_distrib_ind",
			"fail_provide_benefit_due_ind", "fail_provide_benefit_due_amt",
			"plan_blackout_period_ind", "comply_blackout_notice_ind",
			"res_term_plan_adpt_ind", "res_term_plan_adpt_amt",
			"fdcry_trust_ein", "fdcry_trust_name",
			"covered_pbgc_insurance_ind",
			"trust_incur_unrel_tax_inc_ind", "trust_incur_unrel_tax_inc_amt",
			"in_service_distrib_ind", "in_service_distrib_amt",
			"fdcry_trustee_cust_name",
			"fdcry_trust_cust_phon_num", "fdcry_trust_cust_phon_nu_fore",
			"distrib_made_employee_62_ind",
			"premium_filing_confirm_number",
			"acct_perf_ltd_audit_103_8_ind", "acct_perf_ltd_audit_103_12_ind",
			"acct_perf_not_ltd_audit_ind",
			"salaries_allowances_amt", "oth_recordkeeping_fees_amt",
			"iqpa_audit_fees_amt", "trustee_custodial_fees_amt",
			"actuarial_fees_amt", "legal_fees_amt",
			"valuation_appraisal_fees_amt", "other_trustee_fees_expenses_amt",
		),
	},
	zipScheduleC: {
		table:        "fed_data.form_5500_providers",
		conflictKeys: []string{"ack_id", "row_order"},
		requireACKID: "ack_id",
		validCols: newColSet(
			"ack_id", "row_order",
			"provider_eligible_name", "provider_eligible_ein",
			"provider_eligible_us_address1", "provider_eligible_us_address2",
			"provider_eligible_us_city", "provider_eligible_us_state",
			"provider_eligible_us_zip",
			"prov_eligible_foreign_address1", "prov_eligible_foreign_address2",
			"prov_eligible_foreign_city", "prov_eligible_foreign_prov_st",
			"prov_eligible_foreign_cntry", "prov_eligible_foreign_post_cd",
		),
	},
}

// ---------------------------------------------------------------------------
// Sync orchestration
// ---------------------------------------------------------------------------

// Sync fetches and loads DOL Form 5500 data for years 2020 through currentYear-1.
func (d *Form5500) Sync(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string) (*SyncResult, error) {
	log := zap.L().With(zap.String("dataset", "form_5500"))
	var totalRows atomic.Int64

	currentYear := time.Now().Year() - 1

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(2) // ZIPs are large (50-200MB), limit concurrency

	for year := form5500StartYear; year <= currentYear; year++ {
		g.Go(func() error {
			rows, err := d.syncYear(gctx, pool, f, tempDir, year, log)
			if err != nil {
				return err
			}
			totalRows.Add(rows)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return &SyncResult{
		RowsSynced: totalRows.Load(),
		Metadata:   map[string]any{"start_year": form5500StartYear, "end_year": currentYear},
	}, nil
}

// syncYear downloads and processes all 4 Form 5500 ZIPs for a single year.
func (d *Form5500) syncYear(ctx context.Context, pool db.Pool, f fetcher.Fetcher, tempDir string, year int, log *zap.Logger) (int64, error) {
	var totalRows int64

	for _, dl := range form5500Downloads {
		url := fmt.Sprintf(dl.urlPattern, year, year)
		zipPath := filepath.Join(tempDir, fmt.Sprintf("f5500_%s_%d.zip", dl.label, year))

		log.Info("downloading Form 5500 data",
			zap.Int("year", year), zap.String("type", dl.label), zap.String("url", url))

		if _, err := f.DownloadToFile(ctx, url, zipPath); err != nil {
			if strings.Contains(err.Error(), "status 404") {
				log.Info("Form 5500 data not yet available, skipping",
					zap.Int("year", year), zap.String("type", dl.label))
				continue
			}
			return totalRows, eris.Wrapf(err, "form_5500: download %s year %d", dl.label, year)
		}

		rows, err := d.processZip(ctx, pool, zipPath, dl.zipType)
		if err != nil {
			return totalRows, eris.Wrapf(err, "form_5500: process %s year %d", dl.label, year)
		}

		totalRows += rows
		log.Info("processed Form 5500 file",
			zap.Int("year", year), zap.String("type", dl.label), zap.Int64("rows", rows))

		_ = os.Remove(zipPath)
	}

	return totalRows, nil
}

// processZip opens a Form 5500 ZIP and processes the first CSV found.
// Each DOL FOIA ZIP contains exactly one CSV plus a layout.txt file.
func (d *Form5500) processZip(ctx context.Context, pool db.Pool, zipPath string, zt form5500ZipType) (int64, error) {
	zr, err := zip.OpenReader(zipPath)
	if err != nil {
		return 0, eris.Wrap(err, "form_5500: open zip")
	}
	defer zr.Close() //nolint:errcheck

	spec, ok := form5500Specs[zt]
	if !ok {
		return 0, eris.Errorf("form_5500: unknown zip type %d", zt)
	}

	for _, zf := range zr.File {
		if !strings.HasSuffix(strings.ToLower(zf.Name), ".csv") {
			continue
		}

		rc, err := zf.Open()
		if err != nil {
			return 0, eris.Wrapf(err, "form_5500: open file %s in zip", zf.Name)
		}

		n, parseErr := d.parseCSVDynamic(ctx, pool, rc, spec)
		_ = rc.Close()
		if parseErr != nil {
			return 0, parseErr
		}
		return n, nil
	}

	return 0, eris.New("form_5500: no CSV found in zip")
}

// ---------------------------------------------------------------------------
// Dynamic CSV parser — single function handles all 4 CSV types
// ---------------------------------------------------------------------------

// parseCSVDynamic reads a CSV and upserts all recognized columns into the target table.
// It reads the header, intersects with the spec's valid columns, and dynamically
// builds the column list and parsing functions per row.
func (d *Form5500) parseCSVDynamic(ctx context.Context, pool db.Pool, r io.Reader, spec *form5500TableSpec) (int64, error) {
	reader := csv.NewReader(r)
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true

	header, err := reader.Read()
	if err != nil {
		return 0, eris.Wrapf(err, "form_5500: read CSV header for %s", spec.table)
	}

	// Build column mapping: intersect CSV header with valid columns.
	// activeCols[i] = (db column name, csv index, colType) for columns to insert.
	type activeCol struct {
		name  string
		idx   int
		ctype colType
	}
	var activeCols []activeCol
	var dbCols []string

	// Also find the indices for required fields (ack_id, optional EIN).
	ackIDIdx := -1
	einIdx := -1

	for i, rawCol := range header {
		col := strings.ToLower(strings.TrimSpace(rawCol))
		if !spec.validCols[col] {
			continue
		}
		ct := colTypeFor(col)
		activeCols = append(activeCols, activeCol{name: col, idx: i, ctype: ct})
		dbCols = append(dbCols, col)

		if col == spec.requireACKID {
			ackIDIdx = i
		}
		if spec.requireEIN != "" && col == spec.requireEIN {
			einIdx = i
		}
	}

	if len(activeCols) == 0 {
		return 0, eris.Errorf("form_5500: no valid columns found in CSV header for %s", spec.table)
	}

	var batch [][]any
	var totalRows int64

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue // skip malformed rows
		}

		// Skip rows with empty ack_id.
		if ackIDIdx >= 0 && ackIDIdx < len(record) {
			if trimQuotes(record[ackIDIdx]) == "" {
				continue
			}
		}

		// Skip rows with empty EIN (if required).
		if einIdx >= 0 && einIdx < len(record) {
			if trimQuotes(record[einIdx]) == "" {
				continue
			}
		}

		row := make([]any, len(activeCols))
		for j, ac := range activeCols {
			val := ""
			if ac.idx < len(record) {
				val = record[ac.idx]
			}
			row[j] = parseValue(val, ac.ctype)
		}

		batch = append(batch, row)

		if len(batch) >= form5500BatchSize {
			n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
				Table:        spec.table,
				Columns:      dbCols,
				ConflictKeys: spec.conflictKeys,
			}, batch)
			if err != nil {
				return totalRows, eris.Wrapf(err, "form_5500: bulk upsert %s", spec.table)
			}
			totalRows += n
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		n, err := db.BulkUpsert(ctx, pool, db.UpsertConfig{
			Table:        spec.table,
			Columns:      dbCols,
			ConflictKeys: spec.conflictKeys,
		}, batch)
		if err != nil {
			return totalRows, eris.Wrapf(err, "form_5500: bulk upsert %s final batch", spec.table)
		}
		totalRows += n
	}

	return totalRows, nil
}

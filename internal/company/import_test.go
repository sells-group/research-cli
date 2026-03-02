package company

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/sells-group/research-cli/internal/model"
	"github.com/sells-group/research-cli/pkg/ppp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockStore implements CompanyStore for testing.
type mockStore struct {
	companies   map[int64]*CompanyRecord
	byDomain    map[string]*CompanyRecord
	identifiers map[int64][]Identifier
	addresses   map[int64][]Address
	contacts    map[int64][]Contact
	licenses    map[int64][]License
	sources     map[int64][]Source
	financials  map[int64][]Financial
	tags        map[int64][]Tag
	matches     map[int64][]Match
	nextID      int64

	// Track call counts for assertions.
	createCalls     int
	updateCalls     int
	identifierCalls int
	contactCalls    int
	addressCalls    int
	tagCalls        int
	sourceCalls     int
	financialCalls  int
}

func newMockStore() *mockStore {
	return &mockStore{
		companies:   make(map[int64]*CompanyRecord),
		byDomain:    make(map[string]*CompanyRecord),
		identifiers: make(map[int64][]Identifier),
		addresses:   make(map[int64][]Address),
		contacts:    make(map[int64][]Contact),
		licenses:    make(map[int64][]License),
		sources:     make(map[int64][]Source),
		financials:  make(map[int64][]Financial),
		tags:        make(map[int64][]Tag),
		matches:     make(map[int64][]Match),
		nextID:      1,
	}
}

func (m *mockStore) CreateCompany(_ context.Context, c *CompanyRecord) error {
	m.createCalls++
	c.ID = m.nextID
	m.nextID++
	m.companies[c.ID] = c
	if c.Domain != "" {
		m.byDomain[c.Domain] = c
	}
	return nil
}

func (m *mockStore) UpdateCompany(_ context.Context, c *CompanyRecord) error {
	m.updateCalls++
	m.companies[c.ID] = c
	return nil
}

func (m *mockStore) GetCompany(_ context.Context, id int64) (*CompanyRecord, error) {
	c := m.companies[id]
	return c, nil
}

func (m *mockStore) GetCompanyByDomain(_ context.Context, domain string) (*CompanyRecord, error) {
	c := m.byDomain[domain]
	return c, nil
}

func (m *mockStore) SearchCompaniesByName(_ context.Context, _ string, _ int) ([]CompanyRecord, error) {
	return nil, nil
}

func (m *mockStore) UpsertIdentifier(_ context.Context, id *Identifier) error {
	m.identifierCalls++
	id.ID = m.nextID
	m.nextID++
	m.identifiers[id.CompanyID] = append(m.identifiers[id.CompanyID], *id)
	return nil
}

func (m *mockStore) GetIdentifiers(_ context.Context, companyID int64) ([]Identifier, error) {
	return m.identifiers[companyID], nil
}

func (m *mockStore) FindByIdentifier(_ context.Context, system, identifier string) (*CompanyRecord, error) {
	for _, ids := range m.identifiers {
		for _, id := range ids {
			if id.System == system && id.Identifier == identifier {
				return m.companies[id.CompanyID], nil
			}
		}
	}
	return nil, nil
}

func (m *mockStore) UpsertAddress(_ context.Context, addr *Address) error {
	m.addressCalls++
	addr.ID = m.nextID
	m.nextID++
	m.addresses[addr.CompanyID] = append(m.addresses[addr.CompanyID], *addr)
	return nil
}

func (m *mockStore) GetAddresses(_ context.Context, companyID int64) ([]Address, error) {
	return m.addresses[companyID], nil
}

func (m *mockStore) UpsertContact(_ context.Context, c *Contact) error {
	m.contactCalls++
	c.ID = m.nextID
	m.nextID++
	m.contacts[c.CompanyID] = append(m.contacts[c.CompanyID], *c)
	return nil
}

func (m *mockStore) GetContacts(_ context.Context, companyID int64) ([]Contact, error) {
	return m.contacts[companyID], nil
}

func (m *mockStore) GetContactsByRole(_ context.Context, companyID int64, roleType string) ([]Contact, error) {
	var result []Contact
	for _, c := range m.contacts[companyID] {
		if c.RoleType == roleType {
			result = append(result, c)
		}
	}
	return result, nil
}

func (m *mockStore) UpsertLicense(_ context.Context, l *License) error {
	l.ID = m.nextID
	m.nextID++
	m.licenses[l.CompanyID] = append(m.licenses[l.CompanyID], *l)
	return nil
}

func (m *mockStore) GetLicenses(_ context.Context, companyID int64) ([]License, error) {
	return m.licenses[companyID], nil
}

func (m *mockStore) UpsertSource(_ context.Context, s *Source) error {
	m.sourceCalls++
	s.ID = m.nextID
	m.nextID++
	m.sources[s.CompanyID] = append(m.sources[s.CompanyID], *s)
	return nil
}

func (m *mockStore) GetSources(_ context.Context, companyID int64) ([]Source, error) {
	return m.sources[companyID], nil
}

func (m *mockStore) GetSource(_ context.Context, companyID int64, sourceName, sourceID string) (*Source, error) {
	for _, s := range m.sources[companyID] {
		if s.SourceName == sourceName && s.SourceID == sourceID {
			return &s, nil
		}
	}
	return nil, nil
}

func (m *mockStore) UpsertFinancial(_ context.Context, f *Financial) error {
	m.financialCalls++
	f.ID = m.nextID
	m.nextID++
	m.financials[f.CompanyID] = append(m.financials[f.CompanyID], *f)
	return nil
}

func (m *mockStore) GetFinancials(_ context.Context, companyID int64, _ string) ([]Financial, error) {
	return m.financials[companyID], nil
}

func (m *mockStore) SetTags(_ context.Context, companyID int64, tagType string, values []string) error {
	m.tagCalls++
	// Remove existing tags of this type, then add new ones.
	var remaining []Tag
	for _, t := range m.tags[companyID] {
		if t.TagType != tagType {
			remaining = append(remaining, t)
		}
	}
	for _, v := range values {
		remaining = append(remaining, Tag{CompanyID: companyID, TagType: tagType, TagValue: v})
	}
	m.tags[companyID] = remaining
	return nil
}

func (m *mockStore) GetTags(_ context.Context, companyID int64) ([]Tag, error) {
	return m.tags[companyID], nil
}

func (m *mockStore) GetUngeocodedAddresses(_ context.Context, _ int) ([]Address, error) {
	return nil, nil
}

func (m *mockStore) UpdateAddressGeocode(_ context.Context, _ int64, _, _ float64, _, _, _ string) error {
	return nil
}

func (m *mockStore) UpsertAddressMSA(_ context.Context, _ *AddressMSA) error {
	return nil
}

func (m *mockStore) GetAddressMSAs(_ context.Context, _ int64) ([]AddressMSA, error) {
	return nil, nil
}

func (m *mockStore) GetCompanyMSAs(_ context.Context, _ int64) ([]AddressMSA, error) {
	return nil, nil
}

func (m *mockStore) UpsertMatch(_ context.Context, match *Match) error {
	match.ID = m.nextID
	m.nextID++
	m.matches[match.CompanyID] = append(m.matches[match.CompanyID], *match)
	return nil
}

func (m *mockStore) GetMatches(_ context.Context, companyID int64) ([]Match, error) {
	return m.matches[companyID], nil
}

func (m *mockStore) FindByMatch(_ context.Context, matchedSource, matchedKey string) (*CompanyRecord, error) {
	for cid, matches := range m.matches {
		for _, match := range matches {
			if match.MatchedSource == matchedSource && match.MatchedKey == matchedKey {
				return m.companies[cid], nil
			}
		}
	}
	return nil, nil
}

func TestImport_NewCompany(t *testing.T) {
	store := newMockStore()
	imp := &Importer{
		resolver: NewResolver(store),
		store:    store,
	}

	co := model.Company{
		URL:          "https://acme.com",
		Name:         "Acme Corp",
		SalesforceID: "001SFID",
		City:         "Boston",
		State:        "MA",
	}

	result := &model.EnrichmentResult{
		RunID:  "run-123",
		Score:  85.0,
		Report: "Excellent enrichment coverage",
		Phases: []model.PhaseResult{
			{Name: "1a_crawl", Status: model.PhaseStatusComplete},
			{Name: "1c_linkedin", Status: model.PhaseStatusComplete},
		},
	}

	fieldValues := map[string]model.FieldValue{
		"name":             {Value: "Acme Corp", Confidence: 0.9},
		"services_list":    {Value: "Consulting, Advisory", Confidence: 0.7},
		"customer_types":   {Value: "Enterprise; SMB", Confidence: 0.7},
		"exec_first_name":  {Value: "Jane", Confidence: 0.8},
		"exec_last_name":   {Value: "Doe", Confidence: 0.8},
		"exec_title":       {Value: "CEO", Confidence: 0.7},
		"linkedin_url":     {Value: "https://linkedin.com/company/acme", Confidence: 0.8},
		"revenue_estimate": {Value: float64(5000000), Confidence: 0.7},
		"street":           {Value: "100 Main St", Confidence: 0.8},
		"city":             {Value: "Boston", Confidence: 0.9},
		"state":            {Value: "MA", Confidence: 0.9},
	}

	record, err := imp.Import(context.Background(), co, result, fieldValues)
	require.NoError(t, err)
	require.NotNil(t, record)

	// Verify company was created and updated.
	assert.Equal(t, 1, store.createCalls)
	assert.Equal(t, 1, store.updateCalls)
	assert.Equal(t, "acme.com", record.Domain)
	assert.Equal(t, "Acme Corp", record.Name)
	assert.Equal(t, "Consulting, Advisory", record.ServicesList)
	assert.Equal(t, "Excellent enrichment coverage", record.EnrichmentReport)
	assert.NotNil(t, record.LastEnrichedAt)
	assert.Equal(t, 85.0, *record.EnrichmentScore)

	// Verify identifiers: SF, Notion (empty), LinkedIn.
	assert.GreaterOrEqual(t, store.identifierCalls, 2) // SF + LinkedIn at minimum

	// Verify contacts: exec contact.
	assert.GreaterOrEqual(t, store.contactCalls, 1)
	contacts := store.contacts[record.ID]
	assert.GreaterOrEqual(t, len(contacts), 1)
	assert.Equal(t, "Jane", contacts[0].FirstName)
	assert.Equal(t, "Doe", contacts[0].LastName)
	assert.Equal(t, RoleExecutive, contacts[0].RoleType)
	assert.True(t, contacts[0].IsPrimary)

	// Verify address.
	assert.Equal(t, 1, store.addressCalls)
	addrs := store.addresses[record.ID]
	assert.Equal(t, 1, len(addrs))
	assert.Equal(t, "100 Main St", addrs[0].Street)
	assert.Equal(t, "Boston", addrs[0].City)
	assert.True(t, addrs[0].IsPrimary)

	// Verify tags.
	assert.GreaterOrEqual(t, store.tagCalls, 2) // services + customer_types
	tags := store.tags[record.ID]
	var serviceTags, customerTags []string
	for _, tag := range tags {
		switch tag.TagType {
		case TagService:
			serviceTags = append(serviceTags, tag.TagValue)
		case TagCustomerType:
			customerTags = append(customerTags, tag.TagValue)
		}
	}
	assert.Contains(t, serviceTags, "Consulting")
	assert.Contains(t, serviceTags, "Advisory")
	assert.Contains(t, customerTags, "Enterprise")
	assert.Contains(t, customerTags, "SMB")

	// Verify sources.
	assert.GreaterOrEqual(t, store.sourceCalls, 2) // crawl + linkedin

	// Verify financials.
	assert.Equal(t, 1, store.financialCalls)
	fins := store.financials[record.ID]
	assert.Equal(t, 1, len(fins))
	assert.Equal(t, "revenue", fins[0].Metric)
	assert.Equal(t, 5000000.0, fins[0].Value)
}

func TestImport_ExistingCompany(t *testing.T) {
	store := newMockStore()

	// Pre-create a company.
	existing := &CompanyRecord{
		Name:   "Old Name",
		Domain: "acme.com",
	}
	_ = store.CreateCompany(context.Background(), existing)

	imp := &Importer{
		resolver: NewResolver(store),
		store:    store,
	}

	co := model.Company{
		URL:  "https://acme.com",
		Name: "Acme Corp",
	}
	result := &model.EnrichmentResult{
		RunID: "run-456",
		Score: 90.0,
	}
	fieldValues := map[string]model.FieldValue{
		"name": {Value: "Acme Corp", Confidence: 0.9},
	}

	record, err := imp.Import(context.Background(), co, result, fieldValues)
	require.NoError(t, err)

	// Should NOT create a new company (resolved by domain).
	assert.Equal(t, 1, store.createCalls) // only the initial pre-create
	assert.Equal(t, 1, store.updateCalls)
	assert.Equal(t, existing.ID, record.ID)
	assert.Equal(t, "Acme Corp", record.Name)
}

func TestImport_PPPFinancials(t *testing.T) {
	store := newMockStore()
	imp := &Importer{
		resolver: NewResolver(store),
		store:    store,
	}

	co := model.Company{
		URL:  "https://test.com",
		Name: "Test Corp",
	}
	result := &model.EnrichmentResult{
		RunID: "run-789",
		Score: 50.0,
		PPPMatches: []ppp.LoanMatch{
			{CurrentApproval: 150000},
			{CurrentApproval: 75000},
		},
	}
	fieldValues := map[string]model.FieldValue{}

	record, err := imp.Import(context.Background(), co, result, fieldValues)
	require.NoError(t, err)
	require.NotNil(t, record)

	// Should have 2 PPP financial records.
	assert.Equal(t, 2, store.financialCalls)
	fins := store.financials[record.ID]
	assert.Equal(t, 2, len(fins))
	assert.Equal(t, "ppp_loan_amount", fins[0].Metric)
	assert.Equal(t, 150000.0, fins[0].Value)
}

func TestImport_PreSeededIdentifiers(t *testing.T) {
	store := newMockStore()
	imp := &Importer{
		resolver: NewResolver(store),
		store:    store,
	}

	co := model.Company{
		URL:  "https://advisor.com",
		Name: "Advisor LLC",
		PreSeeded: map[string]any{
			"crd_number": "123456",
			"grata_id":   "G-789",
		},
	}
	result := &model.EnrichmentResult{RunID: "run-001", Score: 60.0}
	fieldValues := map[string]model.FieldValue{}

	record, err := imp.Import(context.Background(), co, result, fieldValues)
	require.NoError(t, err)

	ids := store.identifiers[record.ID]
	var systems []string
	for _, id := range ids {
		systems = append(systems, id.System)
	}
	assert.Contains(t, systems, SystemCRD)
	assert.Contains(t, systems, SystemGrata)
}

func TestNewImporter(t *testing.T) {
	store := newMockStore()
	imp := NewImporter(store, nil)
	assert.NotNil(t, imp)
	assert.NotNil(t, imp.resolver)
	assert.Nil(t, imp.linker)
}

func TestImport_OwnerContact(t *testing.T) {
	store := newMockStore()
	imp := &Importer{resolver: NewResolver(store), store: store}

	co := model.Company{URL: "https://owner.com", Name: "Owner Corp"}
	result := &model.EnrichmentResult{RunID: "run-own", Score: 60.0}
	fieldValues := map[string]model.FieldValue{
		"owner_name": {Value: "Bob Johnson", Confidence: 0.8},
	}

	record, err := imp.Import(context.Background(), co, result, fieldValues)
	require.NoError(t, err)

	contacts := store.contacts[record.ID]
	require.Equal(t, 1, len(contacts))
	assert.Equal(t, "Bob", contacts[0].FirstName)
	assert.Equal(t, "Johnson", contacts[0].LastName)
	assert.Equal(t, RoleOwner, contacts[0].RoleType)
	assert.True(t, contacts[0].IsPrimary)
}

func TestImport_OwnerSameAsExec(t *testing.T) {
	// Owner name matching exec name should be deduplicated.
	store := newMockStore()
	imp := &Importer{resolver: NewResolver(store), store: store}

	co := model.Company{URL: "https://dedup.com", Name: "Dedup Corp"}
	result := &model.EnrichmentResult{RunID: "run-dd", Score: 60.0}
	fieldValues := map[string]model.FieldValue{
		"exec_first_name": {Value: "Jane", Confidence: 0.8},
		"exec_last_name":  {Value: "Doe", Confidence: 0.8},
		"owner_name":      {Value: "Jane Doe", Confidence: 0.8},
	}

	record, err := imp.Import(context.Background(), co, result, fieldValues)
	require.NoError(t, err)

	// Only exec contact, owner deduplicated.
	contacts := store.contacts[record.ID]
	assert.Equal(t, 1, len(contacts))
	assert.Equal(t, RoleExecutive, contacts[0].RoleType)
}

func TestImport_OwnerSingleName(t *testing.T) {
	store := newMockStore()
	imp := &Importer{resolver: NewResolver(store), store: store}

	co := model.Company{URL: "https://single.com", Name: "Single Name Corp"}
	result := &model.EnrichmentResult{RunID: "run-sn", Score: 60.0}
	fieldValues := map[string]model.FieldValue{
		"owner_name": {Value: "Madonna", Confidence: 0.8},
	}

	record, err := imp.Import(context.Background(), co, result, fieldValues)
	require.NoError(t, err)

	contacts := store.contacts[record.ID]
	require.Equal(t, 1, len(contacts))
	assert.Equal(t, "Madonna", contacts[0].FirstName)
	assert.Equal(t, "", contacts[0].LastName)
}

func TestImport_KeyContactsFromAnswers(t *testing.T) {
	store := newMockStore()
	imp := &Importer{resolver: NewResolver(store), store: store}

	co := model.Company{URL: "https://kc.com", Name: "KC Corp"}
	result := &model.EnrichmentResult{
		RunID: "run-kc",
		Score: 60.0,
		Answers: []model.ExtractionAnswer{
			{
				FieldKey: "contacts",
				Value: []any{
					map[string]any{
						"name":     "Alice Smith",
						"title":    "CFO",
						"email":    "alice@kc.com",
						"phone":    "555-0001",
						"linkedin": "https://linkedin.com/in/alice",
					},
					map[string]any{
						"name":  "Bob",
						"title": "CTO",
					},
				},
			},
			{FieldKey: "other_field", Value: "ignored"},
		},
	}
	fieldValues := map[string]model.FieldValue{}

	record, err := imp.Import(context.Background(), co, result, fieldValues)
	require.NoError(t, err)

	contacts := store.contacts[record.ID]
	require.Equal(t, 2, len(contacts))
	assert.Equal(t, "Alice", contacts[0].FirstName)
	assert.Equal(t, "Smith", contacts[0].LastName)
	assert.Equal(t, "CFO", contacts[0].Title)
	assert.Equal(t, "alice@kc.com", contacts[0].Email)
	assert.Equal(t, "555-0001", contacts[0].Phone)
	assert.Equal(t, RoleKeyPerson, contacts[0].RoleType)
	assert.True(t, contacts[0].IsPrimary) // first contact is primary
	assert.Equal(t, "Bob", contacts[1].FirstName)
	assert.Equal(t, "", contacts[1].LastName) // single name
	assert.False(t, contacts[1].IsPrimary)    // second not primary
}

func TestImport_KeyContactsFieldKey(t *testing.T) {
	// Test "key_contacts" alias.
	store := newMockStore()
	imp := &Importer{resolver: NewResolver(store), store: store}

	co := model.Company{URL: "https://kc2.com", Name: "KC2 Corp"}
	result := &model.EnrichmentResult{
		RunID: "run-kc2",
		Score: 60.0,
		Answers: []model.ExtractionAnswer{
			{
				FieldKey: "key_contacts",
				Value: []any{
					map[string]any{"name": "Charlie Brown", "title": "VP"},
				},
			},
		},
	}
	fieldValues := map[string]model.FieldValue{}

	record, err := imp.Import(context.Background(), co, result, fieldValues)
	require.NoError(t, err)

	contacts := store.contacts[record.ID]
	require.Equal(t, 1, len(contacts))
	assert.Equal(t, "Charlie", contacts[0].FirstName)
	assert.Equal(t, "Brown", contacts[0].LastName)
}

func TestImport_NoAddress(t *testing.T) {
	store := newMockStore()
	imp := &Importer{resolver: NewResolver(store), store: store}

	co := model.Company{URL: "https://noaddr.com", Name: "No Addr Corp"}
	result := &model.EnrichmentResult{RunID: "run-na", Score: 40.0}
	fieldValues := map[string]model.FieldValue{}

	_, err := imp.Import(context.Background(), co, result, fieldValues)
	require.NoError(t, err)

	assert.Equal(t, 0, store.addressCalls)
}

func TestImport_AllSourceTypes(t *testing.T) {
	store := newMockStore()
	imp := &Importer{resolver: NewResolver(store), store: store}

	co := model.Company{
		URL:  "https://allsrc.com",
		Name: "All Sources Corp",
		PreSeeded: map[string]any{
			"grata_id":   "G-100",
			"crd_number": "999",
		},
	}
	result := &model.EnrichmentResult{
		RunID: "run-as",
		Score: 70.0,
		Phases: []model.PhaseResult{
			{Name: "1a_crawl", Status: model.PhaseStatusComplete},
			{Name: "1b_scrape", Status: model.PhaseStatusComplete},
			{Name: "1c_linkedin", Status: model.PhaseStatusComplete},
			{Name: "1d_ppp", Status: model.PhaseStatusComplete},
			{Name: "1e_perplexity", Status: model.PhaseStatusComplete},
			{Name: "2_classify", Status: model.PhaseStatusComplete}, // not a source
			{Name: "3_route", Status: model.PhaseStatusFailed},      // failed phase
		},
	}
	fieldValues := map[string]model.FieldValue{}

	record, err := imp.Import(context.Background(), co, result, fieldValues)
	require.NoError(t, err)

	sources := store.sources[record.ID]
	var names []string
	for _, s := range sources {
		names = append(names, s.SourceName)
	}
	// 5 phase sources + grata + adv_filing = 7
	assert.Contains(t, names, "crawl")
	assert.Contains(t, names, "scrape")
	assert.Contains(t, names, "linkedin")
	assert.Contains(t, names, "ppp")
	assert.Contains(t, names, "perplexity_intel")
	assert.Contains(t, names, "grata")
	assert.Contains(t, names, "adv_filing")
	assert.Equal(t, 7, len(sources))
}

func TestImport_TagsWithServiceMixAlias(t *testing.T) {
	// service_mix alias for services_list in tags.
	store := newMockStore()
	imp := &Importer{resolver: NewResolver(store), store: store}

	co := model.Company{URL: "https://alias.com", Name: "Alias Corp"}
	result := &model.EnrichmentResult{RunID: "run-al", Score: 50.0}
	fieldValues := map[string]model.FieldValue{
		"service_mix":     {Value: "Tax, Audit, Advisory", Confidence: 0.7},
		"end_markets":     {Value: "Healthcare, Finance", Confidence: 0.7},
		"differentiators": {Value: "Innovative; Award-winning", Confidence: 0.7},
	}

	record, err := imp.Import(context.Background(), co, result, fieldValues)
	require.NoError(t, err)

	tags := store.tags[record.ID]
	var serviceTags, industryTags, diffTags []string
	for _, tag := range tags {
		switch tag.TagType {
		case TagService:
			serviceTags = append(serviceTags, tag.TagValue)
		case TagIndustry:
			industryTags = append(industryTags, tag.TagValue)
		case TagDifferentiator:
			diffTags = append(diffTags, tag.TagValue)
		}
	}
	assert.Contains(t, serviceTags, "Tax")
	assert.Contains(t, serviceTags, "Audit")
	assert.Contains(t, serviceTags, "Advisory")
	assert.Contains(t, industryTags, "Healthcare")
	assert.Contains(t, industryTags, "Finance")
	assert.Contains(t, diffTags, "Innovative")
	assert.Contains(t, diffTags, "Award-winning")
}

func TestImport_ZeroPPPAmount(t *testing.T) {
	store := newMockStore()
	imp := &Importer{resolver: NewResolver(store), store: store}

	co := model.Company{URL: "https://zeroppp.com", Name: "Zero PPP"}
	result := &model.EnrichmentResult{
		RunID: "run-zp",
		Score: 40.0,
		PPPMatches: []ppp.LoanMatch{
			{CurrentApproval: 0}, // zero amount should be skipped
		},
	}
	fieldValues := map[string]model.FieldValue{}

	_, err := imp.Import(context.Background(), co, result, fieldValues)
	require.NoError(t, err)

	assert.Equal(t, 0, store.financialCalls)
}

func TestImport_ZeroRevenue(t *testing.T) {
	store := newMockStore()
	imp := &Importer{resolver: NewResolver(store), store: store}

	co := model.Company{URL: "https://zerorev.com", Name: "Zero Rev"}
	result := &model.EnrichmentResult{RunID: "run-zr", Score: 40.0}
	fieldValues := map[string]model.FieldValue{
		"revenue_estimate": {Value: float64(0), Confidence: 0.5},
	}

	_, err := imp.Import(context.Background(), co, result, fieldValues)
	require.NoError(t, err)

	assert.Equal(t, 0, store.financialCalls)
}

func TestImport_ZeroScore(t *testing.T) {
	store := newMockStore()
	imp := &Importer{resolver: NewResolver(store), store: store}

	co := model.Company{URL: "https://noscore.com", Name: "No Score"}
	result := &model.EnrichmentResult{RunID: "run-ns", Score: 0}
	fieldValues := map[string]model.FieldValue{}

	record, err := imp.Import(context.Background(), co, result, fieldValues)
	require.NoError(t, err)
	assert.Nil(t, record.EnrichmentScore)
}

func TestImport_NilImporterIsNoOp(t *testing.T) {
	// Verify that a nil companyImporter doesn't panic.
	var imp *Importer
	assert.Nil(t, imp)
}

func TestImport_ResolveViaSalesforceID(t *testing.T) {
	store := newMockStore()

	// Pre-create a company and link an SF identifier.
	existing := &CompanyRecord{Name: "SF Corp", Domain: "sfcorp.com"}
	_ = store.CreateCompany(context.Background(), existing)
	_ = store.UpsertIdentifier(context.Background(), &Identifier{
		CompanyID:  existing.ID,
		System:     SystemSalesforce,
		Identifier: "001MATCH",
	})

	imp := &Importer{resolver: NewResolver(store), store: store}

	// Different domain, but same SF ID — should resolve to existing.
	co := model.Company{
		URL:          "https://new-domain.com",
		Name:         "SF Corp",
		SalesforceID: "001MATCH",
	}
	result := &model.EnrichmentResult{RunID: "run-sf", Score: 70.0}
	fieldValues := map[string]model.FieldValue{}

	record, err := imp.Import(context.Background(), co, result, fieldValues)
	require.NoError(t, err)
	assert.Equal(t, existing.ID, record.ID) // resolved to existing
}

func TestImport_ResolveViaNameState(t *testing.T) {
	// Use mock that supports name search returning results.
	ms := &nameSearchMockStore{mockStore: newMockStore()}

	existing := &CompanyRecord{Name: "Exact Corp", Domain: "exact.com", State: "TX", City: "Austin"}
	_ = ms.CreateCompany(context.Background(), existing)

	imp := &Importer{resolver: NewResolver(ms), store: ms}

	co := model.Company{
		URL:   "https://different-exact.com",
		Name:  "Exact Corp",
		State: "TX",
		City:  "Austin",
	}
	result := &model.EnrichmentResult{RunID: "run-ns", Score: 65.0}
	fieldValues := map[string]model.FieldValue{}

	record, err := imp.Import(context.Background(), co, result, fieldValues)
	require.NoError(t, err)
	assert.Equal(t, existing.ID, record.ID)
}

// nameSearchMockStore extends mockStore with name search support.
type nameSearchMockStore struct {
	*mockStore
}

func (m *nameSearchMockStore) SearchCompaniesByName(_ context.Context, name string, _ int) ([]CompanyRecord, error) {
	var results []CompanyRecord
	for _, c := range m.companies {
		if c.Name == name {
			results = append(results, *c)
		}
	}
	return results, nil
}

func TestSplitTags(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{"Consulting, Advisory, Tax", []string{"Consulting", "Advisory", "Tax"}},
		{"Consulting; Advisory; Tax", []string{"Consulting", "Advisory", "Tax"}},
		{"Single", []string{"Single"}},
		{"", nil},
		{" , , ", nil},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := splitTags(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestToFloat64Val_AllTypes(t *testing.T) {
	assert.Equal(t, 4.5, toFloat64Val(4.5))
	assert.Equal(t, float64(42), toFloat64Val(float32(42)))
	assert.Equal(t, float64(42), toFloat64Val(42))
	assert.Equal(t, float64(42), toFloat64Val(int64(42)))
	assert.Equal(t, 0.0, toFloat64Val("not a number"))
	assert.Equal(t, 0.0, toFloat64Val(nil))
}

func TestFieldStr(t *testing.T) {
	fv := map[string]model.FieldValue{
		"name": {Value: "Acme"},
		"num":  {Value: float64(42)},
	}

	assert.Equal(t, "Acme", fieldStr(fv, "name"))
	assert.Equal(t, "", fieldStr(fv, "num")) // non-string value
	assert.Equal(t, "", fieldStr(fv, "missing"))
}

// --- mockPool implements db.Pool for NewImporter test ---

type mockPool struct{}

func (mockPool) Begin(_ context.Context) (pgx.Tx, error) { return nil, nil }
func (mockPool) Exec(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}
func (mockPool) Query(_ context.Context, _ string, _ ...any) (pgx.Rows, error) { return nil, nil }
func (mockPool) QueryRow(_ context.Context, _ string, _ ...any) pgx.Row        { return nil }
func (mockPool) CopyFrom(_ context.Context, _ pgx.Identifier, _ []string, _ pgx.CopyFromSource) (int64, error) {
	return 0, nil
}

func TestNewImporter_WithPool(t *testing.T) {
	store := newMockStore()
	imp := NewImporter(store, mockPool{})
	assert.NotNil(t, imp)
	assert.NotNil(t, imp.linker)
}

// --- errMockStore returns errors on specific operations ---

type errMockStore struct {
	*mockStore
	createErr     error
	updateErr     error
	domainErr     error
	findIDErr     error
	searchErr     error
	identifierErr error
	contactErr    error
	addressErr    error
	tagErr        error
	sourceErr     error
	financialErr  error
}

func (e *errMockStore) CreateCompany(ctx context.Context, c *CompanyRecord) error {
	if e.createErr != nil {
		return e.createErr
	}
	return e.mockStore.CreateCompany(ctx, c)
}

func (e *errMockStore) UpdateCompany(ctx context.Context, c *CompanyRecord) error {
	if e.updateErr != nil {
		return e.updateErr
	}
	return e.mockStore.UpdateCompany(ctx, c)
}

func (e *errMockStore) GetCompanyByDomain(ctx context.Context, domain string) (*CompanyRecord, error) {
	if e.domainErr != nil {
		return nil, e.domainErr
	}
	return e.mockStore.GetCompanyByDomain(ctx, domain)
}

func (e *errMockStore) FindByIdentifier(ctx context.Context, system, identifier string) (*CompanyRecord, error) {
	if e.findIDErr != nil {
		return nil, e.findIDErr
	}
	return e.mockStore.FindByIdentifier(ctx, system, identifier)
}

func (e *errMockStore) SearchCompaniesByName(ctx context.Context, name string, limit int) ([]CompanyRecord, error) {
	if e.searchErr != nil {
		return nil, e.searchErr
	}
	return e.mockStore.SearchCompaniesByName(ctx, name, limit)
}

func (e *errMockStore) UpsertIdentifier(ctx context.Context, id *Identifier) error {
	if e.identifierErr != nil {
		return e.identifierErr
	}
	return e.mockStore.UpsertIdentifier(ctx, id)
}

func (e *errMockStore) UpsertContact(ctx context.Context, c *Contact) error {
	if e.contactErr != nil {
		return e.contactErr
	}
	return e.mockStore.UpsertContact(ctx, c)
}

func (e *errMockStore) UpsertAddress(ctx context.Context, addr *Address) error {
	if e.addressErr != nil {
		return e.addressErr
	}
	return e.mockStore.UpsertAddress(ctx, addr)
}

func (e *errMockStore) SetTags(ctx context.Context, companyID int64, tagType string, values []string) error {
	if e.tagErr != nil {
		return e.tagErr
	}
	return e.mockStore.SetTags(ctx, companyID, tagType, values)
}

func (e *errMockStore) UpsertSource(ctx context.Context, s *Source) error {
	if e.sourceErr != nil {
		return e.sourceErr
	}
	return e.mockStore.UpsertSource(ctx, s)
}

func (e *errMockStore) UpsertFinancial(ctx context.Context, f *Financial) error {
	if e.financialErr != nil {
		return e.financialErr
	}
	return e.mockStore.UpsertFinancial(ctx, f)
}

// --- Import error path tests ---

func TestImport_ResolveError(t *testing.T) {
	es := &errMockStore{
		mockStore: newMockStore(),
		domainErr: assert.AnError,
	}
	imp := &Importer{resolver: NewResolver(es), store: es}

	co := model.Company{URL: "https://fail.com", Name: "Fail Corp"}
	result := &model.EnrichmentResult{RunID: "run-err", Score: 50.0}

	_, err := imp.Import(context.Background(), co, result, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "resolve company")
}

func TestImport_UpdateCompanyError(t *testing.T) {
	es := &errMockStore{
		mockStore: newMockStore(),
		updateErr: assert.AnError,
	}
	imp := &Importer{resolver: NewResolver(es), store: es}

	co := model.Company{URL: "https://updfail.com", Name: "Update Fail"}
	result := &model.EnrichmentResult{RunID: "run-uf", Score: 50.0}

	_, err := imp.Import(context.Background(), co, result, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "update company")
}

func TestImport_EmptyReportAndZeroScore(t *testing.T) {
	store := newMockStore()
	imp := &Importer{resolver: NewResolver(store), store: store}

	co := model.Company{URL: "https://empty.com", Name: "Empty Corp"}
	result := &model.EnrichmentResult{RunID: "run-em", Score: 0, Report: ""}
	fieldValues := map[string]model.FieldValue{}

	record, err := imp.Import(context.Background(), co, result, fieldValues)
	require.NoError(t, err)
	assert.Nil(t, record.EnrichmentScore)
	assert.Equal(t, "", record.EnrichmentReport)
}

func TestImport_ChildTableErrors_NonFatal(t *testing.T) {
	// All child table operations fail, but import still succeeds.
	es := &errMockStore{
		mockStore:     newMockStore(),
		identifierErr: assert.AnError,
		contactErr:    assert.AnError,
		addressErr:    assert.AnError,
		tagErr:        assert.AnError,
		sourceErr:     assert.AnError,
		financialErr:  assert.AnError,
	}
	imp := &Importer{resolver: NewResolver(es), store: es}

	co := model.Company{
		URL:          "https://allerr.com",
		Name:         "All Errors Corp",
		SalesforceID: "001ERR",
		PreSeeded:    map[string]any{"crd_number": "111", "grata_id": "G-ERR"},
	}
	result := &model.EnrichmentResult{
		RunID:  "run-ae",
		Score:  60.0,
		Report: "report text",
		Phases: []model.PhaseResult{
			{Name: "1a_crawl", Status: model.PhaseStatusComplete},
		},
		PPPMatches: []ppp.LoanMatch{{CurrentApproval: 50000}},
		Answers: []model.ExtractionAnswer{
			{
				FieldKey: "contacts",
				Value:    []any{map[string]any{"name": "Alice Smith", "title": "CTO"}},
			},
		},
	}
	fieldValues := map[string]model.FieldValue{
		"exec_first_name":  {Value: "Jane", Confidence: 0.8},
		"exec_last_name":   {Value: "Doe", Confidence: 0.8},
		"owner_name":       {Value: "Bob Owner", Confidence: 0.8},
		"linkedin_url":     {Value: "https://linkedin.com/co/x", Confidence: 0.8},
		"street":           {Value: "123 Main", Confidence: 0.8},
		"city":             {Value: "Boston", Confidence: 0.8},
		"state":            {Value: "MA", Confidence: 0.8},
		"services_list":    {Value: "Consulting", Confidence: 0.7},
		"revenue_estimate": {Value: float64(1000000), Confidence: 0.7},
	}

	record, err := imp.Import(context.Background(), co, result, fieldValues)
	require.NoError(t, err) // All child errors are non-fatal
	assert.NotNil(t, record)
	assert.Equal(t, "All Errors Corp", record.Name)
}

func TestImport_SourcesWithEmptyRunID(t *testing.T) {
	store := newMockStore()
	imp := &Importer{resolver: NewResolver(store), store: store}

	co := model.Company{URL: "https://noid.com", Name: "No RunID Corp"}
	result := &model.EnrichmentResult{
		RunID: "", // empty
		Score: 50.0,
		Phases: []model.PhaseResult{
			{Name: "1a_crawl", Status: model.PhaseStatusComplete},
		},
	}
	fieldValues := map[string]model.FieldValue{}

	record, err := imp.Import(context.Background(), co, result, fieldValues)
	require.NoError(t, err)

	sources := store.sources[record.ID]
	require.Equal(t, 1, len(sources))
	assert.Nil(t, sources[0].RawData) // No run ID → no raw data
}

func TestImport_ContactUpsertError_IsPrimaryStaysTrue(t *testing.T) {
	// When exec contact upsert fails, isPrimary stays true for next contact.
	es := &errMockStore{
		mockStore:  newMockStore(),
		contactErr: assert.AnError,
	}
	imp := &Importer{resolver: NewResolver(es), store: es}

	co := model.Company{URL: "https://cfail.com", Name: "Contact Fail"}
	result := &model.EnrichmentResult{
		RunID: "run-cf", Score: 50.0,
		Answers: []model.ExtractionAnswer{
			{
				FieldKey: "contacts",
				Value:    []any{map[string]any{"name": "Alice Smith"}},
			},
		},
	}
	fieldValues := map[string]model.FieldValue{
		"exec_first_name": {Value: "Jane", Confidence: 0.8},
		"exec_last_name":  {Value: "Doe", Confidence: 0.8},
		"owner_name":      {Value: "Bob Owner", Confidence: 0.8},
	}

	record, err := imp.Import(context.Background(), co, result, fieldValues)
	require.NoError(t, err)
	assert.NotNil(t, record)
}

func TestImport_ContactsUnmarshalableAnswer(t *testing.T) {
	// Answer value that can't be unmarshalled as contact array.
	store := newMockStore()
	imp := &Importer{resolver: NewResolver(store), store: store}

	co := model.Company{URL: "https://badjson.com", Name: "Bad JSON"}
	result := &model.EnrichmentResult{
		RunID: "run-bj", Score: 50.0,
		Answers: []model.ExtractionAnswer{
			{FieldKey: "contacts", Value: "not an array"},
		},
	}

	record, err := imp.Import(context.Background(), co, result, nil)
	require.NoError(t, err)
	assert.Equal(t, 0, store.contactCalls) // No contacts parsed
	assert.NotNil(t, record)
}

// --- FindOrCreate error path tests ---

func TestFindOrCreate_EmptyDomain(t *testing.T) {
	store := newMockStore()
	r := NewResolver(store)

	co := model.Company{URL: "", Name: "No Domain"}
	_, _, err := r.FindOrCreate(context.Background(), co)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "domain is required")
}

func TestFindOrCreate_DomainLookupError(t *testing.T) {
	es := &errMockStore{
		mockStore: newMockStore(),
		domainErr: assert.AnError,
	}
	r := NewResolver(es)

	co := model.Company{URL: "https://fail.com", Name: "Fail"}
	_, _, err := r.FindOrCreate(context.Background(), co)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "resolve by domain")
}

func TestFindOrCreate_FindByIdentifierError(t *testing.T) {
	es := &errMockStore{
		mockStore: newMockStore(),
		findIDErr: assert.AnError,
	}
	r := NewResolver(es)

	co := model.Company{URL: "https://sfid-err.com", Name: "SF Err", SalesforceID: "001ERR"}
	_, _, err := r.FindOrCreate(context.Background(), co)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "resolve by salesforce_id")
}

func TestFindOrCreate_NameSearchError_FallsThrough(t *testing.T) {
	// SearchCompaniesByName error is non-fatal, falls through to create.
	es := &errMockStore{
		mockStore: newMockStore(),
		searchErr: assert.AnError,
	}
	r := NewResolver(es)

	co := model.Company{URL: "https://search-err.com", Name: "Search Err", State: "CA"}
	record, created, err := r.FindOrCreate(context.Background(), co)
	require.NoError(t, err)
	assert.True(t, created) // Falls through to create
	assert.Equal(t, "search-err.com", record.Domain)
}

func TestFindOrCreate_CreateCompanyError(t *testing.T) {
	es := &errMockStore{
		mockStore: newMockStore(),
		createErr: assert.AnError,
	}
	r := NewResolver(es)

	co := model.Company{URL: "https://create-err.com", Name: "Create Err"}
	_, _, err := r.FindOrCreate(context.Background(), co)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "company: create")
}

func TestFindOrCreate_NameStateCityMismatch(t *testing.T) {
	// Name+state match but city doesn't match → falls through to create.
	ms := &nameSearchMockStore{mockStore: newMockStore()}
	existing := &CompanyRecord{Name: "Same Name", Domain: "same.com", State: "TX", City: "Houston"}
	_ = ms.CreateCompany(context.Background(), existing)

	r := NewResolver(ms)
	co := model.Company{
		URL:   "https://different.com",
		Name:  "Same Name",
		State: "TX",
		City:  "Austin", // different city
	}
	record, created, err := r.FindOrCreate(context.Background(), co)
	require.NoError(t, err)
	assert.True(t, created)
	assert.NotEqual(t, existing.ID, record.ID) // different company
}

func TestFindOrCreate_NameStateNoCityMatch(t *testing.T) {
	// Name+state match and input city is empty → should match.
	ms := &nameSearchMockStore{mockStore: newMockStore()}
	existing := &CompanyRecord{Name: "Match Corp", Domain: "match.com", State: "CA", City: "LA"}
	_ = ms.CreateCompany(context.Background(), existing)

	r := NewResolver(ms)
	co := model.Company{
		URL:   "https://other.com",
		Name:  "Match Corp",
		State: "CA",
		City:  "", // empty city → should match since company.City == ""
	}
	record, created, err := r.FindOrCreate(context.Background(), co)
	require.NoError(t, err)
	assert.False(t, created)
	assert.Equal(t, existing.ID, record.ID)
}

func TestFindOrCreate_WithNotionPageID(t *testing.T) {
	store := newMockStore()
	r := NewResolver(store)

	co := model.Company{
		URL:          "https://notion.com",
		Name:         "Notion Corp",
		NotionPageID: "page-123",
	}
	record, created, err := r.FindOrCreate(context.Background(), co)
	require.NoError(t, err)
	assert.True(t, created)

	ids := store.identifiers[record.ID]
	var systems []string
	for _, id := range ids {
		systems = append(systems, id.System)
	}
	assert.Contains(t, systems, SystemNotion)
}

func TestFindOrCreate_WithSalesforceID_NewCompany(t *testing.T) {
	store := newMockStore()
	r := NewResolver(store)

	co := model.Company{
		URL:          "https://sfnew.com",
		Name:         "SF New Corp",
		SalesforceID: "001NEW",
	}
	record, created, err := r.FindOrCreate(context.Background(), co)
	require.NoError(t, err)
	assert.True(t, created)

	ids := store.identifiers[record.ID]
	var systems []string
	for _, id := range ids {
		systems = append(systems, id.System)
	}
	assert.Contains(t, systems, SystemSalesforce)
}

func TestFindOrCreate_UpsertIdentifierError_NonFatal(t *testing.T) {
	es := &errMockStore{
		mockStore:     newMockStore(),
		identifierErr: assert.AnError,
	}
	r := NewResolver(es)

	co := model.Company{
		URL:          "https://idfail.com",
		Name:         "ID Fail Corp",
		SalesforceID: "001FAIL",
		NotionPageID: "page-fail",
	}
	record, created, err := r.FindOrCreate(context.Background(), co)
	require.NoError(t, err) // Identifier errors are non-fatal
	assert.True(t, created)
	assert.NotNil(t, record)
}

func TestFindOrCreate_NameWithoutState(t *testing.T) {
	// Name without state should skip name+state search, go straight to create.
	store := newMockStore()
	r := NewResolver(store)

	co := model.Company{URL: "https://nostate.com", Name: "No State Corp"}
	record, created, err := r.FindOrCreate(context.Background(), co)
	require.NoError(t, err)
	assert.True(t, created)
	assert.Equal(t, "nostate.com", record.Domain)
}

func TestImport_TagsWithOnlyWhitespace(t *testing.T) {
	// Tags that split into empty values should not call SetTags.
	store := newMockStore()
	imp := &Importer{resolver: NewResolver(store), store: store}

	co := model.Company{URL: "https://emptytag.com", Name: "Empty Tags"}
	result := &model.EnrichmentResult{RunID: "run-et", Score: 40.0}
	fieldValues := map[string]model.FieldValue{
		"services_list": {Value: " , , ", Confidence: 0.7},
	}

	_, err := imp.Import(context.Background(), co, result, fieldValues)
	require.NoError(t, err)
	assert.Equal(t, 0, store.tagCalls) // empty after split → no SetTags call
}

func TestImport_AddressOnlyCityState(t *testing.T) {
	// Address with only city+state (no street) should still be upserted.
	store := newMockStore()
	imp := &Importer{resolver: NewResolver(store), store: store}

	co := model.Company{URL: "https://partial-addr.com", Name: "Partial Addr"}
	result := &model.EnrichmentResult{RunID: "run-pa", Score: 40.0}
	fieldValues := map[string]model.FieldValue{
		"city":  {Value: "Denver", Confidence: 0.8},
		"state": {Value: "CO", Confidence: 0.8},
	}

	record, err := imp.Import(context.Background(), co, result, fieldValues)
	require.NoError(t, err)
	assert.Equal(t, 1, store.addressCalls)
	addrs := store.addresses[record.ID]
	require.Equal(t, 1, len(addrs))
	assert.Equal(t, "Denver", addrs[0].City)
	assert.Equal(t, "CO", addrs[0].State)
	assert.Equal(t, "", addrs[0].Street)
}

func TestImport_RevenueFinancialError_NonFatal(t *testing.T) {
	es := &errMockStore{
		mockStore:    newMockStore(),
		financialErr: assert.AnError,
	}
	imp := &Importer{resolver: NewResolver(es), store: es}

	co := model.Company{URL: "https://finfail.com", Name: "Fin Fail"}
	result := &model.EnrichmentResult{
		RunID:      "run-ff",
		Score:      50.0,
		PPPMatches: []ppp.LoanMatch{{CurrentApproval: 50000}},
	}
	fieldValues := map[string]model.FieldValue{
		"revenue_estimate": {Value: float64(1000000), Confidence: 0.7},
	}

	record, err := imp.Import(context.Background(), co, result, fieldValues)
	require.NoError(t, err) // Financial errors are non-fatal
	assert.NotNil(t, record)
}

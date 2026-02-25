// Package mocks provides test doubles for the google client.
package mocks

import (
	"context"

	google "github.com/sells-group/research-cli/pkg/google"
	mock "github.com/stretchr/testify/mock"
)

// MockClient is a mock type for the Client interface.
type MockClient struct {
	mock.Mock
}

// TextSearch provides a mock function with given fields: ctx, query
func (_m *MockClient) TextSearch(ctx context.Context, query string) (*google.TextSearchResponse, error) {
	ret := _m.Called(ctx, query)

	if len(ret) == 0 {
		panic("no return value specified for TextSearch")
	}

	var r0 *google.TextSearchResponse
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, string) (*google.TextSearchResponse, error)); ok {
		return rf(ctx, query)
	}
	if rf, ok := ret.Get(0).(func(context.Context, string) *google.TextSearchResponse); ok {
		r0 = rf(ctx, query)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*google.TextSearchResponse)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, string) error); ok {
		r1 = rf(ctx, query)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// DiscoverySearch provides a mock function with given fields: ctx, req
func (_m *MockClient) DiscoverySearch(ctx context.Context, req google.DiscoverySearchRequest) (*google.DiscoverySearchResponse, error) {
	ret := _m.Called(ctx, req)

	if len(ret) == 0 {
		panic("no return value specified for DiscoverySearch")
	}

	var r0 *google.DiscoverySearchResponse
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, google.DiscoverySearchRequest) (*google.DiscoverySearchResponse, error)); ok {
		return rf(ctx, req)
	}
	if rf, ok := ret.Get(0).(func(context.Context, google.DiscoverySearchRequest) *google.DiscoverySearchResponse); ok {
		r0 = rf(ctx, req)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*google.DiscoverySearchResponse)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, google.DiscoverySearchRequest) error); ok {
		r1 = rf(ctx, req)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// NewMockClient creates a new instance of MockClient.
func NewMockClient(t interface {
	mock.TestingT
	Cleanup(func())
}) *MockClient {
	mock := &MockClient{}
	mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

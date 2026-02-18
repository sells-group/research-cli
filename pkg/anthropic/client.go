package anthropic

import (
	"context"
	"fmt"

	sdk "github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
	"github.com/anthropics/anthropic-sdk-go/packages/jsonl"
	"github.com/rotisserie/eris"
	"go.uber.org/zap"
)

// Client defines the Anthropic API operations used by the pipeline.
type Client interface {
	CreateMessage(ctx context.Context, req MessageRequest) (*MessageResponse, error)
	CreateBatch(ctx context.Context, req BatchRequest) (*BatchResponse, error)
	GetBatch(ctx context.Context, batchID string) (*BatchResponse, error)
	GetBatchResults(ctx context.Context, batchID string) (BatchResultIterator, error)
}

// BatchResultIterator streams individual results from a completed batch.
type BatchResultIterator interface {
	Next() bool
	Item() BatchResultItem
	Err() error
	Close() error
}

// MessageRequest is our own request type for CreateMessage.
type MessageRequest struct {
	Model       string
	MaxTokens   int64
	System      []SystemBlock
	Messages    []Message
	Temperature *float64
}

// SystemBlock represents a system prompt block, optionally with cache control.
type SystemBlock struct {
	Text         string
	CacheControl *CacheControl
}

// CacheControl configures caching for a content block.
type CacheControl struct {
	TTL string // "5m" or "1h"
}

// Message represents a single conversational message.
type Message struct {
	Role    string // "user" or "assistant"
	Content string
}

// MessageResponse is our own response type from CreateMessage.
type MessageResponse struct {
	ID           string
	Model        string
	Content      []ContentBlock
	StopReason   string
	Usage        TokenUsage
	StopSequence string
}

// ContentBlock represents a block of content in a response.
type ContentBlock struct {
	Type string
	Text string
}

// TokenUsage tracks token consumption.
type TokenUsage struct {
	InputTokens              int64
	OutputTokens             int64
	CacheCreationInputTokens int64
	CacheReadInputTokens     int64
}

// modelPricing holds per-million-token pricing for known models.
var modelPricing = map[string][2]float64{
	// model → {input $/MTok, output $/MTok}
	"claude-haiku-4-5-20251001":  {0.80, 4.00},
	"claude-sonnet-4-5-20250929": {3.00, 15.00},
	"claude-opus-4-6":            {15.00, 75.00},
}

// EstimateCost computes an estimated cost in USD from a TokenUsage and model ID.
// Returns 0 for unknown models.
func (u TokenUsage) EstimateCost(model string) float64 {
	pricing, ok := modelPricing[model]
	if !ok {
		return 0
	}
	inCost := (float64(u.InputTokens) / 1e6) * pricing[0]
	outCost := (float64(u.OutputTokens) / 1e6) * pricing[1]
	cacheWriteCost := (float64(u.CacheCreationInputTokens) / 1e6) * pricing[0] * 1.25
	cacheReadCost := (float64(u.CacheReadInputTokens) / 1e6) * pricing[0] * 0.1
	return inCost + outCost + cacheWriteCost + cacheReadCost
}

// LogCost logs token usage and estimated cost with structured zap fields.
func (u TokenUsage) LogCost(model, phase string) {
	cost := u.EstimateCost(model)
	zap.L().Info("cost attribution",
		zap.String("model", model),
		zap.String("phase", phase),
		zap.Int64("input_tokens", u.InputTokens),
		zap.Int64("output_tokens", u.OutputTokens),
		zap.Int64("cache_write_tokens", u.CacheCreationInputTokens),
		zap.Int64("cache_read_tokens", u.CacheReadInputTokens),
		zap.Float64("estimated_cost_usd", cost),
	)
}

// BatchRequest is our own request type for CreateBatch.
type BatchRequest struct {
	Requests []BatchRequestItem
}

// BatchRequestItem is a single item in a batch request.
type BatchRequestItem struct {
	CustomID string
	Params   MessageRequest
}

// BatchResponse is our own response type for batch operations.
type BatchResponse struct {
	ID               string
	ProcessingStatus string
	ResultsURL       string
	RequestCounts    RequestCounts
}

// RequestCounts tallies requests by status.
type RequestCounts struct {
	Processing int64
	Succeeded  int64
	Errored    int64
	Canceled   int64
	Expired    int64
}

// BatchResultItem is a single result from a completed batch.
type BatchResultItem struct {
	CustomID string
	Type     string // "succeeded", "errored", "canceled", "expired"
	Message  *MessageResponse
}

// sdkClient implements Client using the official anthropic-sdk-go.
type sdkClient struct {
	client sdk.Client
}

// NewClient creates a new Anthropic client backed by the SDK.
func NewClient(apiKey string) Client {
	return &sdkClient{
		client: sdk.NewClient(
			option.WithAPIKey(apiKey),
		),
	}
}

func (c *sdkClient) CreateMessage(ctx context.Context, req MessageRequest) (*MessageResponse, error) {
	params := sdk.MessageNewParams{
		Model:     sdk.Model(req.Model),
		MaxTokens: req.MaxTokens,
		Messages:  toSDKMessages(req.Messages),
	}

	if len(req.System) > 0 {
		params.System = toSDKSystemBlocks(req.System)
	}

	if req.Temperature != nil {
		params.Temperature = sdk.Float(*req.Temperature)
	}

	msg, err := c.client.Messages.New(ctx, params)
	if err != nil {
		return nil, eris.Wrap(err, "anthropic: create message")
	}

	return fromSDKMessage(msg), nil
}

func (c *sdkClient) CreateBatch(ctx context.Context, req BatchRequest) (*BatchResponse, error) {
	sdkReqs := make([]sdk.MessageBatchNewParamsRequest, len(req.Requests))
	for i, r := range req.Requests {
		sdkReqs[i] = sdk.MessageBatchNewParamsRequest{
			CustomID: r.CustomID,
			Params: sdk.MessageBatchNewParamsRequestParams{
				Model:     sdk.Model(r.Params.Model),
				MaxTokens: r.Params.MaxTokens,
				Messages:  toSDKMessages(r.Params.Messages),
			},
		}
		if len(r.Params.System) > 0 {
			sdkReqs[i].Params.System = toSDKSystemBlocks(r.Params.System)
		}
		if r.Params.Temperature != nil {
			sdkReqs[i].Params.Temperature = sdk.Float(*r.Params.Temperature)
		}
	}

	batch, err := c.client.Messages.Batches.New(ctx, sdk.MessageBatchNewParams{
		Requests: sdkReqs,
	})
	if err != nil {
		return nil, eris.Wrap(err, "anthropic: create batch")
	}

	return fromSDKBatch(batch), nil
}

func (c *sdkClient) GetBatch(ctx context.Context, batchID string) (*BatchResponse, error) {
	batch, err := c.client.Messages.Batches.Get(ctx, batchID)
	if err != nil {
		return nil, eris.Wrap(err, fmt.Sprintf("anthropic: get batch %s", batchID))
	}

	return fromSDKBatch(batch), nil
}

func (c *sdkClient) GetBatchResults(ctx context.Context, batchID string) (BatchResultIterator, error) {
	stream := c.client.Messages.Batches.ResultsStreaming(ctx, batchID)
	if err := stream.Err(); err != nil {
		return nil, eris.Wrap(err, fmt.Sprintf("anthropic: get batch results %s", batchID))
	}
	return &sdkBatchResultIterator{stream: stream}, nil
}

// sdkBatchResultIterator wraps the SDK's jsonl stream.
type sdkBatchResultIterator struct {
	stream *jsonl.Stream[sdk.MessageBatchIndividualResponse]
	item   BatchResultItem
}

func (it *sdkBatchResultIterator) Next() bool {
	if !it.stream.Next() {
		return false
	}
	resp := it.stream.Current()
	it.item = fromSDKBatchResult(resp)
	return true
}

func (it *sdkBatchResultIterator) Item() BatchResultItem {
	return it.item
}

func (it *sdkBatchResultIterator) Err() error {
	return it.stream.Err()
}

func (it *sdkBatchResultIterator) Close() error {
	return it.stream.Close()
}

// --- SDK type conversion helpers ---

func toSDKMessages(msgs []Message) []sdk.MessageParam {
	out := make([]sdk.MessageParam, len(msgs))
	for i, m := range msgs {
		block := sdk.NewTextBlock(m.Content)
		switch m.Role {
		case "assistant":
			out[i] = sdk.NewAssistantMessage(block)
		default:
			out[i] = sdk.NewUserMessage(block)
		}
	}
	return out
}

func toSDKSystemBlocks(blocks []SystemBlock) []sdk.TextBlockParam {
	out := make([]sdk.TextBlockParam, len(blocks))
	for i, b := range blocks {
		out[i] = sdk.TextBlockParam{
			Text: b.Text,
		}
		if b.CacheControl != nil {
			cc := sdk.NewCacheControlEphemeralParam()
			if b.CacheControl.TTL != "" {
				cc.TTL = sdk.CacheControlEphemeralTTL(b.CacheControl.TTL)
			}
			out[i].CacheControl = cc
		}
	}
	return out
}

func fromSDKMessage(msg *sdk.Message) *MessageResponse {
	blocks := make([]ContentBlock, 0, len(msg.Content))
	for _, b := range msg.Content {
		blocks = append(blocks, ContentBlock{
			Type: b.Type,
			Text: b.Text,
		})
	}

	return &MessageResponse{
		ID:           msg.ID,
		Model:        string(msg.Model),
		Content:      blocks,
		StopReason:   string(msg.StopReason),
		StopSequence: msg.StopSequence,
		Usage: TokenUsage{
			InputTokens:              msg.Usage.InputTokens,
			OutputTokens:             msg.Usage.OutputTokens,
			CacheCreationInputTokens: msg.Usage.CacheCreationInputTokens,
			CacheReadInputTokens:     msg.Usage.CacheReadInputTokens,
		},
	}
}

func fromSDKBatch(batch *sdk.MessageBatch) *BatchResponse {
	return &BatchResponse{
		ID:               batch.ID,
		ProcessingStatus: string(batch.ProcessingStatus),
		ResultsURL:       batch.ResultsURL,
		RequestCounts: RequestCounts{
			Processing: batch.RequestCounts.Processing,
			Succeeded:  batch.RequestCounts.Succeeded,
			Errored:    batch.RequestCounts.Errored,
			Canceled:   batch.RequestCounts.Canceled,
			Expired:    batch.RequestCounts.Expired,
		},
	}
}

func fromSDKBatchResult(resp sdk.MessageBatchIndividualResponse) BatchResultItem {
	item := BatchResultItem{
		CustomID: resp.CustomID,
		Type:     resp.Result.Type,
	}

	if resp.Result.Type == "succeeded" {
		msg := resp.Result.Message
		item.Message = fromSDKMessage(&msg)
	}

	return item
}

package translator

import (
	"encoding/json"
	"fmt"

	"github.com/gomodul/db/query"
)

// RPCType represents the type of RPC
type RPCType string

const (
	RPCTypeJSON  RPCType = "jsonrpc"
	RPCTypeGRPC  RPCType = "grpc"
	RPCTypeThrift RPCType = "thrift"
)

// RPCTranslator translates universal queries to RPC calls
type RPCTranslator struct {
	*BaseTranslator
	rpcType  RPCType
	service  string
	endpoint string
}

// NewRPCTranslator creates a new RPC translator
func NewRPCTranslator(rpcType RPCType, service, endpoint string) *RPCTranslator {
	return &RPCTranslator{
		BaseTranslator: NewBaseTranslator(string(rpcType)),
		rpcType:        rpcType,
		service:        service,
		endpoint:       endpoint,
	}
}

// RPCCall represents a translated RPC call
type RPCCall struct {
	Method   string
	Request  interface{}
	Metadata map[string]string
}

// Translate converts universal query to RPC call
func (t *RPCTranslator) Translate(q *query.Query) (BackendQuery, error) {
	if err := t.Validate(q); err != nil {
		return nil, err
	}

	switch t.rpcType {
	case RPCTypeJSON:
		return t.translateJSONRPC(q)
	case RPCTypeGRPC:
		return t.translateGRPC(q)
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedOperation, t.rpcType)
	}
}

func (t *RPCTranslator) translateJSONRPC(q *query.Query) (*RPCCall, error) {
	method := t.getMethodName(q.Operation, t.getCollectionName(q))
	request := t.buildJSONRPCRequest(q)

	return &RPCCall{
		Method:   method,
		Request:  request,
		Metadata: map[string]string{"rpc": "jsonrpc", "version": "2.0"},
	}, nil
}

func (t *RPCTranslator) translateGRPC(q *query.Query) (*RPCCall, error) {
	method := fmt.Sprintf("/%s.%s/%s", t.service, t.getCollectionName(q), t.getGRPCMethod(q.Operation))
	request := t.buildGRPCRequest(q)

	return &RPCCall{
		Method:   method,
		Request:  request,
		Metadata: map[string]string{"rpc": "grpc", "service": t.service},
	}, nil
}

func (t *RPCTranslator) buildJSONRPCRequest(q *query.Query) map[string]interface{} {
	req := make(map[string]interface{})

	collection := t.getCollectionName(q)

	switch q.Operation {
	case query.OpFind:
		req = map[string]interface{}{
			"jsonrpc": "2.0",
			"method":  t.getMethodName(q.Operation, collection),
			"params": map[string]interface{}{
				"filter":   t.buildRPCFilter(q.Filters),
				"select":   q.Selects,
				"order":    t.buildRPCOrder(q.Orders),
				"limit":    q.Limit,
				"offset":   q.Offset,
			},
		}

	case query.OpCreate:
		req = map[string]interface{}{
			"jsonrpc": "2.0",
			"method":  t.getMethodName(q.Operation, collection),
			"params": map[string]interface{}{
				"data": q.Document,
			},
		}

	case query.OpUpdate:
		req = map[string]interface{}{
			"jsonrpc": "2.0",
			"method":  t.getMethodName(q.Operation, collection),
			"params": map[string]interface{}{
				"filter": t.buildRPCFilter(q.Filters),
				"data":   q.Updates,
			},
		}

	case query.OpDelete:
		req = map[string]interface{}{
			"jsonrpc": "2.0",
			"method":  t.getMethodName(q.Operation, collection),
			"params": map[string]interface{}{
				"filter": t.buildRPCFilter(q.Filters),
			},
		}

	case query.OpCount:
		req = map[string]interface{}{
			"jsonrpc": "2.0",
			"method":  t.getMethodName(q.Operation, collection),
			"params": map[string]interface{}{
				"filter": t.buildRPCFilter(q.Filters),
			},
		}
	}

	// Add ID if present
	if id, ok := req["params"].(map[string]interface{})["id"]; ok {
		req["id"] = id
	} else {
		req["id"] = 1
	}

	return req
}

func (t *RPCTranslator) buildGRPCRequest(q *query.Query) map[string]interface{} {
	req := make(map[string]interface{})

	switch q.Operation {
	case query.OpFind:
		req = map[string]interface{}{
			"filter": t.buildRPCFilter(q.Filters),
			"select": q.Selects,
			"order":  t.buildRPCOrder(q.Orders),
			"limit":  q.Limit,
			"offset": q.Offset,
		}

	case query.OpCreate:
		req = map[string]interface{}{
			"data": q.Document,
		}

	case query.OpUpdate:
		req = map[string]interface{}{
			"filter": t.buildRPCFilter(q.Filters),
			"data":   q.Updates,
		}

	case query.OpDelete:
		req = map[string]interface{}{
			"filter": t.buildRPCFilter(q.Filters),
		}

	case query.OpCount:
		req = map[string]interface{}{
			"filter": t.buildRPCFilter(q.Filters),
		}
	}

	return req
}

func (t *RPCTranslator) getMethodName(operation query.OperationType, collection string) string {
	switch operation {
	case query.OpFind:
		return fmt.Sprintf("%s.find", collection)
	case query.OpCreate:
		return fmt.Sprintf("%s.create", collection)
	case query.OpUpdate:
		return fmt.Sprintf("%s.update", collection)
	case query.OpDelete:
		return fmt.Sprintf("%s.delete", collection)
	case query.OpCount:
		return fmt.Sprintf("%s.count", collection)
	default:
		return fmt.Sprintf("%s.query", collection)
	}
}

func (t *RPCTranslator) getGRPCMethod(operation query.OperationType) string {
	switch operation {
	case query.OpFind:
		return "Find"
	case query.OpCreate:
		return "Create"
	case query.OpUpdate:
		return "Update"
	case query.OpDelete:
		return "Delete"
	case query.OpCount:
		return "Count"
	default:
		return "Query"
	}
}

func (t *RPCTranslator) getCollectionName(q *query.Query) string {
	if q.Collection != "" {
		return q.Collection
	}
	return modelCollectionName(q.Model)
}

func (t *RPCTranslator) buildRPCFilter(filters []*query.Filter) []map[string]interface{} {
	result := make([]map[string]interface{}, 0)

	for _, filter := range filters {
		if len(filter.Nested) > 0 {
			nested := t.buildRPCFilter(filter.Nested)
			if filter.Logic == query.LogicOr {
				result = append(result, map[string]interface{}{
					"or": nested,
				})
			} else if filter.Logic == query.LogicNot {
				result = append(result, map[string]interface{}{
					"not": nested,
				})
			} else {
				result = append(result, nested...)
			}
		} else {
			result = append(result, map[string]interface{}{
				"field":    filter.Field,
				"operator": filter.Operator,
				"value":    filter.Value,
				"logic":    filter.Logic,
			})
		}
	}

	return result
}

func (t *RPCTranslator) buildRPCOrder(orders []*query.Order) []map[string]interface{} {
	result := make([]map[string]interface{}, 0, len(orders))

	for _, order := range orders {
		result = append(result, map[string]interface{}{
			"field":     order.Field,
			"direction": order.Direction,
		})
	}

	return result
}

// JSONRPCRequest represents a JSON-RPC 2.0 request
type JSONRPCRequest struct {
	JSONRPC string      `json:"jsonrpc"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params,omitempty"`
	ID      interface{} `json:"id"`
}

// ToJSON converts the request to JSON bytes
func (r *RPCCall) ToJSON() ([]byte, error) {
	if req, ok := r.Request.(map[string]interface{}); ok {
		jsonReq := &JSONRPCRequest{
			JSONRPC: "2.0",
			Method:  r.Method,
			Params:  req["params"],
			ID:      1,
		}
		return json.Marshal(jsonReq)
	}
	return nil, fmt.Errorf("invalid request type")
}

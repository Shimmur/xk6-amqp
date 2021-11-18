package amqp

import (
	"fmt"

	"github.com/Shimmur/proto_schemas_go/types"
)

func Map_to_struct(m map[string]interface{}) *types.Struct {
	var t = &types.Struct{}
	var fields = make(map[string]*types.Value)

	for k, v := range m {
		typed := get_type(v)
		fields[k] = typed
	}
	t.Fields = fields

	return t
}

func Struct_to_map(s types.Struct) map[string]interface{} {
	m := make(map[string]interface{})
	for k, v := range s.Fields {
		m[k] = get_real(v.Kind)
	}
	return m
}

//TODO: figure out the struct/list types
func get_type(i interface{}) *types.Value {
	switch i := i.(type) {
	case bool:
		return &types.Value{Kind: &types.Value_BoolValue{BoolValue: i}}
	case nil:
		return &types.Value{Kind: &types.Value_NullValue{}}
	case int64:
		return &types.Value{Kind: &types.Value_IntValue{IntValue: i}}
	case float64:
		return &types.Value{Kind: &types.Value_DoubleValue{DoubleValue: i}}
	case string:
		return &types.Value{Kind: &types.Value_StringValue{StringValue: i}}
	case []interface{}:
		var vals = []*types.Value{}

		for _, v := range i {
			vals = append(vals, get_type(v))
		}
		return &types.Value{Kind: &types.Value_ListValue{ListValue: &types.ListValue{Values: vals}}}

	case map[string]interface{}:
		var fields = make(map[string]*types.Value)

		for k, v := range i {
			fields[k] = get_type(v)
		}

		return &types.Value{Kind: &types.Value_StructValue{StructValue: &types.Struct{Fields: fields}}}
	default:
		fmt.Printf("Couldnt get type for (%v), type: %T\n", i, i)
		return nil
	}
}

func get_real(i interface{}) interface{} {
	switch i := i.(type) {
	case *types.Value_StringValue:
		return i.StringValue
	case *types.Value_IntValue:
		return i.IntValue
	case *types.Value_DoubleValue:
		return i.DoubleValue
	case *types.Value_BoolValue:
		return i.BoolValue
	case *types.Value_NullValue:
		return i.NullValue
	case *types.Value_ListValue:
		var real []interface{}

		for _, v := range i.ListValue.Values {
			real = append(real, get_real(v.Kind))
		}
		return real
	case *types.Value_StructValue:
		var real = make(map[string]interface{})

		for k, v := range i.StructValue.Fields {
			real[k] = get_real(v.Kind)
		}
		return real
	default:
		fmt.Printf("Couldnt get real for (%v), type: %T\n", i, i)
		return nil
	}
}

package task

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
)

// Handler 包装任务处理函数，支持类型安全的调用
type Handler struct {
	fn         reflect.Value
	dataType   reflect.Type
	hasContext bool
}

// newHandler 创建任务处理器
func newHandler(handler any) (*Handler, error) {
	if handler == nil {
		return nil, fmt.Errorf("handler cannot be nil")
	}

	fn := reflect.ValueOf(handler)
	if fn.Kind() != reflect.Func {
		return nil, fmt.Errorf("handler must be a function, got %T", handler)
	}

	fnType := fn.Type()

	// 验证函数签名
	// 支持的签名：
	// 1. func(ctx context.Context, data T) error
	// 2. func(data T) error

	numIn := fnType.NumIn()
	numOut := fnType.NumOut()

	if numOut != 1 {
		return nil, fmt.Errorf("handler must return exactly one value (error), got %d", numOut)
	}

	// 检查返回值类型必须是 error
	if !fnType.Out(0).Implements(reflect.TypeOf((*error)(nil)).Elem()) {
		return nil, fmt.Errorf("handler must return error, got %s", fnType.Out(0))
	}

	var dataType reflect.Type
	hasContext := false

	switch numIn {
	case 1:
		// func(data T) error
		dataType = fnType.In(0)
	case 2:
		// func(ctx context.Context, data T) error
		ctxType := reflect.TypeOf((*context.Context)(nil)).Elem()
		if !fnType.In(0).Implements(ctxType) {
			return nil, fmt.Errorf("first parameter must be context.Context, got %s", fnType.In(0))
		}
		dataType = fnType.In(1)
		hasContext = true
	default:
		return nil, fmt.Errorf("handler must have 1 or 2 parameters, got %d", numIn)
	}

	return &Handler{
		fn:         fn,
		dataType:   dataType,
		hasContext: hasContext,
	}, nil
}

// Call 调用处理函数
func (h *Handler) Call(ctx context.Context, data []byte) error {
	// 创建数据对象
	dataVal := reflect.New(h.dataType)

	// 反序列化数据
	if err := json.Unmarshal(data, dataVal.Interface()); err != nil {
		return fmt.Errorf("failed to unmarshal data: %w", err)
	}

	// 准备参数
	var args []reflect.Value
	if h.hasContext {
		args = []reflect.Value{
			reflect.ValueOf(ctx),
			dataVal.Elem(),
		}
	} else {
		args = []reflect.Value{dataVal.Elem()}
	}

	// 调用函数
	results := h.fn.Call(args)

	// 提取错误返回值
	errInterface := results[0].Interface()
	if errInterface == nil {
		return nil
	}

	return errInterface.(error)
}

package router

import "github.com/ssssvbdd/InferGate/services/gateway/internal/model"

// matchCondition 评估 Mongo 风格条件表达式是否命中 metadata。
// 支持操作符：$eq $ne $gt $gte $lt $lte $in $nin；隐式相等；顶层多字段为 AND。
func matchCondition(cond model.Condition, meta map[string]any) bool {
	for field, expr := range cond {
		actual, exists := meta[field]
		if !matchField(actual, exists, expr) {
			return false
		}
	}
	return true
}

func matchField(actual any, exists bool, expr any) bool {
	switch e := expr.(type) {
	case map[string]any:
		for op, want := range e {
			if !matchOp(op, actual, exists, want) {
				return false
			}
		}
		return true
	default:
		// 隐式相等。
		if !exists {
			return false
		}
		return equalValue(actual, expr)
	}
}

func matchOp(op string, actual any, exists bool, want any) bool {
	switch op {
	case "$eq":
		return exists && equalValue(actual, want)
	case "$ne":
		return !exists || !equalValue(actual, want)
	case "$gt":
		return exists && compareNum(actual, want) > 0
	case "$gte":
		return exists && compareNum(actual, want) >= 0
	case "$lt":
		return exists && compareNum(actual, want) < 0
	case "$lte":
		return exists && compareNum(actual, want) <= 0
	case "$in":
		return exists && inList(actual, want)
	case "$nin":
		return !exists || !inList(actual, want)
	default:
		return false
	}
}

func equalValue(a, b any) bool {
	if af, aok := toFloat(a); aok {
		if bf, bok := toFloat(b); bok {
			return af == bf
		}
	}
	return toStr(a) == toStr(b)
}

// compareNum 返回 -1/0/1；无法比较返回 0。
func compareNum(a, b any) int {
	af, aok := toFloat(a)
	bf, bok := toFloat(b)
	if !aok || !bok {
		return 0
	}
	switch {
	case af < bf:
		return -1
	case af > bf:
		return 1
	default:
		return 0
	}
}

func inList(actual, want any) bool {
	list, ok := want.([]any)
	if !ok {
		return false
	}
	for _, item := range list {
		if equalValue(actual, item) {
			return true
		}
	}
	return false
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	default:
		return 0, false
	}
}

func toStr(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	if v == nil {
		return ""
	}
	switch n := v.(type) {
	case bool:
		if n {
			return "true"
		}
		return "false"
	}
	return ""
}

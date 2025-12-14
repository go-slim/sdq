package sdq

// ValidateTopicName 验证 topic 名称
func ValidateTopicName(name string) error {
	if name == "" {
		return ErrInvalidTopic
	}

	// 检查长度
	if len(name) > 200 {
		return ErrInvalidTopic
	}

	// 检查字符（字母、数字、下划线、中划线、冒号、点、斜杠）
	// 冒号用于命名空间分隔，便于 Redis 等存储引擎聚合
	// 点和斜杠用于层级结构命名
	for _, ch := range name {
		isLower := ch >= 'a' && ch <= 'z'
		isUpper := ch >= 'A' && ch <= 'Z'
		isDigit := ch >= '0' && ch <= '9'
		isSpecial := ch == '_' || ch == '-' || ch == ':' || ch == '.' || ch == '/'
		if !isLower && !isUpper && !isDigit && !isSpecial {
			return ErrInvalidTopic
		}
	}

	return nil
}

package sdq

import "testing"

func TestValidateTopicName(t *testing.T) {
	tests := []struct {
		name    string
		wantErr bool
	}{
		{"valid-topic", false},
		{"valid_topic", false},
		{"ValidTopic", false},
		{"topic123", false},
		{"a", false},
		{"ns:topic", false},        // colon for namespace
		{"app:user:events", false}, // multiple colons
		{"test/topic", false},      // slash for hierarchy
		{"test.topic", false},      // dot for hierarchy
		{"a/b/c.d", false},         // mixed
		{"", true},                 // empty
		{"test topic", true},       // contains space
		{"test@topic", true},       // contains @
	}

	for _, tt := range tests {
		err := ValidateTopicName(tt.name)
		if (err != nil) != tt.wantErr {
			t.Errorf("ValidateTopicName(%q) error = %v, wantErr %v", tt.name, err, tt.wantErr)
		}
	}
}

func TestValidateTopicName_TooLong(t *testing.T) {
	// 200 chars should be ok
	longName := make([]byte, 200)
	for i := range longName {
		longName[i] = 'a'
	}
	if err := ValidateTopicName(string(longName)); err != nil {
		t.Errorf("ValidateTopicName(200 chars) error = %v, want nil", err)
	}

	// 201 chars should fail
	tooLong := make([]byte, 201)
	for i := range tooLong {
		tooLong[i] = 'a'
	}
	if err := ValidateTopicName(string(tooLong)); err != ErrInvalidTopic {
		t.Errorf("ValidateTopicName(201 chars) error = %v, want ErrInvalidTopic", err)
	}
}

package pglock

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHash(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
	}{
		{"test", -439409999022904539},
		{"test2", 2271361535601096725},
		{"endeavor-schema-migration", -1553017160562081548},
		{"a", -5808556873153909620},
		{"Hello World", 4420528118743043111},
		{"user_account_lock", -7202378285172227168},
		{"processPayment", -8000211232816623520},
		{"order-12345", 9217005275286586573},
		{"my.resource.v2", -7604882164531270493},
		{"SELECT * FROM users WHERE id = 1", -8436135842703899477},
		{"café-résumé", -1084965660865976875},
		{"  leading-and-trailing-spaces  ", -2603010066059441236},
		{"UPPER_CASE_CONSTANT", -2368041024789287607},
		{"mix3d_C4se-w1th.numb3rs", 9172535630847042829},
		{"api/v2/users/profile/settings", 7357342318655089583},
		{"7a3b9f2e-4d5c-11ee-be56-0242ac120002", -5367769415359627479},
	}

	for _, test := range tests {
		require.Equal(t, test.expected, Hash(test.input))
	}
}

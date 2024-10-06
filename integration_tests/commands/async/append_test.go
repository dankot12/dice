package async

import (
	"log"
	"testing"

	"gotest.tools/v3/assert"
)

func TestAppend(t *testing.T) {
	conn := getLocalConnection()
	defer conn.Close()

	testCases := []struct {
		name     string
		commands []string
		expected []interface{}
	}{
		{
			name:     "Append on non-string key (error)",
			commands: []string{"DEL k1", "LPUSH k1 1", "APPEND k1 value"},
			expected: []interface{}{int64(0), "OK", "(nil)"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			FireCommand(conn, "DEL k1")
			for i, cmd := range tc.commands {
				result := FireCommand(conn, cmd)
				if result == "(nil)" {
					log.Printf("Command %s returned nil", cmd)
				}
				assert.DeepEqual(t, tc.expected[i], result)
			}
		})
	}
}

func TestAppendWithTTL(t *testing.T) {
	conn := getLocalConnection()
	defer conn.Close()

	testCases := []struct {
		name     string
		commands []string
		expected []interface{}
	}{
		{
			name:     "Append preserves TTL",
			commands: []string{"SET k1 Hello EX 2", "APPEND k1 World", "TTL k1"},
			expected: []interface{}{"OK", int64(10)},
		},
		{
			name:     "Append to expired key",
			commands: []string{"SET k1 Hello EX 1", "SLEEP 2", "APPEND k1 World", "GET k1"},
			expected: []interface{}{"OK", "OK", int64(5), "World"}, // "Hello" has expired, new key starts with "World"
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			FireCommand(conn, "DEL k1")
			for i, cmd := range tc.commands {
				result := FireCommand(conn, cmd)
				if i == 2 && tc.name == "Append preserves TTL" {
					ttl := result.(int64)
					assert.Assert(t, ttl > 0, "TTL should be positive but got %d", ttl)
				} else {
					assert.DeepEqual(t, tc.expected[i], result)
				}
			}
		})
	}
}

func TestAppendErrorCases(t *testing.T) {
	conn := getLocalConnection()
	defer conn.Close()

	testCases := []struct {
		name     string
		commands []string
		expected []interface{}
	}{
		{
			name:     "Append to non-string key",
			commands: []string{"DEL k1", "LPUSH k1 value", "APPEND k1 'test'"},
			expected: []interface{}{int64(1), "ERR WRONGTYPE Operation against a key holding the wrong kind of value"},
		},
		{
			name:     "Append with invalid input",
			commands: []string{"SET k1 Hello", "APPEND k1 123@!"},
			expected: []interface{}{"OK", int64(10)}, // Still valid, the value is just treated as a string
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			FireCommand(conn, "DEL k1")
			for i, cmd := range tc.commands {
				result := FireCommand(conn, cmd)
				assert.DeepEqual(t, tc.expected[i], result)
			}
		})
	}
}

func TestAppendWithMultipleClients(t *testing.T) {
	conn1 := getLocalConnection()
	conn2 := getLocalConnection()
	defer conn1.Close()
	defer conn2.Close()

	testCases := []struct {
		name     string
		commands [][]string
		expected []interface{}
	}{
		{
			name: "Append from multiple clients",
			commands: [][]string{
				{"SET k 'base'", "conn1"},
				{"APPEND k ' first'", "conn1"},
				{"APPEND k ' second'", "conn2"},
				{"GET k", "conn1"},
			},
			expected: []interface{}{"OK", int64(11), int64(18), "base first second"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			FireCommand(conn1, "DEL k") // Ensure key is deleted at the start

			for i, cmd := range tc.commands {
				var result interface{}

				// Choose the correct connection based on the second element
				if cmd[1] == "conn1" {
					result = FireCommand(conn1, cmd[0])
				} else if cmd[1] == "conn2" {
					result = FireCommand(conn2, cmd[0])
				}

				// Handle potential nil response from GET
				if result == nil {
					assert.DeepEqual(t, "(nil)", result)
				} else {
					assert.DeepEqual(t, tc.expected[i], result)
				}
			}
		})
	}
}

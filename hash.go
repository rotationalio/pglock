package pglock

import "hash/fnv"

// Hash generates a 64-bit FNV-1a hash of the input string.
//
// This function uses the FNV-1a hash algorithm, which is a fast, non-cryptographic
// hash function that is well-suited for generating unique identifiers for use as
// advisory lock keys.
//
// By using an identical mechanism for generating lock keys, we can ensure that the same
// lock key is used for the same resource across multiple sessions and processes,
// passing a known string instead of a known integer.
func Hash(s string) int64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return int64(h.Sum64())
}

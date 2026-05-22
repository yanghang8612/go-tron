package common

// SystemAccountID is the reserved 20-byte owner of chain-global rooted state
// (dynamic properties, witness schedule, and other consensus-global records in
// later phases). It is a synthetic internal identity, never a user address.
var SystemAccountID = func() AccountID {
	var id AccountID
	for i := range id {
		id[i] = 0xff
	}
	id[AccountIDLength-1] = 0xfe
	return id
}()

// SystemAccountAddress is the canonical internal Address for the system account.
// The network prefix is cosmetic — rooted state keys by AccountID — but a single
// canonical Address must be used everywhere to avoid stateObject cache aliasing.
var SystemAccountAddress = SystemAccountID.Address(AddressPrefixMainnet)

// IsSystemAccount reports whether addr is the reserved system account, by
// AccountID (ignoring the network prefix).
func IsSystemAccount(addr Address) bool {
	return addr.AccountID() == SystemAccountID
}

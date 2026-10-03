package rawdb

// HistoryStagingProofDigest returns the protocol's existing immutable claim
// commitment. Offline resume uses it to compare a frozen plan with a receipt.
func HistoryStagingProofDigest(proof HistoryStagingProof) ([32]byte, error) { return proof.digest() }

// HistoryStagingReceiptDigest returns the exact digest stored in source routes.
func HistoryStagingReceiptDigest(receipt HistoryStagingReceipt) ([32]byte, error) {
	return historyStagingReceiptDigest(receipt)
}

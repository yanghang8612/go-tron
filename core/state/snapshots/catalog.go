package snapshots

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
)

const (
	SnapshotCatalogVersion = 1
	SnapshotCatalogFile    = "snapshot-catalog.json"

	snapshotCatalogSignatureScheme = "ed25519"
)

type SnapshotCatalog struct {
	Version          uint32         `json:"version"`
	PublishedUnix    int64          `json:"publishedUnix"`
	ManifestPath     string         `json:"manifestPath"`
	ManifestChecksum string         `json:"manifestChecksum"`
	VisibleTxStart   uint64         `json:"visibleTxStart"`
	VisibleTxEnd     uint64         `json:"visibleTxEnd"`
	Chain            *ChainIdentity `json:"chain"`
	SignatureScheme  string         `json:"signatureScheme"`
	Signer           string         `json:"signer"`
	Signature        string         `json:"signature"`
}

type snapshotCatalogSignedPayload struct {
	Version          uint32         `json:"version"`
	ManifestPath     string         `json:"manifestPath"`
	ManifestChecksum string         `json:"manifestChecksum"`
	VisibleTxStart   uint64         `json:"visibleTxStart"`
	VisibleTxEnd     uint64         `json:"visibleTxEnd"`
	Chain            *ChainIdentity `json:"chain"`
	SignatureScheme  string         `json:"signatureScheme"`
	Signer           string         `json:"signer"`
}

func PublishSignedSnapshotCatalog(dir string, privateKey ed25519.PrivateKey) (*SnapshotCatalog, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("snapshots: ed25519 private key length %d, want %d", len(privateKey), ed25519.PrivateKeySize)
	}
	manifest, err := LoadProductionManifest(dir)
	if err != nil {
		return nil, err
	}
	if manifest.Chain == nil {
		return nil, errors.New("snapshots: manifest has no chain identity")
	}
	manifestChecksum, err := checksumFile(filepath.Join(dir, ManifestFile))
	if err != nil {
		return nil, err
	}
	catalog := &SnapshotCatalog{
		Version:          SnapshotCatalogVersion,
		PublishedUnix:    time.Now().Unix(),
		ManifestPath:     ManifestFile,
		ManifestChecksum: manifestChecksum,
		VisibleTxStart:   manifest.VisibleTxStart,
		VisibleTxEnd:     manifest.VisibleTxEnd,
		Chain:            cloneChainIdentity(manifest.Chain),
		SignatureScheme:  snapshotCatalogSignatureScheme,
		Signer:           hex.EncodeToString(privateKey.Public().(ed25519.PublicKey)),
	}
	payload, err := catalog.signaturePayload()
	if err != nil {
		return nil, err
	}
	catalog.Signature = hex.EncodeToString(ed25519.Sign(privateKey, payload))
	if err := writeSnapshotCatalog(dir, catalog); err != nil {
		return nil, err
	}
	return catalog, nil
}

func LoadSnapshotCatalog(dir string) (*SnapshotCatalog, error) {
	data, err := os.ReadFile(filepath.Join(dir, SnapshotCatalogFile))
	if err != nil {
		return nil, err
	}
	var catalog SnapshotCatalog
	if err := json.Unmarshal(data, &catalog); err != nil {
		return nil, err
	}
	normalizeSnapshotCatalog(&catalog)
	if err := catalog.Validate(); err != nil {
		return nil, err
	}
	return &catalog, nil
}

func VerifySignedSnapshotCatalog(dir string, expected ChainIdentity, trustedKeys []ed25519.PublicKey) (*SnapshotCatalog, *ManifestVerificationReport, error) {
	catalog, _, report, err := VerifySignedSnapshotCatalogManifest(dir, expected, trustedKeys)
	return catalog, report, err
}

func VerifySignedSnapshotCatalogManifest(dir string, expected ChainIdentity, trustedKeys []ed25519.PublicKey) (*SnapshotCatalog, *Manifest, *ManifestVerificationReport, error) {
	catalog, err := LoadSnapshotCatalog(dir)
	if err != nil {
		return nil, nil, nil, err
	}
	if err := catalog.ValidateChainIdentity(expected); err != nil {
		return nil, nil, nil, err
	}
	if err := catalog.VerifySignature(trustedKeys); err != nil {
		return nil, nil, nil, err
	}
	manifestData, err := os.ReadFile(filepath.Join(dir, catalog.ManifestPath))
	if err != nil {
		return nil, nil, nil, err
	}
	gotChecksum := checksumBytes(manifestData)
	if !strings.EqualFold(gotChecksum, catalog.ManifestChecksum) {
		return nil, nil, nil, fmt.Errorf("snapshots: catalog manifest checksum %s, got %s", catalog.ManifestChecksum, gotChecksum)
	}
	manifest, err := decodeProductionManifest(manifestData)
	if err != nil {
		return nil, nil, nil, err
	}
	if manifest.VisibleTxStart != catalog.VisibleTxStart || manifest.VisibleTxEnd != catalog.VisibleTxEnd {
		return nil, nil, nil, fmt.Errorf("snapshots: catalog visible range [%d,%d], manifest range [%d,%d]",
			catalog.VisibleTxStart, catalog.VisibleTxEnd, manifest.VisibleTxStart, manifest.VisibleTxEnd)
	}
	if err := manifest.ValidateChainIdentity(expected); err != nil {
		return nil, nil, nil, err
	}
	report, err := VerifyLoadedManifestFiles(dir, manifest, VerifyManifestOptions{
		ExpectedChain:     &expected,
		RequireRegistered: true,
		RequireChecksums:  true,
	})
	if err != nil {
		return nil, nil, nil, err
	}
	return catalog, manifest, report, nil
}

func RestoreSnapshotFromVerifiedCatalog(db ethdb.KeyValueWriter, dir string, expected ChainIdentity, trustedKeys []ed25519.PublicKey) (*RestoreVerifiedSnapshotResult, error) {
	return RestoreSnapshotFromVerifiedCatalogWithOptions(db, dir, expected, trustedKeys, RestoreVerifiedSnapshotOptions{})
}

func RestoreSnapshotFromVerifiedCatalogWithOptions(db ethdb.KeyValueWriter, dir string, expected ChainIdentity, trustedKeys []ed25519.PublicKey, opts RestoreVerifiedSnapshotOptions) (*RestoreVerifiedSnapshotResult, error) {
	_, manifest, report, err := VerifySignedSnapshotCatalogManifest(dir, expected, trustedKeys)
	if err != nil {
		return nil, err
	}
	result, err := restoreSnapshotFromVerifiedManifestWithOptions(db, dir, manifest, *report, opts)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (c *SnapshotCatalog) Validate() error {
	if c == nil {
		return errors.New("snapshots: nil snapshot catalog")
	}
	if c.Version != SnapshotCatalogVersion {
		return fmt.Errorf("snapshots: unsupported snapshot catalog version %d", c.Version)
	}
	if c.ManifestPath != ManifestFile {
		return fmt.Errorf("snapshots: catalog manifestPath %q, want %q", c.ManifestPath, ManifestFile)
	}
	if c.ManifestChecksum == "" {
		return errors.New("snapshots: catalog missing manifest checksum")
	}
	if !strings.HasPrefix(strings.ToLower(c.ManifestChecksum), "sha256:") {
		return fmt.Errorf("snapshots: catalog manifest checksum %q must use sha256:<hex>", c.ManifestChecksum)
	}
	if err := validateHexHash("catalog manifestChecksum", strings.TrimPrefix(strings.ToLower(c.ManifestChecksum), "sha256:")); err != nil {
		return err
	}
	if c.VisibleTxEnd < c.VisibleTxStart {
		return fmt.Errorf("snapshots: catalog visible range [%d,%d] is inverted", c.VisibleTxStart, c.VisibleTxEnd)
	}
	if c.Chain == nil {
		return errors.New("snapshots: catalog has no chain identity")
	}
	if err := validateChainIdentity(c.Chain); err != nil {
		return err
	}
	if c.SignatureScheme != snapshotCatalogSignatureScheme {
		return fmt.Errorf("snapshots: catalog signature scheme %q, want %q", c.SignatureScheme, snapshotCatalogSignatureScheme)
	}
	if _, err := decodeHexFixed("catalog signer", c.Signer, ed25519.PublicKeySize); err != nil {
		return err
	}
	if _, err := decodeHexFixed("catalog signature", c.Signature, ed25519.SignatureSize); err != nil {
		return err
	}
	return nil
}

func (c *SnapshotCatalog) ValidateChainIdentity(expected ChainIdentity) error {
	normalizeChainIdentity(&expected)
	if err := validateChainIdentity(&expected); err != nil {
		return fmt.Errorf("snapshots: invalid expected chain identity: %w", err)
	}
	if c == nil || c.Chain == nil {
		return errors.New("snapshots: catalog has no chain identity")
	}
	got := *c.Chain
	normalizeChainIdentity(&got)
	if got != expected {
		return fmt.Errorf("snapshots: catalog chain identity mismatch: got chainId=%d networkId=%d genesisHash=%s forkConfigHash=%s, want chainId=%d networkId=%d genesisHash=%s forkConfigHash=%s",
			got.ChainID, got.NetworkID, got.GenesisHash, got.ForkConfigHash,
			expected.ChainID, expected.NetworkID, expected.GenesisHash, expected.ForkConfigHash)
	}
	return nil
}

func (c *SnapshotCatalog) VerifySignature(trustedKeys []ed25519.PublicKey) error {
	if c == nil {
		return errors.New("snapshots: nil snapshot catalog")
	}
	if len(trustedKeys) == 0 {
		return errors.New("snapshots: no trusted catalog keys")
	}
	signer, err := decodeHexFixed("catalog signer", c.Signer, ed25519.PublicKeySize)
	if err != nil {
		return err
	}
	signature, err := decodeHexFixed("catalog signature", c.Signature, ed25519.SignatureSize)
	if err != nil {
		return err
	}
	trusted := false
	for _, key := range trustedKeys {
		if len(key) == ed25519.PublicKeySize && strings.EqualFold(hex.EncodeToString(key), c.Signer) {
			trusted = true
			break
		}
	}
	if !trusted {
		return errors.New("snapshots: catalog signer is not trusted")
	}
	payload, err := c.signaturePayload()
	if err != nil {
		return err
	}
	if !ed25519.Verify(ed25519.PublicKey(signer), payload, signature) {
		return errors.New("snapshots: invalid catalog signature")
	}
	return nil
}

func (c *SnapshotCatalog) signaturePayload() ([]byte, error) {
	if c == nil {
		return nil, errors.New("snapshots: nil snapshot catalog")
	}
	payload := snapshotCatalogSignedPayload{
		Version:          c.Version,
		ManifestPath:     c.ManifestPath,
		ManifestChecksum: strings.ToLower(strings.TrimSpace(c.ManifestChecksum)),
		VisibleTxStart:   c.VisibleTxStart,
		VisibleTxEnd:     c.VisibleTxEnd,
		Chain:            cloneChainIdentity(c.Chain),
		SignatureScheme:  c.SignatureScheme,
		Signer:           strings.ToLower(strings.TrimSpace(c.Signer)),
	}
	normalizeChainIdentity(payload.Chain)
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return append([]byte("go-tron snapshot catalog v1\n"), data...), nil
}

func writeSnapshotCatalog(dir string, catalog *SnapshotCatalog) error {
	if catalog == nil {
		return errors.New("snapshots: nil snapshot catalog")
	}
	normalizeSnapshotCatalog(catalog)
	if err := catalog.Validate(); err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(catalog, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".snapshot-catalog-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, filepath.Join(dir, SnapshotCatalogFile))
}

func checksumFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return checksumBytes(data), nil
}

func normalizeSnapshotCatalog(catalog *SnapshotCatalog) {
	if catalog == nil {
		return
	}
	catalog.ManifestChecksum = strings.ToLower(strings.TrimSpace(catalog.ManifestChecksum))
	catalog.SignatureScheme = strings.ToLower(strings.TrimSpace(catalog.SignatureScheme))
	catalog.Signer = strings.ToLower(strings.TrimSpace(catalog.Signer))
	catalog.Signature = strings.ToLower(strings.TrimSpace(catalog.Signature))
	normalizeChainIdentity(catalog.Chain)
}

func cloneChainIdentity(in *ChainIdentity) *ChainIdentity {
	if in == nil {
		return nil
	}
	out := *in
	normalizeChainIdentity(&out)
	return &out
}

func decodeHexFixed(field, value string, want int) ([]byte, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	if len(value) != want*2 {
		return nil, fmt.Errorf("snapshots: %s has %d hex chars, want %d", field, len(value), want*2)
	}
	out, err := hex.DecodeString(value)
	if err != nil {
		return nil, fmt.Errorf("snapshots: %s is not hex: %w", field, err)
	}
	return out, nil
}

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

// These copies are diagnostic artifacts, never a canonical history backup.
type historyPackExportEntry struct {
	Block                uint64 `json:"block"`
	File                 string `json:"file"`
	SHA256               string `json:"sha256"`
	EncodedBytes         uint64 `json:"encoded_bytes"`
	DecodedBytes         uint64 `json:"decoded_bytes"`
	Codec                string `json:"codec"`
	SourceCodec          string `json:"source_codec,omitempty"`
	SourcePackBytes      uint64 `json:"source_pack_bytes,omitempty"`
	SourceChunkReadBytes uint64 `json:"source_chunk_read_bytes,omitempty"`
	SourceChunkReads     uint64 `json:"source_chunk_reads,omitempty"`
	Materialized         bool   `json:"materialized,omitempty"`
}

type historyPackExportManifest struct {
	Version    int                             `json:"version"`
	Scope      string                          `json:"scope"`
	Complete   bool                            `json:"inspection_complete"`
	StopReason string                          `json:"inspection_stop_reason"`
	Options    rawdb.HistoryPrevInspectOptions `json:"options"`
	Entries    []historyPackExportEntry        `json:"entries"`
}

type historyPackExport struct {
	directory string
	entries   []historyPackExportEntry
}

// Version 1 copied a self-contained physical value. Version 2 records source
// accounting separately, so a materialized shared pack can never masquerade as
// its original physical size. Both versions' codec/size/checksum name the file.
func validateHistoryPackExportEntry(version int, entry historyPackExportEntry) error {
	if entry.Codec != "raw" && entry.Codec != "snappy1" && entry.Codec != "chunks2" {
		return errors.New("export entry is not a self-contained codec")
	}
	digest, err := hex.DecodeString(entry.SHA256)
	if err != nil || len(digest) != sha256.Size {
		return errors.New("export entry has invalid SHA256")
	}
	if version == 1 {
		if entry.SourceCodec != "" || entry.SourcePackBytes != 0 || entry.SourceChunkReadBytes != 0 || entry.SourceChunkReads != 0 || entry.Materialized {
			return errors.New("version1 export contains version2 source accounting")
		}
		return nil
	}
	if version != 2 || entry.SourcePackBytes == 0 || entry.SourcePackBytes > 128<<20 {
		return errors.New("version2 export has invalid physical source pack size")
	}
	if entry.SourceCodec == "shared3" {
		if !entry.Materialized || entry.Codec != "raw" || entry.SourceChunkReads == 0 || entry.SourceChunkReads > 16385 || entry.SourceChunkReadBytes == 0 || entry.SourceChunkReadBytes > 1<<30 {
			return errors.New("shared3 export lacks bounded materialized source accounting")
		}
	} else if entry.Materialized || entry.SourceCodec != entry.Codec || entry.SourcePackBytes != entry.EncodedBytes || entry.SourceChunkReads != 0 || entry.SourceChunkReadBytes != 0 {
		return errors.New("legacy export source accounting differs from file representation")
	}
	return nil
}

func newHistoryPackExport(directory, chaindata string) (*historyPackExport, error) {
	if !filepath.IsAbs(directory) {
		return nil, errors.New("--export-packs must be an absolute, new directory")
	}
	directory = filepath.Clean(directory)
	parent, err := filepath.EvalSymlinks(filepath.Dir(directory))
	if err != nil {
		return nil, fmt.Errorf("resolve export parent: %w", err)
	}
	directory = filepath.Join(parent, filepath.Base(directory))
	dbPath, err := filepath.EvalSymlinks(chaindata)
	if err != nil {
		return nil, err
	}
	dbPath, err = filepath.Abs(dbPath)
	if err != nil {
		return nil, err
	}
	relative, err := filepath.Rel(dbPath, directory)
	if err != nil || relative == "." || (relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))) {
		return nil, errors.New("export directory must be outside chaindata")
	}
	if err := os.Mkdir(directory, 0700); err != nil {
		return nil, fmt.Errorf("create new diagnostic export directory: %w", err)
	}
	return &historyPackExport{directory: directory}, nil
}

func writeHistoryDiagnosticFile(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(data)
	if writeErr == nil {
		writeErr = f.Sync()
	}
	return errors.Join(writeErr, f.Close())
}

func (e *historyPackExport) writePack(sample rawdb.HistoryPrevPackSample, encoded []byte) error {
	codec, decodedBytes, err := rawdb.InspectStateHistoryPackEncoding(encoded)
	if err != nil {
		return err
	}
	if sample.Status != "complete" || sample.EncodedBytes == 0 || sample.ExportBytes != uint64(len(encoded)) || sample.ExportCodec != codec || sample.DecodedBytes != decodedBytes {
		return errors.New("export pack is not a validated self-contained sample")
	}
	if sample.Codec == "shared3" && codec != "raw" || sample.Codec != "shared3" && sample.Codec != codec {
		return errors.New("export codec does not match its physical source representation")
	}
	name := fmt.Sprintf("%020d.pack", sample.Block)
	if err := writeHistoryDiagnosticFile(filepath.Join(e.directory, name), encoded); err != nil {
		return fmt.Errorf("export pack %d: %w", sample.Block, err)
	}
	digest := sha256.Sum256(encoded)
	e.entries = append(e.entries, historyPackExportEntry{Block: sample.Block, File: name,
		SHA256: hex.EncodeToString(digest[:]), EncodedBytes: uint64(len(encoded)),
		DecodedBytes: sample.DecodedBytes, Codec: codec,
		SourceCodec: sample.Codec, SourcePackBytes: sample.EncodedBytes,
		SourceChunkReadBytes: sample.ChunkReadBytes, SourceChunkReads: sample.ChunkReads,
		Materialized: sample.Codec == "shared3"})
	return nil
}

func (e *historyPackExport) finish(report rawdb.HistoryPrevInspection) error {
	manifest := historyPackExportManifest{Version: 2, Scope: "validated self-contained representations of physical seq=0 samples; shared3 is materialized as raw RLP; file codec/encoded_bytes/SHA256 describe export files, source_* describe physical pack and actual referenced chunk lookups; lookup bytes are not unique storage or disk I/O; excludes repairs, canonical metadata and ancient; not a backup",
		Complete: report.Complete, StopReason: report.StopReason, Options: report.Options, Entries: e.entries}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	if err := writeHistoryDiagnosticFile(filepath.Join(e.directory, "manifest.json"), append(data, '\n')); err != nil {
		return err
	}
	directory, err := os.Open(e.directory)
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

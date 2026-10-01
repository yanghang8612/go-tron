package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"syscall"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	statesnapshots "github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/urfave/cli/v2"
)

const historyStagingMaxPlanBytes = 16 << 30
const historyStagingMaxPlanRowBytes = 4 << 20

type historyStagingPlanReader struct {
	file   *os.File
	scan   *bufio.Scanner
	header historyStagingPlanHeader
	rows   uint64
}

func openHistoryStagingPlan(ctx *cli.Context, c *historyStagingCLIContext) (*historyStagingPlanReader, error) {
	planID := ctx.String("plan-id")
	if !historyStagingSHAPattern.MatchString(planID) {
		return nil, errors.New("history staging requires a 64-hex plan ID")
	}
	path := filepath.Join(historyStagingPlanDirectory(ctx.String("datadir")), planID+".jsonl")
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > historyStagingMaxPlanBytes {
		file.Close()
		return nil, errors.New("history staging plan is not a bounded regular file")
	}
	h := sha256.New()
	if _, err := io.Copy(h, file); err != nil {
		file.Close()
		return nil, err
	}
	if hex.EncodeToString(h.Sum(nil)) != planID {
		file.Close()
		return nil, errors.New("history staging plan digest differs from plan ID")
	}
	if after, err := file.Stat(); err != nil || !os.SameFile(info, after) ||
		info.Size() != after.Size() || info.ModTime() != after.ModTime() {
		file.Close()
		return nil, errors.New("history staging plan changed during hashing")
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		file.Close()
		return nil, err
	}
	scan := bufio.NewScanner(file)
	scan.Buffer(make([]byte, 256<<10), historyStagingMaxPlanRowBytes)
	reader := &historyStagingPlanReader{file: file, scan: scan}
	if !scan.Scan() {
		file.Close()
		return nil, fmt.Errorf("history staging plan missing header: %w", scan.Err())
	}
	if err := decodeHistoryStagingPlanRow(scan.Bytes(), &reader.header); err != nil {
		file.Close()
		return nil, err
	}
	header := reader.header
	if header.Version != 1 || header.JobID != c.event.JobID ||
		header.CandidateSHA256 != c.event.CandidateSHA256 ||
		header.Paths != c.paths || header.Head.HeadHash == ([32]byte{}) ||
		header.GenesisHash == ([32]byte{}) ||
		!historyStagingSHAPattern.MatchString(header.ManifestSHA256) ||
		!historyStagingSHAPattern.MatchString(header.ConfigSHA256) ||
		header.LastBucket > header.FullEligibleLastBucket ||
		header.FullEligibleLastBucket != historyStagingFullEligibleBucket(header.EligibleThrough) ||
		header.HistoryWindow == 0 || header.PruneMode == "" {
		file.Close()
		return nil, errors.New("history staging plan header identity differs")
	}
	c.event.PlanID = planID
	return reader, nil
}

func historyStagingFullEligibleBucket(eligible uint64) uint64 {
	if eligible < 2*rawdb.StateHistoryChunkBucketBlocks-1 {
		return 0
	}
	return (eligible - (rawdb.StateHistoryChunkBucketBlocks - 1)) / rawdb.StateHistoryChunkBucketBlocks
}

// The job-name link is published before the content-addressed name. If a
// process dies between either rename/link or reporting plan_id, the same job
// can recover its fully fsynced plan without rebuilding a different one.
func recoverHistoryStagingJobPlan(dir string, expected historyStagingPlanHeader) (string, bool, error) {
	jobPath := filepath.Join(dir, expected.JobID+".jsonl")
	info, err := os.Lstat(jobPath)
	if os.IsNotExist(err) {
		return "", false, nil
	}
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > historyStagingMaxPlanBytes {
		return "", false, errors.New("history staging job plan is not a bounded regular file")
	}
	file, err := os.Open(jobPath)
	if err != nil {
		return "", false, err
	}
	defer file.Close()
	var header historyStagingPlanHeader
	if err := json.NewDecoder(io.LimitReader(file, 1<<20)).Decode(&header); err != nil {
		return "", false, err
	}
	if !reflect.DeepEqual(header, expected) {
		return "", false, errors.New("history staging job already has a different frozen plan; refuse to overwrite")
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return "", false, err
	}
	h := sha256.New()
	if _, err := io.Copy(h, io.LimitReader(file, historyStagingMaxPlanBytes+1)); err != nil {
		return "", false, err
	}
	planID := hex.EncodeToString(h.Sum(nil))
	if err := linkHistoryStagingDigestPlan(dir, jobPath, planID); err != nil {
		return "", false, err
	}
	return planID, true, nil
}

func linkHistoryStagingDigestPlan(dir, jobPath, planID string) error {
	digestPath := filepath.Join(dir, planID+".jsonl")
	if err := os.Link(jobPath, digestPath); err != nil {
		if !os.IsExist(err) {
			return err
		}
		jobInfo, jobErr := os.Lstat(jobPath)
		digestInfo, digestErr := os.Lstat(digestPath)
		if jobErr != nil || digestErr != nil || !os.SameFile(jobInfo, digestInfo) {
			return errors.New("history staging plan digest name conflicts with a different file")
		}
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func verifyHistoryStagingPlanInputs(ctx *cli.Context, c *historyStagingCLIContext, source ethdb.KeyValueStore, header historyStagingPlanHeader) error {
	boundary, eligible, err := c.inspectBoundary(ctx, source, ctx.String("datadir"))
	if err != nil {
		return err
	}
	if boundary != header.Head || eligible != header.EligibleThrough {
		return errors.New("history staging canonical head, solid, Finish or index changed while planning")
	}
	index, present, err := rawdb.ReadStageProgressRow(source, rawdb.StageStateHistoryIndex)
	if err != nil || !present || !index.HasBlockHash {
		return errors.New("history staging index changed while planning")
	}
	if index.BlockNum != header.IndexBlock || index.BlockHash != header.IndexHash {
		return errors.New("history staging index changed while planning")
	}
	pruneTx := uint64(0)
	if progress, present, err := rawdb.ReadStageProgressRow(source, rawdb.StageSnapshotHotPrune); err != nil {
		return err
	} else if present {
		pruneTx = progress.BlockNum
	}
	if pruneTx != header.PruneTxNum {
		return errors.New("history staging prune watermark changed while planning")
	}
	manifestSHA, err := historyStagingFileSHA256(filepath.Join(c.paths.Cold, statesnapshots.ManifestFile))
	if err != nil || manifestSHA != header.ManifestSHA256 {
		return errors.New("history staging cold manifest changed while planning")
	}
	configSHA := fmt.Sprintf("%064x", 0)
	if path := ctx.String("config"); path != "" {
		configSHA, err = historyStagingFileSHA256(path)
		if err != nil {
			return err
		}
	}
	if configSHA != header.ConfigSHA256 {
		return errors.New("history staging history config changed while planning")
	}
	return nil
}

func (p *historyStagingPlanReader) Close() error {
	if p == nil || p.file == nil {
		return nil
	}
	return p.file.Close()
}

func (p *historyStagingPlanReader) CompleteEligibleCoverage() bool {
	return p != nil && p.header.LastBucket == p.header.FullEligibleLastBucket
}

func (p *historyStagingPlanReader) Next(ctx context.Context) (historyStagingPlanBucket, bool, error) {
	var row historyStagingPlanBucket
	if err := ctx.Err(); err != nil {
		return row, false, err
	}
	if p.rows >= p.header.LastBucket {
		if p.scan.Scan() || p.scan.Err() != nil {
			return row, false, fmt.Errorf("history staging plan has extra rows or parse error: %v", p.scan.Err())
		}
		return row, false, nil
	}
	if !p.scan.Scan() {
		return row, false, fmt.Errorf("history staging plan missing bucket %d: %v", p.rows+1, p.scan.Err())
	}
	if err := decodeHistoryStagingPlanRow(p.scan.Bytes(), &row); err != nil {
		return row, false, fmt.Errorf("history staging plan bucket %d: %w", p.rows+1, err)
	}
	p.rows++
	if row.Proof.Bucket != p.rows || row.Proof.Epoch != 1 ||
		row.Proof.EligibleThrough != p.header.EligibleThrough ||
		row.Proof.FinishBlock != p.header.Head.HeadBlock ||
		row.Proof.FinishHash != p.header.Head.HeadHash ||
		row.Proof.IndexBlock != p.header.IndexBlock ||
		row.Proof.IndexHash != p.header.IndexHash ||
		row.Physical.Bytes > p.header.Limits.MaxBucketBytes ||
		row.Physical.Bytes > rawdb.HistoryStagingMaxCopyPhysicalBytes(p.header.Limits.MaxWorkBytes) {
		return row, false, fmt.Errorf("history staging plan bucket %d identity/budget differs", p.rows)
	}
	if err := rawdb.VerifyHistoryStagingProof(row.Proof); err != nil {
		return row, false, err
	}
	return row, true, nil
}

func decodeHistoryStagingPlanRow(data []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return errors.New("history staging plan row has trailing JSON")
	}
	return nil
}

func encodeHistoryStagingPlanRow(writer io.Writer, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(data)+1 > historyStagingMaxPlanRowBytes {
		return errors.New("history staging plan row exceeds bounded JSONL limit")
	}
	if _, err := writer.Write(data); err != nil {
		return err
	}
	_, err = writer.Write([]byte{'\n'})
	return err
}

func historyStagingClaimID(planID string, bucket uint64) ([32]byte, error) {
	var out [32]byte
	digest, err := hex.DecodeString(planID)
	if err != nil || len(digest) != 32 {
		return out, errors.New("invalid staging plan ID")
	}
	h := sha256.New()
	h.Write([]byte("gtron-history-staging-claim-v1\x00"))
	h.Write(digest)
	var number [8]byte
	for i := 7; i >= 0; i-- {
		number[i] = byte(bucket)
		bucket >>= 8
	}
	h.Write(number[:])
	copy(out[:], h.Sum(nil))
	return out, nil
}

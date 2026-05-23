package domains

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

var (
	ErrNilWriter               = errors.New("domains: nil writer")
	ErrPrefixDeleteUnsupported = errors.New("domains: prefix delete unsupported by backing store")
)

type LatestReader interface {
	GetLatest(owner common.Address, domain kvdomains.KVDomain, key []byte) ([]byte, bool, error)
}

type Writer interface {
	DomainPut(owner common.Address, domain kvdomains.KVDomain, key, value []byte) error
	DomainDel(owner common.Address, domain kvdomains.KVDomain, key []byte) error
	DomainDelPrefix(owner common.Address, domain kvdomains.KVDomain, prefix []byte) error
}

type Store interface {
	LatestReader
	Writer
}

type MutationKind uint8

const (
	MutationPut MutationKind = iota + 1
	MutationDel
	MutationDelPrefix
)

type Mutation struct {
	Seq    uint64
	Kind   MutationKind
	Owner  common.Address
	Domain kvdomains.KVDomain
	Key    []byte
	Value  []byte
}

type GetSource string

const (
	GetSourceOverlay      GetSource = "overlay"
	GetSourceParent       GetSource = "parent"
	GetSourcePrefixDelete GetSource = "prefix-delete"
	GetSourceMiss         GetSource = "miss"
)

type GetEvent struct {
	Owner  common.Address
	Domain kvdomains.KVDomain
	Key    []byte
	Found  bool
	Source GetSource
}

type Hooks struct {
	OnGetLatest func(GetEvent)
	OnMutation  func(Mutation)
}

type Metrics struct {
	Gets          uint64
	OverlayHits   uint64
	ParentHits    uint64
	Misses        uint64
	Puts          uint64
	Deletes       uint64
	PrefixDeletes uint64
}

type Option func(*Overlay)

func WithHooks(h Hooks) Option {
	return func(o *Overlay) {
		o.hooks = h
	}
}

// Overlay is the Phase-1 block/domain mutation layer. It records dirty writes,
// reads through to a parent latest-state reader, and can be discarded or flushed
// in operation order.
type Overlay struct {
	parent   LatestReader
	hooks    Hooks
	metrics  Metrics
	nextSeq  uint64
	exact    map[string]Mutation
	prefixes []Mutation
	ops      []Mutation
}

func NewOverlay(parent LatestReader, opts ...Option) *Overlay {
	o := &Overlay{
		parent: parent,
		exact:  make(map[string]Mutation),
	}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

func (o *Overlay) GetLatest(owner common.Address, domain kvdomains.KVDomain, key []byte) ([]byte, bool, error) {
	if err := validateDomain(domain); err != nil {
		return nil, false, err
	}
	if o == nil {
		return nil, false, nil
	}
	o.metrics.Gets++
	prefixSeq := o.latestPrefixDeleteSeq(owner, domain, key)
	if m, ok := o.exact[logicalKey(owner, domain, key)]; ok && m.Seq > prefixSeq {
		if m.Kind == MutationDel {
			o.metrics.Misses++
			o.emitGet(owner, domain, key, false, GetSourceOverlay)
			return nil, false, nil
		}
		o.metrics.OverlayHits++
		o.emitGet(owner, domain, key, true, GetSourceOverlay)
		return append([]byte(nil), m.Value...), true, nil
	}
	if prefixSeq > 0 {
		o.metrics.Misses++
		o.emitGet(owner, domain, key, false, GetSourcePrefixDelete)
		return nil, false, nil
	}
	if o.parent == nil {
		o.metrics.Misses++
		o.emitGet(owner, domain, key, false, GetSourceMiss)
		return nil, false, nil
	}
	value, ok, err := o.parent.GetLatest(owner, domain, key)
	if err != nil {
		return nil, false, err
	}
	if ok {
		o.metrics.ParentHits++
		o.emitGet(owner, domain, key, true, GetSourceParent)
		return value, true, nil
	}
	o.metrics.Misses++
	o.emitGet(owner, domain, key, false, GetSourceMiss)
	return nil, false, nil
}

func (o *Overlay) DomainPut(owner common.Address, domain kvdomains.KVDomain, key, value []byte) error {
	return o.appendMutation(Mutation{
		Kind:   MutationPut,
		Owner:  owner,
		Domain: domain,
		Key:    append([]byte(nil), key...),
		Value:  append([]byte(nil), value...),
	})
}

func (o *Overlay) DomainDel(owner common.Address, domain kvdomains.KVDomain, key []byte) error {
	return o.appendMutation(Mutation{
		Kind:   MutationDel,
		Owner:  owner,
		Domain: domain,
		Key:    append([]byte(nil), key...),
	})
}

func (o *Overlay) DomainDelPrefix(owner common.Address, domain kvdomains.KVDomain, prefix []byte) error {
	return o.appendMutation(Mutation{
		Kind:   MutationDelPrefix,
		Owner:  owner,
		Domain: domain,
		Key:    append([]byte(nil), prefix...),
	})
}

func (o *Overlay) FlushTo(w Writer) error {
	if w == nil {
		return ErrNilWriter
	}
	for _, m := range o.ops {
		switch m.Kind {
		case MutationPut:
			if err := w.DomainPut(m.Owner, m.Domain, m.Key, m.Value); err != nil {
				return err
			}
		case MutationDel:
			if err := w.DomainDel(m.Owner, m.Domain, m.Key); err != nil {
				return err
			}
		case MutationDelPrefix:
			if err := w.DomainDelPrefix(m.Owner, m.Domain, m.Key); err != nil {
				return err
			}
		default:
			return fmt.Errorf("domains: unknown mutation kind %d", m.Kind)
		}
	}
	o.Discard()
	return nil
}

func (o *Overlay) Discard() {
	if o == nil {
		return
	}
	o.nextSeq = 0
	o.exact = make(map[string]Mutation)
	o.prefixes = nil
	o.ops = nil
}

func (o *Overlay) Mutations() []Mutation {
	if o == nil || len(o.ops) == 0 {
		return nil
	}
	out := make([]Mutation, len(o.ops))
	for i, op := range o.ops {
		out[i] = cloneMutation(op)
	}
	return out
}

func (o *Overlay) Metrics() Metrics {
	if o == nil {
		return Metrics{}
	}
	return o.metrics
}

func (o *Overlay) appendMutation(m Mutation) error {
	if o == nil {
		return errors.New("domains: nil overlay")
	}
	if err := validateDomain(m.Domain); err != nil {
		return err
	}
	o.nextSeq++
	m.Seq = o.nextSeq
	m = cloneMutation(m)
	o.ops = append(o.ops, m)
	switch m.Kind {
	case MutationPut:
		o.metrics.Puts++
		o.exact[logicalKey(m.Owner, m.Domain, m.Key)] = m
	case MutationDel:
		o.metrics.Deletes++
		o.exact[logicalKey(m.Owner, m.Domain, m.Key)] = m
	case MutationDelPrefix:
		o.metrics.PrefixDeletes++
		o.prefixes = append(o.prefixes, m)
	default:
		return fmt.Errorf("domains: unknown mutation kind %d", m.Kind)
	}
	if o.hooks.OnMutation != nil {
		o.hooks.OnMutation(cloneMutation(m))
	}
	return nil
}

func (o *Overlay) latestPrefixDeleteSeq(owner common.Address, domain kvdomains.KVDomain, key []byte) uint64 {
	var seq uint64
	for _, p := range o.prefixes {
		if p.Owner == owner && p.Domain == domain && bytes.HasPrefix(key, p.Key) && p.Seq > seq {
			seq = p.Seq
		}
	}
	return seq
}

func (o *Overlay) emitGet(owner common.Address, domain kvdomains.KVDomain, key []byte, found bool, source GetSource) {
	if o.hooks.OnGetLatest == nil {
		return
	}
	o.hooks.OnGetLatest(GetEvent{
		Owner:  owner,
		Domain: domain,
		Key:    append([]byte(nil), key...),
		Found:  found,
		Source: source,
	})
}

func validateDomain(domain kvdomains.KVDomain) error {
	if !kvdomains.IsRegistered(domain) {
		return fmt.Errorf("domains: unregistered domain %#04x", uint16(domain))
	}
	return nil
}

func cloneMutation(m Mutation) Mutation {
	m.Key = append([]byte(nil), m.Key...)
	m.Value = append([]byte(nil), m.Value...)
	return m
}

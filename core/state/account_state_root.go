package state

import (
	"bytes"
	"sort"

	ethcommon "github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie/trienode"
	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// JournalMark returns the current journal length. Callers use it to mirror
// java-tron's AccountStateCallBack window, which only records AccountStore
// writes made while processing transactions.
func (s *StateDB) JournalMark() int {
	return s.journal.length()
}

// JavaAccountStateRoot updates java-tron's lightweight account-state trie from
// account changes recorded since mark. The trie value is AccountStateEntity:
// Account{address,balance,allowance}; it deliberately excludes full Account
// fields and changes outside the tx loop such as block rewards.
func (s *StateDB) JavaAccountStateRoot(parentRoot tcommon.Hash, mark int) (tcommon.Hash, error) {
	originRoot := normalizeJavaAccountRoot(parentRoot)
	touched := make(map[tcommon.Address]struct{})
	if mark < 0 {
		mark = 0
	}
	if mark > s.journal.length() {
		mark = s.journal.length()
	}
	for _, entry := range s.journal.entries[mark:] {
		switch change := entry.(type) {
		case accountChange:
			touched[change.address] = struct{}{}
		case *accountScalarChange:
			touched[change.address] = struct{}{}
		}
	}
	if len(touched) == 0 {
		return tcommon.Hash(originRoot), nil
	}

	tr, err := s.db.OpenTrie(originRoot)
	if err != nil {
		return tcommon.Hash{}, err
	}
	addrs := make([]tcommon.Address, 0, len(touched))
	for addr := range touched {
		addrs = append(addrs, addr)
	}
	sort.Slice(addrs, func(i, j int) bool {
		return bytes.Compare(addrs[i][:], addrs[j][:]) < 0
	})

	for _, addr := range addrs {
		key, err := rlp.EncodeToBytes(addr.Bytes())
		if err != nil {
			return tcommon.Hash{}, err
		}
		obj := s.getStateObject(addr)
		if obj == nil {
			if err := tr.Delete(key); err != nil {
				return tcommon.Hash{}, err
			}
			continue
		}
		data, err := proto.Marshal(&corepb.Account{
			Address:   addr.Bytes(),
			Balance:   obj.account.Balance(),
			Allowance: obj.account.Allowance(),
		})
		if err != nil {
			return tcommon.Hash{}, err
		}
		if err := tr.Update(key, data); err != nil {
			return tcommon.Hash{}, err
		}
	}

	root, nodes := tr.Commit(false)
	if nodes != nil {
		if err := s.db.TrieDB().Update(root, originRoot, 0, trienode.NewWithNodeSet(nodes), nil); err != nil {
			return tcommon.Hash{}, err
		}
		if err := s.db.TrieDB().Commit(root, false); err != nil {
			return tcommon.Hash{}, err
		}
	}
	return tcommon.Hash(root), nil
}

func normalizeJavaAccountRoot(root tcommon.Hash) ethcommon.Hash {
	if root == (tcommon.Hash{}) {
		return ethtypes.EmptyRootHash
	}
	return ethcommon.Hash(root)
}

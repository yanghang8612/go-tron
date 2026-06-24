package vm

import (
	tcommon "github.com/tronprotocol/go-tron/common"
)

// java_hashmap_order.go reproduces the iteration order of a
// java.util.HashMap<com.google.protobuf.ByteString, Long> keyed on TRON witness
// addresses, as used by java-tron's VoteWitnessProcessor.execute:
//
//	Map<ByteString,Long> voteMap = new HashMap<>();
//	... voteMap.put(addr, ...) ...                         // first-seen insertion
//	for (Map.Entry<ByteString,Long> e : voteMap.entrySet()) {
//	    accountCapsule.addVotes(e.getKey(), e.getValue());      // <-- this order
//	}
//
// (VoteWitnessProcessor.java:54,83-84,105-108). The account's `votes` repeated
// field is appended in entrySet() order and then protobuf-serialized into state,
// so the order is consensus-load-bearing: a contract VOTEWITNESS that merges
// votes for several distinct witnesses must write them in this exact order or the
// state root diverges from java-tron.
//
// entrySet() iterates the bucket table in index order, and within each bucket
// follows the bin's singly-linked `Node.next` chain. That chain order is NOT a
// simple function of insertion or of (hash & cap): it is produced by HashMap's
// full put/resize/treeify machinery — sequential appends, the lo/hi order-
// preserving split on each table doubling, conversion of a long bin to a
// red-black tree (which moves the tree root to the front via moveRootToFront and
// re-links via putTreeVal's prev/next splice), and the untreeify-on-split below
// UNTREEIFY_THRESHOLD. The only faithful way to reproduce it is to port that
// machinery; this file is a 1:1 port of java.util.HashMap (OpenJDK) restricted to
// the doubly-linked bin chains that determine iteration order. It is validated
// byte-for-byte against a real protobuf-3.25.8 HashMap over 324 cases
// (TestJavaHashMapOrderMatchesGolden), including every treeified bin.
//
// ByteString.hashCode (protobuf 3.x): h := len(b); for each byte: h = h*31 +
// int32(int8(b)) (SIGNED byte, Java-int wraparound); 0 → 1. HashMap.hash spread:
// h ^ (h >>> 16). All arithmetic and comparisons are done as Java `int` (int32,
// signed), which the bin index (h & (cap-1)) and the red-black hash comparisons
// both depend on.
//
// `entries` must be the DISTINCT witness addresses in first-seen order (the
// caller deduplicates, mirroring voteMap's merge). The returned slice is a
// reordering of that same set.
func javaHashMapOrder(entries []tcommon.Address) []tcommon.Address {
	n := len(entries)
	if n <= 1 {
		out := make([]tcommon.Address, n)
		copy(out, entries)
		return out
	}
	hm := newJavaHashMap()
	for i := range entries {
		hm.put(entries[i])
	}
	return hm.iterationOrder()
}

const (
	jhmTreeifyThreshold   = 8
	jhmUntreeifyThreshold = 6
	jhmMinTreeifyCapacity = 64
	jhmDefaultInitialCap  = 16
	jhmDefaultLoadFactorN = 3 // load factor 0.75 = 3/4
	jhmDefaultLoadFactorD = 4
)

// byteStringHashCode mirrors com.google.protobuf.ByteString.hashCode() (protobuf
// 3.x): seed = length, fold each byte as h = h*31 + signedByte, then 0 → 1.
// int32 throughout so it wraps exactly like a Java `int`.
func byteStringHashCode(data []byte) int32 {
	h := int32(len(data))
	for _, b := range data {
		h = h*31 + int32(int8(b)) // signed byte, sign-extended (Java semantics)
	}
	if h == 0 {
		return 1
	}
	return h
}

// javaHashSpread is HashMap.hash(): h ^ (h >>> 16) as a Java int. The shift is
// the *unsigned* >>> 16, but the XOR result is reinterpreted as a signed int32
// (which is what the bin index and tree comparisons use).
func javaHashSpread(h int32) int32 {
	return int32(uint32(h) ^ (uint32(h) >> 16))
}

// jhmNode mirrors java.util.HashMap.Node / TreeNode. Only the fields that affect
// iteration order are modelled: the singly-linked `next` chain (what entrySet
// walks), the `prev` back-link (maintained by tree splice/move), the red-black
// child/parent/color pointers, and a `tree` flag.
type jhmNode struct {
	hash int32
	key  tcommon.Address
	next *jhmNode
	prev *jhmNode

	left   *jhmNode
	right  *jhmNode
	parent *jhmNode
	red    bool
	tree   bool
}

type javaHashMap struct {
	tab       []*jhmNode
	cap       int
	size      int
	threshold int
}

func newJavaHashMap() *javaHashMap { return &javaHashMap{} }

func (m *javaHashMap) put(key tcommon.Address) {
	h := javaHashSpread(byteStringHashCode(key[:]))
	if m.cap == 0 {
		m.resize()
	}
	i := int(h) & (m.cap - 1)
	p := m.tab[i]
	if p == nil {
		m.tab[i] = &jhmNode{hash: h, key: key}
	} else if p.tree {
		if existing := m.putTreeVal(i, p, h, key); existing != nil {
			return // key already present (not expected: caller pre-dedups)
		}
	} else {
		binCount := 0
		cur := p
		for {
			if cur.next == nil {
				newn := &jhmNode{hash: h, key: key, prev: cur}
				cur.next = newn
				if binCount >= jhmTreeifyThreshold-1 {
					m.treeifyBin(i)
				}
				break
			}
			if cur.next.key == key {
				return // already present
			}
			cur = cur.next
			binCount++
		}
	}
	m.size++
	if m.size > m.threshold {
		m.resize()
	}
}

func (m *javaHashMap) resize() {
	oldCap := m.cap
	oldThr := m.threshold
	var newCap, newThr int
	if oldCap > 0 {
		newCap = oldCap << 1
		newThr = oldThr << 1
	} else {
		newCap = jhmDefaultInitialCap
		newThr = jhmDefaultInitialCap * jhmDefaultLoadFactorN / jhmDefaultLoadFactorD
	}
	m.cap = newCap
	m.threshold = newThr
	newTab := make([]*jhmNode, newCap)
	if oldCap > 0 {
		for j := 0; j < oldCap; j++ {
			e := m.tab[j]
			if e == nil {
				continue
			}
			m.tab[j] = nil
			if e.next == nil {
				newTab[int(e.hash)&(newCap-1)] = e
			} else if e.tree {
				m.split(e, newTab, j, oldCap)
			} else {
				var loHead, loTail, hiHead, hiTail *jhmNode
				cur := e
				for cur != nil {
					nxt := cur.next
					if int(cur.hash)&oldCap == 0 {
						if loTail == nil {
							loHead = cur
						} else {
							loTail.next = cur
						}
						cur.prev = loTail
						loTail = cur
					} else {
						if hiTail == nil {
							hiHead = cur
						} else {
							hiTail.next = cur
						}
						cur.prev = hiTail
						hiTail = cur
					}
					cur = nxt
				}
				if loTail != nil {
					loTail.next = nil
					newTab[j] = loHead
				}
				if hiTail != nil {
					hiTail.next = nil
					newTab[j+oldCap] = hiHead
				}
			}
		}
	}
	m.tab = newTab
}

// treeifyBin mirrors HashMap.treeifyBin: below MIN_TREEIFY_CAPACITY it resizes
// instead of building a tree; otherwise it marks the bin's nodes as tree nodes
// (preserving the existing next chain) and treeifies them.
func (m *javaHashMap) treeifyBin(index int) {
	if m.cap < jhmMinTreeifyCapacity {
		m.resize()
		return
	}
	e := m.tab[index]
	if e == nil {
		return
	}
	// Convert the bin's nodes to tree nodes in place, keeping the next/prev chain.
	for cur := e; cur != nil; cur = cur.next {
		cur.tree = true
		cur.left, cur.right, cur.parent = nil, nil, nil
	}
	m.treeify(e, index)
}

// treeify mirrors TreeNode.treeify: insert each bin node (in next-chain order)
// into a red-black tree comparing by hash (tie-break only on equal hash, which
// never happens for distinct witness addresses), then moveRootToFront.
func (m *javaHashMap) treeify(head *jhmNode, index int) {
	var root *jhmNode
	x := head
	for x != nil {
		nxt := x.next
		x.left, x.right = nil, nil
		if root == nil {
			x.parent = nil
			x.red = false
			root = x
		} else {
			k := x.key
			h := x.hash
			p := root
			for {
				var dir int
				ph := p.hash
				if ph > h {
					dir = -1
				} else if ph < h {
					dir = 1
				} else {
					dir = jhmTieBreak(k, p.key)
				}
				xp := p
				if dir <= 0 {
					p = p.left
				} else {
					p = p.right
				}
				if p == nil {
					x.parent = xp
					if dir <= 0 {
						xp.left = x
					} else {
						xp.right = x
					}
					root = m.balanceInsertion(root, x)
					break
				}
			}
		}
		x = nxt
	}
	m.moveRootToFront(index, root)
}

// putTreeVal mirrors TreeNode.putTreeVal: locate-or-insert into the tree, and on
// insert splice the new node into the bin's prev/next chain right after its tree
// parent (xp.next = x; x.prev = xp; ...), then rebalance and moveRootToFront.
// Returns the existing node if the key is already present (nil on insert).
func (m *javaHashMap) putTreeVal(index int, head *jhmNode, h int32, key tcommon.Address) *jhmNode {
	root := head
	for root.parent != nil {
		root = root.parent
	}
	p := root
	for {
		var dir int
		ph := p.hash
		if ph > h {
			dir = -1
		} else if ph < h {
			dir = 1
		} else if p.key == key {
			return p
		} else {
			dir = jhmTieBreak(key, p.key)
		}
		xp := p
		if dir <= 0 {
			p = p.left
		} else {
			p = p.right
		}
		if p == nil {
			x := &jhmNode{hash: h, key: key, tree: true}
			xpn := xp.next
			xp.next = x
			x.prev = xp
			x.next = xpn
			if xpn != nil {
				xpn.prev = x
			}
			x.parent = xp
			if dir <= 0 {
				xp.left = x
			} else {
				xp.right = x
			}
			root = m.balanceInsertion(root, x)
			m.moveRootToFront(index, root)
			return nil
		}
	}
}

// split mirrors TreeNode.split: walk the tree bin's next chain, partition into
// lo/hi preserving order, then untreeify a half at or below UNTREEIFY_THRESHOLD
// else (only when the bin actually split) re-treeify it.
func (m *javaHashMap) split(b *jhmNode, newTab []*jhmNode, index, oldCap int) {
	var loHead, loTail, hiHead, hiTail *jhmNode
	lc, hc := 0, 0
	e := b
	for e != nil {
		nxt := e.next
		e.next = nil
		if int(e.hash)&oldCap == 0 {
			e.prev = loTail
			if loTail == nil {
				loHead = e
			} else {
				loTail.next = e
			}
			loTail = e
			lc++
		} else {
			e.prev = hiTail
			if hiTail == nil {
				hiHead = e
			} else {
				hiTail.next = e
			}
			hiTail = e
			hc++
		}
		e = nxt
	}
	if loHead != nil {
		if lc <= jhmUntreeifyThreshold {
			newTab[index] = m.untreeify(loHead)
		} else {
			newTab[index] = loHead
			if hiHead != nil {
				m.treeify(loHead, index)
			}
		}
	}
	if hiHead != nil {
		if hc <= jhmUntreeifyThreshold {
			newTab[index+oldCap] = m.untreeify(hiHead)
		} else {
			newTab[index+oldCap] = hiHead
			if loHead != nil {
				m.treeify(hiHead, index+oldCap)
			}
		}
	}
}

// untreeify mirrors TreeNode.untreeify: rebuild a plain singly-linked bin from a
// tree bin's next chain (java allocates fresh Node replacements; we mirror that
// to drop the tree flag and child pointers).
func (m *javaHashMap) untreeify(head *jhmNode) *jhmNode {
	var hd, tl *jhmNode
	for q := head; q != nil; q = q.next {
		n := &jhmNode{hash: q.hash, key: q.key}
		if tl == nil {
			hd = n
		} else {
			tl.next = n
			n.prev = tl
		}
		tl = n
	}
	return hd
}

// jhmTieBreak stands in for TreeNode.tieBreakOrder, reached only when two keys
// have the EXACT same 32-bit spread hash. For distinct registered-witness
// addresses (the only inputs the vote opcode accepts) this is unreachable; java's
// tieBreakOrder falls back to System.identityHashCode there, which is not
// reproducible, so we make the collision explicit rather than silently diverge.
func jhmTieBreak(a, b tcommon.Address) int {
	panic("javaHashMapOrder: two witness addresses produced identical 32-bit HashMap spread hashes; " +
		"java HashMap would tie-break via System.identityHashCode, which is not reproducible")
}

func (m *javaHashMap) moveRootToFront(index int, root *jhmNode) {
	if root == nil {
		return
	}
	first := m.tab[index]
	if root != first {
		rn := root.next
		rp := root.prev
		if rn != nil {
			rn.prev = rp
		}
		if rp != nil {
			rp.next = rn
		}
		if first != nil {
			first.prev = root
		}
		root.next = first
		root.prev = nil
		m.tab[index] = root
	}
}

func (m *javaHashMap) rotateLeft(root, p *jhmNode) *jhmNode {
	if p == nil {
		return root
	}
	r := p.right
	if r != nil {
		p.right = r.left
		if r.left != nil {
			r.left.parent = p
		}
		r.parent = p.parent
		if p.parent == nil {
			root = r
			r.red = false
		} else if p.parent.left == p {
			p.parent.left = r
		} else {
			p.parent.right = r
		}
		r.left = p
		p.parent = r
	}
	return root
}

func (m *javaHashMap) rotateRight(root, p *jhmNode) *jhmNode {
	if p == nil {
		return root
	}
	l := p.left
	if l != nil {
		p.left = l.right
		if l.right != nil {
			l.right.parent = p
		}
		l.parent = p.parent
		if p.parent == nil {
			root = l
			l.red = false
		} else if p.parent.right == p {
			p.parent.right = l
		} else {
			p.parent.left = l
		}
		l.right = p
		p.parent = l
	}
	return root
}

func (m *javaHashMap) balanceInsertion(root, x *jhmNode) *jhmNode {
	x.red = true
	for {
		xp := x.parent
		if xp == nil {
			x.red = false
			return x
		}
		if !xp.red || xp.parent == nil {
			return root
		}
		xpp := xp.parent
		if xp == xpp.left {
			xppr := xpp.right
			if xppr != nil && xppr.red {
				xppr.red = false
				xp.red = false
				xpp.red = true
				x = xpp
			} else {
				if x == xp.right {
					x = xp
					root = m.rotateLeft(root, x)
					xp = x.parent
					if xp != nil {
						xpp = xp.parent
					} else {
						xpp = nil
					}
				}
				if xp != nil {
					xp.red = false
					if xpp != nil {
						xpp.red = true
						root = m.rotateRight(root, xpp)
					}
				}
			}
		} else {
			xppl := xpp.left
			if xppl != nil && xppl.red {
				xppl.red = false
				xp.red = false
				xpp.red = true
				x = xpp
			} else {
				if x == xp.left {
					x = xp
					root = m.rotateRight(root, x)
					xp = x.parent
					if xp != nil {
						xpp = xp.parent
					} else {
						xpp = nil
					}
				}
				if xp != nil {
					xp.red = false
					if xpp != nil {
						xpp.red = true
						root = m.rotateLeft(root, xpp)
					}
				}
			}
		}
	}
}

// iterationOrder walks the bucket table in index order, each bin along its
// next chain — exactly java HashMap's entrySet() / HashIterator.
func (m *javaHashMap) iterationOrder() []tcommon.Address {
	out := make([]tcommon.Address, 0, m.size)
	for b := 0; b < m.cap; b++ {
		for e := m.tab[b]; e != nil; e = e.next {
			out = append(out, e.key)
		}
	}
	return out
}

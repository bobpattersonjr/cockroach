// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Bram Gruneir (bram.gruneir@gmail.com)

package storage

import (
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
)

type (
	// RangeTree represents holds the metadata about the RangeTree.  There is
	// only one for the whole cluster.
	RangeTree proto.RangeTree
	// RangeTreeNode represents a node in the RangeTree, each range
	// has only a single node.
	RangeTreeNode proto.RangeTreeNode
)

// TODO(bram): Add parent keys
// TODO(bram): see if we can limit the times we set nodes (db writes)
// TODO(bram): Add delete for merging nodes.

// set saves the RangeTree node to the db.
func (n *RangeTreeNode) set(batch engine.Engine, ms *engine.MVCCStats, timestamp proto.Timestamp) error {
	if err := engine.MVCCPutProto(batch, ms, engine.RangeTreeNodeKey(n.Key), timestamp, nil, (*proto.RangeTreeNode)(n)); err != nil {
		return err
	}
	return nil
}

// set saves the RangeTreeRoot to the db.
func (t *RangeTree) set(batch engine.Engine, ms *engine.MVCCStats, timestamp proto.Timestamp) error {
	if err := engine.MVCCPutProto(batch, ms, engine.KeyRangeTreeRoot, timestamp, nil, (*proto.RangeTree)(t)); err != nil {
		return err
	}
	return nil
}

// SetupRangeTree creates a new RangeTree.  This should only be called as part of
// store.BootstrapRange.
func SetupRangeTree(batch engine.Engine, ms *engine.MVCCStats, timestamp proto.Timestamp, startKey proto.Key) error {
	tree := (*RangeTree)(&proto.RangeTree{
		RootKey: startKey,
	})
	node := (*RangeTreeNode)(&proto.RangeTreeNode{
		Key:   startKey,
		Black: true,
	})
	if err := node.set(batch, ms, timestamp); err != nil {
		return err
	}
	if err := tree.set(batch, ms, timestamp); err != nil {
		return err
	}
	return nil
}

// GetRangeTree fetches the RangeTree proto.
func GetRangeTree(batch engine.Engine, timestamp proto.Timestamp) (*RangeTree, error) {
	tree := &proto.RangeTree{}
	ok, err := engine.MVCCGetProto(batch, engine.KeyRangeTreeRoot, timestamp, nil, tree)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, util.Errorf("Could not find the range tree:%s", engine.KeyRangeTreeRoot)
	}
	return (*RangeTree)(tree), nil
}

// getRangeTreeNode return the RangeTree node for the given key.  If the key is
// nil, an empty RangeTreeNode is returned.
func getRangeTreeNode(batch engine.Engine, timestamp proto.Timestamp, key *proto.Key) (*RangeTreeNode, error) {
	node := &proto.RangeTreeNode{}
	if key == nil {
		return (*RangeTreeNode)(node), nil
	}
	ok, err := engine.MVCCGetProto(batch, engine.RangeTreeNodeKey(*key), timestamp, nil, node)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, util.Errorf("Could not find the range tree node:%s",
			engine.RangeTreeNodeKey(*key))
	}
	return (*RangeTreeNode)(node), nil
}

// getRootNode returns the RangeTree node for the root of the RangeTree.
func (t *RangeTree) getRootNode(batch engine.Engine, timestamp proto.Timestamp) (*RangeTreeNode, error) {
	return getRangeTreeNode(batch, timestamp, &t.RootKey)
}

// InsertRange adds a new range to the RangeTree.  This should only be called
// from operations that create new ranges, such as range.splitTrigger.
func (t *RangeTree) InsertRange(batch engine.Engine, ms *engine.MVCCStats, timestamp proto.Timestamp, key proto.Key) error {
	root, err := t.getRootNode(batch, timestamp)
	if err != nil {
		return err
	}
	updatedRootNode, err := t.insert(batch, ms, timestamp, root, key)
	if err != nil {
		return err
	}
	if t.RootKey.Equal(updatedRootNode.Key) {
		t.RootKey = updatedRootNode.Key
		if err = t.set(batch, ms, timestamp); err != nil {
			return err
		}
	}
	return nil
}

// insert performs the insertion of a new range into the RangeTree.  It will
// recursivly call insert until it finds the correct location.  It will not
// overwite an already existing key, but that case should not occur.
func (t *RangeTree) insert(batch engine.Engine, ms *engine.MVCCStats, timestamp proto.Timestamp, node *RangeTreeNode, key proto.Key) (*RangeTreeNode, error) {
	if node.Key == nil {
		// Insert the new node here.
		node.Key = key
		if err := node.set(batch, ms, timestamp); err != nil {
			return nil, err
		}
	} else if key.Less(node.Key) {
		// Walk down the tree to the left.
		left, err := getRangeTreeNode(batch, timestamp, node.LeftKey)
		if err != nil {
			return nil, err
		}
		left, err = t.insert(batch, ms, timestamp, left, key)
		if err != nil {
			return nil, err
		}
		if node.LeftKey == nil || (*node.LeftKey).Equal(left.Key) {
			node.LeftKey = &left.Key
			if err := node.set(batch, ms, timestamp); err != nil {
				return nil, err
			}
		}
	} else {
		// Walk down the tree to the right.
		right, err := getRangeTreeNode(batch, timestamp, node.RightKey)
		if err != nil {
			return nil, err
		}
		right, err = t.insert(batch, ms, timestamp, right, key)
		if err != nil {
			return nil, err
		}
		if node.RightKey == nil || (*node.RightKey).Equal(right.Key) {
			node.RightKey = &right.Key
			if err := node.set(batch, ms, timestamp); err != nil {
				return nil, err
			}
		}
	}
	return node.walkUpRot23(batch, ms, timestamp)
}

// isRed will return true only if n exists and is not set to black.
func (n *RangeTreeNode) isRed() bool {
	if n == nil {
		return false
	}
	return !n.Black
}

// walkUpRot23 walks up and rotates the tree using the Left Leaning Red
// Black 2-3 algorithm.
func (n *RangeTreeNode) walkUpRot23(batch engine.Engine, ms *engine.MVCCStats, timestamp proto.Timestamp) (*RangeTreeNode, error) {
	// Should we rotate left?
	right, err := getRangeTreeNode(batch, timestamp, n.RightKey)
	if err != nil {
		return nil, err
	}
	left, err := getRangeTreeNode(batch, timestamp, n.LeftKey)
	if err != nil {
		return nil, err
	}
	if right.isRed() && !left.isRed() {
		n, err = n.rotateLeft(batch, ms, timestamp)
		if err != nil {
			return nil, err
		}
	}

	// Should we rotate right?
	left, err = getRangeTreeNode(batch, timestamp, n.LeftKey)
	if err != nil {
		return nil, err
	}
	leftLeft, err := getRangeTreeNode(batch, timestamp, left.LeftKey)
	if err != nil {
		return nil, err
	}
	if left.isRed() && leftLeft.isRed() {
		n, err = n.rotateRight(batch, ms, timestamp)
		if err != nil {
			return nil, err
		}
	}

	// Should we flip?
	right, err = getRangeTreeNode(batch, timestamp, n.RightKey)
	if err != nil {
		return nil, err
	}
	left, err = getRangeTreeNode(batch, timestamp, n.LeftKey)
	if err != nil {
		return nil, err
	}
	if left.isRed() && right.isRed() {
		n, err = n.flip(batch, ms, timestamp)
		if err != nil {
			return nil, err
		}
	}
	return n, nil
}

// rotateLeft performs a left rotation around the node n.
func (n *RangeTreeNode) rotateLeft(batch engine.Engine, ms *engine.MVCCStats, timestamp proto.Timestamp) (*RangeTreeNode, error) {
	right, err := getRangeTreeNode(batch, timestamp, n.RightKey)
	if err != nil {
		return nil, err
	}
	if right.Black {
		return nil, util.Error("rotating a black node")
	}
	n.RightKey = right.LeftKey
	right.LeftKey = &n.Key
	right.Black = n.Black
	n.Black = false
	if err = n.set(batch, ms, timestamp); err != nil {
		return nil, err
	}
	if err = right.set(batch, ms, timestamp); err != nil {
		return nil, err
	}
	return n, nil
}

// rotateRight performs a right rotation around the node n.
func (n *RangeTreeNode) rotateRight(batch engine.Engine, ms *engine.MVCCStats, timestamp proto.Timestamp) (*RangeTreeNode, error) {
	left, err := getRangeTreeNode(batch, timestamp, n.LeftKey)
	if err != nil {
		return nil, err
	}
	if left.Black {
		return nil, util.Error("rotating a black node")
	}
	n.LeftKey = left.RightKey
	left.RightKey = &n.Key
	left.Black = n.Black
	n.Black = false
	if err = n.set(batch, ms, timestamp); err != nil {
		return nil, err
	}
	if err = left.set(batch, ms, timestamp); err != nil {
		return nil, err
	}
	return n, nil
}

// flip swaps the color of the node n and both of its children.  Both those
// children must exist.
func (n *RangeTreeNode) flip(batch engine.Engine, ms *engine.MVCCStats, timestamp proto.Timestamp) (*RangeTreeNode, error) {
	left, err := getRangeTreeNode(batch, timestamp, n.LeftKey)
	if err != nil {
		return nil, err
	}
	right, err := getRangeTreeNode(batch, timestamp, n.RightKey)
	if err != nil {
		return nil, err
	}
	n.Black = !n.Black
	left.Black = !left.Black
	right.Black = !right.Black
	if err = n.set(batch, ms, timestamp); err != nil {
		return nil, err
	}
	if err = left.set(batch, ms, timestamp); err != nil {
		return nil, err
	}
	if err = right.set(batch, ms, timestamp); err != nil {
		return nil, err
	}
	return n, nil
}

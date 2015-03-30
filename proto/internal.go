// Copyright 2014 The Cockroach Authors.
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
// Author: Spencer Kimball (spencer.kimball@gmail.com)
// Author: Matt Tracy (matt@cockroachlabs.com)

package proto

import (
	"container/heap"
	"sort"

	"github.com/cockroachdb/cockroach/util"
	gogoproto "github.com/gogo/protobuf/proto"
)

const (
	// InternalRangeLookup looks up range descriptors, containing the
	// locations of replicas for the range containing the specified key.
	InternalRangeLookup = "InternalRangeLookup"
	// InternalHeartbeatTxn sends a periodic heartbeat to extant
	// transaction rows to indicate the client is still alive and
	// the transaction should not be considered abandoned.
	InternalHeartbeatTxn = "InternalHeartbeatTxn"
	// InternalGC garbage collects values based on expired timestamps
	// for a list of keys in a range. This method is called by the
	// range leader after a snapshot scan. The call goes through Raft,
	// so all range replicas GC the exact same values.
	InternalGC = "InternalGC"
	// InternalPushTxn attempts to resolve read or write conflicts between
	// transactions. Both the pusher (args.Txn) and the pushee
	// (args.PushTxn) are supplied. However, args.Key should be set to the
	// transaction ID of the pushee, as it must be directed to the range
	// containing the pushee's transaction record in order to consult the
	// most up to date txn state. If the conflict resolution can be
	// resolved in favor of the pusher, returns success; otherwise returns
	// an error code either indicating the pusher must retry or abort and
	// restart the transaction.
	InternalPushTxn = "InternalPushTxn"
	// InternalResolveIntent resolves existing write intents for a key or
	// key range.
	InternalResolveIntent = "InternalResolveIntent"
	// InternalMerge merges a given value into the specified key. Merge is a
	// high-performance operation provided by underlying data storage for values
	// which are accumulated over several writes. Because it is not
	// transactional, Merge is currently not made available to external clients.
	//
	// The logic used to merge values of different types is described in more
	// detail by the "Merge" method of engine.Engine.
	InternalMerge = "InternalMerge"
	// InternalTruncateLog discards a prefix of the raft log.
	InternalTruncateLog = "InternalTruncateLog"
)

// ToValue generates a Value message which contains an encoded copy of this
// TimeSeriesData in its "bytes" field. The returned Value will also have its
// "tag" string set to the TIME_SERIES constant.
func (ts *InternalTimeSeriesData) ToValue() (*Value, error) {
	b, err := gogoproto.Marshal(ts)
	if err != nil {
		return nil, err
	}
	return &Value{
		Bytes: b,
		Tag:   gogoproto.String(_CR_TS.String()),
	}, nil
}

// InternalTimeSeriesDataFromValue attempts to extract an InternalTimeSeriesData
// message from the "bytes" field of the given value.
func InternalTimeSeriesDataFromValue(value *Value) (*InternalTimeSeriesData, error) {
	if value.GetTag() != _CR_TS.String() {
		return nil, util.Errorf("value is not tagged as containing TimeSeriesData: %v", value)
	}
	var ts InternalTimeSeriesData
	err := gogoproto.Unmarshal(value.Bytes, &ts)
	if err != nil {
		return nil, util.Errorf("TimeSeriesData could not be unmarshalled from value: %v %s", value, err)
	}
	return &ts, nil
}

// AccumulateFrom accumulates values from the supplied InternalTimeSeriesSample
// into the existing values of the destination InternalTimeSeriesSample.
func (dest *InternalTimeSeriesSample) AccumulateFrom(src *InternalTimeSeriesSample) {
	// Accumulate int values
	dest.IntCount += src.IntCount
	if dest.IntCount > 1 {
		// Keep explicit Max and Min value. This is complicated because these
		// can be nil in the original messages, in which case the Sum is both the
		// min and the max.
		valOrDefault := func(maybeVal *int64, defaultVal int64) int64 {
			if maybeVal != nil {
				return *maybeVal
			}
			return defaultVal
		}
		var (
			destMax = valOrDefault(dest.IntMax, dest.GetIntSum())
			destMin = valOrDefault(dest.IntMax, dest.GetIntSum())
			srcMax  = valOrDefault(src.IntMin, src.GetIntSum())
			srcMin  = valOrDefault(src.IntMin, src.GetIntSum())
		)
		if destMax >= srcMax {
			dest.IntMax = gogoproto.Int64(destMax)
		} else {
			dest.IntMax = gogoproto.Int64(srcMax)
		}
		if destMin <= srcMin {
			dest.IntMin = gogoproto.Int64(destMin)
		} else {
			dest.IntMin = gogoproto.Int64(srcMin)
		}
	}
	if dest.IntCount > 0 {
		dest.IntSum = gogoproto.Int64(dest.GetIntSum() + src.GetIntSum())
	}

	// Accumulate float values
	dest.FloatCount += src.FloatCount
	if dest.FloatCount > 1 {
		// Keep explicit Max and Min value. This is complicated because these
		// can be nil in the original messages, in which case the Sum is both the
		// min and the max.
		valOrDefault := func(maybeVal *float32, defaultVal float32) float32 {
			if maybeVal != nil {
				return *maybeVal
			}
			return defaultVal
		}
		var (
			destMax = valOrDefault(dest.FloatMax, dest.GetFloatSum())
			destMin = valOrDefault(dest.FloatMax, dest.GetFloatSum())
			srcMax  = valOrDefault(src.FloatMin, src.GetFloatSum())
			srcMin  = valOrDefault(src.FloatMin, src.GetFloatSum())
		)
		if destMax >= srcMax {
			dest.FloatMax = gogoproto.Float32(destMax)
		} else {
			dest.FloatMax = gogoproto.Float32(srcMax)
		}
		if destMin <= srcMin {
			dest.FloatMin = gogoproto.Float32(destMin)
		} else {
			dest.FloatMin = gogoproto.Float32(srcMin)
		}
	}
	if dest.FloatCount > 0 {
		dest.FloatSum = gogoproto.Float32(dest.GetFloatSum() + src.GetFloatSum())
	}
}

// byOffset implements sort.Interface for slices of InternalTimeSeriesSample,
// describing how to sort by offset.
type byOffset []*InternalTimeSeriesSample

func (a byOffset) Len() int           { return len(a) }
func (a byOffset) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byOffset) Less(i, j int) bool { return a[i].Offset < a[j].Offset }

// sampleSliceHeap implements a min heap for sorted slices of
// InternalTimeSeriesSample. The considered value is the offset of the first
// element of each slice.
type sampleSliceHeap [][]*InternalTimeSeriesSample

func (h sampleSliceHeap) Len() int           { return len(h) }
func (h sampleSliceHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h sampleSliceHeap) Less(i, j int) bool { return h[i][0].Offset < h[j][0].Offset }
func (h *sampleSliceHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.([]*InternalTimeSeriesSample))
}

func (h *sampleSliceHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// MergeInternalTimeSeriesData merges InternalTimeSeriesData messages. The
// result of this merge should be the same as the RocksDB Merge operator which
// normally combines these values inside of the engine.
//
// The resulting InternalTimeSeriesData will have at most one sample per offset
// value; that is, if any of the source data sets both contained a sample with the
// same offset, then the merged set will have a single sample for that offset
// with the accumulated values from the original samples. This also applies
// if any of the original data sets by itself contained multiple samples with the
// same offset; they would be accumulated into a single sample in the result.
//
// All source InternalTimeSeriesData objects must have the same start
// timestamp and sample duration, or an error will be returned.
//
// In the interest of optimization, this method may modify the source
// InternalTimeSeriesData objects provided by sorting their sample collections.
func MergeInternalTimeSeriesData(sources ...*InternalTimeSeriesData) (*InternalTimeSeriesData, error) {
	var output *InternalTimeSeriesData

	// Validate that each supplied source is valid, and that all supplied
	// sources have the same start timestamp and sample duration.
	for _, src := range sources {
		if src == nil {
			return nil, util.Errorf("MergeInternalTimeSeriesData was called with a nil source.")
		}
		if output == nil {
			output = &InternalTimeSeriesData{
				StartTimestampNanos: src.StartTimestampNanos,
				SampleDurationNanos: src.SampleDurationNanos,
			}
		} else {
			if a, b := src.StartTimestampNanos, output.StartTimestampNanos; a != b {
				return nil, util.Errorf(
					"source messages to be merged have different start timestamps: %d != %d",
					a, b)
			}
			if a, b := src.SampleDurationNanos, output.SampleDurationNanos; a != b {
				return nil, util.Errorf(
					"source messages to be merged have different sample durations: %d != %d",
					a, b)
			}
		}
	}

	// Sort samples in each source so that they can be merged. Place each slice
	// into a min heap, which is used to merge the values together in sorted
	// order.
	sourceSamples := make(sampleSliceHeap, 0, len(sources))
	for _, src := range sources {
		if len(src.Samples) == 0 {
			continue
		}
		sort.Sort(byOffset(src.Samples))
		sourceSamples = append(sourceSamples, src.Samples)
	}

	heap.Init(&sourceSamples)
	for len(sourceSamples) > 0 {
		// Pop the next sample source off the heap. This will be the source with
		// the next lowest offset.
		nextSourceSlice := heap.Pop(&sourceSamples).([]*InternalTimeSeriesSample)
		nextSource := nextSourceSlice[0]

		// Based on previously merged samples, we are either adding a new sample
		// to the output or we are accumulating data into the last sample added.
		var nextMerged *InternalTimeSeriesSample
		if len(output.Samples) == 0 ||
			nextSource.Offset != output.Samples[len(output.Samples)-1].Offset {
			nextMerged = &InternalTimeSeriesSample{
				Offset: nextSource.Offset,
			}
			output.Samples = append(output.Samples, nextMerged)
		} else {
			nextMerged = output.Samples[len(output.Samples)-1]
		}
		nextMerged.AccumulateFrom(nextSource)

		// If nextSource has additional elements, push it back onto the heap
		// after removing its first element.
		if len(nextSourceSlice) > 1 {
			heap.Push(&sourceSamples, nextSourceSlice[1:])
		}
	}

	return output, nil
}

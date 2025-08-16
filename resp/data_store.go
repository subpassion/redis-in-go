package resp

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

type DataStore struct {
	db      map[string]*RespValue
	mu      sync.Mutex
	list_ch chan string
}

func CreatedDataStore() DataStore {
	return DataStore{db: make(map[string]*RespValue), list_ch: make(chan string, 1)}
}

func (data_store *DataStore) Get(key string) (*RespValue, bool) {
	data_store.mu.Lock()
	defer data_store.mu.Unlock()

	var resp_value, exist = data_store.db[key]
	return resp_value, exist
}

func (data_store *DataStore) Set(key string, value *RespValue) {
	data_store.mu.Lock()
	defer data_store.mu.Unlock()
	data_store.db[key] = value
}

func (data_store *DataStore) Delete(key string) {
	data_store.mu.Lock()
	defer data_store.mu.Unlock()
	delete(data_store.db, key)
}

func (data_store *DataStore) NotifyListUpdated(list_name string) {
	select {
	case data_store.list_ch <- list_name:
	default:
	}
}

func (data_store *DataStore) WaitForListToBeUpdate(timeout time.Duration) (string, error) {
	if timeout == 0 {
		return <-data_store.list_ch, nil
	}

	select {
	case list_name := <-data_store.list_ch:
		return list_name, nil
	case <-time.After(timeout):
		return "", fmt.Errorf("timeout occurred")
	}
}

type StreamEntries struct {
	entries map[string]*RespValue // TODO: probably other data structure can be used here
}

// TODO: use different data structure
type StreamStore struct {
	stream         map[StreamId]StreamEntries
	last_stream_id StreamId
}

type StreamId struct {
	ms  uint64
	seq uint64
}

func (stream_id StreamId) ToString() string {
	return fmt.Sprintf("%d-%d", stream_id.ms, stream_id.seq)
}

func (stream_id StreamId) IsValid() bool {
	return stream_id.ms != 0 || stream_id.seq != 0
}

func FromString(stream_id string) (StreamId, error) {
	var parts = strings.Split(stream_id, "-")
	var ms, ms_err = strconv.ParseUint(parts[0], 10, 64)
	if ms_err != nil {
		return StreamId{}, fmt.Errorf("failed to parse millisecond part in the stream id")
	}

	var seq uint64 = 0
	if len(parts) == 2 {
		var seq_parsed, seq_err = strconv.ParseUint(parts[1], 10, 64)
		if seq_err != nil {
			return StreamId{}, fmt.Errorf("failed to parse sequence part in the stream id")
		}
		seq = seq_parsed
	}

	return StreamId{ms: ms, seq: seq}, nil
}

func parse_stream_id_for_xadd(stream_id string, last_stream_id StreamId) (StreamId, error) {
	if stream_id == "*" {
		var ms = time.Now().UnixMilli()
		var seq uint64 = 0
		if last_stream_id.seq != 0 {
			seq = last_stream_id.seq + 1
		}
		return StreamId{ms: uint64(ms), seq: seq}, nil
	}

	var parts = strings.Split(stream_id, "-")
	if len(parts) != 2 {
		return StreamId{}, fmt.Errorf("failed to parse the stream id key: %s", strconv.Quote(stream_id))
	}

	var ms, ms_err = strconv.ParseUint(parts[0], 10, 64)
	if ms_err != nil {
		return StreamId{}, fmt.Errorf("failed to parse millisecond part in the stream id")
	}

	var seq uint64 = 0
	if parts[1] == "*" {
		if ms == 0 && !last_stream_id.IsValid() {
			seq = 1
		} else if last_stream_id.ms == ms {
			seq = last_stream_id.seq + 1
		}
	} else {
		var seq_err error
		seq, seq_err = strconv.ParseUint(parts[1], 10, 64)
		if seq_err != nil {
			return StreamId{}, fmt.Errorf("failed to parse sequence part in the stream id")
		}
	}

	return StreamId{ms: ms, seq: seq}, nil
}

func (stream_store *StreamStore) ParseStreamIdForXadd(stream_id string) (StreamId, error) {
	var current_stream_id, current_entry_err = parse_stream_id_for_xadd(stream_id, stream_store.last_stream_id)
	if current_entry_err != nil {
		return StreamId{}, current_entry_err
	}

	if len(stream_store.stream) != 0 {
		if current_stream_id.ms == 0 && current_stream_id.seq == 0 {
			return StreamId{}, fmt.Errorf("ERR The ID specified in XADD must be greater than 0-0")
		}

		if current_stream_id.ms < stream_store.last_stream_id.ms ||
			(current_stream_id.ms == stream_store.last_stream_id.ms && current_stream_id.seq <= stream_store.last_stream_id.seq) {
			return StreamId{}, fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}
	}

	stream_store.last_stream_id = current_stream_id
	return current_stream_id, nil
}

func (stream_store *StreamStore) ParseIncompleteStreamId(stream_id string, seq_number uint64) (StreamId, error) {
	if stream_id == "-" {
		return StreamId{ms: 0, seq: 1}, nil
	}
	if stream_id == "+" {
		return StreamId{ms: math.MaxUint64, seq: math.MaxUint64}, nil
	}

	var stream_id_parts = strings.Split(stream_id, "-")
	if len(stream_id_parts) == 2 {
		return FromString(stream_id)
	}

	if len(stream_id_parts) == 1 {
		var ms, err = strconv.ParseUint(stream_id, 10, 64)
		if err != nil {
			return StreamId{}, fmt.Errorf("ERR failed to parse stream id in xrange: %s", stream_id)
		}
		return StreamId{ms: ms, seq: seq_number}, nil
	}
	return StreamId{}, fmt.Errorf("ERR invalid stream id: %s", stream_id)
}

func (stream_store *StreamStore) AddToStream(stream_id StreamId, entry_key string, entry_value *RespValue) {
	stream_entry, exists := stream_store.stream[stream_id]
	if !exists {
		stream_entry = StreamEntries{entries: make(map[string]*RespValue)}
	}

	stream_entry.entries[entry_key] = entry_value
	stream_store.stream[stream_id] = stream_entry
}

func get_sorted_stream_ids(stream *map[StreamId]StreamEntries) []StreamId {
	var order_stream_ids = make([]StreamId, len(*stream))
	var i = 0
	for key := range *stream {
		order_stream_ids[i] = key
		i++
	}

	// TODO: map doesn't sort keys :(, so I didn't come up with anything better than this,
	// Should be removed once a better data structure will be used
	slices.SortFunc(order_stream_ids, func(a, b StreamId) int {
		if a.ms != b.ms {
			return int(a.ms - b.ms)
		}
		return int(a.seq - b.seq)
	})
	return order_stream_ids
}

func (stream_store *StreamStore) GetEntries(stream_id_begin StreamId, stream_id_end StreamId) RespValue {
	var order_stream_ids = get_sorted_stream_ids(&stream_store.stream)
	var result = RespValue{Type: Array, Arr: make([]RespValue, 0)}
	for _, stream_id := range order_stream_ids {
		if (stream_id.ms >= stream_id_begin.ms && stream_id.ms <= stream_id_end.ms) &&
			(stream_id.seq >= stream_id_begin.seq && stream_id.seq <= stream_id_end.seq) {
			var stream_entry_result = RespValue{Type: Array, Arr: make([]RespValue, 0)}
			stream_entry_result.Arr = append(stream_entry_result.Arr, RespValue{Type: BulkString, Str: stream_id.ToString()})

			var entries_result = RespValue{Type: Array, Arr: make([]RespValue, 0)}
			var entries = stream_store.stream[stream_id].entries
			for entry_key, entry_value := range entries {
				entries_result.Arr = append(entries_result.Arr, RespValue{Type: BulkString, Str: entry_key}, *entry_value)
			}

			stream_entry_result.Arr = append(stream_entry_result.Arr, entries_result)
			result.Arr = append(result.Arr, stream_entry_result)
		}
	}
	return result
}

func (stream_store *StreamStore) GetEntriesXRead(stream_key string, stream_id StreamId) RespValue {
	var order_stream_ids = get_sorted_stream_ids(&stream_store.stream)
	var result_arr = make([]RespValue, 0)
	var entry_arr = RespValue{Type: Array, Arr: make([]RespValue, 0)}

	for _, stored_stream_id := range order_stream_ids {
		if stored_stream_id.ms > stream_id.ms || (stored_stream_id.ms == stream_id.ms && stored_stream_id.seq > stream_id.seq) {
			var stream_entry_result = RespValue{Type: Array, Arr: make([]RespValue, 0)}
			stream_entry_result.Arr = append(stream_entry_result.Arr, RespValue{Type: BulkString, Str: stored_stream_id.ToString()})

			var entries_result = RespValue{Type: Array, Arr: make([]RespValue, 0)}
			var entries = stream_store.stream[stored_stream_id].entries
			for entry_key, entry_value := range entries {
				entries_result.Arr = append(entries_result.Arr, RespValue{Type: BulkString, Str: entry_key}, *entry_value)
			}

			stream_entry_result.Arr = append(stream_entry_result.Arr, entries_result)
			entry_arr.Arr = append(entry_arr.Arr, stream_entry_result)
			result_arr = append(result_arr, entry_arr)
		}
	}

	var result = RespValue{Type: Array, Arr: make([]RespValue, 0)}
	if len(result_arr) != 0 {
		result.Arr = append(result.Arr, RespValue{Type: BulkString, Str: stream_key})
		result.Arr = append(result.Arr, result_arr...)
	}

	return result
}

func CreateStreamStore() StreamStore {
	return StreamStore{stream: make(map[StreamId]StreamEntries), last_stream_id: StreamId{ms: 0, seq: 0}}
}

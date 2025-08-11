package resp

import (
	"fmt"
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

type StreamStore struct {
	stream  map[string]StreamEntries
	last_id string
}

func parse_entry_key(entry_key string) (int, int, error) {
	var parts = strings.Split(entry_key, "-")
	if len(parts) != 2 {
		return -1, -1, fmt.Errorf("failed to parse the stream id key: %s", strconv.Quote(entry_key))
	}

	var ms_time, ms_err = strconv.Atoi(parts[0])
	if ms_err != nil {
		return -1, -1, fmt.Errorf("failed to parse millisecond part in the stream id")
	}

	var sequence_number, sequence_err = strconv.Atoi(parts[1])
	if sequence_err != nil {
		return -1, -1, fmt.Errorf("failed to pares sequence part in the stream id")
	}

	return ms_time, sequence_number, nil
}

func (stream_store *StreamStore) AddToStream(stream_id string, entry_key string, entry_value *RespValue) error {
	stream_entry, exists := stream_store.stream[stream_id]
	if !exists {
		stream_entry = StreamEntries{entries: make(map[string]*RespValue)}
		stream_store.stream[stream_id] = stream_entry
	}

	if stream_store.last_id != "" {
		var last_entry_ms, last_entry_sq, last_entry_err = parse_entry_key(stream_store.last_id)
		if last_entry_err != nil {
			return last_entry_err
		}

		var current_entry_ms, current_entry_sq, current_entry_err = parse_entry_key(stream_id)
		if current_entry_err != nil {
			return current_entry_err
		}

		if current_entry_ms == 0 && current_entry_sq == 0 {
			return fmt.Errorf("ERR The ID specified in XADD must be greater than 0-0")
		}

		if current_entry_ms < last_entry_ms ||
			(current_entry_ms == last_entry_ms && current_entry_sq <= last_entry_sq) {
			return fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}
	}

	stream_entry.entries[entry_key] = entry_value

	stream_store.stream[stream_id] = stream_entry
	stream_store.last_id = stream_id

	return nil
}

func CreateStreamStore() StreamStore {
	return StreamStore{stream: make(map[string]StreamEntries)}
}

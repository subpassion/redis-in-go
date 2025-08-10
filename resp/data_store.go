package resp

import (
	"fmt"
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

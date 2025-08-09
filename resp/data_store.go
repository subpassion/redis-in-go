package resp

import "sync"

type DataStore struct {
	db map[string]*RespValue
	mu sync.Mutex
}

func (data_store *DataStore) Get(key string) (*RespValue, bool) {
	data_store.mu.Lock()
	defer data_store.mu.Unlock()
	if data_store.db == nil {
		return &RespValue{}, false
	}

	var resp_value, exist = data_store.db[key]
	return resp_value, exist
}

func (data_store *DataStore) Set(key string, value *RespValue) {
	data_store.mu.Lock()
	defer data_store.mu.Unlock()
	if data_store.db == nil {
		data_store.db = make(map[string]*RespValue)
	}
	data_store.db[key] = value
}

func (data_store *DataStore) Delete(key string) {
	data_store.mu.Lock()
	defer data_store.mu.Unlock()
	if data_store.db != nil {
		delete(data_store.db, key)
	}
}

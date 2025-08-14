package resp

import (
	"fmt"
	"log"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"
)

type RespType = int

const (
	SimpleString RespType = iota
	BulkString
	BulkNullString
	Integer
	Array
	Stream
	Err
)

type RespValue struct {
	Type   RespType
	Int    int64
	Str    string
	Arr    []RespValue
	Stream StreamStore
}

const (
	CRLF = "\r\n"
)

var DB_STORE DataStore = CreatedDataStore()

func FindCRLF(resp_data string, when string) (int, int, error) {
	var res = strings.Index(resp_data, CRLF)
	if res == -1 {
		return -1, -1, fmt.Errorf("couldn't find the %s. %s", strconv.Quote(CRLF), when)
	}
	return res, res + len(CRLF), nil
}

func GetType(value *RespValue) string {
	switch value.Type {
	case Array:
		return "list"
	case Stream:
		return "stream"
	case BulkString, SimpleString, BulkNullString:
		return "string"
	default:
		return "none"
	}
}

// find a index of key-pair value stored in array.
func get_value_index_with_key(resp_array []RespValue, key string) int {
	idx := slices.IndexFunc(resp_array, func(c RespValue) bool { return c.Type == BulkString && strings.ToLower(c.Str) == key })
	if idx == -1 || idx+1 == len(resp_array) {
		return -1
	}

	return idx + 1
}

func validate_resp_type(value *RespValue, expected_type RespType) error {
	if value.Type != expected_type {
		return fmt.Errorf("value was not of expected type. Expected: %d, got %d", expected_type, value.Type)
	}
	return nil
}

// TODO: use indexing rather than copying strings
func parse(resp_data string) (RespValue, string, error) {
	if len(resp_data) == 0 {
		return RespValue{}, "", fmt.Errorf("got an empty string")
	}

	var prefix = resp_data[0]
	resp_data, _ = strings.CutPrefix(resp_data, string(prefix))

	switch prefix {
	case '+':
		var end_of_string, crlf_ends, err = FindCRLF(resp_data, "Error occurred while parsing Simple String")
		if err != nil {
			return RespValue{}, "", err
		}
		return RespValue{Type: SimpleString, Str: resp_data[:end_of_string]}, resp_data[crlf_ends:], nil
	case ':':
		var int_ends, crlf_ends, err = FindCRLF(resp_data, "Error occurred while parsing Integer")
		if err != nil {
			return RespValue{}, "", err
		}

		var resp_int, parse_err = strconv.ParseInt(resp_data[:int_ends], 10, 64)
		if parse_err != nil {
			return RespValue{}, "", fmt.Errorf("couldn't pares RESP integer: %s", err)
		}
		return RespValue{Type: Integer, Int: resp_int}, resp_data[crlf_ends:], nil
	case '$':
		var bulk_string_length_ends, bulk_str_start, err = FindCRLF(resp_data, "error occurred when parsing Bulk String")
		if bulk_string_length_ends == -1 {
			return RespValue{}, "", err
		}

		var bulk_str_length, parse_error = strconv.Atoi(resp_data[:bulk_string_length_ends])
		if parse_error != nil {
			return RespValue{}, "", parse_error
		}

		var bulk_str_end = bulk_str_start + int(bulk_str_length)
		return RespValue{Type: BulkString, Str: resp_data[bulk_str_start:bulk_str_end]}, resp_data[bulk_str_end+len(CRLF):], nil
	case '*':
		var n_elements_ends, array_element_start, err = FindCRLF(resp_data, "error occurred while parsing array size.")
		if n_elements_ends == -1 {
			return RespValue{}, "", err
		}

		n_elements, err := strconv.Atoi(resp_data[:n_elements_ends])
		if err != nil {
			return RespValue{}, "", fmt.Errorf("couldn't determine array size: %s", err.Error())
		}

		var array_data = resp_data[array_element_start:]
		var resp_objects []RespValue
		for range n_elements {
			var resp_object, next, err = parse(array_data)
			if err != nil {
				return RespValue{}, "", err
			}

			array_data = next
			resp_objects = append(resp_objects, resp_object)
		}

		if len(resp_objects) != int(n_elements) {
			return RespValue{}, "", fmt.Errorf("number of parsed elements don't match number of expected elements. Expected %d, got %d", n_elements, len(resp_objects))
		}

		return RespValue{Type: Array, Arr: resp_objects}, array_data, nil
	default:
		return RespValue{}, "", fmt.Errorf("unknown command prefix: `%s`", string(prefix))
	}
}

func ParseResp(resp_data string) (RespValue, error) {
	var resp_value, left, err = parse(resp_data)
	if err != nil {
		return RespValue{}, err
	}
	if left != "" {
		return RespValue{}, fmt.Errorf("data was not completely parsed. Remaining %s", strconv.Quote(left))
	}
	return resp_value, nil
}

func Serialize(resp_value *RespValue) string {
	switch resp_value.Type {
	case SimpleString:
		return fmt.Sprintf("+%s%s", resp_value.Str, CRLF)
	case BulkString:
		return fmt.Sprintf("$%d%s%s%s", len(resp_value.Str), CRLF, resp_value.Str, CRLF)
	case BulkNullString:
		return fmt.Sprintf("$-1%s", CRLF)
	case Integer:
		return fmt.Sprintf(":%d%s", resp_value.Int, CRLF)
	case Err:
		return fmt.Sprintf("-%s%s", resp_value.Str, CRLF)
	case Array:
		var res = ""
		for _, val := range resp_value.Arr {
			res += Serialize(&val)
		}
		return fmt.Sprintf("*%d%s%s", len(resp_value.Arr), CRLF, res)
	default:
		panic(fmt.Errorf("Serialize is not implemented: %v", resp_value.Type))
	}
}

func HandleCommand(command_with_args *RespValue) (RespValue, error) {
	if command_with_args.Type != Array {
		return RespValue{}, fmt.Errorf("command was not encoded as RespArray")
	}

	if len(command_with_args.Arr) == 0 {
		return RespValue{}, fmt.Errorf("got an empty command")
	}

	// TODO: shall we just simply assume that customer will always send the bulk strings?
	for _, val := range command_with_args.Arr {
		if type_err := validate_resp_type(&val, BulkString); type_err != nil {
			return RespValue{}, type_err
		}
	}

	var command = strings.ToLower(command_with_args.Arr[0].Str)
	var arguments = command_with_args.Arr[1:]
	// TODO: handle errors properly
	switch command {
	case "ping":
		var pong = RespValue{Str: "PONG", Type: SimpleString}
		return pong, nil
	case "echo":
		if len(arguments) == 0 {
			return RespValue{Type: Err, Str: "ERR echo command expects 1 argument but got 0"}, nil
		}
		return arguments[0], nil
	case "get":
		if len(arguments) == 0 {
			return RespValue{Type: Err, Str: "ERR get expects one argument but got 1"}, nil
		}

		var resp_value, value_exists = DB_STORE.Get(arguments[0].Str)
		if !value_exists {
			return RespValue{Type: BulkNullString}, nil
		}

		return *resp_value, nil
	case "set":
		if len(arguments) < 2 {
			return RespValue{Type: Err, Str: fmt.Sprintf("ERR set expects at least 2 arguments but got :%d", len(arguments))}, nil
		}

		var key = arguments[0].Str
		var value = arguments[1]
		// TODO: check if the key already exits
		log.Printf("Set %s to %s", strconv.Quote(key), strconv.Quote(value.Str))
		DB_STORE.Set(key, &value)

		// TODO: create a normal command parser
		var px_idx = get_value_index_with_key(arguments, "px")
		if px_idx != -1 {
			var px_obj = arguments[px_idx]
			var px, px_parse_err = strconv.Atoi(px_obj.Str)
			if px_parse_err != nil {
				return RespValue{}, px_parse_err
			}

			var value_expired_timer = time.NewTimer(time.Duration(px) * time.Millisecond)
			go func() {
				<-value_expired_timer.C
				log.Printf("Deleting %s from the DB\n", strconv.Quote(key))
				DB_STORE.Delete(key)
			}()
		}

		return RespValue{Type: SimpleString, Str: "OK"}, nil
	case "rpush":
		if len(arguments) < 2 {
			return RespValue{Type: Err, Str: fmt.Sprintf("ERR rpush expects at least 2 arguments but got: %d", len(arguments))}, nil
		}

		var list_name = arguments[0].Str
		var list, list_exits = DB_STORE.Get(list_name)
		var list_elements = arguments[1:]
		if list_exits {
			list.Arr = append(list.Arr, list_elements...)
		} else {
			list = &RespValue{Type: Array, Arr: list_elements}
		}

		DB_STORE.Set(list_name, list)
		DB_STORE.NotifyListUpdated(list_name)
		var reply = RespValue{Type: Integer, Int: int64(len(list.Arr))}
		return reply, nil
	case "lpush":
		if len(arguments) < 2 {
			return RespValue{Type: Err, Str: fmt.Sprintf("ERR lpush expects at least 2 arguments but got: %d", len(arguments))}, nil
		}

		var list_name = arguments[0].Str
		var new_list = RespValue{Type: Array, Arr: make([]RespValue, len(arguments)-1)}
		for i, j := len(arguments)-1, 0; i > 0; i, j = i-1, j+1 {
			new_list.Arr[j] = arguments[i]
		}

		var old_list, old_list_exists = DB_STORE.Get(list_name)
		if old_list_exists {
			new_list.Arr = append(new_list.Arr, old_list.Arr...)
		}

		DB_STORE.Set(list_name, &new_list)
		DB_STORE.NotifyListUpdated(list_name)
		var reply = RespValue{Type: Integer, Int: int64(len(new_list.Arr))}
		return reply, nil
	case "lrange":
		if len(arguments) < 3 {
			return RespValue{Type: Err, Str: fmt.Sprintf("ERR lrange expects at least 3 arguments but got: %d", len(arguments))}, nil
		}

		var list_name = arguments[0].Str
		var list, list_exits = DB_STORE.Get(list_name)
		var empty_list = RespValue{Type: Array, Arr: []RespValue{}}
		if !list_exits {
			return empty_list, nil
		}

		var list_len = len(list.Arr)
		var from, from_err = strconv.Atoi(arguments[1].Str)
		if from_err != nil {
			return RespValue{}, fmt.Errorf("failed to parse start index")
		}

		if from < 0 {
			from = max(0, list_len+from)
		}

		var to, to_err = strconv.Atoi(arguments[2].Str)
		if to_err != nil {
			return RespValue{}, fmt.Errorf("failed to parse end index")
		}

		if to < 0 {
			to = max(0, list_len+to)
		}

		if from > to || from >= list_len {
			return empty_list, nil
		}

		var list_range = RespValue{Type: Array, Arr: list.Arr[from:min(to+1, list_len)]}
		return list_range, nil
	case "llen":
		if len(arguments) < 1 {
			return RespValue{Type: Err, Str: fmt.Sprintf("ERR llen expects at least 1 arguments but got: %d", len(arguments))}, nil
		}

		var list_name = arguments[0].Str
		var reply = RespValue{Type: Integer, Int: 0}
		if list, exits := DB_STORE.Get(list_name); exits {
			reply.Int = int64(len(list.Arr))
		}

		return reply, nil
	case "lpop":
		if len(arguments) < 1 {
			return RespValue{Type: Err, Str: fmt.Sprintf("ERR lpop expects at least 1 arguments but got: %d", len(arguments))}, nil
		}

		var list_name = arguments[0].Str
		var list, exits = DB_STORE.Get(list_name)
		if !exits || len(list.Arr) == 0 {
			return RespValue{Type: BulkNullString}, nil
		}

		var n_elem_to_remove = 1
		if len(arguments) == 2 {
			from_cmd_n_elem_to_remove, err := strconv.Atoi(arguments[1].Str)
			if err != nil {
				return RespValue{}, err
			}

			n_elem_to_remove = from_cmd_n_elem_to_remove
		}

		var removed = list.Arr[0:n_elem_to_remove]
		list.Arr = list.Arr[n_elem_to_remove:]
		DB_STORE.Set(list_name, list)

		if len(removed) > 1 {
			return RespValue{Type: Array, Arr: removed}, nil
		} else {
			return RespValue{Type: BulkString, Str: removed[0].Str}, nil
		}
	case "blpop":
		if len(arguments) < 2 {
			return RespValue{Type: Err, Str: fmt.Sprintf("ERR blpop expects at least 2 arguments but got: %d", len(arguments))}, nil
		}

		var list_name = arguments[0].Str
		var timeout, tm_err = strconv.ParseFloat(arguments[1].Str, 64)
		if tm_err != nil {
			return RespValue{}, fmt.Errorf("failed to parse timeout argument: %s", tm_err.Error())
		}

		var list, exits = DB_STORE.Get(list_name)
		if !exits || len(list.Arr) == 0 {
			var key, err = DB_STORE.WaitForListToBeUpdate(time.Duration(timeout * float64(time.Second)))
			if err != nil {
				return RespValue{Type: BulkNullString}, nil
			}

			list, _ = DB_STORE.Get(key)
		}

		var removed = list.Arr[0]
		list.Arr = list.Arr[1:]
		DB_STORE.Set(list_name, list)
		return RespValue{Type: Array, Arr: []RespValue{arguments[0], removed}}, nil
	case "type":
		if len(arguments) != 1 {
			return RespValue{Type: Err, Str: fmt.Sprintf("ERR type expects 1 arguments but got: %d", len(arguments))}, nil
		}

		var key = arguments[0].Str
		var reply = RespValue{Type: SimpleString, Str: "none"}
		if val, exists := DB_STORE.Get(key); exists {
			reply.Str = GetType(val)
		}

		return reply, nil
	case "xadd":
		if len(arguments) < 2 {
			return RespValue{Type: Err, Str: fmt.Sprintf("xadd expects at least 2 arguments but got :%d", len(arguments))}, nil
		}

		var stream_name = arguments[0].Str
		var stream_id = arguments[1].Str

		var stream_store, exists = DB_STORE.Get(stream_name)
		if !exists {
			stream_store = &RespValue{Type: Stream, Stream: CreateStreamStore()}
		}

		var processed_stream_id, err = stream_store.Stream.ParseStreamIdForXadd(stream_id)
		if err != nil {
			return RespValue{Type: Err, Str: err.Error()}, nil
		}

		for entry_idx := 2; entry_idx < len(arguments)-1; entry_idx += 2 {
			var entry_key = arguments[entry_idx]
			var entry_value = arguments[entry_idx+1]
			stream_store.Stream.AddToStream(processed_stream_id, entry_key.Str, &entry_value)
		}

		DB_STORE.Set(stream_name, &RespValue{Type: Stream, Stream: stream_store.Stream})
		return RespValue{Type: BulkString, Str: processed_stream_id.ToString()}, nil
	case "xrange":
		if len(arguments) < 3 {
			return RespValue{Type: Err, Str: fmt.Sprintf("xrange expects at least 3 arguments but got :%d", len(arguments))}, nil
		}

		var stream, stream_exists = DB_STORE.Get(arguments[0].Str)
		if !stream_exists {
			return RespValue{Type: Array, Arr: make([]RespValue, 0)}, nil
		}

		var stream_id_begin, stream_id_begin_err = stream.Stream.ParseIncompleteStreamId(arguments[1].Str, 0)
		if stream_id_begin_err != nil {
			return RespValue{Type: Err, Str: stream_id_begin_err.Error()}, nil
		}

		var stream_id_end, stream_id_end_err = stream.Stream.ParseIncompleteStreamId(arguments[2].Str, math.MaxUint64)
		if stream_id_end_err != nil {
			return RespValue{Type: Err, Str: stream_id_begin_err.Error()}, nil
		}

		var result = stream.Stream.GetEntries(stream_id_begin, stream_id_end)
		return result, nil
	case "xread":
		if len(arguments) < 3 {
			return RespValue{Type: Err, Str: fmt.Sprintf("xread expects at least 3 arguments but got :%d", len(arguments))}, nil
		}

		var stream_key_start = slices.IndexFunc(arguments, func(c RespValue) bool { return strings.ToLower(c.Str) == "streams" })
		if stream_key_start == -1 {
			return RespValue{Type: Err, Str: "ERR missing required `streams` argument"}, nil
		} else {
			stream_key_start += 1
		}

		var n_stream_keys = (len(arguments) - stream_key_start) / 2
		var n_stream_ids = (len(arguments) - (stream_key_start + n_stream_keys))
		if n_stream_keys != n_stream_ids {
			return RespValue{Type: Err, Str: "ERR number of stream keys don't match number of stream ids"}, nil
		}

		var result = RespValue{Type: Array, Arr: make([]RespValue, 0)}
		for stream_key_idx := stream_key_start; stream_key_idx <= n_stream_keys; stream_key_idx++ {
			var stream_key = arguments[stream_key_idx].Str
			var stream, stream_exists = DB_STORE.Get(stream_key)
			if !stream_exists {
				return RespValue{Type: Array, Arr: make([]RespValue, 0)}, nil
			}

			var stream_id_idx = stream_key_start + n_stream_keys
			var stream_id, stream_id_begin_err = stream.Stream.ParseIncompleteStreamId(arguments[stream_id_idx].Str, 0)
			if stream_id_begin_err != nil {
				return RespValue{Type: Err, Str: stream_id_begin_err.Error()}, nil
			}

			result.Arr = append(result.Arr, stream.Stream.GetEntriesXRead(stream_key, stream_id))
		}

		return result, nil
	default:
		return RespValue{Type: Err, Str: fmt.Sprintf("ERR unknown command: %s", command)}, nil
	}
}

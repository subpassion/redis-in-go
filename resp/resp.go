package resp

import (
	"fmt"
	"log"
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
)

type RespValue struct {
	Type RespType
	Int  int64
	Str  string
	Arr  []RespValue
}

const (
	CRLF = "\r\n"
)

var DB_STORE DataStore

func FindCRLF(resp_data string, when string) (int, int, error) {
	var res = strings.Index(resp_data, CRLF)
	if res == -1 {
		return -1, -1, fmt.Errorf("couldn't find the %s. %s", strconv.Quote(CRLF), when)
	}
	return res, res + len(CRLF), nil
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

		var bulk_str_length, parse_error = strconv.ParseInt(resp_data[:bulk_string_length_ends], 10, 64)
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

		n_elements, err := strconv.ParseInt(resp_data[:n_elements_ends], 10, 64)
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

func HandleCommand(command_with_args *RespValue) (string, error) {
	if command_with_args.Type != Array {
		return "", fmt.Errorf("command was not encoded as RespArray")
	}

	if len(command_with_args.Arr) == 0 {
		return "", fmt.Errorf("got an empty command")
	}

	// TODO: shall we just simply assume that customer will always send the bulk strings?
	for _, val := range command_with_args.Arr {
		if type_err := validate_resp_type(&val, BulkString); type_err != nil {
			return "", type_err
		}
	}

	var command = strings.ToLower(command_with_args.Arr[0].Str)
	var arguments = command_with_args.Arr[1:]

	// TODO: handle errors properly
	switch command {
	case "ping":
		var pong = RespValue{Str: "PONG", Type: SimpleString}
		return Serialize(&pong), nil
	case "echo":
		if len(arguments) == 0 {
			return "", fmt.Errorf("echo command expects one argument but got 0")
		}
		return Serialize(&arguments[0]), nil
	case "get":
		if len(arguments) == 0 {
			return "", fmt.Errorf("get expects one argument but got 0")
		}

		var resp_value, value_exists = DB_STORE.Get(arguments[0].Str)
		if !value_exists {
			return Serialize(&RespValue{Type: BulkNullString}), nil
		}

		return Serialize(resp_value), nil
	case "set":
		if len(arguments) < 2 {
			return "", fmt.Errorf("set expects at least 2 arguments but got :%d", len(arguments))
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
			var px, px_parse_err = strconv.ParseInt(px_obj.Str, 10, 64)
			if px_parse_err != nil {
				return "", px_parse_err
			}

			var value_expired_timer = time.NewTimer(time.Duration(px) * time.Millisecond)
			go func() {
				<-value_expired_timer.C
				log.Printf("Deleting %s from the DB\n", strconv.Quote(key))
				DB_STORE.Delete(key)
			}()
		}

		return Serialize(&RespValue{Type: SimpleString, Str: "OK"}), nil
	default:
		return "", fmt.Errorf("unknown command: %s", command)
	}
}

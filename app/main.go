package main

import (
	"log"
	"my-redis/resp"
	"net"
	"os"
)

func main() {
	conn_addr := "0.0.0.0:6379"
	l, err := net.Listen("tcp", conn_addr)
	if err != nil {
		log.Println("Failed to bind to", conn_addr, err.Error())
		os.Exit(1)
	}

	log.Println("Listening on:", conn_addr)
	for {
		con, err := l.Accept()
		if err != nil {
			log.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handle(con)
	}
}

func handle(con net.Conn) {
	defer func(con net.Conn) {
		err := con.Close()
		if err != nil {
			log.Println("Failed to close connection: ", err.Error())
			os.Exit(1)
		}
	}(con)

	data := make([]byte, 1024)
	for {
		n, err := con.Read(data)
		if err != nil {
			log.Print("Failed to read data:", err.Error())
			break
		}

		var resp_command, resp_err = resp.ParseResp(string(data[:n]))
		if resp_err != nil {
			log.Printf("Failed to parse resp request: %s\n", resp_err.Error())
			continue // TODO: what is the difference between abort and return
		}

		var resp_value, command_err = resp.HandleCommand(&resp_command)
		if command_err != nil {
			log.Printf("Failed to handle resp command due to internal error: %s\n", command_err.Error())
			continue
		}

		if resp_value.Type == resp.Err {
			log.Printf("%s\n", resp_value.Str)
		}

		_, err = con.Write([](byte)(resp.Serialize(&resp_value)))
		if err != nil {
			log.Printf("Filed to write PONG response:%s\n", err.Error())
			continue
		}
	}
}

package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

var (
	ErrUnknownCommand = errors.New("ошибка: неизвестная команда")
	ErrInvalidSyntax  = errors.New("ошибка: неверный синтаксис команды")
)

func main() {
	cache := NewMyCache(1 * time.Second)
	defer cache.Close()

	listener, err := net.Listen("tcp", ":12345")
	if err != nil {
		log.Fatalf("Не удалос запустить сервер: %v", err)
	}
	defer listener.Close()

	fmt.Println("Сервер кеша запущен на порту :12345...")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Ошибка при принятии соединения: %v", err)
			continue
		}
		go handleConnection(conn, cache)
	}
}

func handleConnection(conn net.Conn, cache Cache) {
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {
		err := conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		if err != nil {
			log.Printf("Не удалось установить дедлайн: %v", err)
			return
		}

		request, err := reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) {
				log.Printf("Клиент %s отключён по таймауту( не присылал данные 30 секунд )", conn.RemoteAddr())
				fmt.Fprintf(conn, "Ошибка: превышено время ожидания(Таймаут 30 секунд)")
				return
			}
			log.Printf("Клиент %s разорвал соедиение", conn.RemoteAddr())
			return

		}
		request = strings.TrimSpace(request)

		response, err := processCommand(request, cache)
		if err != nil {
			fmt.Fprintf(conn, "%v\n", err)
			continue
		}

		_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))

		fmt.Fprintf(conn, "%s\n", response)
	}
}

func processCommand(req string, cache Cache) (string, error) {
	parts := strings.Fields(req)
	if len(parts) == 0 {
		return "", ErrInvalidSyntax
	}

	command := strings.ToUpper(parts[0])

	switch command {
	case "SET":
		// SET key value
		if len(parts) < 3 {
			return "", fmt.Errorf("%w: используйте 'SET <key> <value>'", ErrInvalidSyntax)
		}
		key := parts[1]
		value := parts[2]
		cache.Set(key, value)
		return "OK", nil
	case "GET":
		if len(parts) < 2 {
			return "", fmt.Errorf("%w: используйте 'GET <key>'", ErrInvalidSyntax)
		}
		key := parts[1]
		val, ok := cache.Get(key)
		if !ok {
			return "(nil)", nil
		}
		log.Printf("[DEBUG] Найдено в кэше: значение='%v', тип=%T", val, val)
		strVal, ok := val.(string)
		if !ok {
			return "", fmt.Errorf("ошибка: внутренний тип данных не является строкой")
		}
		return strVal, nil
		// return val.(string), nil
	case "FLUSH":
		cache.Flush()
		return "Данные очищены", nil
	case "DEL":
		// DEL <key>
		if len(parts) < 2 {
			return "", fmt.Errorf("%w: используйте 'DEL <key>'", ErrInvalidSyntax)
		}
		cache.Delete(parts[1])
		return "OK", nil

	default:
		return "", fmt.Errorf("%w: '%s'", ErrUnknownCommand, command)
	}

}

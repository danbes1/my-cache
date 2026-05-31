package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	ErrUnknownCommand = errors.New("ошибка: неизвестная команда")
	ErrInvalidSyntax  = errors.New("ошибка: неверный синтаксис команды")
)

type ctxKey int

const (
	ctxKeyReqId ctxKey = iota
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

	var requestCounter int64

	var wg sync.WaitGroup

	shutdown := make(chan struct{})
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {

		for {
			conn, err := listener.Accept()
			if err != nil {
				select {
				case <-shutdown:
					return
				default:

					log.Printf("Ошибка при принятии соединения: %v", err)
					continue

				}
			}

			requestCounter++

			reqId := fmt.Sprintf("req-%d", requestCounter)
			ctx := context.WithValue(context.Background(), ctxKeyReqId, reqId)

			wg.Add(1)
			go func() {
				defer wg.Done()
				handleConnection(ctx, conn, cache)
			}()
		}
	}()

	<-sigChan //ожидаем что пользователь нажмёт Ctrl+C
	fmt.Printf("\r\nПолучен сигнал остановки, начинаем плавное завершение\r\n")

	close(shutdown)
	listener.Close()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	waitChan := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitChan)
	}()

	select {
	case <-waitChan:
		fmt.Println("[SHUTDOWN] Все клиенты завершили работу")
	case <-shutdownCtx.Done():
		fmt.Println("[SHUTDOWN] Время ожидания истекло! Принудительное завершение")
	}

}

func handleConnection(ctx context.Context, conn net.Conn, cache Cache) {
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {
		err := conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		if err != nil {
			log.Printf("Не удалось установить дедлайн: %v", err)
			return
		}

		reqId := ctx.Value(ctxKeyReqId).(string)
		log.Printf("[%s] Клиент %s подключился", reqId, conn.RemoteAddr())

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

		response, err := processCommand(ctx, request, cache)
		if err != nil {
			fmt.Fprintf(conn, "%v\n", err)
			continue
		}

		_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))

		fmt.Fprintf(conn, "%s\n", response)
	}
}

func processCommand(ctx context.Context, req string, cache Cache) (string, error) {

	reqId := ctx.Value(ctxKeyReqId).(string)

	parts := strings.Fields(req)
	if len(parts) == 0 {
		return "", ErrInvalidSyntax
	}

	command := strings.ToUpper(parts[0])

	log.Printf("[%s] Выполнение команды: %s", reqId, command)
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

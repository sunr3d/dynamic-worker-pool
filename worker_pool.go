package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	minWorkers  = flag.Int("min", 1, "Минимальное кол-во воркеров (>= 1)")
	maxWorkers  = flag.Int("max", 10, "Максимальное кол-во воркеров (> min)")
	jobChanSize = flag.Int("size", 2, "Размер канала для задач (>= 1)")
	words       = []string{"Привет", "Мир", "Golang", "VK", "Конкурентность", "Каналы", "Воркеры", "Горутины"}
)

type WorkerPool struct {
	dataChan       chan string
	stopCh         map[int]chan struct{}
	processedItems int64
	startTime      time.Time
	wg             *sync.WaitGroup
	mu             *sync.Mutex
	workerCount    int32
	maxWorkers     int32
	minWorkers     int32
}

func New(minWorkers, maxWorkers int32, jobChanSize int) *WorkerPool {
	if minWorkers < 1 || minWorkers > maxWorkers || jobChanSize < 1 {
		panic("Некорректные параметры конфигурации: minWorkers не может быть < 1, maxWorkers должен быть > minWorkers, jobChanSize не может быть < 1.")
	}
	return &WorkerPool{
		dataChan:    make(chan string, jobChanSize),
		stopCh:      make(map[int]chan struct{}),
		startTime:   time.Now(),
		wg:          &sync.WaitGroup{},
		mu:          &sync.Mutex{},
		workerCount: 0,
		minWorkers:  minWorkers,
		maxWorkers:  maxWorkers,
	}
}

func (wp *WorkerPool) worker(ctx context.Context, id int) {
	defer wp.wg.Done()
	stopCh := make(chan struct{})

	wp.mu.Lock()
	wp.stopCh[id] = stopCh
	wp.mu.Unlock()

	for {
		select {
		case data, ok := <-wp.dataChan:
			if !ok {
				log.Printf("Воркер #%d завершает работу, канал закрыт и пустует\n", id)
				return
			}
			time.Sleep(200 * time.Millisecond)
			fmt.Printf("Воркер #%d обработал строку: %s\n", id, data)
			atomic.AddInt64(&wp.processedItems, 1)
		case <-stopCh:
			log.Printf("Воркер #%d остановлен планировщиком загрузки\n", id)
			return
		case <-ctx.Done():
			log.Printf("Воркер #%d завершает работу, получен сигнал прерывания\n", id)
			return
		}
	}
}

func (wp *WorkerPool) addWorker(ctx context.Context, id int) {
	if atomic.LoadInt32(&wp.workerCount) >= wp.maxWorkers {
		return
	}

	wp.wg.Add(1)
	atomic.AddInt32(&wp.workerCount, 1)
	log.Printf("Воркер #%d добавлен в пул.\n", id)
	go wp.worker(ctx, id)
}

func (wp *WorkerPool) removeWorker() {
	if atomic.LoadInt32(&wp.workerCount) <= wp.minWorkers {
		log.Println("Невозможно удалить воркера, достигнуто минимальное количество")
		return
	}

	wp.mu.Lock()
	defer wp.mu.Unlock()

	if len(wp.stopCh) == 0 {
		return
	}

	var maxID int
	for id := range wp.stopCh {
		if id > maxID {
			maxID = id
		}
	}
	if stopCh, ok := wp.stopCh[maxID]; ok {
		close(stopCh)
		delete(wp.stopCh, maxID)
	}

	atomic.AddInt32(&wp.workerCount, -1)
}

func (wp *WorkerPool) printStats() {
	elapsedTime := time.Since(wp.startTime)
	processedNum := atomic.LoadInt64(&wp.processedItems)
	avgPerSecond := float64(processedNum) / elapsedTime.Seconds()

	log.Println("Статистика работы пула воркеров:")
	log.Printf("- Время работы: %v\n", elapsedTime)
	log.Printf("- Обработано элементов: %d\n", processedNum)
	log.Printf("- Среднее кол-во элементов в секунду: %.2f\n", avgPerSecond)
}

func (wp *WorkerPool) monitorLoad(ctx context.Context) {
	log.Println("Мониторинг загрузки воркеров запущен.")
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Мониторинг загрузки воркеров завершает работу.")
			return
		case <-ticker.C:
			loadFactor := float64(len(wp.dataChan)) / float64(cap(wp.dataChan))
			workers := atomic.LoadInt32(&wp.workerCount)

			log.Printf("МОНИТОРИНГ: Текущая загрузка: %.0f%%, Количество воркеров: %d.\n", loadFactor*100, workers)
			if loadFactor > 0.5 && workers < wp.maxWorkers {
				wp.addWorker(ctx, int(workers)+1)
			}

			if loadFactor < 0.4 && workers > wp.minWorkers {
				wp.removeWorker()
			}
		}
	}
}

func (wp *WorkerPool) gracefulShutdown(ctx context.Context, ctxCancel context.CancelFunc) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		select {
		case sig := <-sigChan:
			log.Printf("Получен сигнал завершения: %s\n", sig)
			signal.Stop(sigChan)
			ctxCancel()
			wp.wg.Wait()
			log.Println("Graceful shutdown завершен.")
			return
		case <-ctx.Done():
			signal.Stop(sigChan)
			return
		}
	}()
}

func sendData(ctx context.Context, data []string, dataChan chan<- string) {
	defer func() {
		if r := recover(); r != nil {
			log.Println("Попытка закрыть уже закрытый канал:", r)
		}
	}()
	defer close(dataChan)

	for i := 0; i < 10; i++ {
		for j := 0; j < len(data); j++ {
			select {
			case <-ctx.Done():
				return
			case dataChan <- data[j]:
				time.Sleep(1 * time.Millisecond)
			}
		}

		time.Sleep(1 * time.Second)

		for j := 0; j < len(data)/4; j++ {
			select {
			case <-ctx.Done():
				return
			case dataChan <- data[j]:
				time.Sleep(50 * time.Millisecond)
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func main() {
	flag.Parse()
	log.Printf("Конфиг: minWorkers=%d, maxWorkers=%d, jobChanSize=%d\n", *minWorkers, *maxWorkers, *jobChanSize)

	log.Println("Запуск программы...")
	ctx, cancel := context.WithCancel(context.Background())

	pool := New(int32(*minWorkers), int32(*maxWorkers), *jobChanSize)
	pool.gracefulShutdown(ctx, cancel)

	for i := 1; i <= int(pool.minWorkers); i++ {
		pool.addWorker(ctx, i)
	}

	go pool.monitorLoad(ctx)

	dataString := make([]string, 0, len(words)**jobChanSize)
	for i := 0; i < *jobChanSize; i++ {
		dataString = append(dataString, words...)
	}

	sendData(ctx, dataString, pool.dataChan)
	pool.wg.Wait()
	cancel()
	time.Sleep(100 * time.Millisecond)
	log.Printf("Основная программа завершает работу.\n\n")
	pool.printStats()
}

package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/AnnaTarantina/pipeline-go/logger"
)

// Настройки буфера
const bufferDrainInterval = 30 * time.Second
const bufferSize = 10

type RingIntBuffer struct {
	array []int
	head  int
	count int
	size  int
	m     sync.Mutex
}

func NewRingIntBuffer(size int) *RingIntBuffer {
	return &RingIntBuffer{
		array: make([]int, size),
		head:  0,
		count: 0,
		size:  size,
	}
}

func (r *RingIntBuffer) Push(el int) {
	r.m.Lock()
	defer r.m.Unlock()

	if r.count < r.size {
		tail := (r.head + r.count) % r.size
		r.array[tail] = el
		r.count++
	} else {
		r.array[r.head] = el
		r.head = (r.head + 1) % r.size
	}
}

func (r *RingIntBuffer) Get() []int {
	r.m.Lock()
	defer r.m.Unlock()

	if r.count == 0 {
		return nil
	}

	result := make([]int, r.count)
	for i := 0; i < r.count; i++ {
		idx := (r.head + i) % r.size
		result[i] = r.array[idx]
	}

	r.head = 0
	r.count = 0
	return result
}

// Stage общий интерфейс для всех стадий пайплайна
type Stage interface {
	Run(in <-chan int, done <-chan bool) <-chan int
}

// Фильтрация положительных чисел
type PositiveFilter struct {
	logger *logger.Logger
}

func NewPositiveFilter() *PositiveFilter {
	return &PositiveFilter{
		logger: logger.New("PositiveFilter"),
	}
}

func (f *PositiveFilter) Run(in <-chan int, done <-chan bool) <-chan int {
	f.logger.Info("Starting positive number filter")
	out := make(chan int)
	go func() {
		defer close(out)
		count := 0
		for {
			select {
			case num, ok := <-in:
				if !ok {
					f.logger.Info("Received signal to stop, processed %d numbers", count)
					return
				}
				f.logger.Debug("Received number: %d", num)
				if num > 0 {
					count++
					f.logger.Info("Number %d passed positive filter", num)
					select {
					case out <- num:
						f.logger.Debug("Sent number %d to next stage", num)
					case <-done:
						f.logger.Warn("Stopped by done signal during send")
						return
					}
				} else {
					f.logger.Info("Number %d filtered out (not positive)", num)
				}
			case <-done:
				f.logger.Warn("Stopped by done signal during receive")
				return
			}
		}
	}()
	return out
}

// Фильтрация чисел, кратных 3 (исключая 0)
type MultipleOfThreeFilter struct {
	logger *logger.Logger
}

func NewMultipleOfThreeFilter() *MultipleOfThreeFilter {
	return &MultipleOfThreeFilter{
		logger: logger.New("MultipleOfThreeFilter"),
	}
}

func (f *MultipleOfThreeFilter) Run(in <-chan int, done <-chan bool) <-chan int {
	f.logger.Info("Starting multiple of three filter")
	out := make(chan int)
	go func() {
		defer close(out)
		count := 0
		for {
			select {
			case num, ok := <-in:
				if !ok {
					f.logger.Info("Received signal to stop, processed %d numbers", count)
					return
				}
				f.logger.Debug("Received number: %d", num)
				if num != 0 && num%3 == 0 {
					count++
					f.logger.Info("Number %d passed multiple-of-three filter", num)
					select {
					case out <- num:
						f.logger.Debug("Sent number %d to next stage", num)
					case <-done:
						f.logger.Warn("Stopped by done signal during send")
						return
					}
				} else {
					f.logger.Info("Number %d filtered out (not multiple of 3 or zero)", num)
				}
			case <-done:
				f.logger.Warn("Stopped by done signal during receive")
				return
			}
		}
	}()
	return out
}

// Буферизация с периодической выгрузкой
type BufferStage struct {
	size     int
	interval time.Duration
	logger   *logger.Logger
}

func NewBufferStageWithLogging(size int, interval time.Duration) *BufferStage {
	return &BufferStage{
		size:     size,
		interval: interval,
		logger:   logger.New("BufferStage"),
	}
}

func (b *BufferStage) Run(in <-chan int, done <-chan bool) <-chan int {
	b.logger.Info("Starting buffer stage (size: %d, interval: %v)", b.size, b.interval)
	out := make(chan int)
	buffer := NewRingIntBuffer(b.size)

	go func() {
		defer close(out)
		for {
			select {
			case num, ok := <-in:
				if !ok {
					b.logger.Info("Received signal to stop, flushing remaining buffer")
					// Выгрузка остатков при завершении источника
					data := buffer.Get()
					for _, n := range data {
						select {
						case out <- n:
							b.logger.Debug("Flushed number %d from buffer", n)
						case <-done:
							b.logger.Warn("Stopped by done signal during flush")
							return
						}
					}
					b.logger.Info("Buffer flushed completely")
					return
				}
				b.logger.Debug("Adding number %d to buffer", num)
				buffer.Push(num)
				b.logger.Info("Number %d added to buffer (current count: %d)", num, buffer.count)
			case <-done:
				b.logger.Warn("Stopped by done signal during buffer operation")
				data := buffer.Get()
				for _, n := range data {
					select {
					case out <- n:
						b.logger.Debug("Sent number %d from buffer", n)
					default:
					}
				}
				return
			}
		}
	}()

	// Горутина периодической выгрузки
	go func() {
		b.logger.Info("Starting periodic buffer drain timer (%v)", b.interval)
		ticker := time.NewTicker(b.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				b.logger.Info("Timer tick - draining buffer")
				data := buffer.Get()
				if len(data) > 0 {
					b.logger.Info("Draining %d items from buffer", len(data))
					for _, num := range data {
						select {
						case out <- num:
							b.logger.Debug("Sent drained number %d", num)
						case <-done:
							b.logger.Warn("Stopped by done signal during drain")
							return
						}
					}
				} else {
					b.logger.Debug("Buffer empty, nothing to drain")
				}
			case <-done:
				b.logger.Info("Periodic drain stopped by done signal")
				return
			}
		}
	}()

	return out
}

type DataSourceStage struct {
	logger *logger.Logger
}

func NewDataSourceStage() *DataSourceStage {
	return &DataSourceStage{
		logger: logger.New("DataSourceStage"),
	}
}

func (s *DataSourceStage) Run(_ <-chan int, done <-chan bool) <-chan int {
	s.logger.Info("Starting data source stage")
	// Игнорируем входной канал (источник)
	out := make(chan int)
	go func() {
		defer close(out)
		scanner := bufio.NewScanner(os.Stdin)
		s.logger.Info("Ready to accept input, starting scanner")
		for {
			fmt.Print("Введите число (или 'exit' для выхода): ")
			scanner.Scan()

			input := scanner.Text()
			if strings.EqualFold(input, "exit") {
				s.logger.Info("Exit command received, stopping...")
				fmt.Println("Программа завершила работу!")
				return
			}

			num, err := strconv.Atoi(input)
			if err != nil {
				s.logger.Error("Invalid input '%s': %v", input, err)
				fmt.Println("Ошибка: введено не число. Попробуйте снова.")
				continue
			}

			s.logger.Info("Valid number received: %d", num)
			select {
			case out <- num:
				s.logger.Debug("Sent number %d to pipeline", num)
			case <-done:
				s.logger.Warn("Stopped by done signal during send")
				return
			}
		}
	}()
	return out
}

type ConsumerStage struct {
	logger *logger.Logger
}

func NewConsumerStage() *ConsumerStage {
	return &ConsumerStage{
		logger: logger.New("ConsumerStage"),
	}
}

func (c *ConsumerStage) Run(in <-chan int, done <-chan bool) <-chan int {
	c.logger.Info("Starting consumer stage")
	// Возвращаем закрытый канал
	out := make(chan int)
	close(out)

	go func() {
		count := 0
		for {
			select {
			case num, ok := <-in:
				if !ok {
					c.logger.Info("Pipeline ended, consumed total: %d numbers", count)
					return
				}
				count++
				c.logger.Info("Consumed number: %d (total: %d)", num, count)
				fmt.Printf("Получены данные: %d\n", num)
			case <-done:
				c.logger.Warn("Consumer stopped by done signal")
				return
			}
		}
	}()

	return out
}

type Pipeline struct {
	stages []Stage
	done   chan bool
	logger *logger.Logger
}

func NewPipelineWithLogging(stages ...Stage) *Pipeline {
	return &Pipeline{
		stages: stages,
		done:   make(chan bool),
		logger: logger.New("Pipeline"),
	}
}

// Execute запускает весь пайплайн от источника до потребителя
func (p *Pipeline) Execute() {
	p.logger.Info("=" + string(make([]rune, 68)) + "=")
	p.logger.Info("PIPELINE EXECUTION STARTED")
	p.logger.Info("=" + string(make([]rune, 68)) + "=")

	current := p.stages[0].Run(nil, p.done)

	for i := 1; i < len(p.stages)-1; i++ {
		p.logger.Info("Connecting stage %d to pipeline", i+1)
		current = p.stages[i].Run(current, p.done)
	}

	if len(p.stages) > 1 {
		p.logger.Info("Starting final stage")
		p.stages[len(p.stages)-1].Run(current, p.done)
	}

	p.logger.Info("Pipeline running, waiting for completion...")
	<-p.done
	p.logger.Info("Pipeline execution completed")
}

// Завершение работы пайплайна
func (p *Pipeline) Stop() {
	p.logger.Info("Stopping pipeline...")
	close(p.done)
}

func main() {
	// Собираем пайплайн как цепочку стадий с логированием
	pipeline := NewPipelineWithLogging(
		NewDataSourceStage(),       // источник
		NewPositiveFilter(),        // фильтр > 0
		NewMultipleOfThreeFilter(), // фильтр кратных 3, ≠0
		NewBufferStageWithLogging(bufferSize, bufferDrainInterval), // буфер
		NewConsumerStage(), // потребитель
	)

	pipeline.Execute()
}

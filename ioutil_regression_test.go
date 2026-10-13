package engineio

import (
	"sync"
	"testing"
)

func TestConnReaderConcurrentClose(t *testing.T) {
	for iteration := 0; iteration < 100; iteration++ {
		done := make(chan struct{}, 1)
		reader := newConnReader(nil, done)
		start := make(chan struct{})
		var workers sync.WaitGroup
		for worker := 0; worker < 16; worker++ {
			workers.Add(1)
			go func() {
				defer workers.Done()
				<-start
				if err := reader.Close(); err != nil {
					t.Error(err)
				}
			}()
		}
		close(start)
		workers.Wait()
		if len(done) != 1 {
			t.Fatalf("expected one completion signal, got %d", len(done))
		}
		<-done
		if err := reader.Close(); err != nil {
			t.Fatal(err)
		}
		if len(done) != 0 {
			t.Fatal("repeated close sent another completion signal")
		}
	}
}

func TestConnReaderLateClose(t *testing.T) {
	done := make(chan struct{}, 1)
	reader := newConnReader(nil, done)
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		if err := reader.Close(); err != nil {
			t.Error(err)
		}
	}()
	<-closed
	if len(done) != 1 {
		t.Fatal("late close did not retain completion signal")
	}
	<-done
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if len(done) != 0 {
		t.Fatal("late repeated close sent another completion signal")
	}
}

func TestConnReaderCloseWithoutChannel(t *testing.T) {
	if err := (*connReader)(nil).Close(); err != nil {
		t.Fatal(err)
	}
	if err := newConnReader(nil, nil).Close(); err != nil {
		t.Fatal(err)
	}
}

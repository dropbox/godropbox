package context2

import (
	"context"
	"testing"
	"time"
)

func TestStripCancelAndDeadline(outerT *testing.T) {
	outerT.Run("TimeoutDoesntPropagate", func(t *testing.T) {
		timeout := 1 * time.Second
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		procrastinator := StripCancelAndDeadline(ctx)

		deadline, ok := procrastinator.Deadline()
		if ok {
			t.Fatal("Procrastinator had a deadline:", deadline)
		}

		time.Sleep(timeout + 1*time.Second)
		select {
		case <-procrastinator.Done():
			t.Fatal("Procrastinator got cancelled.")
		default:
		}

		if procrastinator.Err() != nil {
			t.Fatal("Non-nil error:", procrastinator.Err())
		}
	})

	outerT.Run("CancelPropDown", func(t *testing.T) {
		parent, parentCancel := context.WithCancel(context.Background())
		procrastinator := StripCancelAndDeadline(parent)
		child, childCancel := context.WithCancel(procrastinator)

		parentCancel()

		select {
		case <-procrastinator.Done():
			t.Fatal("Procrastinator got cancelled.")
		case <-child.Done():
			t.Fatal("Child got cancelled.")
		default:
		}

		childCancel()

		select {
		case <-procrastinator.Done():
			t.Fatal("Procrastinator got cancelled.")
		default:
		}
	})

	outerT.Run("CancelPropUp", func(t *testing.T) {
		parent, parentCancel := context.WithCancel(context.Background())
		procrastinator := StripCancelAndDeadline(parent)
		_, childCancel := context.WithCancel(procrastinator)

		childCancel()

		select {
		case <-procrastinator.Done():
			t.Fatal("Procrastinator got cancelled.")
		case <-parent.Done():
			t.Fatal("Parent got cancelled.")
		default:
		}

		parentCancel()

		select {
		case <-procrastinator.Done():
			t.Fatal("Procrastinator got cancelled.")
		default:
		}
	})

	outerT.Run("Value", func(t *testing.T) {
		key1, value1 := "bacon", "sandwich"
		key2, value2 := "fred", "rogers"

		parent := context.WithValue(context.Background(), key1, value1)
		procrastinator := StripCancelAndDeadline(parent)
		child := context.WithValue(procrastinator, key2, value2)

		if procrastinator.Value(key1) != value1 {
			t.Fatal("Procrastinator has wrong/missing key1.")
		}
		if procrastinator.Value(key2) != nil {
			t.Fatal("Procrastinator has key2.")
		}

		if child.Value(key1) != value1 {
			t.Fatal("Child has wrong/missing key1.")
		}
		if child.Value(key2) != value2 {
			t.Fatal("Child has wrong/missing key2.")
		}
	})
}

func TestStripDeadline(outerT *testing.T) {
	outerT.Run("DeadlineDoesntPropagate", func(t *testing.T) {
		timeout := 1 * time.Second
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		procrastinator := StripDeadline(ctx)

		deadline, ok := procrastinator.Deadline()
		if ok {
			t.Fatal("Procrastinator had a deadline:", deadline)
		}
	})

	outerT.Run("DeadlineCausesCancel", func(t *testing.T) {
		timeout := 1 * time.Second
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		procrastinator := StripDeadline(ctx)

		<-procrastinator.Done()

		if procrastinator.Err() == nil {
			t.Fatal("Expected error")
		}
	})

	outerT.Run("CancelPropDown", func(t *testing.T) {
		parent, parentCancel := context.WithCancel(context.Background())
		procrastinator := StripDeadline(parent)
		child, childCancel := context.WithCancel(procrastinator)
		defer childCancel()

		parentCancel()

		<-child.Done()
	})

	outerT.Run("CancelPropUp", func(t *testing.T) {
		parent, parentCancel := context.WithCancel(context.Background())
		procrastinator := StripDeadline(parent)
		_, childCancel := context.WithCancel(procrastinator)

		childCancel()

		select {
		case <-procrastinator.Done():
			t.Fatal("Procrastinator got cancelled.")
		case <-parent.Done():
			t.Fatal("Parent got cancelled.")
		default:
		}

		parentCancel()

		<-procrastinator.Done()
	})

	outerT.Run("Value", func(t *testing.T) {
		key1, value1 := "bacon", "sandwich"
		key2, value2 := "fred", "rogers"

		parent := context.WithValue(context.Background(), key1, value1)
		procrastinator := StripDeadline(parent)
		child := context.WithValue(procrastinator, key2, value2)

		if procrastinator.Value(key1) != value1 {
			t.Fatal("Procrastinator has wrong/missing key1.")
		}
		if procrastinator.Value(key2) != nil {
			t.Fatal("Procrastinator has key2.")
		}

		if child.Value(key1) != value1 {
			t.Fatal("Child has wrong/missing key1.")
		}
		if child.Value(key2) != value2 {
			t.Fatal("Child has wrong/missing key2.")
		}
	})
}

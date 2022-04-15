// Dropbox-specific workarounds for (self-inflicted) problems with the builtin context package.
package context2

import (
	"context"
	"os"
	"os/signal"
	"time"
)

// Take a parent context and create a child that totally ignores the parent's cancellation signals
// (including cancel funcs, deadlines, and timeouts).
//
// Sometimes it's useful to create a child context with all the metadata of the parent but not the
// cancellation/deadline of the parent. This function lets you do that.
//
// For example, an RPC server might initiate background activity to be carried out after the request
// returns a result but still want the background RPCs to be traceable to the causing request.
// Courier cancels the context of the request once its handler func returns, which will cancel all
// the background activity if that context gets passed in. On the other hand, starting from
// context.Background() loses the tracing metadata from the originating request, making debugging
// more difficult.
//
// WARNING: Stopwatches attached to contexts get broken if the child context outlives the parent,
// which is exacerbated when you use StripCancelAndDeadline to decouple the background goroutine's
// lifetime from the parent request. It's a good idea to specifically nuke the stopwatch metadata on
// top of calling StripCancelAndDeadline.
//
// This kind of problem may apply to other libraries that use context-attached metadata, too. Use
// with care.
func StripCancelAndDeadline(parent context.Context) context.Context {
	return &cancelStrippedCtx{parent}
}

type cancelStrippedCtx struct {
	context.Context
}

func (*cancelStrippedCtx) Deadline() (deadline time.Time, ok bool) {
	return
}

func (*cancelStrippedCtx) Done() <-chan struct{} {
	return nil
}

func (*cancelStrippedCtx) Err() error {
	return nil
}

func (c *cancelStrippedCtx) Value(key interface{}) interface{} {
	return c.Context.Value(key)
}

// The first time the process receives any of the given signals, cancels the returned context and
// deregisters the signal handler.
func WithCancelOnSignal(ctx context.Context, signals ...os.Signal) context.Context {
	ctx, cancel := context.WithCancel(ctx)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, signals...)
	go func() {
		select {
		case <-ctx.Done():
		case _ = <-sigChan:
			cancel()
		}
		signal.Stop(sigChan)
	}()
	return ctx
}

// Creates a child context that has no Deadline() metadata. The child will still get cancelled if
// the parent's context deadline expires, or if the parent gets cancelled.
//
// This is a workaround for Courier's deadline propagation. The Courier client implementation will
// look at the Deadline() on the context passed to an outbound call and send that deadline along
// with the request to the remote host. This sounds great in theory (it's just a straightforward
// generalization of the in-process deadline to RPCs, right?), but in practice, it's not. In the
// event the deadline expires, all hosts involved in the RPC's call stack will race to throw an
// error, and the remote server might actually send back a deadline error before the deadline on the
// clientside context has expired. It is also somewhat commonly the case that a deadline error will
// get mangled and be rendered unidentifiable as such by the time it gets back to the caller.
//
// This makes it impossible for an RPC handler to tell for certain whether an error from an outbound
// RPC is due to a deadline originating from the handler's calling client vs. from a timeout set by
// the handler or by one of the services that handler calls. Since the two timeouts often diverge
// significantly, you want very much to know which is the case in order to decide whether to count
// an availability hit. Refusing to forward a client deadline achieves this - if you get any error
// back from your outbound RPC, you know it's not because the client gave your outbound RPC too
// little timeout to complete.
//
// Note that it's okay to propagate the cancellation channel because, in order for the remote host
// to send back an error due to some cancellation signal you sent, you have to send a cancel message
// over the wire first. So you can check the context you passed to the outbound call to see if it
// has an Err() set, and, if so, you can safely write the other error from the outbound RPC off as
// irrelevant.
func StripDeadline(parent context.Context) context.Context {
	return &deadlineStrippedCtx{parent}
}

type deadlineStrippedCtx struct {
	wrapped context.Context
}

func (d *deadlineStrippedCtx) Deadline() (time.Time, bool) {
	return time.Time{}, false
}

func (d *deadlineStrippedCtx) Done() <-chan struct{} {
	return d.wrapped.Done()
}

func (d *deadlineStrippedCtx) Err() error {
	return d.wrapped.Err()
}

func (d *deadlineStrippedCtx) Value(key interface{}) interface{} {
	return d.wrapped.Value(key)
}

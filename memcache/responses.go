package memcache

import (
	"fmt"

	"godropbox/errors"
)

func NewStatusCodeError(status ResponseStatus) error {
	switch status {
	case StatusNoError:
		return nil
	case StatusKeyNotFound:
		return errors.New("Key not found")
	case StatusKeyExists:
		return errors.New("Key exists")
	case StatusValueTooLarge:
		return errors.New("Value too large")
	case StatusInvalidArguments:
		return errors.New("Invalid arguments")
	case StatusItemNotStored:
		return errors.New("Item not stored")
	case StatusIncrDecrOnNonNumericValue:
		return errors.New("Incr/decr on non-numeric value")
	case StatusUnknownCommand:
		return errors.New("Unknown command")
	case StatusOutOfMemory:
		return errors.New("Server out of memory")
	case StatusNotSupported:
		return errors.New("Not supported")
	case StatusInternalError:
		return errors.New("Server internal error")
	case StatusBusy:
		return errors.New("Server busy")
	case StatusTempFailure:
		return errors.New("Temporary server failure")
	default:
		return errors.Newf("Invalid status: %d", int(status))
	}
}

// NOTE(opaugam) - once the client trips into invalid state it will fail all
// requests. This would lead to excessive exclog traces when using a large
// key set in GetMulti() for instance. Define a dedicated error for that very
// situation (e.g code-path aborted due to the state being invalid) so that
// higher layers have the option to ignore. The first error that trips the
// state is the only one worth reporting.
type InvalidStateError struct{}

func (e *InvalidStateError) Error() string {
	return "Skipping due to previous error"
}

func NewInvalidStateError() error {
	return &InvalidStateError{}
}

// The genericResponse is an union of all response types.  Response interfaces
// will cover the fact that there's only one implementation for everything.
type genericResponse struct {
	// err and status are used by all responses.
	err    error
	status ResponseStatus

	// key is used by get / mutate / count responses.  The rest is used only
	// by get response.
	item Item

	// set to true only for get response
	allowNotFound bool

	// count is used by count response.
	count uint64

	// versions is used by version response.
	versions map[string]string

	// statEntries is used by stat response.
	statEntries map[string](map[string]string)

	// shard address which returned the response.
	// todo(aleksei) remove after memcache vortex2 latency investigation is done
	shardAddress string
}

func (r *genericResponse) Status() ResponseStatus {
	return r.status
}

func (r *genericResponse) Error() error {
	if r.err != nil {
		return r.err
	}
	if r.status == StatusNoError {
		return nil
	}
	if r.allowNotFound && r.status == StatusKeyNotFound {
		return nil
	}
	return NewStatusCodeError(r.status)
}

func (r *genericResponse) Key() string {
	return r.item.Key
}

func (r *genericResponse) Value() []byte {
	return r.item.Value
}

func (r *genericResponse) Flags() uint32 {
	return r.item.Flags
}

func (r *genericResponse) DataVersionId() uint64 {
	return r.item.DataVersionId
}

func (r *genericResponse) Count() uint64 {
	return r.count
}

func (r *genericResponse) Versions() map[string]string {
	return r.versions
}

func (r *genericResponse) Entries() map[string](map[string]string) {
	return r.statEntries
}

func (r *genericResponse) ShardAddress() string {
	return r.shardAddress
}

func (r *genericResponse) SetShardAddress(address string) {
	r.shardAddress = address
}

// This creates a Response from an error.
func NewErrorResponse(err error) Response {
	return &genericResponse{
		err: err,
	}
}

// This creates a Response from status.
func NewResponse(status ResponseStatus) Response {
	return &genericResponse{
		status: status,
	}
}

// This creates a GetResponse from an error.
func NewGetErrorResponse(key string, err error) GetResponse {
	resp := &genericResponse{
		err:           err,
		allowNotFound: true,
	}
	resp.item.Key = key
	return resp
}

// This creates a normal GetResponse.
func NewGetResponse(
	key string,
	status ResponseStatus,
	flags uint32,
	value []byte,
	version uint64) GetResponse {

	resp := &genericResponse{
		status:        status,
		allowNotFound: true,
	}
	resp.item.Key = key
	if status == StatusNoError {
		if value == nil {
			resp.item.Value = []byte{}
		} else {
			resp.item.Value = value
		}
		resp.item.Flags = flags
		resp.item.DataVersionId = version
	}
	return resp
}

// This creates a MutateResponse from an error.
func NewMutateErrorResponse(key string, err error) MutateResponse {
	resp := &genericResponse{
		err: err,
	}
	resp.item.Key = key
	return resp
}

// This creates a normal MutateResponse.
func NewMutateResponse(
	key string,
	status ResponseStatus,
	version uint64) MutateResponse {

	resp := &genericResponse{
		status: status,
	}
	resp.item.Key = key
	if status == StatusNoError {
		resp.item.DataVersionId = version
	}
	return resp
}

// This creates a CountResponse from an error.
func NewCountErrorResponse(key string, err error) CountResponse {
	resp := &genericResponse{
		err: err,
	}
	resp.item.Key = key
	return resp
}

// This creates a normal CountResponse.
func NewCountResponse(
	key string,
	status ResponseStatus,
	count uint64) CountResponse {

	resp := &genericResponse{
		status: status,
	}
	resp.item.Key = key
	if status == StatusNoError {
		resp.count = count
	}
	return resp
}

// This creates a VersionResponse from an error.
func NewVersionErrorResponse(
	err error,
	versions map[string]string) VersionResponse {
	return &genericResponse{
		err:      err,
		versions: versions,
	}
}

// This creates a normal VersionResponse.
func NewVersionResponse(
	status ResponseStatus,
	versions map[string]string) VersionResponse {

	resp := &genericResponse{
		status:   status,
		versions: versions,
	}
	return resp
}

// This creates a StatResponse from an error.
func NewStatErrorResponse(
	err error,
	entries map[string](map[string]string)) StatResponse {
	return &genericResponse{
		err:         err,
		statEntries: entries,
	}
}

// This creates a normal StatResponse.
func NewStatResponse(
	status ResponseStatus,
	entries map[string](map[string]string)) StatResponse {

	resp := &genericResponse{
		status:      status,
		statEntries: entries,
	}
	return resp
}

func noShardsError(key string) error {
	return errors.Newf("No Memcache shards! Key: '%s'", key)
}

func connectionError(shard string, err error) error {
	errString := fmt.Sprintf("Connection unavailable for memcache shard %s", shard)
	if err == nil {
		return errors.New(errString)
	}
	return errors.Wrapf(err, errString)
}

package memcache

//
// Magic Byte
//

const (
	reqMagicByte  uint8 = 0x80
	respMagicByte uint8 = 0x81
)

//
// Response Status
//

type ResponseStatus uint16

const (
	StatusNoError ResponseStatus = iota
	StatusKeyNotFound
	StatusKeyExists
	StatusValueTooLarge
	StatusInvalidArguments
	StatusItemNotStored
	StatusIncrDecrOnNonNumericValue
	StatusVbucketBelongsToAnotherServer // Not used
	StatusAuthenticationError           // Not used
	StatusAuthenticationContinue        // Not used
)

const (
	StatusUnknownCommand ResponseStatus = 0x81 + iota
	StatusOutOfMemory
	StatusNotSupported
	StatusInternalError
	StatusBusy
	StatusTempFailure
)

//
// Command Opcodes
//

type opCode uint8

const (
	opGet opCode = iota
	opSet
	opAdd
	opReplace
	opDelete
	opIncrement
	opDecrement
	opQuit // Unsupported
	opFlush
	opGetQ // Unsupported
	opNoOp // Unsupported
	opVersion
	opGetK
	opGetKQ // Unsupported
	opAppend
	opPrepend
	opStat
	opSetQ       // Unsupported
	opAddQ       // Unsupported
	opReplaceQ   // Unsupported
	opDeleteQ    // Unsupported
	opIncrementQ // Unsupported
	opDecrementQ // Unsupported
	opQuitQ      // Unsupported
	opFlushQ     // Unsupported
	opAppendQ    // Unsupported
	opPrependQ   // Unsupported
	opVerbosity
	opTouch // Unsupported
	opGAT   // Unsupported
	opGATQ  // Unsupported
)

// More unsupported opcodes:
//   0x20     SASL list mechs
//   0x21     SASL Auth
//   0x22     SASL Step
//   0x30     RGet
//   0x31     RSet
//   0x32     RSetQ
//   0x33     RAppend
//   0x34     RAppendQ
//   0x35     RPrepend
//   0x36     RPrependQ
//   0x37     RDelete
//   0x38     RDeleteQ
//   0x39     RIncr
//   0x3a     RIncrQ
//   0x3b     RDecr
//   0x3c     RDecrQ
//   0x3d     Set VBucket
//   0x3e     Get VBucket
//   0x3f     Del VBucket
//   0x40     TAP Connect
//   0x41     TAP Mutation
//   0x42     TAP Delete
//   0x43     TAP Flush
//   0x44     TAP Opaque
//   0x45     TAP VBucket Set
//   0x46     TAP Checkpoint Start
//   0x47     TAP Checkpoint End

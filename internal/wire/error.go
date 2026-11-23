package wire

import (
	"net/http"
	"strings"
	"unicode/utf8"
)

// Code is an error code. The list is closed in protocol version 1.
type Code string

// The error codes of ADR 0005.
const (
	CodeInvalidRequest       Code = "invalid_request"
	CodeUnauthenticated      Code = "unauthenticated"
	CodeForbidden            Code = "forbidden"
	CodeStatementForbidden   Code = "statement_forbidden"
	CodeDatabaseNotFound     Code = "database_not_found"
	CodeDatabaseExists       Code = "database_exists"
	CodeNeedsChannel         Code = "needs_channel"
	CodeChannelBusy          Code = "channel_busy"
	CodeInFlight             Code = "in_flight"
	CodeStaleSequence        Code = "stale_sequence"
	CodeSequenceGap          Code = "sequence_gap"
	CodeSequenceConflict     Code = "sequence_conflict"
	CodeChannelLost          Code = "channel_lost"
	CodeRequestTooLarge      Code = "request_too_large"
	CodeTooManyChannels      Code = "too_many_channels"
	CodeOverloaded           Code = "overloaded"
	CodeInvalidState         Code = "invalid_state"
	CodeTransactionAborted   Code = "transaction_aborted"
	CodeBusy                 Code = "busy"
	CodeTimeout              Code = "timeout"
	CodeCancelled            Code = "cancelled"
	CodeResultTooLarge       Code = "result_too_large"
	CodeSQLError             Code = "sql_error"
	CodeCommitFailed         Code = "commit_failed"
	CodeCommitOutcomeUnknown Code = "commit_outcome_unknown"
	CodeNoLeader             Code = "no_leader"
	CodeNotServing           Code = "not_serving"
	CodeOwnerUnreachable     Code = "owner_unreachable"
	CodeBehind               Code = "behind"
	CodeMaintenance          Code = "maintenance"
	CodeInternal             Code = "internal"
)

// codes lists every code, in the order of ADR 0005's errors table.
var codes = []Code{
	CodeInvalidRequest, CodeUnauthenticated, CodeForbidden, CodeStatementForbidden, CodeDatabaseNotFound,
	CodeDatabaseExists, CodeNeedsChannel, CodeChannelBusy, CodeInFlight, CodeStaleSequence, CodeSequenceGap,
	CodeSequenceConflict, CodeChannelLost, CodeRequestTooLarge, CodeTooManyChannels, CodeOverloaded,
	CodeInvalidState, CodeTransactionAborted, CodeBusy, CodeTimeout, CodeCancelled, CodeResultTooLarge,
	CodeSQLError, CodeCommitFailed, CodeCommitOutcomeUnknown, CodeNoLeader, CodeNotServing,
	CodeOwnerUnreachable, CodeBehind, CodeMaintenance, CodeInternal,
}

// Status is the HTTP status of a response whose whole body is an error with
// this code. A code that only ever ends the response of an accepted request
// returns 200, the status of that response. An unknown code returns 500.
func (c Code) Status() int {
	switch c {
	case CodeInvalidRequest, CodeSQLError:
		return http.StatusBadRequest
	case CodeUnauthenticated:
		return http.StatusUnauthorized
	case CodeForbidden, CodeStatementForbidden:
		return http.StatusForbidden
	case CodeDatabaseNotFound:
		return http.StatusNotFound
	case CodeDatabaseExists, CodeNeedsChannel, CodeChannelBusy, CodeInFlight, CodeStaleSequence,
		CodeSequenceGap, CodeSequenceConflict, CodeInvalidState:
		return http.StatusConflict
	case CodeChannelLost:
		return http.StatusGone
	case CodeRequestTooLarge:
		return http.StatusRequestEntityTooLarge
	case CodeTooManyChannels, CodeOverloaded:
		return http.StatusTooManyRequests
	case CodeTransactionAborted, CodeBusy, CodeTimeout, CodeCancelled, CodeResultTooLarge,
		CodeCommitFailed, CodeCommitOutcomeUnknown:
		return http.StatusOK
	case CodeNoLeader, CodeNotServing, CodeOwnerUnreachable, CodeBehind, CodeMaintenance:
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

// Recordable reports whether an error with this code can be a channel
// request's recorded outcome.
func (c Code) Recordable() bool {
	switch c {
	case CodeInvalidRequest, CodeStatementForbidden, CodeInvalidState, CodeTransactionAborted, CodeBusy,
		CodeTimeout, CodeCancelled, CodeResultTooLarge, CodeSQLError, CodeCommitFailed, CodeMaintenance,
		CodeInternal, CodeOverloaded:
		return true
	}
	return false
}

// TxnState is a channel's transaction state.
type TxnState string

// The transaction states.
const (
	TxnNone    TxnState = "none"
	TxnOpen    TxnState = "open"
	TxnAborted TxnState = "aborted"
)

// Error is the error object of ADR 0005. A zero integer, an empty string and
// a nil BatchIndex are absent fields.
type Error struct {
	Code               Code
	NotExecuted        bool
	Final              bool
	Message            string
	RequestID          string
	ChannelID          string
	Seq                int64
	TxnState           TxnState
	SQLiteCode         int64
	SQLiteExtendedCode int64
	BatchIndex         *int64
	Cause              string // "maintenance" or empty
	Txn                int64
	Epoch              int64
}

// NewError returns an error with the flags of an answer given before
// acceptance: not executed and not final, except that sequence_conflict is
// final and commit_outcome_unknown is not "not executed". A running request
// interrupted with channel_lost must also clear NotExecuted; a recorded
// failure takes its flags from Recorded.
func NewError(code Code, msg string) *Error {
	return &Error{
		Code:        code,
		NotExecuted: code != CodeCommitOutcomeUnknown,
		Final:       code == CodeSequenceConflict,
		Message:     msg,
	}
}

// Recorded sets the flags of a recorded failure and returns e: final, and
// not executed unless the failure is of an execute or batch accepted while a
// transaction was open (txnStatement). It panics if the code is never
// recorded, which is a bug in the caller.
func (e *Error) Recorded(txnStatement bool) *Error {
	if !e.Code.Recordable() {
		panic("wire: " + string(e.Code) + " is never recorded")
	}
	e.Final = true
	e.NotExecuted = !txnStatement
	return e
}

func (e *Error) Error() string { return string(e.Code) + ": " + e.Message }

// CutMessage returns the longest prefix of msg, cut at a character boundary,
// whose JSON string encoding, quotes included, takes at most limit bytes.
// Invalid UTF-8 in the prefix is replaced with U+FFFD, which the encoding
// needs; the replacement never makes the encoding longer.
func CutMessage(msg string, limit int) string {
	n := 2
	for i, r := range msg {
		n += quotedLen(r)
		if n > limit {
			msg = msg[:i]
			break
		}
	}
	return strings.ToValidUTF8(msg, "�")
}

// quotedLen is the length of r inside a JSON string as appendQuoted writes
// it. An invalid byte counts as U+FFFD.
func quotedLen(r rune) int {
	switch {
	case r == '"', r == '\\', r == '\b', r == '\f', r == '\n', r == '\r', r == '\t':
		return 2
	case r < 0x20:
		return 6
	}
	return utf8.RuneLen(r)
}

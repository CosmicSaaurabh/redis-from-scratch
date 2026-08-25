# ADR-006: INCRBYFLOAT uses float64 and cannot match Redis on every platform

- Status: accepted
- Date: 2026-08-25
- Phase: 1

## Context

The differential suite in `test/compat` compares replies against a real
`redis-server` byte for byte.
`INCRBYFLOAT` was the only command that would not match, and investigating why
turned up something worth recording.

Redis accumulates in C `long double` and formats with `%.17Lf`, trimming
trailing zeros.
What `long double` means is platform-dependent:

| platform | long double | `INCRBYFLOAT f 10.5` then `0.1` |
|---|---|---|
| x86-64 Linux | 80-bit extended | `10.6` |
| arm64 macOS | 64-bit, same as double | `10.59999999999999964` |

Redis does not agree with itself across platforms.
Go has no 80-bit float, so byte-exact agreement with every Redis build is not
achievable at all.

## Decision

Accumulate in `float64` and format with the shortest decimal that round-trips.

`10.5 + 0.1` produces `10.6`, which matches Redis on x86-64 Linux, the platform
this server targets and the one CI runs on.
It does not match Redis on arm64 macOS, where Redis itself produces the longer
string.

The compat suite compares `INCRBYFLOAT` numerically within `1e-9` rather than
byte for byte, with the reason stated at the assertion rather than hidden behind
a loose comparison.

## Consequences

`INCRBYFLOAT` carries roughly 15 significant decimal digits instead of 18.
A client accumulating money in cents as a float across millions of operations
would drift sooner than under Redis on x86 - and would be wrong under either,
because that is not what floating point is for.

Every other command in the differential suite matches Redis exactly, including
error messages: `INCR` at both ends of the int64 range, `GETRANGE`'s negative
index clamping, `SET` option conflicts, arity errors and unknown-command errors
were all verified against `redis-server` 8.4 rather than against the
documentation.

## Alternatives rejected

**Arbitrary-precision decimal via `math/big.Float`.**
Would give exact and platform-stable results, at the cost of an allocation and
a much slower path for a command whose entire point is to be fast, plus output
that matches *neither* Redis build.

**Emulating 80-bit extended precision in software.**
Correct in principle, hundreds of lines of bit manipulation, and it would still
only match Redis on x86.

**Formatting with `%.17f` to imitate Redis's format string.**
Would match Redis on macOS and stop matching it on Linux, which is the wrong
trade for the deployment target.

**Refusing to implement `INCRBYFLOAT`.**
The command is genuinely useful and the divergence is at the seventeenth
significant digit.

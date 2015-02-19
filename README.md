# Molly: Grounded Fault Injection

## Installation

Molly is written in Scala and compiled using SBT.  Molly depends on the [C4 Overlog runtime](https://github.com/bloom-lang/c4) and [Z3 theorem prover](https://z3.codeplex.com/).

The top-level `Makefile` should be handle a one-click build on OS X.


## Running

Add the native library dependencies to your loader path.  On OS X:

```
export LD_LIBRARY_PATH=lib/c4/build/src/libc4/:lib/z3/build/z3-dist/lib/
```

Run

```
sbt "run-main edu.berkeley.cs.boom.molly.SyncFTChecker"
```

and see the usage message for more details.

# Molly: Lineage-driven Fault Injection

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

### Example

In this directory, run

```
sbt "run-main edu.berkeley.cs.boom.molly.SyncFTChecker \
	../examples_ft/delivery/simplog.ded \
	../examples_ft/delivery/deliv_assert.ded \
	--EOT 4 \
	--EFF 2 \
	--nodes a,b,c \
	--crashes 0 \
	--prov-diagrams"
```

Molly will find a counterexample.  The `./output` directory will contain an HTML report which shows visualizations that explain the counterexample and the program lineage that was used to find it.  To view this report:

- Safari users: just open `index.html`
- Chrome / Firefox users: due to these browsers' same-origin policies, you need to start up a local web server to host the resources loaded by this page.  Just run `python -m SimpleHTTPServer` in the output directory, then browse to the local address that it prints.

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


#### Programs

Programs are submitted in the Dedalus language.  Dedalus is a distributed variant of Datalog: program statements are if-then rules of the forms:

     conclusion(bindings1)[@annotation] :- premise1(bindings2), premise2(bindings2) [...], notin premisen(bindings3), [...];
     
The conclusions and premises are relations; any variables in the conclusion (bindings1) must be bound in the body. Premises may be positive or negative; if the latter, they are preceded by "notin" and all variables (bindings3) must be bound
in positive premises.

Conclusions can have temporal annotations of the following forms:

 * @next -- the conclusions hold at the *successor* time.
 * @async -- the conclusions hold at an undefined time.
 * (no annotation) -- the conclusions hold whenever the premises hold.
 
The first attribute of every relation is a *location specifier* indicating the identity of a network endpoint.

The first two rules in simplog.ded are *persistence rules*.  They ensure that the contents of log and nodes persist over time:

     node(Node, Neighbor)@next :- node(Node, Neighbor);
     log(Node, Pload)@next :- log(Node, Pload);

The next rule says that for every pair of records in bcast and node that agree in their first column, there should (at some
unknown time) be a record in log that takes its first column from the second column of the node record, and its second column
from the second column of the bcast record.  Intuitively, this captures multicast communication: when some Node1 has a bcast record, for
every Node2 about which it knows, it forwards the payload of that record to Node2.

     log(Node2, Pload)@async :- bcast(Node1, Pload), node(Node1, Node2);


Finally, the last line says that any node that receives a broadcast should put it in its log:

     log(Node, Pload) :- bcast(Node, Pload);

#### Specifications

Molly needs a way to check whether injected failures actually violated program correctness properties.  A natural way to express such properties is as an implication of the form "*If* some precondition holds, *then* some postcondition must hold."  For example, the broadcast protocol described above can succinctly be expressed in the following way:

 * _Precondition_: *Any* correct process delivers a message *m*
 * _Postcondition_: *All* correct processes deliver *m*
 
Any execution in which the precondition holds but the postcondition does not is a counterexample to the correctness property.  Executions in which the precondition does not hold (we can always always find one by dropping all messages) are vacuously correct.


You may specify correctness properties by providing rules that define two special relations:

 * pre() 
 * post()
 
For example:

    pre(X, Pl) :- log(X, Pl), notin bcast(X, Pl)@1, notin crash(X, X, _);

For every node X that has a payload Pl in its log, there is a record (X, Pl) in pre, provided that X was not the original broadcaster and X did not crash.

    post(X, Pl) :- log(X, Pl), notin missing_log(_, Pl);
    missing_log(A, Pl) :- log(X, Pl), node(X, A), notin log(A, Pl);
    
There is a record (X, Pl) in post if some node X has a payload Pl in its log, and there are *no* nodes that do not.
    
    
## More information

Molly is described in a [SIGMOD paper](http://people.ucsc.edu/~palvaro/molly.pdf).

Dedalus is described [here](http://www.eecs.berkeley.edu/Pubs/TechRpts/2009/EECS-2009-173.html).


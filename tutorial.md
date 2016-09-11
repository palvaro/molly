Dedalus is a relational logic language intended for specifying and implementing distributed systems.
Much like SQL, it allows programmers to describe their system at a high level in terms of data relationships.
Unlike SQL, it has constructs to characterize details such as mutable state and asynchronous communication.

Instead of trying to explain the Dedalus language all at once, we will present it incrementally in the context of
examples.

For our first Dedalus program, we will implement one of the simplest imaginable distributed programs: a broadcast protocol.
Much as in SQL, the first thing we want to figure out is the set of relations (or tables) necessary to capture the
messages, internal events and states of the protocol.  To a first approximation, we will want to represent the broadcast messages,
the discrete local event that begins a broadcast, and a persistent ``memory'' of received broadcasts.  Because a broadcast
(either implicitly or explicitly) is sent to a *group* of computers, we also want to represent the group of participants.
We will deal with that relation first.

     member(Node, Other)@next :- member(Node, Other);

Note first that we did not need to *declare* any tables; unlike SQL, dedalus infers the declarations from *rules* in which tables appear.
this rule states that there is a table called member with two columns, and at an given time, if there is a record in member, the same record
is in member at the next time.  In other words, member is an append-only relation with immutable rows; a straightforward
inductive argument establishes that if a row is ever inserted into member, it stays there forever.  The @next notation is unique to Dedalus.


We can add a few *facts* to our program to populate the member relation:

member("a", "b")@1;
member("a", "c")@1;



Facts in Dedalus are like bare insert statements in SQL: they reference predicates like rules do, but unlike rules,
they can only bind arguments to constants.  Consider the first fact.  It says that there is a record <"a", "b">
in the table member, at time 1.  Every relation in dedalus is *distributed* across nodes via horizontal partitioning:
the physical location of a record is always the value taken by its first column.  So informally, the first fact says that
there is a record <"a", "b"> located at the Dedalus instance "a" (in practice, this would be an identifier of a process, such as "10.0.0.1:8000" -- indicating a dedalus process running on 10.0.0.1 and bound to port 8000)
at time 1 on `a`'s (respectively, 10.0.0.1's) clock.  The next two facts indicate that node `a` also knows about nodes `b` and `c`.
The inductive rule for member above ensures that it *always* knows about those nodes.


We use the same persistence pattern for our ``memory'' of received broadcasts:

     log(Node, Message)@next :- log(Node, Message);

Here comes the fun part.  In relational logic, a broadcast or multicast is really just a join between a local event and a persistent
list of group members.

     log(Other, Message)@async :- bcast(Node, Message), member(Node, Other);

This rule says that if there is ever a tuple <Node, Message> in bcast, it should be placed in the log table at some node Other, for every
other node we know about.  Note that this rule follows two constraints that are required by Dedalus:

 * All of the tables appearing in the body (the right-hand side) of a rule must have the same bindings for the first column.  This captures the intuition that a given node can only draw conclusions from local information.

 * When the table in the head (the left-hand side) of a rule has a different binding for its first column than the tables in the body, the rule must have the @async indicator.  This captures the intuition that when data moves across a network, we have no control over the timing of its visibility.

The only thing necessary to get the protocol running now is to provide it with a stimulus in the form of a bcast event:

     bcast("a", "Hello, world!")@1;

Note that because there is no inductive persistence rule for bcast, it is an *ephemeral* event -- true for a moment in time (here, at time 1).
Let's consider how evaluation works in the abstract before running this protocol using Molly.  At time 1, on node 10.0.0.1:8000,
because of the fact we have already inserted, we see that out broadcast rule can take the following bindings:

     log("b", "Hello, world!")@async :- bcast("a", "Hello, world!"), member("a", "b");
     log("c", "Hello, world!")@async :- bcast("a", "Hello, world!"), member("a", "c");

Based on this inference, we can see that a message should appear at each member.  At what time will it appear?  We don't know!

Before we can run this Dedalus program using Molly, we need to do a little bit more work.  Molly is a verification tool: in addition
to giving it programs, we need to tell it how to check if an execution is successful.  In practice, it is common to express correctness invariants as *implications*, having the form "IF it is possible to achieve propery X, THEN the system achieves X." If the correctness property were not stated in this way, then there almost always exists a trivial "bad" execution -- for example, the execution in which all nodes crash before doing anything.  A durability invariant might say "IF a write is acknowledged AND some servers remain up, THEN the write is durable."  An agreement invariant might say "IF anyone reaches a decision, THEN everyone else reaches the same decision."  Executions in which the precondition is false are called vacuously correct.

During the design phase, programmers will very often want to watch their protocol run a few times before rolling up their sleeves and taking on the difficult (indeed, often more difficult than writing the program itself!) task of writing down invariants.  In this case, we have found it useful to start with trivially true invariants, and refine them later (Molly badly wants your development to be test-driven, but there is always an escape hatch).  We will return to wring invariants in the next section, but for now let's write down a precondition/postcondition pair that is *always* true.

     pre(X) :- log(X, _);
     post(X) :- pre(X);

It is easy to convince ourselves that the invariant always holds.  In any execution of the program, either there exists an X (and Y) such that  `log(X, Y)`, or not.  If the latter, the precondition is false and the execution is "correct". If the former, then due to the second rule, surely `post(X)` is also true.  The invariant holds if `pre` is false or `post` is true, so the invariant always holds.



Let's run it!

     sbt "run-main edu.berkeley.cs.boom.molly.SyncFTChecker -N a,b,c -t 5 -f 0 demo.ded"

Molly will have created an output directory called `output`.  To view this as a web page, we can

     cd output
     python -m SimpleHTTPServer 8080
     
and then point our browser to `http://localhost:8080`.  The files I generated when I ran the command are [here](demo_html).


## Invariants.

Molly supports a simple convention:

 * Define a table called *pre* that captures preconditions.  Intuitively, [...]
 * Define a table called *post* that captures postconditions.  When Molly executes, for every row in pre(), it checks if there is a matching row in post().  If there is not, it reports an invariant violation.

This is somewhat abstract, but it should become clear in the context of an example.  The classic correctness criterion for a "reliable broadcast" protocol is the following:

*If a correct process delivers a broadcast message, then all correct processes deliver it.




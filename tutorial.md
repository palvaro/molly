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

member("tcp:10.0.0.1:8000", "tcp:10.0.0.2:8000")@1;
member("tcp:10.0.0.1:8000", "tcp:10.0.0.3:8000")@1;
member("tcp:10.0.0.1:8000", "tcp:10.0.0.4:8000")@1;

Facts in Dedalus are like bare insert statements in SQL: they reference predicates like rules do, but unlike rules,
they can only bind arguments to constants.  Consider the first fact.  It says that there is a record <"tcp:10.0.0.1:8000", "tcp:10.0.0.2:8000">
in the table member, at time 1.  Every relation in dedalus is *distributed* across nodes via horizontal partitioning:
the physical location of a record is always the value taken by its first column.  So informally, the first fact says that
there is a record <"tcp:10.0.0.1:8000", "tcp:10.0.0.2:8000"> located at the Dedalus instance running on 10.0.0.1 and bound to port 8000,
at time 1 on 10.0.0.1's clock.  The next two facts indicate that node 10.0.0.1 also knows about nodes 10.0.0.3 and 10.0.0.4.
the inductive rule for member above ensures that it *always* knows about those nodes.


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

     bcast("tcp:10.0.0.1:8000", "Hello, world!")@1;

Note that because there is no inductive persistence rule for bcast, it is an *ephemeral* event -- true for a moment in time (here, at time 1).
Let's consider how evaluation works in the abstract before running this protocol using Molly.  At time 1, on node 10.0.0.1:8000,
because of the fact we have already inserted, we see that out broadcast rule can take the following bindings:

     log("tcp:10.0.0.2:8000", "Hello, world!")@async :- bcast("tcp:10.0.0.1:8000", "Hello, world!"), member("tcp:10.0.0.1:8000", "tcp:10.0.0.2:8000");
     log("tcp:10.0.0.3:8000", "Hello, world!")@async :- bcast("tcp:10.0.0.1:8000", "Hello, world!"), member("tcp:10.0.0.1:8000", "tcp:10.0.0.3:8000");
     log("tcp:10.0.0.4:8000", "Hello, world!")@async :- bcast("tcp:10.0.0.1:8000", "Hello, world!"), member("tcp:10.0.0.1:8000", "tcp:10.0.0.4:8000");

Based on this inference, we can see that a message should appear at each member.  At what time will it appear?  We don't know!



Before we can run this Dedalus program using Molly, we need to do a little bit more work.  Molly is a verification tool: in addition
to giving it programs, we need to tell it how to check if an execution is successful.  Because it is so common to express correctness
invariants as *implications*, Molly supports a simple convention:

 * Define a table called *pre* that captures preconditions.  Intuitively, [...]
 * Define a table called *post* that captures postconditions.  When Molly executes, for every row in pre(), it checks if there is a matching row in post().  If there is not, it reports an invariant violation.

This is somewhat abstract, but it should become clear in the context of an example.  The classic correctness criterion for a "reliable broadcast" protocol is the following:

*If a correct process delivers a broadcast message, then all correct processes deliver it.



Let's run it!

     sbt "run-main edu.berkeley.cs.boom.molly.SyncFTChecker -N tcp:10.0.0.1:8000,tcp:10.0.0.2:8000,tcp:10.0.0.3:8000 -t 5 -f 0 demo.ded"


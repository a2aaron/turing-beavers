# Explanation of Exploration Algorithm
This follows the method described by https://bbchallenge.org/method

The goal here is to enumerate all of the potential canidates for BB(5). This needs to be smaller than the entire space
of all 5-state 2-symbol Turing machines, since there it's a lot of them (24^10 total machines). However, almost all of
these machines are really boring, and are trivially identical to lots of other machines. Hence, we use a different
method for getting all of the machines: We start with a machine with all of it's transitions undefined[1]. Every
time the machine tries to run an undefined transition, we create a copy of the machine and fill in that transition with
each possible transition that could go there.

[1] Actually, we do define the first transition, but I'll explain this later.

## Tree Pruning
The above can be put into a more concrete algorithm, which is described below:

1. Let `in_progress` be an empty stack (or queue or whatever) of 'in-progress' machines
2. Add `1RB---_------_------_------_------` to `in_progress`
3. While there are any machines in `in_progress`, remove one of the machines from `in_progress`. Call this machine `M`.
Then, simulate `M` until one of the following occurs:
    - `M` halts: Mark `M` as halting
    - `M` has been ran for BB(5) steps or uses more than BB_SPACE(5) cells: Mark `M` as undecided.[1]
    - `M` has been ran for BB(4) steps, but has not visited all 5 states: Mark `M` as non-halting.[2] 
    - `M` encounters an undefine transition: Add some set of machines to `in_progress` which are copies of `M` with the 
    undefine transition replaced by a defined transition.[3]

[1] In practice, the space condition is hit more often here than the time condition
[2] This works since if `M` was halting, it would imply that there exists a 4-state 2-symbol Turing machine which runs
for longer than BB(4) but eventually halts (one could create this machine by taking `M` and replacing the 5th state it
visits with the halt state in the transition table). However, this is impossible, since BB(4) is already the longest
running 4-staet 2-symbol machine which eventually halts.
[3] The set of transitions which we will use for this step is described below.

## Transition Enumation
Naively, since there are 2 symbols, 2 directions, and 5 + 1 target states, there are 24 possible transitions we could
use. However, we can actually cut down how many transitions we consider with the following rules:

1. If we are defining the very first transition, we fix the first transition to be `0A -> 1RB`. This is valid since the
only other meaningfully different transitions are `0A -> 0_A` and `0A -> 1_A`. In both of those cases, the machine will
obviously never halt, since it will constantly be moving left or right and remain in state A. Note that this is why the 
machine starts with one transition defined, instead of zero.

2. If we are defining the very last transition, we fix the target state to be the halt state. This is needed
because if there's no halt state, then obviously the machine cannot halt. In fact, we can just fix the transition to be
_ -> 1LZ (or something equivalent) since it won't matter what symbol or direction we move since we're about to halt
anyways.

3. Otherwise, we're defining some transition that isn't the last one. In this case, we will have a set of possible
target states. The target states will be all of the visited states (all of the states which have at least one defined
transition) and the "lowest" unvisited state (where we consider state A to be the lowest and state E to be the highest).

For the last condition, an example may help: Suppose we have already visited states A, B, C (and have not visited states
D and E). In this case, the target states will be A, B, C, and D.

In fact, we can list out all the cases. Since we start with only state A being defined/visited and always add the
"lowest" unvisited state, then we will always visit the new states in the order A, B, C, D, E.

Another way to say this: The set of target states is just the states which have a defined transition in the table, plus
the next state which would come after that. Also: Note that this case deliberately excludes the halt state from
consideration (obviously if we define the target state for the transition we are about to take as the halt state then
the machine will immediately halt, which is boring)

All of this just describes the target state. We then split each target state into one of four transitions, which will be:
1. Write 0 + shift left + go to target state
2. Write 1 + shift left + go to target state
3. Write 0 + shift right + go to target state
4. Write 1 + shift right + go to target state
---- MODULE MC ----
EXTENDS Mnevis, TLC

\* MV CONSTANT declarations@modelParameterConstants
CONSTANTS
k1
----

\* MV CONSTANT declarations@modelParameterConstants
CONSTANTS
l1, l2
----

\* MV CONSTANT declarations@modelParameterConstants
CONSTANTS
n1, n2, n3
----

\* MV CONSTANT declarations@modelParameterConstants
CONSTANTS
t1, t2
----

\* MV CONSTANT definitions K
const_157807449226135000 == 
{k1}
----

\* MV CONSTANT definitions L
const_157807449226136000 == 
{l1, l2}
----

\* MV CONSTANT definitions N
const_157807449226137000 == 
{n1, n2, n3}
----

\* MV CONSTANT definitions T
const_157807449226138000 == 
{t1, t2}
----

\* SYMMETRY definition
symm_157807449226139000 == 
Permutations(const_157807449226135000) \union Permutations(const_157807449226136000) \union Permutations(const_157807449226137000) \union Permutations(const_157807449226138000)
----

\* CONSTANT definitions @modelParameterConstants:4Started
const_157807449226140000 == 
1
----

\* CONSTANT definitions @modelParameterConstants:5Follower
const_157807449226141000 == 
2
----

\* CONSTANT definitions @modelParameterConstants:6LockerUpCommand
const_157807449226142000 == 
3
----

\* CONSTANT definitions @modelParameterConstants:7Committed
const_157807449226143000 == 
4
----

\* CONSTANT definitions @modelParameterConstants:8Up
const_157807449226144000 == 
5
----

\* CONSTANT definitions @modelParameterConstants:9CleanedUp
const_157807449226145000 == 
6
----

\* CONSTANT definitions @modelParameterConstants:10Leader
const_157807449226146000 == 
7
----

\* CONSTANT definitions @modelParameterConstants:11PendingBlacklistNotify
const_157807449226147000 == 
8
----

\* CONSTANT definitions @modelParameterConstants:12PendingNotifyUp
const_157807449226148000 == 
9
----

\* CONSTANT definitions @modelParameterConstants:13Aborted
const_157807449226149000 == 
10
----

\* CONSTANT definitions @modelParameterConstants:14NotStarted
const_157807449226150000 == 
11
----

\* CONSTANT definitions @modelParameterConstants:15LockNotGranted
const_157807449226151000 == 
12
----

\* CONSTANT definitions @modelParameterConstants:16Stopped
const_157807449226152000 == 
13
----

\* CONSTANT definitions @modelParameterConstants:17Offline
const_157807449226153000 == 
14
----

\* CONSTANT definitions @modelParameterConstants:18BlacklistCommand
const_157807449226154000 == 
15
----

\* CONSTANT definitions @modelParameterConstants:19Blacklisted
const_157807449226155000 == 
16
----

\* CONSTANT definitions @modelParameterConstants:20PendingConsistentRead
const_157807449226156000 == 
17
----

\* CONSTANT definitions @modelParameterConstants:21Down
const_157807449226157000 == 
18
----

\* CONSTANT definitions @modelParameterConstants:22Candidate
const_157807449226158000 == 
19
----

\* CONSTANT definitions @modelParameterConstants:23PendingBlacklistConfirm
const_157807449226159000 == 
20
----

\* CONSTANT definitions @modelParameterConstants:24LockerDownCommand
const_157807449226160000 == 
21
----

\* CONSTANT definitions @modelParameterConstants:25CommitCommand
const_157807449226161000 == 
22
----

\* CONSTANT definitions @modelParameterConstants:26PendingCommitResult
const_157807449226162000 == 
23
----

\* CONSTANT definitions @modelParameterConstants:27LockGranted
const_157807449226163000 == 
24
----

\* CONSTANT definitions @modelParameterConstants:28Nop
const_157807449226164000 == 
25
----

\* CONSTRAINT definition @modelParameterContraint:0
constr_157807449226165000 ==
/\ node_change_ctr < 3
/\ link_change_ctr < 3
/\ TermLessThanEq(3)
/\ LastIndexLessThanEq(5)
----
=============================================================================
\* Modification History
\* Created Fri Jan 03 19:01:32 CET 2020 by GUNMETAL

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
n1, n2
----

\* MV CONSTANT declarations@modelParameterConstants
CONSTANTS
t1, t2
----

\* MV CONSTANT definitions K
const_157847242052714000 == 
{k1}
----

\* MV CONSTANT definitions L
const_157847242052715000 == 
{l1, l2}
----

\* MV CONSTANT definitions N
const_157847242052716000 == 
{n1, n2}
----

\* MV CONSTANT definitions T
const_157847242052717000 == 
{t1, t2}
----

\* SYMMETRY definition
symm_157847242052718000 == 
Permutations(const_157847242052714000) \union Permutations(const_157847242052715000) \union Permutations(const_157847242052716000) \union Permutations(const_157847242052717000)
----

\* CONSTRAINT definition @modelParameterContraint:0
constr_157847242052719000 ==
/\ node_change_ctr < 3
/\ link_change_ctr < 3
/\ LastIndexLessThanEq(5)
----
=============================================================================
\* Modification History
\* Created Wed Jan 08 09:33:40 CET 2020 by GUNMETAL

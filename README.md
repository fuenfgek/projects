# Advanced Databases and Information Systems Project II 
## Implementation of Join Algorithm for SPARQL Query Processing

### Task:
The task is to implement two most popular join algorithms, namely Hash join and Sort-merge join
algorithms for a specific SPARQL query over an RDF dataset. The Waterloo SPARQL Diversity Test Suite
WatDiv dataset from University of Waterloo https://dsg.uwaterloo.ca/watdiv/ consists of diverse
RDF triple stores in different size. Based on two selected datasets from it, the SPARQL query of the form: <br>
(?a)---follows--->(?b)---friendOf--->(?c)---likes--->(?d)---hasReview--->(?e) <br>
is to be evaluated. In the query, (?a), (?b), (?c), (?d), (?e) are variables, and follows, friendOf, <br>
likes, hasReview are properties. The answer of the query is the list of mapped values of all the variables <br>
(?a), (?b), (?c), (?d), (?e). <br> <br>

This repo contains a python file with a hash join and sort merge implementation for 
the following query on the above mentioned dataset: <br>
SELECT follows.subject, follows.object, friendOf.object, likes.object, hasReview.object <br>
FROM follows, friendOf, likes, hasReview <br>
WHERE follows.object = friendOf.subject <br>
AND friendOf.object = likes.subject <br>
AND likes.object = hasReview.subject <br>
The query in the form of relational algebra is as follows:

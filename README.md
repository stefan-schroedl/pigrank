# PigRank

## Purpose

Apache Pig UDFs for computing ranking measures.

## Installation
m
To build the JARs from the source release, run:

```
./gradlew assemble
```

## DCG

Compute *(normalized) discounted cumulative gain* or rank-weighted average - see [details](https://en.wikipedia.org/wiki/Discounted_cumulative_gain). Called with an unordered bag.

> DCG(normalization, cutoff, predictor, target)

with
* *normalization:* either the string                                                                                                 
  * "normalized": divide DCG by maximum achievable (i.e., compute nDCG).
  * "weighted_average": divide DCG by total sum of logarithmic discount factors.                  
  * "unnormalized": absolute DCG.                                                                                                                              
* *cutoff:* maximum rank to consider in measure, as a string. Values of zero or less are interpreted as 'no cutoff'.       
* *predictor:* zero-based column index of the ranking score, as a string.
* *target:* zero-based column index of the target score, as a string.

### Example

<dl>
 <pre>
 define NDCG pigrank.DCG('normalized', '-1', '1', '2');
 -- nDCG without rank cutoff
 -- assuming the second column contains ranking scores, the third one the target
 
 data = load 'input' using PigStorage('\t') as (
         query:chararray,
         score:double,
         target:double
 );

 data_gr = group data by query;

 eval = foreach data_gr
 generate
         flatten(group) as query,
         NDCG(data) 

 store eval into 'output';

</pre>
</dl>

## MRR

Compute *Mean Reciprocal Rank (MRR)* from an unordered bag; i.e., the inverse of the rank of the first non-zero target - see [details](https://en.wikipedia.org/wiki/Mean_reciprocal_rank).

> MRR(predictor, target)

### Example

<dl>
<pre>
 define MRR pigrank.MRR('1', '2');
 -- the second column contains ranking scores, the third one the target
 
 data = load 'input' using PigStorage('\t') as (
    query:chararray,
    score:double,
    target:double
 );

 data_gr = group data by query;

 eval = foreach data_gr
 generate
    flatten(group) as query,
    MRR(data) as mrr
;

 store eval into 'output';
</pre>
</dl>

## Similarity

Called with two unordered bags, computes the similarity of two rankings according to one of the following measures:   
* *Jaccard coefficient:* Size of intersection over size of union.                                                                       
* *Cosine similarity:* Cosine between two vectors with dimensions corresponding to items, and inverse ranks as weights.   
* *Rank-biased Overlap (RBO):* Set overlap at common prefix length, averaged over exponential user top-down exploration. RBO accounts for uneven list size across rankings and queries. See [this paper](http://www.umiacs.umd.edu/~wew/papers/wmz10_tois.pdf) for details.                                                                                                                                
 
> Similarity(simType, param, ID1, rankScore1, ID2, rankScore2)

with

* *simType:* type of similarity function, one of the strings "jaccard", "cosine", or "rbo".
* *param:* parameter for similarity function. 
  * For *jaccard* or *cosine*, the maximum rank to include (cutoff). Values less than one are interpreted as "no cutoff". 
  * For *rbo*, the "persistence" probability that the user will look at the next rank. Typical values: 0.9 (resp. 0.98) means that the first 10 (resp. 50) ranks have 86% of the weight if the evaluation.
* *ID1:* Unique identifier for items in the first bag, used to test for equality with items in the second bag (zero-based column index).
* *rankScore1:* Zero-based column index of ranking score for the first bag (column index).
* *ID2:* Identifier for items in the second bag (column index).
* *rankScore2:*" Ranking score for the second bag (column index).

### Example

<dl>
<pre>
 define JACCARD   pigrank.Similarity('jaccard', '-1', '2', '3', '2', '3');
 define JACCARD_2 com.a9.pigudf.eval.ranking.Similarity('jaccard', '2', '2', '3', '2', '3');
 define COSINE    com.a9.pigudf.eval.ranking.Similarity('cosine', '-1', '2', '3', '2', '3');
 define RBO       com.a9.pigudf.eval.ranking.Similarity('rbo', '0.9', '2', '3', '2', '3');

 data = load 'input' using PigStorage('\t') as (
         query:chararray,
         treatment:chararray,
         asin:chararray,
         score:double
 );

 data_gr = group data by (query, treatment);

 data_gr = foreach data_gr
 generate
         flatten(group) as (query, treatment),
         data
 ;

 split data_gr into data1 if treatment=='t1', data2 otherwise;

 side_by_side = cogroup data1 by query, data2 by query;

 eval = foreach side_by_side
 generate
         flatten(group) as query,
         flatten(data1.data) as group_t1,
         flatten(data2.data) as group_t2
 ;

 describe eval;

 eval = foreach eval
 generate
         query,
         JACCARD(group_t1, group_t2),
         JACCARD_2(group_t1, group_t2),
         COSINE(group_t1, group_t2),
         RBO(group_t1, group_t2)
 ;

 store eval into 'output';
</pre>
</dl>

## Notes

All ranking measures deal with possible ties in score values by computing the *expectation* over all valid rankings.

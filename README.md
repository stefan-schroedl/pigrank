# PigRank

[![Build Status](https://secure.travis-ci.org/stefan-schroedl/pigrank.png?branch=master)](http://travis-ci.org/stefan-schroedl/pigrank)

## Purpose

Apache Pig UDFs for computing ranking measures.

## Installation

To build the JAR from the source release, run:

```
./gradlew assemble
```

## Overview

This project provides three user-defined functions for the Apache Pig language useful for [Learning-to-Rank applications](https://en.wikipedia.org/wiki/Learning_to_rank#Evaluation_measures): *DCG* and *MRR* as evaluation measures, and *Similarity* to compare two distinct rankings.

*DCG* and *MRR* expect as input unordered bags of tuples; each tuple should have one column containing the rank score, and one column containing the target. The UDF sorts the bag in descending order of the former, and uses the latter one to compute the ranking quality. For DCG, any positive numbers are valid, while for MRR, any nonzero value will be regarded as a positive target.

*Similarity* expects two bags of tuples with corresponding rank score columns. Two additional columns are used as unique identifiers to decide whether an item in the first list is identical to one in the second list. 

Note that *ties* in the rank score can give rise to multiple different rankings and hence rank measures. *DCG* and *MRR* take this into account by computing the *expectation* over all possible rankings.

## DCG

Compute [*(normalized) discounted cumulative gain*](https://en.wikipedia.org/wiki/Discounted_cumulative_gain) or rank-weighted average, with weights logarithmically decreasing with rank.

> DCG(normalization, cutoff, scoreCol, targetCol)

with
* *normalization:* one of the strings  
  * "unnormalized": Absolute DCG. 
  * "normalized": Divide DCG by maximum achievable (i.e., compute nDCG).
  * "weighted_average": Divide DCG by total sum of logarithmic discount factors.                  
                                                                                                                             
* *cutoff:* Maximum rank to consider in measure, as a string. Values of zero or less are interpreted as 'no cutoff'.       
* *scoreCol:* Zero-based column index of the ranking score, as a string.
* *targetCol:* Zero-based column index of the target score, as a string.

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

Compute [*Mean Reciprocal Rank (MRR)*](https://en.wikipedia.org/wiki/Mean_reciprocal_rank) from an unordered bag; i.e., the inverse of the rank of the first non-zero target.

> MRR(scoreCol, targetCol)

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
* [*Jaccard coefficient:*](https://en.wikipedia.org/wiki/Jaccard_index) Size of intersection over size of union.                                                                       
* [*Cosine similarity:*](https://en.wikipedia.org/wiki/Cosine_similarity) Cosine between two vectors with dimensions corresponding to items, and inverse ranks as weights.   
* [*Rank-biased Overlap (RBO):*](http://www.umiacs.umd.edu/~wew/papers/wmz10_tois.pdf) Set overlap at common prefix length, averaged over exponential user top-down exploration. RBO has an intuitive probabilistic interpretation, and accounts for uneven list size across rankings and queries.
 
> Similarity(simType, param, idCol1, scoreCol1, idCol2, scoreCol2)

with

* *simType:* Type of similarity function, one of the strings "jaccard", "cosine", or "rbo".
* *param:* Parameter for similarity function. 
  * For *jaccard* or *cosine*, the maximum rank to include (cutoff). Values less than one are interpreted as "no cutoff". 
  * For *rbo*, the "persistence" probability that the user will look at the next rank. Typical values: 0.9 (resp. 0.98) means that the first 10 (resp. 50) ranks have 86% of the weight if the evaluation.
* *idCol1:* Unique identifier for items in the first bag, used to test for equality with items in the second bag (zero-based column index).
* *scoreCol1:* Zero-based column index of ranking score for the first bag.
* *idCol2:* Identifier for items in the second bag (column index).
* *scoreCol2:*" Ranking score for the second bag (column index).

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


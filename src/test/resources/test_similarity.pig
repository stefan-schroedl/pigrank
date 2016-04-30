
define JACCARD   pigrank.Similarity('jaccard', '-1', '2', '3', '2', '3');
define JACCARD_2 pigrank.Similarity('jaccard', '2', '2', '3', '2', '3');
define COSINE    pigrank.Similarity('cosine', '-1', '2', '3', '2', '3');
define RBO       pigrank.Similarity('rbo', '0.9', '2', '3', '2', '3');

data = load 'input' as (
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

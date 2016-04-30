
define DCG     pigrank.DCG('unnormalized', '-1', '1', '2');
define DCG_3   pigrank.DCG('unnormalized', '3', '1', '2');
define NDCG    pigrank.DCG('normalized', '-1', '1', '2');
define WTD_AVG pigrank.DCG('weighted_average', '-1', '1', '2');

data = load 'input' as (
        query:chararray,
        score:double,
        target:double
);

data_gr = group data by query;

eval = foreach data_gr
generate
        flatten(group) as query,
        DCG(data),
        DCG_3(data),
        NDCG(data),
        WTD_AVG(data)
;

store eval into 'output';

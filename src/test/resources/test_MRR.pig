
define MRR pigrank.MRR('1', '2');

data = load 'input' as (
        query:chararray,
        score:double,
        target:double
);

data_gr = group data by query;

eval = foreach data_gr 
generate 
        flatten(group) as query,
        MRR(data)
;

store eval into 'output';

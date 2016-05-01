package pigrank;

import java.util.*;

import java.io.IOException;

import org.apache.pig.pigunit.PigTest;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.Test;


public class DCGTest {

  final static String[] pigScript = {
    " define DCG     pigrank.DCG('unnormalized', '-1', '1', '2');",
    " define DCG_3   pigrank.DCG('unnormalized', '3', '1', '2');",
    " define NDCG    pigrank.DCG('normalized', '-1', '1', '2');",
    " define WTD_AVG pigrank.DCG('weighted_average', '-1', '1', '2');",
    " ",
    " data = load 'input' as (",
    "         query:chararray,",
    "         score:double,",
    "         target:double",
    " );",
    " ",
    " data_gr = group data by query;",
    " ",
    " eval = foreach data_gr",
    " generate",
    "         flatten(group) as query,",
    "         DCG(data),",
    "         DCG_3(data),",
    "         NDCG(data),",
    "         WTD_AVG(data)",
    " ;",
    " ",
    " store eval into 'output';" };

  @Test
  public void testDCG() throws IOException, ParseException {

    PigTest test = new PigTest(pigScript);

    String[] input = { "q1\t1.0\t5", "q1\t2.0\t0", "q1\t3.0\t2", "q1\t4.0\t0", "q1\t5.0\t0",
                       "q2\t2.1\t0", "q2\t2.0\t0",
                       "q3\t5\t10",
                       "q4\t5.0\t0", "q4\t3.0\t0", "q4\t4.0\t0", "q4\t2.0\t1.5", "q4\t4.0\t0.5", "q4\t1.0\t0", "q4\t4.0\t1" };
    String[] expected = {
      "(q1,2.934264036172708,1.0,0.4685930805099648,0.9951855928353252)",
      "(q2,0.0,0.0,0.0,0.0)",
      "(q3,10.0,10.0,1.0,10.0)",
      "(q4,1.3151139364844586,0.5654648767857287,0.5523531026111765,0.3614936962253256)" };

    test.assertOutput("data", input, "eval", expected);
  };
};

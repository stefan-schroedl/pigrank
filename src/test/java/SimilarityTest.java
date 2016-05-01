package pigrank;

import java.io.IOException;

import org.apache.pig.pigunit.PigTest;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.Test;

public class SimilarityTest {

  final static String[] pigScript = {
    " ",
    " define JACCARD   pigrank.Similarity('jaccard', '-1', '2', '3', '2', '3');",
    " define JACCARD_2 pigrank.Similarity('jaccard', '2', '2', '3', '2', '3');",
    " define COSINE    pigrank.Similarity('cosine', '-1', '2', '3', '2', '3');",
    " define RBO       pigrank.Similarity('rbo', '0.9', '2', '3', '2', '3');",
    " ",
    " data = load 'input' as (",
    "         query:chararray,",
    "         treatment:chararray,",
    "         asin:chararray,",
    "         score:double",
    " );",
    " ",
    " data_gr = group data by (query, treatment);",
    " ",
    " data_gr = foreach data_gr ",
    " generate",
    "         flatten(group) as (query, treatment),",
    "         data",
    " ;",
    " ",
    " split data_gr into data1 if treatment=='t1', data2 otherwise;",
    " ",
    " side_by_side = cogroup data1 by query, data2 by query;",
    " ",
    " eval = foreach side_by_side",
    " generate",
    "         flatten(group) as query,",
    "         flatten(data1.data) as group_t1,",
    "         flatten(data2.data) as group_t2",
    " ;",
    " ",
    " describe eval;",
    " ",
    " eval = foreach eval",
    " generate",
    "         query,",
    "         JACCARD(group_t1, group_t2),",
    "         JACCARD_2(group_t1, group_t2),",
    "         COSINE(group_t1, group_t2),",
    "         RBO(group_t1, group_t2)",
    " ;",
    " ",
    " store eval into 'output';" };


  @Test
  public void testSimilarity() throws IOException, ParseException {

    PigTest test = new PigTest(pigScript);

    String[] input = { "q1\tt1\ta1a\t9", "q1\tt1\ta2\t8",
                       "q1\tt2\ta1b\t9", "q1\tt2\ta2\t8",

                       "q2\tt1\ta1a\t9", "q2\tt1\ta2\t8", "q2\tt1\ta3\t7", "q2\tt1\ta4\t6", "q2\tt1\ta5\t5", "q2\tt1\ta6\t4",
                       "q2\tt2\ta1b\t9", "q2\tt2\ta2\t8",

                       "q3\tt1\ta1\t8",
                       "q3\tt2\ta1\t7" };

    String[] expected = {
      "(q1,0.3333333333333333,0.3333333333333333,0.2,0.45)",
      "(q2,0.14285714285714285,0.3333333333333333,0.18310050856745497,0.45)",
      "(q3,1.0,1.0,1.0,1.0)" };

    test.assertOutput("data", input, "eval", expected);
  };
};

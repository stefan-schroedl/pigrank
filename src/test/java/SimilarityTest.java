package pigrank;

import java.io.IOException;

import org.apache.pig.pigunit.PigTest;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.Test;

public class SimilarityTest {

  private static final String PIG_PREFIX = "./src/test/pig/";
  private static final String PIG_SIMILARITY = PIG_PREFIX + "test_similarity.pig";

  @Test
  public void testSimilarity() throws IOException, ParseException {

    PigTest test = new PigTest(PIG_SIMILARITY);

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

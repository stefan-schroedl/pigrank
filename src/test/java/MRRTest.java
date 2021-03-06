package pigrank;

import java.io.IOException;

import org.apache.pig.pigunit.PigTest;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.Test;

public class MRRTest {

  final static String[] pigScript = {
    " define MRR pigrank.MRR('1', '2');",
    " ",
    " data = load 'input' as (",
    "         query:chararray,",
    "         score:double,",
    "         target:double",
    " );",
    " ",
    " data_gr = group data by query;",
    " ",
    " eval = foreach data_gr ",
    " generate ",
    "         flatten(group) as query,",
    "         MRR(data)",
    " ;",
    " ",
    " store eval into 'output';" };


  @Test
  public void testMRR() throws IOException, ParseException {
    
    PigTest test = new PigTest(pigScript);

    String[] input = { "q1\t1.0\t0", "q1\t2.0\t1", "q1\t3.0\t0", "q1\t4.0\t0", "q1\t5.0\t0",
                       "q2\t2.1\t0", "q2\t2.0\t0",
                       "q3\t5\t10",
                       "q4\t5.0\t0", "q4\t3.0\t0", "q4\t4.0\t0", "q4\t2.0\t1", "q4\t4.0\t1", "q4\t1.0\t0", "q4\t4.0\t1",
                       "q5\t5.0\t0", "q5\t3.0\t0", "q5\t4.0\t1", "q5\t2.0\t1", "q5\t5.0\t0", "q5\t1.0\t0", "q5\t2.0\t0",
                       "q6\t4.0\t0", "q6\t4.0\t0", "q6\t4.0\t1", "q6\t4.0\t0", "q6\t4.0\t1" };
    String[] expected = { "(q1,0.25)", "(q2,0.0)", "(q3,1.0)", "(q4,0.4444444444444444)", "(q5,0.3333333333333333)", "(q6,0.6416666666666667)" };
    
    test.assertOutput("data", input, "eval", expected);
  };
};

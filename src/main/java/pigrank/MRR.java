package pigrank;

import java.io.IOException;

import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;
import org.apache.pig.impl.util.WrappedIOException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 * Pig UDF to compute mean reciprocal rank.
 *
 * Called with an (unordered) bag, returns the inverse of the rank of first
 * non-zero target column. <p>
 * Example use in a pig script:
 * <pre>
 * 
 * -- the second column contains ranking scores, the third one the target
 * define MRR pigrank.MRR('1', '2');
 *
 * data = load 'input' using PigStorage('\t') as (
 *    query:chararray,
 *    score:double,
 *    target:double
 * );
 *
 * data_gr = group data by query;
 *
 * eval = foreach data_gr
 * generate
 *    flatten(group) as query,
 *    MRR(data) as mrr
 *;
 *
 * store eval into 'output';
 * </pre>
 *
 */

public class MRR extends EvalFunc<Double> {

  int predictorField;
  int targetField;
  int minCols;
  Ranking ranking;

  /**
   * Constructor for MRR function
   *
   * @param strPredictorField zero-based column index of ranking score,
   *            as a string.
   * @param strTargetField zero-based column index of ranking score,
   *            as a string.
   */

  public MRR(String strPredictorField, String strTargetField) {

    predictorField = Integer.parseInt(strPredictorField);
    targetField = Integer.parseInt(strTargetField);
    minCols = 1 + Math.max(predictorField, targetField);
  }

  /**
   * Entry point for UDF
   *
   * @param input bag containing tuples of rows to rank
   * @return Double MRR value
   */

  @Override
  public Double exec(Tuple input) throws IOException {

    if (input == null || input.size() != 1) {
      throw new ExecException(
        "Expecting a single bag, but found "
        + input.toString());
    }

    try {

      DataBag bag = (DataBag)input.get(0);
      if(bag==null)
        return null;

      Iterator it = bag.iterator();

      ranking = new Ranking((int)bag.size());

      while (it.hasNext()){

        Tuple t = (Tuple)it.next();

        if (t == null || t.size() < minCols) {
          System.err.println(this.getClass().getSimpleName() + " expected tuple with at least " + minCols + "columns, got  " + t);
          return null;
        };

        if (t.get(predictorField) != null && t.get(targetField) != null) {
          Double pred   = DataType.toDouble(t.get(predictorField));
          Double target = DataType.toDouble(t.get(targetField));
          ranking.addItem(null, pred, target);
        }
      }

      ranking.rank();
      
      return ranking.getMRR(Integer.MAX_VALUE);

    } catch (NumberFormatException nfe) {
      System.err.println("Failed to process input in class " + this.getClass().getSimpleName() + "; error - " + nfe.getMessage());
      return null;
    } catch (Exception e) {
      throw WrappedIOException.wrap("Caught exception in class " + this.getClass().getSimpleName() + " while processing input row ", e);
    }
  };

  @Override
  public Schema outputSchema(Schema input) {

    try {
      if (input == null || input.size() != 1 || input.getField(0).type != DataType.BAG) {
        throw new IllegalArgumentException("Expected a bag as argument; found: " + DataType.findTypeName(input.getField(0).type));
      }

      Schema bagSchema = input.getField(0).schema;
      Schema tupleSchema = bagSchema.getField(0).schema;

      if (tupleSchema.size() < minCols) {
        throw new IllegalArgumentException("The tuple must contain at least " + minCols + " columns");
      }

      if (!DataType.isNumberType(tupleSchema.getField(predictorField).type)) {
        throw new IllegalArgumentException("Expected numeric input type for predictor, but received schema of type " + DataType.findTypeName(tupleSchema.getField(predictorField).type));
      }

      if (!DataType.isNumberType(tupleSchema.getField(targetField).type)) {
        throw new IllegalArgumentException("Expected numeric input type for target, but received schema of type " + DataType.findTypeName(tupleSchema.getField(targetField).type));
      }

      String resultName = "mrr_" + input.getField(0).schema.getField(0).schema.getField(targetField).alias;
      resultName += "_by_" + input.getField(0).schema.getField(0).schema.getField(predictorField).alias;

      // Construct our output schema consisting of a Double field
      return new Schema(new FieldSchema(resultName, DataType.DOUBLE));

    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }

  };
};

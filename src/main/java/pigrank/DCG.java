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
 * Pig UDF to compute (normalized) discounted cumulative gain or rank-weighted average.
 *
 * Called with an unordered bag.
 *
 * <pre>
 * Example use in a pig script:
 *
 * -- nDCG without rank cutoff
 * -- the second column contains ranking scores, the third one the target
 * define NDCG pigrank.DCG('normalized', '-1', '1', '2');
 *
 * data = load 'input' using PigStorage('\t') as (
 *         query:chararray,
 *         score:double,
 *         target:double
 * );
 *
 * data_gr = group data by query;
 *
 * eval = foreach data_gr
 * generate
 *         flatten(group) as query,
 *         NDCG(data)     as ndcg
 * ;
 *
 * store eval into 'output';
 * </pre>
 */

public class DCG extends EvalFunc<Double> {

  public enum NormType { NORM_MAX, NORM_WEIGHT, NORM_NONE }

  int predictorField;
  int targetField;
  int minCols;
  int cutoff;
  NormType normType;
  Ranking ranking;

  /**
   * Constructor for DCG function
   *
   * @param normalization normalization mode. One of three strings:
   *   - "normalized": divide DCG by maximum achievable (i.e., nDCG).
   *   - "weighted_average": divide DCG by total sum of logarithmic discount factors.
   *   - "unnormalized": absolute DCG.
   * @param strCutoff maximum rank to consider in measure, as a string. Values of zero
   *   or less are interpreted as 'no cutoff'.
   * @param strPredictorField zero-based column index of ranking score,
   *   as a string.
   * @param strTargetField zero-based column index of ranking score,
   *   as a string.
   */

  public DCG(String normalization, String strCutoff, String strPredictorField, String strTargetField) throws IllegalArgumentException {

    normalization = normalization.toLowerCase();

    if (normalization.equals("normalized")) {
      normType = NormType.NORM_MAX;
    } else if (normalization.equals("weighted_average")) {
      normType = NormType.NORM_WEIGHT;
    } else if (normalization.equals("unnormalized")) {
      normType = NormType.NORM_NONE;
    } else {
      throw new IllegalArgumentException("unknown normalization '" + normalization + "', expected one of 'normalized', 'weighted_average', or 'unnormalized'");
    }
    cutoff = Integer.parseInt(strCutoff);
    if (cutoff <= 0) {
      cutoff = Integer.MAX_VALUE;
    }
    predictorField = Integer.parseInt(strPredictorField);
    targetField = Integer.parseInt(strTargetField);
    minCols = 1 + Math.max(predictorField, targetField);
  }

  /**
   * Entry point for UDF
   *
   * @param input bag containing tuples of rows to rank
   * @return Double DCG value
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

      switch(normType) {
      case NORM_MAX:
        return ranking.getNDCG(cutoff);
      case NORM_WEIGHT:
        return ranking.getDCG(cutoff, true);
      default:
        return ranking.getDCG(cutoff, false);
      }

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

      // Construct our output schema consisting of a Double field

      String resultName = "";
      switch(normType) {
      case NORM_MAX:
        resultName = "ndcg";
        break;
      case NORM_WEIGHT:
        resultName = "rank_wtd_avg";
        break;
      default:
        resultName = "dcg";
        break;
      }
      if (cutoff <  Integer.MAX_VALUE) {
        resultName = resultName + "_" + cutoff;
      }

      resultName += "_" + input.getField(0).schema.getField(0).schema.getField(targetField).alias;
      resultName += "_by_" + input.getField(0).schema.getField(0).schema.getField(predictorField).alias;

      return new Schema(new FieldSchema(resultName, DataType.DOUBLE));

    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }

  };
};

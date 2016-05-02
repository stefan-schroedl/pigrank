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
 * Pig UDF to compute similarity score of two rankings.
 *
 * Called with two unordered bags.
 *
 * Supports the following three similarity functions:
 * <ul>
 * <li> Jaccard coefficient: size of intersection over size of union
 * <li> Cosine similarity: cosine between two vectors with dimensions corresponding
 *   to items, and inverse ranks as weights.
 * <li>Rank-biased Overlap (RBO): set overlap at common prefix length, averaged over
 *     exponential user top-down exploration. RBO accounts for uneven list size across
 *   rankings and queries. See <a href="http://www.umiacs.umd.edu/~wew/papers/wmz10_tois.pdf">this paper</a>
 *   for details.
 * </ul>
 * <pre>
 * Example use in a pig script:
 *
 * define JACCARD   pigrank.Similarity('jaccard', '-1', '2', '3', '2', '3');
 * define JACCARD_2 pigrank.Similarity('jaccard', '2', '2', '3', '2', '3');
 * define COSINE    pigrank.Similarity('cosine', '-1', '2', '3', '2', '3');
 * define RBO       pigrank.Similarity('rbo', '0.9', '2', '3', '2', '3');
 *
 * data = load 'input' using PigStorage('\t') as (
 *         query:chararray,
 *         treatment:chararray,
 *         asin:chararray,
 *         score:double
 * );
 *
 * data_gr = group data by (query, treatment);
 *
 * data_gr = foreach data_gr
 * generate
 *         flatten(group) as (query, treatment),
 *         data
 * ;
 *
 * split data_gr into data1 if treatment=='t1', data2 otherwise;
 *
 * side_by_side = cogroup data1 by query, data2 by query;
 *
 * eval = foreach side_by_side
 * generate
 *         flatten(group) as query,
 *         flatten(data1.data) as group_t1,
 *         flatten(data2.data) as group_t2
 * ;
 *
 * describe eval;
 *
 * eval = foreach eval
 * generate
 *         query,
 *         JACCARD(group_t1, group_t2),
 *         JACCARD_2(group_t1, group_t2),
 *         COSINE(group_t1, group_t2),
 *         RBO(group_t1, group_t2)
 * ;
 *
 * store eval into 'output';
 * </pre>
 *
 */

public class Similarity extends EvalFunc<Double> {

  public enum SimType { SIM_JACCARD, SIM_COSINE, SIM_RBO }

  /** type of similarity algorithm */
  SimType simType;

  /** unique item identifiers for the two bags (column indices) */
  int[] idField;

  /** ranking scores for the two bags (column indices) */
  int[] predictorField;

  /** minimum number of columns expected in data tuples */
  int[] minCols;

  /** rank cutoff for jaccard and cosine similarity */
  int cutoff;

  /** persistence probability for rbo similarity */
  double persistence;

  /**
   * Constructor for Similarity function
   *
   * @param strSimType type of similarity function, one of "jaccard", "cosine", or "rbo".
   * @param strParam parameter for similarity function.
   *        - for "jaccard" or "cosine", the maximum rank to include (cutoff).
   *          Values less than one are interpreted as 'no cutoff'.
   *        - for "rbo", the "persistence" probability that the user will look
   *          at the next rank. Typical values: 0.9 (resp. 0.98) means that the
   *          first 10 (resp. 50) ranks have 86% of the weight if the evaluation.
   * @param strIDField1 unique identifier for items in the first bag, used to test
   *        for equality with items in the second bag (zero-based column index).
   * @param strPredictorField1 zero-based column index of ranking score for the
   *        first bag (column index).
   * @param strIDField2 identifier for items in the second bag (column index).
   * @param strPredictorField2 ranking score for the second bag (xolumn index).
   */

  public Similarity(String strSimType, String strParam,
                    String strIDField1, String strPredictorField1,
                    String strIDField2, String strPredictorField2) throws IllegalArgumentException {

    strSimType = strSimType.toLowerCase();
    if (strSimType.equals("jaccard")) {
      simType = SimType.SIM_JACCARD;
    } else if (strSimType.equals("cosine")) {
      simType = SimType.SIM_COSINE;
    } else if (strSimType.equals("rbo")) {
      simType = SimType.SIM_RBO;
    } else {
      throw new IllegalArgumentException("unknown similiarity type '" + strSimType + "', expected one of 'jaccard', 'cosine', or 'rbo'");
    }

    if (simType == SimType.SIM_JACCARD || simType == SimType.SIM_COSINE) {
      cutoff = Integer.parseInt(strParam);
      if (cutoff <= 0) {
        cutoff = Integer.MAX_VALUE;
      }
    } else {
      persistence = Double.parseDouble(strParam);
    }

    idField = new int[2];
    predictorField = new int[2];
    minCols = new int[2];

    idField[0] = Integer.parseInt(strIDField1);
    predictorField[0] = Integer.parseInt(strPredictorField1);
    minCols[0] = 1 + Math.max(idField[0], predictorField[0]);

    idField[1] = Integer.parseInt(strIDField2);
    predictorField[1] = Integer.parseInt(strPredictorField2);
    minCols[1] = 1 + Math.max(idField[1], predictorField[1]);
  }

  /**
   * Entry point for UDF
   *
   * @param input tuple of 2 bags containing rows to rank
   * @return Double similarity value
   */

  @Override
  public Double exec(Tuple input) throws IOException {

    if (input == null || input.size() != 2) {
      throw new ExecException(
        "Expecting two bags, but found "
        + input.toString());
    }

    Ranking[] ranking = new Ranking[2];

    try {

      // iterate over the two ranked lists
      for (int i = 0; i < 2; i++) {
        DataBag bag = (DataBag)input.get(i);
        if(bag == null)
          return null;

        ranking[i] = new Ranking((int)bag.size());

        Iterator it = bag.iterator();
        while (it.hasNext()){
          Tuple t = (Tuple)it.next();

          if (t == null || t.size() < minCols[i]) {
            System.err.println(this.getClass().getSimpleName() + " expected tuple with at least " + minCols[i] + "columns, got  " + t);
            return null;
          };

          if (t.get(idField[i]) != null && t.get(predictorField[i]) != null) {
            String id = DataType.toString(t.get(idField[i]));
            Double pred = DataType.toDouble(t.get(predictorField[i]));
            ranking[i].addItem(id, pred, 0.0);
          }
        }

        ranking[i].rank();
      }

      switch(simType) {
      case SIM_JACCARD:
        return ranking[0].jaccardSimilarity(ranking[1], cutoff);
      case SIM_COSINE:
        return ranking[0].cosineSimilarity(ranking[1], cutoff);
      default:
        return ranking[0].rboSimilarity(ranking[1], persistence);
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
      if (input == null || input.size() != 2) {
        throw new IllegalArgumentException("Expected two bags as argument; found: " + input);
      }

      for (int i = 0; i < 2; i++) {

        if (input.getField(i).type != DataType.BAG) {
          throw new IllegalArgumentException("Expected a bag, found: " + DataType.findTypeName(input.getField(i).type));
        }

        Schema bagSchema = input.getField(i).schema;
        Schema tupleSchema = bagSchema.getField(0).schema;

        if (tupleSchema.size() < minCols[i]) {
          throw new IllegalArgumentException("The tuple must contain at least " + minCols[i] + " columns");
        }

        if (!DataType.isNumberType(tupleSchema.getField(predictorField[i]).type)) {
          throw new IllegalArgumentException("Expected numeric input type for predictor, but received schema of type " + DataType.findTypeName(tupleSchema.getField(predictorField[i]).type));
        }
      }

      // construct output field name
      String resultName = "";
      switch(simType) {
      case SIM_JACCARD:
        resultName = "jaccard_sim";
        break;
      case SIM_COSINE:
        resultName = "cosine_sim";
        break;
      default:
        resultName = "rbo_sim";
        break;
      }
      if (simType != SimType.SIM_RBO) {
        if (cutoff <  Integer.MAX_VALUE) {
          resultName += "_" + cutoff;
        }
      } else {
        resultName += "_" + persistence;
      }

      // if the two ranking score names are different, add to name
      String predField1 = input.getField(0).schema.getField(0).schema.getField(predictorField[0]).alias;
      String predField2 = input.getField(1).schema.getField(0).schema.getField(predictorField[1]).alias;
      if (!predField1.equals(predField2)) {
        resultName += "_" + predField1 + "_" + predField2;
      }

      // Construct our output schema consisting of a Double field
      return new Schema(new FieldSchema(resultName, DataType.DOUBLE));

    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }

  };
};

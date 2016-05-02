package pigrank;


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.HashMap;

/** auxiliary class to represent one row in a ranked list. */
class RankItem implements Comparable<RankItem> {

  /**
   * @param id unique identifier for item; used for comparison with other ranking
   * @param score rank score
   * @param target target value
   */
  public RankItem(String id, double score, double target) {
    this.id = id;
    this.score = score;
    this.target = target;
  };

  /** order descending by first element **/
  @Override
  public int compareTo(RankItem o2) {
    double diff = ((RankItem) o2).score - this.score;
    return (diff < 0 ? -1 : (diff > 0 ? 1 : 0));
  };

  /** unique identifier used for similarity measures */
  public String id;

  /** score used for ranking */
  public double score;

  /** target used for ranking quality measures */
  public double target;
};


/**
 * class to compute rank measures.
 *
 * usage:
 * <ol>
 * <li>repeatly call method addItem() with id/score/target triples, for each row 
 *    in the query group.
 * <li>method rank() sorts the items in decreasing order of score.
 * <ol>
 * <li>retrieve a rank measure (e.g., getMRR(), getDCG(), ...), or
 * <li>retrieve a ranking similarity to another ranking (jaccardSimilarity(), ...)
 * </ol>
 * </ol>
 * note: MRR and DCG are sensitive to how ties are broken, so we compute
 * these values as an expectation over all possible permutations of
 * the tied items. <p>
 * if there are items with the same score on either side of the
 * rank cutoff, we consider all items with this score. Therefore
 * items ranked greater than max_rank aren't strictly ignored.
 */

public class Ranking extends ArrayList<RankItem> {

  protected static final double LOG2 = Math.log(2.0);

  /**
   * @param size initial memory capacity allocated
   */
  public Ranking(int size) {
    ensureCapacity(size);
  }

  public void addItem(String id, double score, double target) {
    add(new RankItem(id, score, target));
  }

  /**
   * Sort items in decreasing order of rank score
   */
  public void rank() {
    Collections.sort(this);
  }

  public double getTarget(int i) {
    return get(i).target;
  }

  public double getScore(int i) {
    return get(i).score;
  }

  public String getID(int i) {
    return get(i).id;
  }

  public String toString() {
    StringBuffer strBuf = new StringBuffer();
    for (int i = 0; i < size(); i++) {
      strBuf.append(i).append("\t").append(getID(i)).append("\t").append(getScore(i)).append("\t").append(getTarget(i));
    }
    return strBuf.toString();
  }

  /**
   * mean reciprocal rank
   * @param cutoff ranks greater than this are ignored
   * @return mrr value
   */
  public double getMRR(int cutoff) {
    int maxIter = Math.min(size(), cutoff);
    int tiedCount = 0;        // number of items with same score as current one
    int tiedTargetCount = 0;  // number of positive targets with same score as current one
    double lastScore = Double.POSITIVE_INFINITY;
    int tiedTopRank = 0;
    for (int i = 0; i < maxIter || (i < size() && tiedTargetCount > 0); i++) {
      double score = getScore(i);
      if (score != lastScore) {
        if (tiedTargetCount > 0) {
          // positive target found, lower items are irrelevant
          break;
        }

        // reset tie counters
        tiedTopRank = i;
        tiedCount = 1;
        tiedTargetCount = (getTarget(i) > 0.0) ? 1 : 0;
        lastScore = score;

      } else {
        // score == lastScore
        tiedCount++;
        if (getTarget(i) > 0.0) {
          tiedTargetCount++;
        }
      }
    }

    if (tiedTargetCount > 0) {
      // we iterate over each position and determine the probability that the *first*
      // non-zero target is in that position
      double expMrr = 0.0;
      double pNoPos = 1.0; // P(t[0:i-1]=0) - probability that we haven't seen a positive yet
      for (int j = 0; j < tiedCount - tiedTargetCount + 1; j++) {
        double pPos = ((double) tiedTargetCount) / (tiedCount - j); // P(t[j]=1|t[0:j-1]=0)
        expMrr += pNoPos * pPos / (tiedTopRank + j + 1);
        pNoPos *= (1.0 - pPos);
      }
      return expMrr;
    }


    return 0.0;
  }

  /**
   * discounted cumulative gain
   * @param cutoff ranks greater than this are ignored
   * @param normalized if true, divide by total position weight
   * @return DCG value
   */
  public double getDCG(int cutoff, boolean normalized) {

    double sum = 0.0;        // discounted sum of target values
    double sumWeight = 0.0;  // sum of discount factors

    int tiedCount = 0;       // number of items with same score as current one
    double tiedWeight = 0.0; // sum of discount factors for items with the current item's score
    double tiedSum = 0;      // sum of targets with the current item's score
    double lastScore = Double.POSITIVE_INFINITY;

    for (int i = 0; i < size(); i++) {

      double weight = LOG2 / Math.log(2.0 + i);
      double score = getScore(i);
      if (score != lastScore && tiedWeight > 0.0) {

        // expected DCG for a set of tied items is just
        // (mean of targets) x (sum of the discount factors)
        sumWeight += tiedWeight;
        sum += tiedWeight * tiedSum / tiedCount;

        // reset tie counters
        tiedCount = 0;
        tiedWeight = 0;
        tiedSum = 0.0;
        lastScore = score;

        if (i >= cutoff) {
          break;
        }
      }

      tiedCount++;
      if (i < cutoff) {
        // record all targets for tied ASINs, but only record discount
        // factors for top min(size(), cutoff) positions
        tiedWeight += weight;
      }
      tiedSum += getTarget(i);
    }

    // last group of ties
    if (tiedWeight > 0.0) {
      sumWeight += tiedWeight;
      sum += tiedWeight * tiedSum / tiedCount;
    }

    return normalized ? sum / sumWeight : sum;
  }

  // best possible DCG under perfect ranking
  public double getMaxDCG(int cutoff) {
    ArrayList<Double> targets = new ArrayList<Double>(size());
    for (int i = 0; i < size(); i++) {
      targets.add(getTarget(i));
    }
    Collections.sort(targets, Collections.reverseOrder());

    double sum = 0.0;

    for (int i = 0; i < Math.min(size(), cutoff); i++) {
      sum += targets.get(i) / Math.log(2.0 + i);
    }

    return sum * LOG2;
  }

  public double getNDCG(int cutoff) {
    double maxDCG = getMaxDCG(cutoff);
    if (maxDCG == 0.0) {
      return 0.0;
    }
    return getDCG(cutoff, false) / maxDCG;
  }

  /** set-based similarity: cardinality of intersection, divided by
   * cardinality of union
   *
   * @param other another ranking to compare this one with
   * @param cutoff maximum number of top items to include in comparison
   * @return jaccard coefficient [0-1]
   */
  public double jaccardSimilarity(Ranking other, int cutoff) {

    HashSet<String> ids = new HashSet<String>(2*size());
    for (int i = 0; i < Math.min(size(), cutoff); i++) {
      ids.add(getID(i));
    }

    HashSet<String> idsOther = new HashSet<String>(2*other.size());
    for (int i = 0; i < Math.min(other.size(), cutoff); i++) {
      idsOther.add(other.getID(i));
    }

    HashSet<String> union = new HashSet<String>(ids);
    union.addAll(idsOther);

    if (union.size() == 0) {
      return 0.0;
    }

    HashSet<String> intersection = new HashSet<String>(ids);
    intersection.retainAll(idsOther);

    return (double)intersection.size() / union.size();
  }

  /**
   * returns a cosine between two vectors in n-dimensional space with
   * comparable objects in a sequence serving as its coordinates and
   * the i-th coordinate - having value 1/i
   *
   * @param other another ranking to compare this one with
   * @param cutoff maximum number of top items to include in comparison
   * @return cosine similarity value [0-1]
   */
  public double cosineSimilarity(Ranking other, int cutoff) {

    // swap such that r1 is the shorter ranking
    Ranking r1;
    Ranking r2;
    if (size() > other.size()) {
      r1 = other;
      r2 = this;
    } else {
      r1 = this;
      r2 = other;
    }

    int s1 = Math.min(r1.size(), cutoff);
    int s2 = Math.min(r2.size(), cutoff);

    if (s1 == 0 || s2 == 0) {
      return 0.0;
    }

    // build hash item -> weight
    HashMap<String, Double> idWeight = new HashMap<String, Double>(2*r1.size());
    double sumWtWt = 0.0; // sum of squared weights
    for (int i = 0; i < s1; i++) {
      double wt = 1.0 / (i + 1.0);
      sumWtWt += wt * wt;
      idWeight.put(r1.getID(i), wt);
    }

    double prod = 0.0; // scalar product of vectors

    for (int i = 0; i < s2; i++) {
      String id = r2.getID(i);
      if (idWeight.containsKey(id)) {
        prod += idWeight.get(id) / (i + 1.0);
      }
    }

    double sumWtWt2 = sumWtWt;
    // sum of squared weights of longer vector
    for (int i = s1; i < s2; i++) {
      sumWtWt2 += 1.0 / ((i + 1.0) * (i + 1.0));
    }

    return prod /  Math.sqrt(sumWtWt * sumWtWt2);
  }

  /** Rank biased overlap score (RBO)
   *
   * @see <a href="http://www.umiacs.umd.edu/~wew/papers/wmz10_tois.pdf">Webber et al (2010), A Similarity Measure for Indefinite Ranking</a>
   *
   * @param other another ranking to compare this one with
   * @param p probability of scanning the next result in the list
   * @return rank-based overlap score [0-1]
   */
  public double rboSimilarity(Ranking other, double p) {

    // swap such that r1 is the shorter ranking
    Ranking r1;
    Ranking r2;
    if (size() > other.size()) {
      r1 = other;
      r2 = this;
    } else {
      r1 = this;
      r2 = other;
    }

    int s1 = r1.size();
    int s2 = r2.size();

    if (s1 == 0 || s2 == 0) {
      return 0.0;
    }

    // set of items seen in first list, up to current rank
    HashSet<String> ids1 = new HashSet<String>(2*s1);
    // set of items seen in second list, up to current rank
    HashSet<String> ids2 = new HashSet<String>(2*s2);

    double sum1 = 0.0; // prefix score up to current rank
    double[] overlap = new double[s2+1]; // size of intersection, for each (1-based) rank
    overlap[0] = 0;
    for (int i = 0; i < s1; i++) {
      int d = i + 1;
      String id1 = r1.getID(i);
      String id2 = r2.getID(i);
      overlap[d] = overlap[i];
      if (id1.equals(id2)) {
        if (!(ids1.contains(id1) && ids2.contains(id2))) {
          overlap[d] += 1.0;
        }
      } else {
        // id1 != id2
        if (ids2.contains(id1) && (!ids1.contains(id1))) {
          overlap[d] += 1.0;
        }
        if (ids1.contains(id2) && (!ids2.contains(id2))) {
          overlap[d] += 1.0;
        }
      }

      ids1.add(id1);
      ids2.add(id2);

      sum1 += overlap[d]/d * Math.pow(p, d);
    }

    // continue with rest of longer list
    for (int i = s1; i < s2; i++) {
      int d = i + 1;
      String id2 = r2.getID(i);
      overlap[d] = overlap[i];
      if (!ids2.contains(id2)) {
        if (ids1.contains(id2)) {
          overlap[d] += 1;
        }
        ids2.add(id2);
      }
      sum1 += overlap[d]/d * Math.pow(p, d);
    }

    double sum2 = 0.0;
    for (int i = s1; i < s2; i++) {
      int d = i + 1;
      sum2 += overlap[s1] * (d-s1) / (d*s1) * Math.pow(p,d);
    }

    double sum3 = ((overlap[s2]-overlap[s1])/s2 +overlap[s1]/s1) * Math.pow(p, s2);

    // eq. 32 in the paper
    return (1.0 - p)/p * (sum1 + sum2) + sum3;
  }

};

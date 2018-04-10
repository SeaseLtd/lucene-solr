/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.queries.mlt.query;

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.queries.mlt.MoreLikeThisParameters;
import org.apache.lucene.queries.mlt.MoreLikeThisTestBase;
import org.apache.lucene.queries.mlt.terms.scorer.ScoredTerm;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.PriorityQueue;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

public class MoreLikeThisQueryBuilderTest extends MoreLikeThisTestBase {
  private MoreLikeThisQueryBuilder builderToTest;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    builderToTest = new MoreLikeThisQueryBuilder(getDefaultParams());
  }

  @Test
  public void boostOff_shouldBuildQueryWithNoBoost() throws Exception {
    MoreLikeThisParameters defaultParams = getDefaultParams();
    builderToTest = new MoreLikeThisQueryBuilder(defaultParams);
    PriorityQueue<ScoredTerm> interestingTerms = this.buildInterestingTermsQueue();

    Query query = builderToTest.createQuery(interestingTerms);

    assertThat(query.toString(), is("field2:term5 field1:term3 field1:term2 field1:term1 field2:term4"));
  }

  @Test
  public void boostOn_shouldBuildQueryWithDefaultBoost() throws Exception {
    MoreLikeThisParameters params = getDefaultParams();
    MoreLikeThisParameters.BoostProperties boostConfiguration = params.getBoostConfiguration();
    boostConfiguration.setBoost(true);
    builderToTest = new MoreLikeThisQueryBuilder(params);
    PriorityQueue<ScoredTerm> interestingTerms = this.buildInterestingTermsQueue();

    Query query = builderToTest.createQuery(interestingTerms);

    assertThat(query.toString(), is("(field2:term5)^1.0 (field1:term3)^3.0 (field1:term2)^4.0 (field1:term1)^5.0 (field2:term4)^7.0"));
  }

  @Test
  public void boostOn_singleBoostAcrossFields_shouldBuildQueryPreservingTermsRelationAndBoost() throws Exception {
    MoreLikeThisParameters params = getDefaultParams();
    params.setBoost(true);
    params.setQueryTimeBoostFactor(2.0f);
    builderToTest = new MoreLikeThisQueryBuilder(params);
    PriorityQueue<ScoredTerm> interestingTerms = this.buildInterestingTermsQueue();

    Query query = builderToTest.createQuery(interestingTerms);

    assertThat(query.toString(), is("(field2:term5)^2.0 (field1:term3)^6.0 (field1:term2)^8.0 (field1:term1)^10.0 (field2:term4)^14.0"));
  }

  @Test
  public void boostOn_differentBoostAcrossFields_shouldBuildQueryPreservingTermsRelation() throws Exception {
    MoreLikeThisParameters params = getDefaultParams();
    params.setBoost(true);
    Map<String, Float> fieldToQueryTimeBoostFactor = new HashMap<>();
    fieldToQueryTimeBoostFactor.put("field1",2.0f);
    fieldToQueryTimeBoostFactor.put("field2",3.0f);
    params.setFieldToQueryTimeBoostFactor(fieldToQueryTimeBoostFactor);
    builderToTest = new MoreLikeThisQueryBuilder(params);
    PriorityQueue<ScoredTerm> interestingTerms = this.buildInterestingTermsQueue();

    Query query = builderToTest.createQuery(interestingTerms);
    //boost was already applied when extracting the interesting terms, to build the query it is ok to have it normalized
    assertThat(query.toString(), is("(field2:term5)^3.0 (field1:term3)^9.0 (field1:term2)^12.0 (field1:term1)^14.999999 (field2:term4)^20.999998"));
  }

  private PriorityQueue<ScoredTerm> buildInterestingTermsQueue() {
    ScoredTerm term1 = new ScoredTerm("term1", "field1", 0.5f);
    ScoredTerm term2 = new ScoredTerm("term2", "field1", 0.4f);
    ScoredTerm term3 = new ScoredTerm("term3", "field1", 0.3f);

    ScoredTerm term4 = new ScoredTerm("term4", "field2", 0.7f);
    ScoredTerm term5 = new ScoredTerm("term5", "field2", 0.1f);

    FreqQ queue = new FreqQ(5);
    queue.add(term1);
    queue.add(term2);
    queue.add(term3);
    queue.add(term4);
    queue.add(term5);

    return queue;
  }

  /**
   * PriorityQueue that orders words by score.
   */
  protected static class FreqQ extends PriorityQueue<ScoredTerm> {
    FreqQ(int maxSize) {
      super(maxSize);
    }

    @Override
    protected boolean lessThan(ScoredTerm a, ScoredTerm b) {
      return a.score < b.score;
    }
  }
}

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

package org.apache.lucene.queries.mlt.terms;

import java.io.IOException;
import java.io.Reader;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.mlt.MoreLikeThisParameters;
import org.apache.lucene.queries.mlt.terms.scorer.ScoredTerm;
import org.apache.lucene.queries.mlt.terms.scorer.TFIDFScorer;
import org.apache.lucene.queries.mlt.terms.scorer.TermScorer;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.SmallFloat;

public abstract class InterestingTermsRetriever {

  protected MoreLikeThisParameters parameters;
  protected TermScorer interestingTermsScorer = new TFIDFScorer();
  protected IndexReader ir;

  /**
   * Extract term frequencies from the field in input.
   * This is used when no term vector is stored for the specific field
   *
   * @param perFieldTermFrequencies a Map of terms and their frequencies per field
   */
  protected void updateTermFrequenciesCount(IndexableField field, DocumentTermFrequencies perFieldTermFrequencies)
      throws IOException {
    String fieldName = field.name();
    String fieldStringContent = field.stringValue();
    Reader fieldReaderContent = field.readerValue();

    Analyzer analyzer = parameters.getAnalyzer();
    if(parameters.getFieldToAnalyzer()!=null && parameters.getFieldToAnalyzer().get(fieldName)!=null){
      analyzer = parameters.getFieldToAnalyzer().get(fieldName);
    }
    final int maxNumTokensParsed = parameters.getMaxNumTokensParsed();

    if (analyzer == null) {
      throw new UnsupportedOperationException("To use MoreLikeThis without " +
          "term vectors, you must provide an Analyzer");
    }

    if (fieldStringContent != null) {
      try (TokenStream analysedTextStream= analyzer.tokenStream(fieldName, fieldStringContent)) {
        processTokenStream(perFieldTermFrequencies, fieldName, maxNumTokensParsed, analysedTextStream);
      }
    }else if(fieldReaderContent!=null){
      try (TokenStream analysedTextStream= analyzer.tokenStream(fieldName, fieldReaderContent)) {
        processTokenStream(perFieldTermFrequencies, fieldName, maxNumTokensParsed, analysedTextStream);
      }
    }
  }

  private void processTokenStream(DocumentTermFrequencies perFieldTermFrequencies, String fieldName, int maxNumTokensParsed, TokenStream analysedTextStream) throws IOException {
    int tokenCount = 0;
    // for every token
    CharTermAttribute termAtt = analysedTextStream.addAttribute(CharTermAttribute.class);
    analysedTextStream.reset();
    while (analysedTextStream.incrementToken()) {
      String word = termAtt.toString();
      tokenCount++;
      if (tokenCount > maxNumTokensParsed) {
        break;
      }
      if (isNoiseWord(word)) {
        continue;
      }
      perFieldTermFrequencies.increment(fieldName,word,1);
    }
    analysedTextStream.end();
  }

  protected int getTotalTermsCount(DocumentTermFrequencies perFieldTermFrequencies) {
    int totalTermsCount = 0;
    Collection<DocumentTermFrequencies.FieldTermFrequencies> termFrequencies = perFieldTermFrequencies.getAll();
    for (DocumentTermFrequencies.FieldTermFrequencies singleFieldTermFrequencies : termFrequencies) {
      totalTermsCount += singleFieldTermFrequencies.size();
    }
    return totalTermsCount;
  }

  /**
   * determines if the passed term is likely to be of interest in "more like" comparisons
   *
   * @param term The term being considered
   * @return true if should be ignored, false if should be used in further analysis
   */
  protected boolean isNoiseWord(String term) {
    int maxWordLen = parameters.getMaxWordLen();
    int minWordLen = parameters.getMinWordLen();
    final Set<?> stopWords = parameters.getStopWords();

    int len = term.length();
    if (minWordLen > 0 && len < minWordLen) {
      return true;
    }
    if (maxWordLen > 0 && len > maxWordLen) {
      return true;
    }
    return stopWords != null && stopWords.contains(term);
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

  public MoreLikeThisParameters getParameters() {
    return parameters;
  }

  public void setParameters(MoreLikeThisParameters parameters) {
    this.parameters = parameters;
  }
}

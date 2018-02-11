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

  /**
   * Given the term frequencies per field, this method creates a PriorityQueue based on Score.
   *
   * @param perFieldTermFrequencies a per field map of words keyed on the term(String) with Int objects representing frequencies as the values.
   */
  protected PriorityQueue<ScoredTerm> retrieveInterestingTerms(DocumentTermFrequencies perFieldTermFrequencies) throws IOException {
    final int minTermFreq = parameters.getMinTermFreq();
    final int maxQueryTerms = parameters.getMaxQueryTerms();
    final int minDocFreq = parameters.getMinDocFreq();
    final int maxDocFreq = parameters.getMaxDocFreq();
    final int queueSize = Math.min(maxQueryTerms, this.getTotalTermsCount(perFieldTermFrequencies));

    FreqQ interestingTerms = new FreqQ(queueSize); // will order words by score
    for (DocumentTermFrequencies.FieldTermFrequencies fieldTermFrequencies : perFieldTermFrequencies.getAll()) {
      String fieldName = fieldTermFrequencies.getFieldName();
      float fieldBoost = parameters.getPerFieldQueryTimeBoost(fieldName);
      CollectionStatistics fieldStats = new IndexSearcher(ir).collectionStatistics(fieldName);
      for (Map.Entry<String, DocumentTermFrequencies.Int> termFrequencyEntry : fieldTermFrequencies.getAll()) { // for every term
        String word = termFrequencyEntry.getKey();
        int tf = termFrequencyEntry.getValue().frequency; // term freq in the source doc

        if (minTermFreq > 0 && tf < minTermFreq) {
          continue; // filter out words that don't occur enough times in the source
        }

        final Term currentTerm = new Term(fieldName, word);
        int docFreq = ir.docFreq(currentTerm);

        if (docFreq == 0) {
          continue; //term not present in the index for that field, it's not possible to estimate how interesting it is
        }
        if (minDocFreq > 0 && docFreq < minDocFreq) {
          continue; // filter out words that don't occur in enough docs
        }

        if (docFreq > maxDocFreq) {
          continue; // filter out words that occur in too many docs
        }

        final TermStatistics currentTermStat = new TermStatistics(currentTerm.bytes(), docFreq, ir.totalTermFreq(currentTerm));
        float score = interestingTermsScorer.score(fieldName, fieldStats, currentTermStat, tf);
        // Boost should affect which terms ends up to be interesting
        score = fieldBoost * score;

        if (interestingTerms.size() < queueSize) {
          // there is still space in the interestingTerms
          interestingTerms.add(new ScoredTerm(word, fieldName, score));// there was idf, possibly we want the stats there
        } else {
          ScoredTerm minScoredTerm = interestingTerms.top();
          if (minScoredTerm.score < score) { // current term deserve a space as it is more interesting than the top
            minScoredTerm.update(word, fieldName, score);
            interestingTerms.updateTop();
          }
        }
      }
    }
    return interestingTerms;
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

/* $Id:$
 *
 * WordWar.
 * Copyright (C) 2012 Toke Eskildsen, te@ekot.dk
 *
 * This is confidential source code. Unless an explicit written permit has been obtained,
 * distribution, compiling and all other use of this code is prohibited.    
  */
package dk.ekot.luso;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.apache.lucene.util.PriorityQueue;

import java.util.*;
import java.util.concurrent.*;

/**
 * Hackish simple test of integer/float-pair sorting.
 */
@SuppressWarnings("ALL")
public class Luso {

  public static void main(String[] args) throws InterruptedException, ExecutionException {
    if (args.length < 3) {
      usage();
    }
    int threads = Integer.parseInt(args[1]);
    List<Integer> docs = new ArrayList<>(args.length-1);
    for (String arg: Arrays.asList(args).subList(2, args.length)) {
      docs.add(Integer.parseInt(arg));
    }

    ExecutorService executor = Executors.newFixedThreadPool(threads);
    System.out.println("Starting " + threads + " threads with extraction method " + args[0]);
    List<Future<String>> extractors = new ArrayList<>(threads);
    for (int i = 0 ; i < threads ; i++) {
      extractors.add(executor.submit(createExtractor(args[0], "extractor_" + (i+1), docs)));
    }
    for (Future<String> extractor: extractors) {
      System.out.println(extractor.get());
    }
    System.exit(0);
  }

  private static Callable<String> createExtractor(String method, String id, List<Integer> numdocs) {
    switch (method) {
      case "pq" : return new PriorityQueueExtract(id, numdocs);
      case "ip" : return new LuceneInPlaceExtract(id, numdocs);
      case "as" : return new LuceneArraySortExtract(id, numdocs);
      default:
        System.err.println("Extraction method '" + method + "' is unknown");
        usage();
    }
    throw new IllegalStateException("Logic error: Luso should have exited by now due to unknown extraction method");
  }

  private static void usage() {
    System.out.println("Usage: Luso method threads numdocs*");
    System.out.println("");
    System.out.println("Methods: pq (Priority Queue)");
    System.out.println("         ip (Lucene InPlace with atomic arrays)");
    System.out.println("         as (Array Sort with ScoreDocs)");
    System.exit(0);
  }

  private static abstract class Extractor implements Callable<String> {
    private final StringBuilder sb = new StringBuilder();
    private final String id;
    private final List<Integer> numdocs;

    public Extractor(String id, List<Integer> numdocs) {
      this.id = id;
      this.numdocs = numdocs;
    }

    @Override
    public String call() throws Exception {
      sb.append("Extraction method " + getName() + ": " + id + "\n");
      for (int numdoc: numdocs) {
        long extractTime = -System.nanoTime();
        extract(new Random(87), numdoc);
        extractTime += System.nanoTime();
        long randomTime = measureRandom(numdoc);
        sb.append(String.format("  %,10d docIDs in %,7d ms\n", numdoc, (extractTime-randomTime)/1000000));
      }
      return sb.toString();
    }

    public abstract void extract(Random random, int numdoc);

    private long measureRandom(int numdocs) {
      long startTime = System.nanoTime();
      final Random random = new Random(87);
      for (int i = 0 ; i < numdocs ; i++) {
        if (random.nextFloat() == 0.1f && i == 0) {
            throw new RuntimeException("Never thrown");
        }
      }
      return System.nanoTime() - startTime;
    }

    public abstract String getName();
  }

  private static class PriorityQueueExtract extends Extractor {
    private PriorityQueueExtract(String id, List<Integer> numdocs) {
      super(id, numdocs);
    }

    @Override
    public void extract(Random random, int numdoc) {
      PriorityQueue<ScoreDoc> queue = new PriorityQueue<ScoreDoc>(numdoc) {
          @Override
          protected ScoreDoc getSentinelObject() {
            // Always set the doc Id to MAX_VALUE so that it won't be favored by
            // lessThan. This generally should not happen since if score is not NEG_INF,
            // TopScoreDocCollector will always add the object to the queue.
            return new ScoreDoc(Integer.MAX_VALUE, Float.NEGATIVE_INFINITY);
          }

          @Override
          protected final boolean lessThan(ScoreDoc hitA, ScoreDoc hitB) {
            if (hitA.score == hitB.score)
              return hitA.doc > hitB.doc;
            else
              return hitA.score < hitB.score;
          }
      };

      for (int i = 0 ; i < numdoc ; i++) {
          queue.insertWithOverflow(new ScoreDoc(i, random.nextFloat()));
      }
      while (queue.size() > 0) {
          queue.pop();
      }
    }

    @Override
    public String getName() {
      return "pq (Priority Queue)";
    }
  }

  private static class LuceneInPlaceExtract extends Extractor {
    public LuceneInPlaceExtract(String id, List<Integer> numdocs) {
      super(id, numdocs);
    }

    @Override
    public void extract(Random random, int numdoc) {
      final int[] docs = new int[numdoc];
      final float[] scores = new float[numdoc];
      for (int i = 0 ; i < numdoc ; i++) {
          docs[i] = i;
          scores[i] = random.nextFloat();
      }
      new InPlaceMergeSorter() {
          @Override
          protected int compare(int i, int j) {
              return scores[i] == scores[j] ? 0 :
                      scores[i] < scores[j] ? -1 : 1;
          }

          @Override
          protected void swap(int i, int j) {
              float ts = scores[j];
              scores[j] = scores[i];
              scores[i] = ts;
              int td = docs[j];
              docs[j] = docs[i];
              docs[i] = td;
          }
      }.sort(0, numdoc);
      for (int i = 0 ; i < numdoc ; i++) {
          if (docs[i] == 1 && scores[i] == 0.1f) {
              throw new RuntimeException("Never thrown");
          }
      }
    }

    @Override
    public String getName() {
      return "ip (Lucene InPlace with atomic arrays)";
    }
  }

  private static class LuceneArraySortExtract extends Extractor {
    public LuceneArraySortExtract(String id, List<Integer> numdocs) {
      super(id, numdocs);
    }

    @Override
    public void extract(Random random, int numdoc) {
      final ScoreDoc[] docs = new ScoreDoc[numdoc];
      for (int i = 0 ; i < numdoc ; i++) {
          docs[i] = new ScoreDoc(i, random.nextFloat());
      }
      Arrays.sort(docs, new Comparator<ScoreDoc>() {
        @Override
        public int compare(ScoreDoc hitA, ScoreDoc hitB) {
          if (hitA.score == hitB.score) {
            return hitB.doc - hitA.doc;
          }
          return hitA.score < hitB.score ? -1 : 1;
        }
      });
      for (int i = 0 ; i < numdoc ; i++) {
          if (docs[i].doc == 1 && docs[i].score == 0.1f) {
              throw new RuntimeException("Never thrown");
          }
      }
    }

    @Override
    public String getName() {
      return "as (Array Sort with ScoreDocs)";
    }
  }
}

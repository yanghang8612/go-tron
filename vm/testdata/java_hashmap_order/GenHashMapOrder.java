// GenHashMapOrder.java — SV-2 golden-vector generator.
//
// Reproduces, with the SAME com.google.protobuf.ByteString and java.util.HashMap
// that java-tron's VoteWitnessProcessor.execute uses, the iteration order of
//   Map<ByteString,Long> voteMap = new HashMap<>();
//   ... voteMap.put(addr, count) ...
//   for (Map.Entry<ByteString,Long> e : voteMap.entrySet()) { ... }
// (VoteWitnessProcessor.java:54,83-84,105). That entrySet() order is the order
// in which votes are appended to the account's `votes` repeated field and thus
// serialized into state, so it is consensus-load-bearing.
//
// java-tron pins protobuf-java 3.25.8 (protocol/build.gradle), so ByteString
// .hashCode() here is byte-identical to the node's. We build the map exactly as
// the processor does (distinct 21-byte TRON addresses, first-seen insertion),
// then print, per case, the input addresses (insertion order) and the resulting
// entrySet() key order. The Go test asserts javaHashMapOrder() reproduces the
// entrySet() order for every case.
//
// Build/run (protobuf-java jar from the gradle cache):
//   JAR=~/.gradle/caches/modules-2/files-2.1/com.google.protobuf/protobuf-java/3.25.8/2ba593767658038775b2ea9724c3686609874470/protobuf-java-3.25.8.jar
//   javac -cp "$JAR" GenHashMapOrder.java
//   java  -cp "$JAR:." GenHashMapOrder > golden.txt
//
// Output format (one case per line):
//   <k> ; <in_hex_0>,<in_hex_1>,... ; <out_hex_0>,<out_hex_1>,...
// where in_hex are the insertion-order addresses and out_hex the entrySet() order.

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class GenHashMapOrder {

  static final int ADDR_LEN = 21; // TRON address: 0x41 + 20 bytes

  static String hex(byte[] b) {
    StringBuilder sb = new StringBuilder(b.length * 2);
    for (byte x : b) {
      sb.append(Character.forDigit((x >> 4) & 0xf, 16));
      sb.append(Character.forDigit(x & 0xf, 16));
    }
    return sb.toString();
  }

  // Build voteMap the way VoteWitnessProcessor does and print the case.
  static void emit(List<byte[]> addrs, StringBuilder out) {
    Map<ByteString, Long> voteMap = new HashMap<>();
    long c = 1;
    for (byte[] a : addrs) {
      ByteString key = ByteString.copyFrom(a);
      // getOrDefault + put mirrors the processor's merge; value is irrelevant to
      // iteration order but we keep the call shape identical.
      voteMap.put(key, voteMap.getOrDefault(key, 0L) + (c++));
    }
    StringBuilder in = new StringBuilder();
    for (int i = 0; i < addrs.size(); i++) {
      if (i > 0) {
        in.append(',');
      }
      in.append(hex(addrs.get(i)));
    }
    StringBuilder ord = new StringBuilder();
    boolean first = true;
    for (Map.Entry<ByteString, Long> e : voteMap.entrySet()) {
      if (!first) {
        ord.append(',');
      }
      first = false;
      ord.append(hex(e.getKey().toByteArray()));
    }
    out.append(voteMap.size()).append(" ; ").append(in).append(" ; ").append(ord).append('\n');
  }

  // finalCap as HashMap would size it for n distinct keys (default load 0.75,
  // initial table 16, threshold 12 → 16/24/32/48/64 ...). Used only to engineer
  // adversarial same-bucket addresses; the golden itself comes from real HashMap.
  static int tableSizeFor(int n) {
    int cap = 16;
    while (n > (int) (cap * 0.75f)) {
      cap <<= 1;
    }
    return cap;
  }

  static int spread(int h) {
    return h ^ (h >>> 16);
  }

  // ByteString.hashCode for protobuf 3.x: starts from size, then h*31 + (signed)
  // byte, with the empty-string→1 guard. Mirrored here ONLY to craft collisions;
  // correctness is checked against the real map's entrySet().
  static int byteStringHash(byte[] b) {
    int h = b.length;
    for (byte x : b) {
      h = h * 31 + x; // x is signed byte, sign-extended to int — matches protobuf
    }
    return h == 0 ? 1 : h;
  }

  static byte[] addrFromSeed(Random rnd) {
    byte[] a = new byte[ADDR_LEN];
    rnd.nextBytes(a);
    a[0] = 0x41;
    return a;
  }

  // Craft an address whose spread(hash) lands in `targetBucket` for `cap`.
  static byte[] addrInBucket(Random rnd, int targetBucket, int cap) {
    for (int tries = 0; tries < 1_000_000; tries++) {
      byte[] a = addrFromSeed(rnd);
      if ((spread(byteStringHash(a)) & (cap - 1)) == targetBucket) {
        return a;
      }
    }
    throw new RuntimeException("could not craft bucket address");
  }

  public static void main(String[] args) {
    StringBuilder out = new StringBuilder();
    // Header marks the generator version + protobuf version so a regen is auditable.
    out.append("# SV-2 java HashMap<ByteString,Long> entrySet() order golden\n");
    out.append("# protobuf-java 3.25.8 ; java.util.HashMap ; format: k ; in ; out\n");

    // 1) Random addresses, sizes 1..30, several trials each (fixed seeds → stable).
    for (int k = 1; k <= 30; k++) {
      for (int trial = 0; trial < 6; trial++) {
        Random rnd = new Random(0x5eed_0000L + k * 100L + trial);
        LinkedHashSet<String> seen = new LinkedHashSet<>();
        List<byte[]> addrs = new ArrayList<>();
        while (addrs.size() < k) {
          byte[] a = addrFromSeed(rnd);
          if (seen.add(hex(a))) {
            addrs.add(a);
          }
        }
        emit(addrs, out);
      }
    }

    // 2) Adversarial: many addresses forced into the SAME bucket (relative order
    //    must collapse to insertion order). Cover the table size for each k.
    for (int k = 2; k <= 30; k++) {
      int cap = tableSizeFor(k);
      for (int bucket : new int[] {0, 1, cap - 1, cap / 2}) {
        Random rnd = new Random(0xC0FFEE00L + k * 97L + bucket);
        List<byte[]> addrs = new ArrayList<>();
        LinkedHashSet<String> seen = new LinkedHashSet<>();
        while (addrs.size() < k) {
          byte[] a = addrInBucket(rnd, bucket, cap);
          if (seen.add(hex(a))) {
            addrs.add(a);
          }
        }
        emit(addrs, out);
      }
    }

    // 3) Resize-boundary stress: build exactly at k=12,13,24,25 (the thresholds
    //    where the table grows 16→32→64) with mixed buckets that straddle the
    //    high bit which decides lo/hi split on resize.
    for (int k : new int[] {11, 12, 13, 23, 24, 25, 30}) {
      for (int trial = 0; trial < 4; trial++) {
        int cap = tableSizeFor(k);
        Random rnd = new Random(0xBADD_5EedL + k * 31L + trial);
        List<byte[]> addrs = new ArrayList<>();
        LinkedHashSet<String> seen = new LinkedHashSet<>();
        // Force pairs into buckets b and b+oldCap so they share a final bucket but
        // came through different intermediate tables.
        int oldCap = cap >> 1;
        while (addrs.size() < k) {
          int b = rnd.nextInt(Math.max(1, oldCap));
          int targetBucket = (addrs.size() % 2 == 0) ? b : (b + oldCap) & (cap - 1);
          byte[] a = addrInBucket(rnd, targetBucket, cap);
          if (seen.add(hex(a))) {
            addrs.add(a);
          }
        }
        emit(addrs, out);
      }
    }

    System.out.print(out);
  }
}

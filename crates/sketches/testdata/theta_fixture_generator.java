// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Standalone fixture generator for iceberg-sketches. Emits the exact serialized bytes of theta
// CompactSketches in every mode, plus MurmurHash3 test vectors, all with seed 9001. See README.md
// for the compile/run incantation. NOT part of the Cargo build (a dev oracle, like dev/spark/).

import java.nio.charset.StandardCharsets;

import org.apache.datasketches.Family;
import org.apache.datasketches.Util;
import org.apache.datasketches.hash.MurmurHash3;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.UpdateSketch;

public class ThetaFixtureGenerator {
    static String hex(byte[] bytes) {
        StringBuilder builder = new StringBuilder();
        for (byte value : bytes) {
            builder.append(String.format("%02x", value & 0xff));
        }
        return builder.toString();
    }

    static void dumpSketch(String name, CompactSketch sketch) {
        byte[] bytes = sketch.toByteArray();
        System.out.println(name
            + " | bytes=" + bytes.length
            + " | empty=" + sketch.isEmpty()
            + " | ordered=" + sketch.isOrdered()
            + " | retained=" + sketch.getRetainedEntries()
            + " | theta=" + sketch.getThetaLong()
            + " | est=" + sketch.getEstimate()
            + " | hex=" + hex(bytes));
    }

    static void dumpHash(String label, byte[] bytes) {
        long[] hash = MurmurHash3.hash(bytes, Util.DEFAULT_UPDATE_SEED);
        System.out.println(label + " | " + Long.toUnsignedString(hash[0]) + " | " + Long.toUnsignedString(hash[1]));
    }

    public static void main(String[] args) {
        long seed = Util.DEFAULT_UPDATE_SEED;
        System.out.println("DEFAULT_UPDATE_SEED=" + seed);
        System.out.println("computeSeedHash(9001)=" + (Util.computeSeedHash(seed) & 0xffff));

        // ---- serialized-sketch fixtures ----
        UpdateSketch empty = UpdateSketch.builder().setSeed(seed).build();
        dumpSketch("EMPTY", empty.compact());

        UpdateSketch single = UpdateSketch.builder().setSeed(seed).build();
        single.update(1L);
        dumpSketch("SINGLE", single.compact());

        UpdateSketch exact = UpdateSketch.builder().setSeed(seed).build();
        for (long i = 0; i < 10; i++) {
            exact.update(i);
        }
        dumpSketch("EXACT10", exact.compact());

        UpdateSketch est = UpdateSketch.builder().setSeed(seed).setLogNominalEntries(4).build();
        for (long i = 0; i < 1000; i++) {
            est.update(i);
        }
        dumpSketch("EST1000_LGK4", est.compact());

        // Estimation-breadth fixtures (Y1-reviewer): a second seed-independent value set at lgK=8,
        // and the DEFAULT lgK=12 (4096 nominal — what Iceberg's theta_sketch_agg uses) at 100k.
        UpdateSketch est5000 = UpdateSketch.builder().setSeed(seed).setLogNominalEntries(8).build();
        for (long i = 0; i < 5000; i++) {
            est5000.update(1_000_000L + i * 7);
        }
        dumpSketch("EST5000_LGK8", est5000.compact());

        UpdateSketch est100k = UpdateSketch.builder().setSeed(seed).build(); // default lgK=12
        for (long i = 0; i < 100000; i++) {
            est100k.update(i);
        }
        dumpSketch("EST100K_LGK12", est100k.compact());

        // Unordered compact form (the ORDERED flag is clear; same set, hash-table order).
        dumpSketch("EXACT10_UNORDERED", exact.compact(false, null));

        // ---- ALPHA-family fixtures (Wave-5 Y3) ------------------------------------------------
        // The family Iceberg's NDV pipeline actually builds: ThetaSketchAgg.createAggregationBuffer
        // → UpdateSketch.builder.setFamily(Family.ALPHA).build(). In EXACT mode (theta==MAX) Alpha is
        // byte-identical to QuickSelect; in ESTIMATION mode they DIVERGE. Iceberg's `ndv` reads the
        // COMPACT sketch's getEstimate (NDVSketchUtil: CompactSketch.wrap(bytes).getEstimate()), which
        // is the STANDARD estimator, NOT the Alpha update sketch's sampling estimator.
        UpdateSketch alphaExact = UpdateSketch.builder().setSeed(seed).setFamily(Family.ALPHA).build();
        for (long i = 0; i < 10; i++) {
            alphaExact.update(i);
        }
        dumpSketch("ALPHA_EXACT10", alphaExact.compact()); // == EXACT10 bytes (equivalence pin)

        UpdateSketch alphaSingle =
            UpdateSketch.builder().setSeed(seed).setFamily(Family.ALPHA).build();
        alphaSingle.update(1L);
        dumpSketch("ALPHA_SINGLE", alphaSingle.compact()); // == SINGLE bytes

        UpdateSketch alphaEmpty =
            UpdateSketch.builder().setSeed(seed).setFamily(Family.ALPHA).build();
        dumpSketch("ALPHA_EMPTY", alphaEmpty.compact()); // == EMPTY bytes

        // Byte-inlinable estimation fixture: lgK 9 (Alpha minimum), 520 distinct longs `0..520`.
        // retained=514, theta=9080515283922012160, COMPACT getEstimate=522.0863661049558 → ndv 522.
        UpdateSketch alphaEst =
            UpdateSketch.builder().setSeed(seed).setFamily(Family.ALPHA).setLogNominalEntries(9).build();
        for (long i = 0; i < 520; i++) {
            alphaEst.update(i);
        }
        dumpSketch("ALPHA_EST_LGK9_520", alphaEst.compact());

        // Deep-resize estimation case: lgK 9, 50_000 distinct of a seed-independent value set (repeated
        // dirty rebuilds). retained=536, theta=99944646323968464, COMPACT getEstimate=49464.654... .
        UpdateSketch alphaDeep =
            UpdateSketch.builder().setSeed(seed).setFamily(Family.ALPHA).setLogNominalEntries(9).build();
        for (long i = 0; i < 50000; i++) {
            alphaDeep.update(1_000_000L + i * 7);
        }
        dumpSketch("ALPHA_DEEP_LGK9_50K", alphaDeep.compact());

        // The headline lgK=12-default estimation pins (only the COMPACT estimate / retained / theta are
        // inlined; the full multi-KB blobs are not). n=7000 → ndv 6963; n=1_000_000 → ndv 1_004_032.
        UpdateSketch alpha7000 = UpdateSketch.builder().setSeed(seed).setFamily(Family.ALPHA).build();
        for (long i = 0; i < 7000; i++) {
            alpha7000.update(i);
        }
        CompactSketch alpha7000c = alpha7000.compact();
        System.out.println("ALPHA_EST7000_LGK12 | retained=" + alpha7000c.getRetainedEntries()
            + " | theta=" + alpha7000c.getThetaLong()
            + " | compactEst=" + alpha7000c.getEstimate()
            + " | ndv=" + ((long) alpha7000c.getEstimate())
            + " | updateSamplingEst=" + alpha7000.getEstimate());

        UpdateSketch alpha1m = UpdateSketch.builder().setSeed(seed).setFamily(Family.ALPHA).build();
        for (long i = 0; i < 1_000_000L; i++) {
            alpha1m.update(i);
        }
        CompactSketch alpha1mc = alpha1m.compact();
        System.out.println("ALPHA_EST1M_LGK12 | retained=" + alpha1mc.getRetainedEntries()
            + " | theta=" + alpha1mc.getThetaLong()
            + " | compactEst=" + alpha1mc.getEstimate()
            + " | ndv=" + ((long) alpha1mc.getEstimate())
            + " | updateSamplingEst=" + alpha1m.getEstimate());

        // ---- MurmurHash3 vectors (byte tails 1..=18) ----
        String base = "0123456789abcdefXYZ";
        for (int len = 1; len <= 18; len++) {
            dumpHash("len" + len, base.substring(0, len).getBytes(StandardCharsets.US_ASCII));
        }
        // ---- MurmurHash3 vectors (representative longs) ----
        long[] longs = {0L, 1L, 2L, -1L, 123456789L, Long.MIN_VALUE, Long.MAX_VALUE};
        for (long value : longs) {
            long[] hash = MurmurHash3.hash(new long[]{value}, seed);
            System.out.println("long" + value + " | " + Long.toUnsignedString(hash[0]) + " | " + Long.toUnsignedString(hash[1]));
        }

        // ---- MurmurHash3 multi-block byte vectors (Y1-reviewer hash breadth) ----
        // pattern: byte[i] = (i*31+7) & 0xff, spanning many 16-byte blocks.
        for (int n : new int[]{32, 64, 100, 1000}) {
            byte[] pattern = new byte[n];
            for (int i = 0; i < n; i++) {
                pattern[i] = (byte) ((i * 31 + 7) & 0xff);
            }
            dumpHash("pattern" + n, pattern);
        }
        for (int n : new int[]{1, 8, 16, 17, 32, 33}) {
            dumpHash("zeros" + n, new byte[n]);
        }
        for (int n : new int[]{1, 7, 8, 15, 16, 17, 31, 32}) {
            byte[] ones = new byte[n];
            java.util.Arrays.fill(ones, (byte) 0xff);
            dumpHash("ff" + n, ones);
        }
        // ---- MurmurHash3 multi-element long[] vectors ----
        long[][] arrays = {{1L, 2L}, {1L, 2L, 3L}, {1L, 2L, 3L, 4L}, {0L, 0L, 0L}, {-1L, -1L}};
        for (long[] array : arrays) {
            long[] hash = MurmurHash3.hash(array, seed);
            System.out.println("longarr" + java.util.Arrays.toString(array)
                + " | " + Long.toUnsignedString(hash[0]) + " | " + Long.toUnsignedString(hash[1]));
        }
    }
}

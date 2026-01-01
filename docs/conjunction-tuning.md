# Conjunction Detection Parameter Tuning

This document records tuning experiments for the coarse-fine conjunction detection algorithm.

## Parameters

- **tolerance-km**: Coarse detection threshold (also affects pair reduction geometry)
- **step-seconds**: Time step for coarse sweep. It must be <= tolerance-km / 15, since 15 km/s is approx. max relative
  speed of two LEO satellites
- **lookahead-hours**: How far ahead to scan (fixed at 6 hours for benchmarking)
- **threshold-km**: Final conjunction threshold (fixed at 5.0 km)

## Sample Size Comparison

The following table proves that results on a random 25% satellite sample (7,397 satellites) scale predictably to the
full catalog (29,586 satellites). Therefore, tuning can be done with confidence on the smaller sample for faster
iteration.

| Sats   | Tol (km) | Step (s) | Hours | Detections | Events | Conj  | Dedup | Coarse  | Refine | Total   |
|--------|----------|----------|-------|------------|--------|-------|-------|---------|--------|---------|
| 29,586 | 50       | 4        | 6     | 2.56M      | 563K   | 3,958 | 3,470 | 1223.4s | 27.1s  | 1256.9s |
| 7,397  | 50       | 4        | 6     | 155K       | 35K    | 222   | 202   | 318.7s  | 0.9s   | 319.7s  |

## Tuning Results

Benchmark results from 25% satellite catalog (7,397 satellites).

| Config           | Detections | Events | Conj | Dedup | Coarse | Refine | **Total** |
|------------------|------------|--------|------|-------|--------|--------|-----------|
| tol=60, step=4   | 233K       | 50K    | 252  | 216   | 304.2s | 1.4s   | 305.8s    |
| tol=75, step=5   | 329K       | 79K    | 252  | 216   | 249.2s | 2.2s   | 251.6s    |
| tol=90, step=6   | 452K       | 115K   | 250  | 215   | 207.3s | 3.2s   | 210.7s    |
| tol=105, step=7  | 600K       | 154K   | 251  | 215   | 177.1s | 5.0s   | 182.5s    |
| tol=120, step=8  | 766K       | 194K   | 251  | 215   | 155.0s | 6.3s   | 161.8s    |
| tol=135, step=9  | 923K       | 239K   | 250  | 214   | 136.3s | 7.5s   | 144.3s    |
| tol=150, step=10 | 1.1M       | 283K   | 253  | 216   | 127.2s | 9.6s   | 137.5s    |
| tol=165, step=11 | 1.3M       | 329K   | 250  | 214   | 110.9s | 10.3s  | 121.9s    |
| tol=180, step=12 | 1.4M       | 374K   | 249  | 214   | 106.0s | 12.9s  | 119.8s    |
| tol=195, step=13 | 1.6M       | 420K   | 253  | 217   | 96.6s  | 14.7s  | 112.3s    |
| tol=210, step=14 | 1.8M       | 469K   | 250  | 214   | 91.1s  | 18.3s  | 110.5s    |
| tol=225, step=15 | 1.9M       | 520K   | 250  | 214   | 85.0s  | 20.0s  | 106.2s    |
| tol=240, step=16 | 2.1M       | 570K   | 252  | 215   | 79.8s  | 22.4s  | 103.5s    |
| tol=255, step=17 | 2.3M       | 619K   | 248  | 212   | 74.7s  | 23.7s  | 99.9s     |
| tol=270, step=18 | 2.5M       | 667K   | 248  | 212   | 68.1s  | 23.6s  | 93.1s     |
| tol=285, step=19 | 2.7M       | 712K   | 250  | 214   | 64.5s  | 25.0s  | 91.1s     |
| tol=300, step=20 | 2.8M       | 758K   | 252  | 216   | 63.7s  | 28.9s  | 94.3s     |
| tol=315, step=21 | 3.0M       | 802K   | 250  | 214   | 59.1s  | 29.3s  | 90.1s     |
| tol=330, step=22 | 3.1M       | 847K   | 248  | 212   | 56.5s  | 29.5s  | 87.9s     |
| tol=345, step=23 | 3.3M       | 893K   | 250  | 214   | 53.4s  | 30.9s  | 86.3s     |
| tol=360, step=24 | 3.5M       | 938K   | 250  | 215   | 51.4s  | 31.8s  | 85.2s     |
| tol=375, step=25 | 3.6M       | 982K   | 249  | 213   | 49.4s  | 33.5s  | 85.0s     |
| tol=390, step=26 | 3.8M       | 1.0M   | 253  | 217   | 47.7s  | 38.7s  | 88.6s     |
| tol=405, step=27 | 3.9M       | 1.1M   | 252  | 216   | 45.8s  | 40.7s  | 88.7s     |
| tol=420, step=28 | 4.1M       | 1.1M   | 250  | 214   | 44.9s  | 44.7s  | 92.1s     |
| tol=435, step=29 | 4.2M       | 1.2M   | 251  | 215   | 43.4s  | 45.0s  | 90.9s     |
| tol=450, step=30 | 4.4M       | 1.2M   | 249  | 213   | 42.1s  | 47.5s  | 92.1s     |
| tol=465, step=31 | 4.5M       | 1.2M   | 251  | 215   | 40.7s  | 48.7s  | 91.9s     |
| tol=480, step=32 | 4.7M       | 1.3M   | 251  | 215   | 39.9s  | 51.3s  | 93.8s     |
| tol=495, step=33 | 4.8M       | 1.3M   | 250  | 214   | 38.9s  | 54.2s  | 96.0s     |
| tol=510, step=34 | 5.0M       | 1.4M   | 249  | 213   | 37.9s  | 54.6s  | 95.3s     |
| tol=525, step=35 | 5.1M       | 1.4M   | 249  | 213   | 36.8s  | 59.8s  | 99.6s     |
| tol=540, step=36 | 5.3M       | 1.5M   | 249  | 213   | 36.0s  | 58.3s  | 97.5s     |
| tol=555, step=37 | 5.4M       | 1.5M   | 251  | 215   | 34.8s  | 61.6s  | 99.5s     |
| tol=570, step=38 | 5.6M       | 1.5M   | 248  | 212   | 34.0s  | 63.6s  | 101.0s    |
| tol=585, step=39 | 5.7M       | 1.6M   | 251  | 215   | 32.8s  | 63.0s  | 99.2s     |
| tol=600, step=40 | 5.9M       | 1.6M   | 250  | 214   | 32.3s  | 63.8s  | 99.4s     |

### Optimal Parameters: tolerance=375km, step=25s

## Running the Benchmark

Define parameters in `src/main/java/io/salad109/conjunctionapi/conjunction/internal/ConjunctionBenchmark.java`

```bash
# Run on Linux
./mvnw spring-boot:run -Dspring-boot.run.profiles=benchmark-conjunction

# Run on Windows
./mvnw spring-boot:run "-Dspring-boot.run.profiles=benchmark-conjunction"
```
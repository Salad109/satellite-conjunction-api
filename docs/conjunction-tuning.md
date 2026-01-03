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
| tol=105, step=7  | 595K       | 152K   | 226  | 206   | 139.3s | 3.6s   | 143.6s    |
| tol=120, step=8  | 754K       | 193K   | 227  | 207   | 137.8s | 5.1s   | 143.7s    |
| tol=135, step=9  | 914K       | 237K   | 225  | 205   | 130.0s | 6.4s   | 137.5s    |
| tol=150, step=10 | 1.1M       | 282K   | 227  | 207   | 122.2s | 7.4s   | 130.8s    |
| tol=165, step=11 | 1.2M       | 327K   | 225  | 205   | 117.1s | 8.6s   | 126.8s    |
| tol=180, step=12 | 1.4M       | 373K   | 225  | 205   | 107.1s | 10.4s  | 118.9s    |
| tol=195, step=13 | 1.6M       | 420K   | 226  | 206   | 101.2s | 11.8s  | 114.5s    |
| tol=210, step=14 | 1.7M       | 469K   | 225  | 205   | 100.0s | 13.4s  | 115.2s    |
| tol=225, step=15 | 1.9M       | 520K   | 226  | 206   | 96.1s  | 15.0s  | 113.0s    |
| tol=240, step=16 | 2.1M       | 571K   | 229  | 208   | 91.1s  | 16.5s  | 109.5s    |
| tol=255, step=17 | 2.3M       | 623K   | 227  | 207   | 86.3s  | 18.3s  | 106.9s    |
| tol=270, step=18 | 2.5M       | 675K   | 227  | 207   | 83.3s  | 19.7s  | 105.2s    |
| tol=285, step=19 | 2.7M       | 728K   | 227  | 207   | 79.9s  | 21.1s  | 103.5s    |
| tol=300, step=20 | 2.8M       | 782K   | 225  | 204   | 75.8s  | 23.5s  | 101.7s    |
| tol=315, step=21 | 3.0M       | 835K   | 227  | 207   | 73.4s  | 24.9s  | 101.0s    |
| tol=330, step=22 | 3.2M       | 891K   | 226  | 206   | 70.9s  | 27.7s  | 101.2s    |
| tol=345, step=23 | 3.4M       | 947K   | 227  | 207   | 61.8s  | 32.2s  | 97.2s     |
| tol=360, step=24 | 3.6M       | 1.0M   | 230  | 209   | 64.4s  | 31.3s  | 98.6s     |
| tol=375, step=25 | 3.8M       | 1.1M   | 226  | 206   | 65.6s  | 28.4s  | 96.8s     |
| tol=390, step=26 | 4.0M       | 1.1M   | 225  | 205   | 61.8s  | 35.6s  | 100.4s    |
| tol=405, step=27 | 4.1M       | 1.2M   | 226  | 206   | 59.4s  | 36.5s  | 99.1s     |
| tol=420, step=28 | 4.3M       | 1.2M   | 225  | 205   | 57.3s  | 37.3s  | 98.0s     |
| tol=435, step=29 | 4.5M       | 1.3M   | 228  | 207   | 57.1s  | 39.1s  | 99.8s     |
| tol=450, step=30 | 4.7M       | 1.4M   | 225  | 205   | 55.7s  | 42.3s  | 101.9s    |
| tol=465, step=31 | 4.9M       | 1.4M   | 226  | 206   | 54.2s  | 42.6s  | 100.7s    |
| tol=480, step=32 | 5.1M       | 1.5M   | 226  | 206   | 53.6s  | 45.7s  | 103.4s    |
| tol=495, step=33 | 5.3M       | 1.5M   | 225  | 205   | 52.2s  | 50.5s  | 106.9s    |
| tol=510, step=34 | 5.5M       | 1.6M   | 227  | 207   | 50.2s  | 52.0s  | 106.5s    |
| tol=525, step=35 | 5.7M       | 1.7M   | 224  | 204   | 50.1s  | 55.4s  | 109.8s    |
| tol=540, step=36 | 5.9M       | 1.7M   | 225  | 205   | 48.6s  | 64.2s  | 117.5s    |
| tol=555, step=37 | 6.1M       | 1.8M   | 228  | 208   | 47.7s  | 58.3s  | 110.7s    |
| tol=570, step=38 | 6.3M       | 1.9M   | 228  | 208   | 46.5s  | 70.6s  | 122.0s    |
| tol=585, step=39 | 6.5M       | 1.9M   | 227  | 206   | 46.5s  | 62.9s  | 114.8s    |
| tol=600, step=40 | 6.7M       | 2.0M   | 225  | 205   | 46.6s  | 64.1s  | 116.4s    |

### Optimal Parameters: tolerance=375km, step=25s

## Running the Benchmark

Define parameters in `src/main/java/io/salad109/conjunctionapi/conjunction/internal/ConjunctionBenchmark.java`

```bash
# Run on Linux
./mvnw spring-boot:run -Dspring-boot.run.profiles=benchmark-conjunction

# Run on Windows
./mvnw spring-boot:run "-Dspring-boot.run.profiles=benchmark-conjunction"
```
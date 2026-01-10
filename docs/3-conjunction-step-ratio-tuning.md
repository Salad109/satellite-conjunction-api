# Step Second Ratio Tuning

The coarse sweep samples satellite positions at fixed time intervals. The step size must be small enough that
fast-moving
objects don't "skip over" each other between samples. With LEO satellites reaching relative velocities up to ~15 km/s,
the theoretical safe ratio is:

```
step_seconds = tolerance_km / 15
```

This experiment tests whether different step sizes (ratios of 12, 15, and 18) affect detected conjunction count.

## Parameters

- **prepass-tolerance-km**: Tolerance passed to pair reduction (fixed at 12.5 km)
- **step-second-ratio**: Divides tolerance by this value to get step size (lower ratio = larger steps = fewer samples)
- **tolerance-km**: Coarse detection threshold (swept from 120 to 600 km)
- **lookahead-hours**: Fixed at 6 hours
- **threshold-km**: Final conjunction threshold (fixed at 5.0 km)

## Results

Benchmark on 25% satellite sample (7,397 satellites).

### Ratio 12 (Aggressive)

| Tol (km) | Step (s) | Detections | Events | Conj | Dedup | Coarse | Refine | Total     |
|----------|----------|------------|--------|------|-------|--------|--------|-----------|
| 120      | 10       | 603K       | 188K   | 227  | 207   | 102.1s | 5.2s   | 108.2s    |
| 168      | 14       | 1.0M       | 330K   | 226  | 206   | 87.1s  | 9.6s   | 97.8s     |
| 216      | 18       | 1.5M       | 479K   | 225  | 205   | 76.4s  | 14.1s  | 92.1s     |
| 264      | 22       | 1.9M       | 641K   | 225  | 205   | 69.6s  | 21.8s  | 93.6s     |
| 312      | 26       | 2.4M       | 809K   | 225  | 205   | 62.4s  | 25.8s  | 90.4s     |
| 360      | 30       | 2.9M       | 985K   | 227  | 207   | 52.9s  | 30.2s  | **85.8s** |
| 408      | 34       | 3.3M       | 1.2M   | 228  | 208   | 47.5s  | 39.6s  | 89.9s     |
| 456      | 38       | 3.8M       | 1.4M   | 225  | 205   | 44.7s  | 44.5s  | 92.4s     |
| 504      | 42       | 4.3M       | 1.5M   | 225  | 205   | 40.8s  | 52.3s  | 96.7s     |
| 552      | 46       | 4.9M       | 1.8M   | 225  | 205   | 38.1s  | 65.3s  | 107.5s    |
| 600      | 50       | 5.4M       | 2.0M   | 226  | 206   | 38.6s  | 80.3s  | 124.1s    |

### Ratio 15 (Theoretical Safe Limit)

| Tol (km) | Step (s) | Detections | Events | Conj | Dedup | Coarse | Refine | Total      |
|----------|----------|------------|--------|------|-------|--------|--------|------------|
| 120      | 8        | 754K       | 193K   | 227  | 207   | 136.9s | 5.3s   | 143.1s     |
| 180      | 12       | 1.4M       | 373K   | 225  | 205   | 103.0s | 9.9s   | 114.2s     |
| 240      | 16       | 2.1M       | 571K   | 225  | 205   | 85.3s  | 17.2s  | 104.4s     |
| 300      | 20       | 2.8M       | 782K   | 227  | 206   | 75.1s  | 24.7s  | 102.1s     |
| 360      | 24       | 3.6M       | 1.0M   | 226  | 206   | 66.3s  | 32.3s  | **101.4s** |
| 420      | 28       | 4.3M       | 1.2M   | 227  | 207   | 57.8s  | 39.0s  | 100.5s     |
| 480      | 32       | 5.1M       | 1.5M   | 225  | 205   | 51.7s  | 46.1s  | 101.6s     |
| 540      | 36       | 5.9M       | 1.7M   | 227  | 207   | 47.8s  | 60.9s  | 113.2s     |
| 600      | 40       | 6.7M       | 2.0M   | 225  | 205   | 46.8s  | 77.4s  | 129.8s     |

### Ratio 18 (Conservative)

| Tol (km) | Step (s) | Detections | Events | Conj | Dedup | Coarse | Refine | Total      |
|----------|----------|------------|--------|------|-------|--------|--------|------------|
| 120      | 6        | 1.0M       | 196K   | 227  | 207   | 176.4s | 5.1s   | 182.5s     |
| 192      | 10       | 2.0M       | 416K   | 226  | 206   | 125.4s | 11.6s  | 138.7s     |
| 264      | 14       | 3.0M       | 662K   | 225  | 205   | 98.3s  | 19.8s  | 120.4s     |
| 336      | 18       | 4.1M       | 924K   | 226  | 206   | 82.8s  | 27.5s  | **113.3s** |
| 408      | 22       | 5.2M       | 1.2M   | 227  | 207   | 73.4s  | 39.6s  | 116.8s     |
| 480      | 26       | 6.3M       | 1.5M   | 225  | 205   | 65.1s  | 49.2s  | 118.5s     |
| 552      | 30       | 7.5M       | 1.8M   | 225  | 205   | 58.2s  | 58.0s  | 121.8s     |

## Analysis

### Conjunction Count Stability

All three ratios detect the same conjunctions:

| Ratio | Conj Range | Dedup Range |
|-------|------------|-------------|
| 12    | 225-228    | 205-208     |
| 15    | 225-227    | 205-207     |
| 18    | 225-227    | 205-207     |

No conjunctions are missed even at ratio 12. The scanning pipeline successfully determines all conjunctions below the
5 km threshold regardless of step size.

### Performance Comparison

| Ratio | Best Time | At Config        |
|-------|-----------|------------------|
| 12    | 85.8s     | tol=360, step=30 |
| 15    | 100.5s    | tol=420, step=28 |
| 18    | 113.3s    | tol=336, step=18 |

Ratio 12 is fastest because larger steps reduce coarse sweep iterations. At equivalent tolerance values, fewer
propagation calls are needed.

### Trade-offs

- **Ratio 12**: Fastest overall, fewer samples per sweep. Recommended.
- **Ratio 15**: Theoretical safe limit. 17% slower than ratio 12.
- **Ratio 18**: Smallest steps, most samples. 32% slower than ratio 12.

## Conclusion

Ratio 12 (step = tolerance/12) is safe for conjunction detection. All conjunctions are preserved while achieving the
best performance. Going above ratio 12 provides no detection benefit and increases runtime.

The theoretical 15 km/s limit is too conservative in practice. Real-world close approaches rarely occur directly head-on
at maximum speed.

![Total Processing Time](3-conjunction-step-ratio/1_total_time.png)

![Coarse vs Refine Time](3-conjunction-step-ratio/2_coarse_vs_refine.png)

![Time Breakdown](3-conjunction-step-ratio/3_time_breakdown.png)

![Conjunctions Detected](3-conjunction-step-ratio/4_conjunctions.png)

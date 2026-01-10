# Pre-pass Tolerance Tuning

The pair reduction filter uses a geometric tolerance to determine which satellite pairs could potentially collide. A
larger tolerance includes more pairs (safer but slower), while a smaller tolerance excludes more pairs (faster but risks
missing conjunctions).

This experiment finds the minimum pre-pass tolerance that detects all conjunctions.

## Parameters

- **prepass-tolerance-km**: Tolerance passed to pair reduction (swept from 2.5 to 20.0 km)
- **tolerance-km**: Coarse sweep detection threshold (swept from 120 to 1200 km)
- **step-second-ratio**: Fixed at 12
- **lookahead-hours**: Fixed at 6 hours
- **threshold-km**: Final conjunction threshold (fixed at 5.0 km)

## Results

Benchmark on 25% satellite sample (7,397 satellites) at tolerance=360 km.

| Prepass (km) | Detections | Events | Conj | Dedup | Coarse | Refine | Total |
|--------------|------------|--------|------|-------|--------|--------|-------|
| 2.5          | 285K       | 69K    | 142  | 126   | 9.1s   | 2.2s   | 11.6s |
| 5.0          | 329K       | 84K    | 187  | 169   | 9.6s   | 2.6s   | 12.5s |
| 7.5          | 475K       | 126K   | 215  | 197   | 11.2s  | 4.0s   | 15.6s |
| 10.0         | 534K       | 151K   | 222  | 204   | 11.9s  | 4.8s   | 17.2s |
| 12.5         | 603K       | 174K   | 225  | 206   | 12.8s  | 5.4s   | 18.7s |
| 15.0         | 632K       | 186K   | 224  | 204   | 13.2s  | 5.7s   | 19.3s |
| 17.5         | 661K       | 196K   | 224  | 204   | 13.7s  | 6.3s   | 20.5s |
| 20.0         | 741K       | 223K   | 225  | 204   | 15.0s  | 7.1s   | 22.7s |

## Analysis

### Conjunction Count by Prepass Tolerance

| Prepass (km) | Conj Range | Dedup Range | Status       |
|--------------|------------|-------------|--------------|
| 2.5          | 138-143    | 123-126     | Missing ~80  |
| 5.0          | 185-190    | 167-171     | Missing ~35  |
| 7.5          | 214-219    | 196-201     | Missing ~8   |
| 10.0         | 220-227    | 202-208     | Missing ~1   |
| 12.5         | 223-228    | 203-208     | **Complete** |
| 15.0         | 223-228    | 203-208     | Complete     |
| 17.5         | 224-228    | 203-208     | Complete     |
| 20.0         | 223-228    | 203-208     | Complete     |

At prepass=2.5 km, we detect only ~126 deduplicated conjunctions versus ~206 at prepass=12.5 km. The pair reduction
filter can be too aggressive and exclude satellite pairs that do have close approaches.

### Convergence Point

Conjunction counts stabilize at **prepass=12.5 km**:

- Below 12.5 km: Missing conjunctions (pair reduction too aggressive)
- At 12.5 km: All 205-208 deduplicated conjunctions detected
- Above 12.5 km: Same detection count, but slower due to more candidate pairs

## Conclusion

**Optimal pre-pass tolerance is 12.5 km (for 5.0 km conjunction threshold)**

This is the minimum value that captures all conjunction events. The relation `prepass > final threshold tolerance` is
expected, since reduction geometry is approximate, and extra margin is needed.

![Total Processing Time](2-conjunction-prepass/1_total_time.png)

![Coarse vs Refine Time](2-conjunction-prepass/2_coarse_vs_refine.png)

![Time Breakdown](2-conjunction-prepass/3_time_breakdown.png)

![Conjunctions Detected](2-conjunction-prepass/4_conjunctions.png)

## Running the Benchmark

```bash
# Run on Linux
./mvnw spring-boot:run -Dspring-boot.run.profiles=benchmark-conjunction

# Run on Windows
./mvnw spring-boot:run "-Dspring-boot.run.profiles=benchmark-conjunction"
```

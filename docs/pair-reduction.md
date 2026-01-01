# Pair Reduction Benchmark

With ~30,000 tracked objects, the naive approach would check over 400 million satellite pairs. The system applies
several sequential filters to reduce computational load.

## Results

| Strategy                        |   Unique Pairs | % of Original |      Time |  Throughput/sec |
|---------------------------------|---------------:|--------------:|----------:|----------------:|
| Full set (29,583 satellites)    |    437,562,153 |        100.0% |     664ms |     659,203,315 |
| Skip debris on debris           |    190,915,570 |        43.63% |     1.76s |     248,887,321 |
| Only overlapping apogee/perigee |     80,289,038 |        18.35% |     1.94s |     225,551,838 |
| Intersecting orbital planes     |     22,813,396 |         5.21% |    23.84s |      18,350,901 |
| **All strategies combined**     | **14,708,895** |     **3.36%** | **4.22s** | **103,802,524** |

## Filter Details

### Skip debris on debris

Excludes pairs where both objects are debris. Debris-on-debris conjunctions are of low importance since neither object
can maneuver.

### Only overlapping apogee/perigee

Two satellites can only collide if their orbital altitude ranges overlap. Compares perigee/apogee of two satellites with
a tolerance buffer.

### Intersecting orbital planes

Non-coplanar orbits only intersect at two points (the line of nodes). The filter checks if the orbital radii at these
intersection points are within tolerance.

## Running the Benchmark

```bash
# Run on Linux
./mvnw spring-boot:run -Dspring-boot.run.profiles=benchmark-filter

# Run on Windows
./mvnw spring-boot:run "-Dspring-boot.run.profiles=benchmark-filter"
```

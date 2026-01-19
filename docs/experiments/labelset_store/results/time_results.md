# Time results using `/usr/bin/time -pv`

The app was run `/usr/bin/time -pv taskset -c 10-16 ./target/release/chronoxide-ingester`, as you can see we use `taskset` to pin the app to a specific core (7 cores, 10 to 16).

## FlatInterned
```
Command being timed: "taskset -c 10-16 ./target/release/chronoxide-ingester"
User time (seconds): 1045.91
System time (seconds): 3.53
Percent of CPU this job got: 101%
Elapsed (wall clock) time (h:mm:ss or m:ss): 17:17.60
Average shared text size (kbytes): 0
Average unshared data size (kbytes): 0
Average stack size (kbytes): 0
Average total size (kbytes): 0
Maximum resident set size (kbytes): 17911312
Average resident set size (kbytes): 0
Major (requiring I/O) page faults: 1
Minor (reclaiming a frame) page faults: 5042629
Voluntary context switches: 236
Involuntary context switches: 14337
Swaps: 0
File system inputs: 72
File system outputs: 2848
Socket messages sent: 0
Socket messages received: 0
Signals delivered: 0
Page size (bytes): 4096
Exit status: 0
```

## KeySetDictEncoded
```
Command being timed: "taskset -c 10-16 ./target/release/chronoxide-ingester"
User time (seconds): 1421.49
System time (seconds): 3.94
Percent of CPU this job got: 101%
Elapsed (wall clock) time (h:mm:ss or m:ss): 23:21.86
Average shared text size (kbytes): 0
Average unshared data size (kbytes): 0
Average stack size (kbytes): 0
Average total size (kbytes): 0
Maximum resident set size (kbytes): 15408400
Average resident set size (kbytes): 0
Major (requiring I/O) page faults: 0
Minor (reclaiming a frame) page faults: 5798237
Voluntary context switches: 848
Involuntary context switches: 20873
Swaps: 0
File system inputs: 0
File system outputs: 3704
Socket messages sent: 0
Socket messages received: 0
Signals delivered: 0
Page size (bytes): 4096
Exit status: 0
```

| Metric                                 | FlatInterned | KeySetDictEncoded |
|:---------------------------------------|:-------------|:------------------|
| User time (seconds)                    | 1045.91      | 1421.49           |
| System time (seconds)                  | 3.53         | 3.94              |
| Percent of CPU this job got            | 101%         | 101%              |
| Elapsed (wall clock) time              | 17:17.60     | 23:21.86          |
| Maximum resident set size (kbytes)     | 17911312     | 15408400          |
| Minor (reclaiming a frame) page faults | 5042629      | 5798237           |
| Voluntary context switches             | 236          | 848               |
| Involuntary context switches           | 14337        | 20873             |
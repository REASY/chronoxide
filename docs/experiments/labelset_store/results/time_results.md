# Time results using `/usr/bin/time -pv`

The app was run `/usr/bin/time -pv taskset -c 10-16 ./target/release/chronoxide-ingester`, as you can see we use `taskset` to pin the app to a specific core (7 cores, 10 to 16).

## FlatInterned
```
Command being timed: "taskset -c 10-16 ./target/release/chronoxide-ingester"
User time (seconds): 1045.78
System time (seconds): 4.55
Percent of CPU this job got: 101%
Elapsed (wall clock) time (h:mm:ss or m:ss): 17:17.47
Average shared text size (kbytes): 0
Average unshared data size (kbytes): 0
Average stack size (kbytes): 0
Average total size (kbytes): 0
Maximum resident set size (kbytes): 18928756
Average resident set size (kbytes): 0
Major (requiring I/O) page faults: 0
Minor (reclaiming a frame) page faults: 5296890
Voluntary context switches: 178
Involuntary context switches: 19186
Swaps: 0
File system inputs: 0
File system outputs: 2808
Socket messages sent: 0
Socket messages received: 0
Signals delivered: 0
Page size (bytes): 4096
Exit status: 0
```

## KeySetDictEncoded
```
Command being timed: "taskset -c 10-16 ./target/release/chronoxide-ingester"
User time (seconds): 1378.54
System time (seconds): 3.71
Percent of CPU this job got: 101%
Elapsed (wall clock) time (h:mm:ss or m:ss): 22:36.16
Average shared text size (kbytes): 0
Average unshared data size (kbytes): 0
Average stack size (kbytes): 0
Average total size (kbytes): 0
Maximum resident set size (kbytes): 12366240
Average resident set size (kbytes): 0
Major (requiring I/O) page faults: 0
Minor (reclaiming a frame) page faults: 3656262
Voluntary context switches: 755
Involuntary context switches: 24622
Swaps: 0
File system inputs: 0
File system outputs: 3576
Socket messages sent: 0
Socket messages received: 0
Signals delivered: 0
Page size (bytes): 4096
Exit status: 0
```

| Metric                                 | FlatInterned | KeySetDictEncoded |
|:---------------------------------------|:-------------|:------------------|
| User time (seconds)                    | 1045.78      | 1378.54           |
| System time (seconds)                  | 4.55         | 3.71              |
| Percent of CPU this job got            | 101%         | 101%              |
| Elapsed (wall clock) time              | 17:17.47     | 22:36.16          |
| Maximum resident set size (kbytes)     | 18928756     | 12366240          |
| Minor (reclaiming a frame) page faults | 5296890      | 3656262           |
| Voluntary context switches             | 178          | 755               |
| Involuntary context switches           | 19186        | 24622             |
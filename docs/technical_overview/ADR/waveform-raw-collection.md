# Collection of raw Waveform HL7 messages

# Status

Work has commenced on the Waveform feature (ARC project #1249) as of October 2025.

# Issue to solve/summary
Sufficient sample size is required by our researchers to investigate certain medical conditions.

Therefore, we wish to collect data from as many patients as possible.

And therefore, we need to *start* collecting as soon as possible. If we spend three months developing
the full pipeline and only then start collecting data, we will have lost those three months' of data
forever.

The quickest way to start collecting data as soon as possible is to store the raw HL7 messages.
This allows us to defer future processing until the rest of the pipeline has been developed, and
has other benefits (see later).

# Actual decisions

This section is the main one that needs to be dated as it's most likely to change. It should
be considered append-only in the case that decisions change, to preserve the
decision-making history.

## As of 2025-10-31

All unprocessed HL7 messages will be stored on disk.

Write HL7 messages to disk in individual files as soon as they are received, in a folder
structured by bed number, stream id, and fact timestamp (not received timestamp). Undecided
on resolution of timestamp folders, perhaps hourly?

Individual messages are compressed on a schedule and the originals deleted.
Use bzip2 compression. Group files in a `tar` archive to maintain good compression ratio.

The `.tar.bz2` files are named by the bed, stream id, and earliest and latest fact timestamp.

# Details
Since there is a large amount of data coming in and not much space to store it, we will need
to use compression.

Various compression schemes were tested using a small dump of messages that Elise captured in 2024.

| Compression   | setting        | size (bytes) | Compression ratio | Time taken on GAE01 (s) |
|---------------|----------------|-------------|-------------------|-------------------------| 
| Original file | n/a            | 22891961    | n/a               | n/a                     |
| `gzip`        | `-6` (default) | 4009423     | 5.7               | 0.7                     |
| `bzip2`       | `-9` (default) | 2413045     | 8.9               | 1.9                     |
| `xz`          | (default)      | 2350476     | 9.7               | 10.2                    |

Brotli was not tested as the command line utility is not installed by default, but may be worth
exploring, as we would likely accept some inconvenience for more compactness given our space constraints.

At first glance, it looks like bzip2 is the best option in terms of reducing size without being
excessively slow.

## Will the compression be able to keep up?

15TB over 18 months is an average of ~300kB/s. Not taking into account formatting inefficiencies — HL7
is rather verbose compared to the raw numbers that make up the actual data — even the slowest
algorithm tested (xz) should be able to keep up with this quantity of data on one CPU with a ~10x margin.
Although we could throw more CPUs at the problem if necessary, GAE01 is going to need
CPU for other things, so being low impact has some value here. bzip2, with its further 5x speed gain
seems like a good choice given the difference in compactness vs xz is so minimal.

## Message grouping

The test dump file contains about 40,000 messages. If messages are compressed in smaller groups,
the compression ratio gets worse.

From the table below, it's clear that the naive approach of storing only one compressed message per file
has a disastrous effect on bzip2's compression ratio.

| Group size (messages) | Compressed size  (bytes) |
|-----------------------|--------------------------|
| 20000                 | 2415889                  |
| 10000                 | 2425154                  |
| 1000                  | 2527588                  |
| 500                   | 2740853                  |
| 200                   | 3092383                  |
| 100                   | 3456118                  |
| 50                    | 3834291                  |
| 20                    | 4513532                  |
| 10                    | 5326667                  |
| 5                     | 6577100                  |
| 3                     | 7956113                  |
| 2                     | 9574110                  |
| 1                     | 13611362                 |

I am assuming that compressing a tar archive will behave very similarly to the above, which
was tested using simple text concatenation.

## Message naming etc

Given the large quantity of messages we will have, they need to be findable.
A likely operation would be to reprocess messages between two dates (or from a date up to the present day).

Finding files by bed number and stream id may also be required, so those will be part of the filename too.

We may be subsequently told that some data is not needed (eg. they only want patients
with certain conditions). Grouping data by patient (bed) and stream allows for selective deletion.

## Compression scheduling
Having established that messages must be grouped before compression in batches of ~1000,
observe that ~1000 messages from the same stream equates to about ~10–100 seconds of data,
assuming 5 samples per message, and 50–300Hz frequency.

If we store that much in memory, we risk losing it in the event of a sudden outage (system crash,
power outage, etc). It is safer to write each message to disk as a separate file, and then come back later
to archive it. This increases the amount of disk churn, but I believe that it's worth this trade-off.
This means that the replay function must be able to recognise either the plain or .tar.bz2 "schemas".

# Consequences/limitations
Storing raw messages has the extra advantage of allowing us to reprocess old data
should we discover bugs in the pipeline that might invalidate the processed data we have collected.
(see also: Immutable Data Store (IDS))

There is a limited amount of space on the GAE local storage. `/gae` is currently
300GB on GAE01, of which about 114GB is free. There is the possibility that more
storage will be added, but it can't be guaranteed.

The total amount of data we plan to collect is in the order of 15TB. So clearly this raw HL7 buffer
can only buy us a certain amount of time.

## Relationship to Emap-star

Note that although we have multiple instances of Emap writing into different database schemas, there
will only be one that is running the waveform listener, and that will always be writing to the
same location on disk regardless of whether it's `star-a` or `star-dev`, etc.

When we want to bring down that instance of Emap and replace it with another, there will be a brief
period when Smartlinx won't be able to connect. We currently don't know if Smartlinx
will attempt to retry with data it has buffered, or if you're in danger of losing some
data every time you bring the Waveform listener down.

The scheduled compression of raw HL7 storage will operate on a different schedule
to the Waveform collation, and does a somewhat analogous task. The former is described in this
document, the latter gathers up data points before writing them out as SQL arrays.
This is liable to cause some developer (and user) confusion, but they do serve different purposes!

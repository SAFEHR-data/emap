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

## As of 2025-12-04

Writing loads of small files was too slow and took up a lot of filesystem space in the directory
structure itself, so it was changed to write directly to bz2 files. One file per bed location, changing
every N minutes to a new archive. (N is configurable, suggest 5–15 minutes).

The tradeoff here is that more data is in memory that is at risk of loss in an outage.
In particular with the large block size of bz2, you can't start compressing until you
have quite a lot of data.

We perform a clean shutdown that flushes out everything in memory to disk, but this only helps
in the case of a planned shutdown. A sudden host reboot or power outage would lose a few minutes
of data, but luckily these are rare.

I also switched from using tar archives to concatenated plain data with a single ASCII FS character appended,
because tar has a per-file overhead comparable in size to the files we are trying to store.

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

These figures come from combining the messages with simple concatenation.

Using a tar archive hugely increases the uncompressed size of the data (~2x). Other archiving
formats are available but the overhead is still large (eg. 80 bytes) when each file is only ~300 bytes
to start with.

## Message naming etc

Given the large quantity of messages we will have, they need to be findable.
A likely operation would be to reprocess messages between two dates (or from a date up to the present day).

Finding files by bed number may also be required, so that will be part of the filename too.

While finding by channel/variable id would be useful, most messages have multiple channels so this can't be used.

We may be subsequently told that some data is not needed (eg. they only want patients
with certain conditions). Grouping data by time and patient (bed) allows for selective deletion.

## Archive management

Having established that messages must be grouped before compression in batches of ~1000,
observe that ~1000 messages from the same stream equates to about ~10–100 seconds of data,
assuming 5 samples per message, and 50–300Hz frequency.

That data is at risk while it is still in memory, so we should try to close off old bz2 archives
in a timely fashion so in the event of a sudden outage (system crash, power outage, etc) the loss
is minimised.

So we need to close off files that haven't been written to in a certain amount of time.

Which archive a message goes into is based on the message header timestamp, NOT when we receive the message.
This is to make the behaviour more deterministic, and to make messages easier to locate
should that be needed. However, it does mean that if we receive messages out of order, we might want to
write to a file for a timeslot that we have already closed. To avoid overwriting existing archives, a random
string is used to make each file name unique, and it is possible (and harmless) to have two archives covering
the same time slot.

We are not making any guarantees to our users about collecting all the data (nobody is getting paged
when this goes down...) so it's very much on a best effort basis.

## Replaying messages

Saved messages should be replayable.

To avoid reconfiguring the already running waveform-reader on the fly,
when we want to replay data it would be simpler
to start another instance of it that has been set to read from the save directory between dates X and Y.

It would add the messages to the same queue as the "live" waveform-reader and then exit cleanly.

Care must be taken not to re-save the messages that are being read from disk!

# Consequences/limitations
Storing raw messages has the extra advantage of allowing us to reprocess old data
should we discover bugs in the pipeline that might invalidate the processed data we have collected.
(see also: Immutable Data Store (IDS))

There is a limited amount of space on the GAE local storage. `/gae` is currently
300GB on GAE01, of which about 114GB is free. There is the possibility that more
storage will be added, but it can't be guaranteed.

The total amount of data we plan to collect is in the order of 15TB. So clearly this raw HL7 buffer
can only buy us a certain amount of time. We considered uploading raw HL7 messages
to a share on the DSH, although a plan for getting them back out again would have been required.
However, we concluded that bed numbers + timestamps might be sufficiently identifying that this data
in fact *can't* go on the DSH.

## Dealing with multiple Emap instances 

Note that although we have multiple instances of Emap writing into different database schemas, there
will only be one that is running the waveform listener. To avoid having multiple instances
trying to save HL7 messages into the same directory, we will run this as a special standalone instance
of waveform for the time being (currently `emap-dev`).

This strategy should hold until we want to have Waveform data in production emap-star.

When we want to bring down that instance of Emap and replace it with another (eg. a code change in the
waveform-reader), there will be a brief
period when Smartlinx won't be able to connect. As it's currently configured, Smartlinx
does not do any buffering, so data loss will occur as soon as we stop listening.

We are hoping to get buffering turned on so that it will retry when we come back.

In addition to buffering, you can turn on the requirement for HL7 ACKs. We are not doing this yet,
so Smartlinx will take the success of each TCP connection as a sign that the data has been delivered.

## Relationship to Emap-star

The archival + compression of raw HL7 files is not the same as "Waveform collation",
although it does a somewhat analogous task. The former is described in this
document, the latter gathers up data points before writing them out as large rabbitmq messages (and then SQL arrays).
This is liable to cause some developer (and user) confusion, but they do serve different purposes!


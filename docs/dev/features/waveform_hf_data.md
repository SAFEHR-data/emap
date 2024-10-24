# Waveform (high-frequency) data

Waveform data is data such as ECG traces and ventilator pressure readings that is sampled multiple times a second.

## User data requirements

Data requirements differ depending on the project and cover concerns such as
how far back the data needs to go, how up-to-date it needs to be, and quality issues like
how many gaps it can tolerate.

One project is interested in looking at 30 second snapshots before and after making some adjustment
to the patient's care. (eg. change ventilator settings)

In the future they may wish to look at a period of ~12 hours, to see if secretions
are building up gradually over time and maybe some intervention is needed.

How long do we need to keep waveform data for? The data is very large, so being able to delete data older than
a configurable time period has been implemented to mitigate storage problems. I foresee 1-7 days being a useful
value.
This is implemented as a Spring scheduled task in the core processor. Notably this is the first
background database operation in Emap, so it could happen alongside regular Emap processing.
This will produce significant "churn" in the database; I'm assuming postgres is designed to handle this, but
it's something to consider if any performance problems appear.

How live does it have to be? Our standard guarantee is no more than a minute out of date,
but in practice it's typically 20 seconds. We have aimed for similar.

## Config options added

Core:
  - `core.waveform.retention_hours` periodically delete data more than this many hours old
  - `core.waveform.is_non_current_test_data` for testing only - when deciding which data to delete/retain, if set to true,
     then treat the "now" point as the most recent observation date in the waveform table, rather than the actual
     current time. Purpose is to avoid test data getting immediately deleted because it's too old, which could happen
     if we have a fixed set of test data with observation dates way in the past.

Waveform Generator:
  - `waveform.hl7.send_host`, `waveform.hl7.send_port` - the host and port to send the generated data to
  - `waveform.synthetic.num_patients` - number of different patients (locations) to generate data for
  - `waveform.synthetic.start_datetime` observation date to start generating data from or null to simulate live data
    (see comment in code for more)
  - `waveform.synthetic.end_datetime` if not null, exit the generator when observation date hits this date
  - `waveform.synthetic.warp_factor` How many times real time to generate data at. Live mode implies warp factor = 1.

Waveform Reader:
  - `waveform.hl7.listen_port` port inside the container to listen on for waveform HL7 generator
  - `waveform.hl7.source_address_allow_list` comma-separated list of source IP addresses to accept connections from.
      Enter your Smartlinx server IP address(es) here.
      For testing only: if the list contains the value "ALL", then all source IP addresses are allowed. This is
      the only form of authentication so don't use this setting in production.
      Not currently supported: hostnames or IP ranges.
  - `waveform.hl7.test_dump_file` If specified, read messages from this file and then exit - intended for validation

## Container housekeeping (setup script)
The waveform processing feature is enabled or disabled in the global configuration file. I've added
a "features" section for this, and taken the opportunity to also add the `fakeuds` container to make that easier
to turn on and off.

Because the waveform feature flag will include/exclude the relevant docker compose files from
the docker commands it generates, you can continue to
run a simple `emap docker up -d` to bring up a production instance of emap without the waveform containers coming up.
Feature flag implementation is different here than for, say, SDEs because that's a feature within the core
processor, whereas disabling waveform data requires an entire container to be disabled.

## Design details

### Emap DB design

A key part of the waveform data table design is the use of SQL arrays, without which the storage would be
extremely inefficient. This way we can store up to 3000 data points in a single database row.

There is one set of metadata per row regardless of array size,
so increasing the array size increases the storage efficiency.

But there is a tradeoff with timeliness. Since we don't append to arrays once written, we have to wait for all
data points to arrive before we can write them in one go. 3000 points at 300Hz is 10 seconds worth of data.
Having to wait for too much data would mean that our aim to make Emap no more than 10-20 seconds out of date would
no longer hold.
(see also https://github.com/UCLH-DHCT/emap/issues/65 )

As far as we know, there is no mechanism for correcting or updating waveform data,
so the audit table will always be empty.
Although we do delete old data for storage capacity reasons, moving it to the audit table in this case
would defeat the whole purpose of its deletion!

Stream metadata is stored in the `visit_observation_type` table, as it is for visit observations.
Waveform data semantically could have gone in the `visit_observation` table if it weren't for the
storage efficiency problems this would cause.


### Core processor logic (orphan data problem)

See issue https://github.com/UCLH-DHCT/emap/issues/36

The HL7 messages that Emap has been ingesting up to this point tend to be highly redundant.
Every message contains basic patient demographic/encounter data.
This means that if we receive messages out of order, keeping the database in a usable state at all intermediate points is not too hard.
For example, if we receive a test order for a patient that we haven't yet received any other information for (admission, demographics, etc)
we can still create the encounter/mrn/demographics from that message alone.

Unfortunately this is not the case for waveform data.
The only patient ID info we have in a waveform HL7 message is the bed location, the time, and (presumably) the machine identifier.
This raises the possibility that we will receive some waveform data and have no idea which encounter to attach it to.

NB: Although it may seem unlikely that a patient would start generating ventilator/monitor data before we have seen their admit messages,
consider what happens if the main HL7 feed goes down for a bit but the waveform feed doesn't.

I can see several ways around this problem:
- Just reject the data if we don't know who is in that location
    - pro: simple
    - con: we lose data
- Write the "orphan" waveform data to the database with its bed location, but without a link to a visit/encounter, then fix it up later
    - pro: eventually correct
    - cons
        - will need careful consideration as to when to perform this check
            - every time a new visit is opened, see if there is some orphan waveform data waiting for it?
            - or just intermittently check for it?
        - Emap users will have to detect and exclude orphan data
- Don't ever try to link the waveform data to a hospital visit
    - pro: really easy for us!!
    - cons: terrible for the user, this is not the Emap way

We currently don't store any orphan data in Emap, so this will be a new paradigm for us.


A closely related problem:
- Patient A is assigned to bed 1
- The main HL7 feed goes down
- Patient B is assigned to bed 1
- We receive waveform data for bed 1, and wrongly assign it to person A

We may need to actively monitor the overall liveness of our data, and perform whatever fix we come up with for this problem in this case.
Possible heuristics to answer the question "Is patient A definitely still assigned to bed 1?":
- Have any ADT events happened (to anyone) in the database within the last few minutes? May not be a good measure in
the middle of the night, when waveform data will still be coming in.
- We have recently (last 1 minute?) received waveform data for bed 1 that we assigned to patient A.
 Even if the feed has gone down, there's no way that machine switched patients so quickly.

Other solution is to fix it up later when the feed comes back, but that involves a lot of continuously rechecking stuff,
and we will have been giving out wrong information in the meantime. And then we will need that audit table!


### Performance monitoring

Performance metrics that we need to watch:

- How much CPU time does the waveform HL7 reader take to parse HL7 and generate interchange messages?
- Can core proc process waveform messages quicker than the rate at which they're being produced, on top of the messages it's already processing? (live mode)
- How much does this slow down a validation run? (catchup mode)
- How much storage space does this take in the postgres DB?
- Can we run the read queries we need to do in a reasonable time?

### Storage efficiency

My initial tests assumed that there will be 30 patients generating data from one 50Hz and one 300Hz waveform source at all times.
(In actual fact it's going to be more like 5-10 patients
ventilated at any one time, but the ventilator has more than two data streams)

At this rate of data flow, my very naive DB implementation using 1 data point per row
resulted in ~100GB of backend postgres disk usage being generated per day - clearly
far too much if we're aiming for the UDS to stay under 1TB (which used to be the limit we were observing), given
that there could be three or more Emap instances in the DB at once (two lives and at least one in dev)

You can calculate a theoretical minimum:
30 * 86400 * (300 + 50) = 907 million data points per day.
I don't know what numerical type the original data uses, but assuming 8 bytes per data point, that's **~8GB per day**.
If we keep only the last 7 days of data, that caps it at **~60GB** per instance of Emap.
Allowing for some row metadata, the underlying DB to be padded/aligned/whatever, and it will be a bit more.
Am assuming compression is impossible.

Using SQL arrays vastly improves the actual efficiency vs 1 per row.

#### Further improvements

See issue https://github.com/UCLH-DHCT/emap/issues/62 for a discussion of further improvements.

### HL7 ingress

There is a piece of software in the hospital called Smartlinx which collects data from little dongles
which plug into the back of ventilators and bedside monitors.
It can be easily configured to stream HL7 waveform data in our direction. However, a large concern is that we avoid
jeopardizing the existing flow of (non-waveform) data that goes from Smartlinx to Epic.

Therefore we will need to have our own Smartlinx server (with accompanying software licence) to run it on, so we
are separate from the existing one.

So far, it has only been used to provide one patient's waveform data at a time, and only for short periods. It's
unknown how it would cope if it were sending *all* patients' data to us.

To receive waveform data from Smartlinx, you must listen on a TCP port which Smartlinx then connects to.
This seems rather unusual to me! We should be validating the source IP address at the very least if this is how it has to work.

Can Smartlinx replay missed messages if we go down? No.

Does Smartlinx support/require acknowledgement messages? No.

Will we need to do our own buffering? Can we do the same sort of thing the IDS does?
Maybe. See issue https://github.com/UCLH-DHCT/emap/issues/48 for why we might have to implement some sort of buffering.

HL7 messages are not particularly space efficient: messages from bedside monitors have ~40 measurements per message;
ventilators have ~1-10 per message (50Hz); ECGs (300Hz) not known.
So this will be a high volume of text relative to the actual data.
Although this inefficiency might keep a CPU core or two on the GAE fairly busy, at least it won't have any effect on the Emap queue and DB.
Since we plan to use SQL arrays in the DB, and Emap doesn't have a mechanism to collate multiple incoming interchange messages,
we want each interchange message to result in one DB waveform row being created.
Therefore I collect up to 3000 data points in memory for each patient+data stream, collate it and send as a single
interchange message, so it can become a single row in the DB.

The HL7 messages contain two timestamps. The "capsule axon" time (observation time?), and the server time (in MSH?).
I've forgotten the difference, but Elise knows.
The local time on the ventilators that has to be set manually twice a year to account for DST is not in the HL7 messages.

### Pre-release validation

This requires the ability to replay waveform HL7 messages. We currently do this using a text HL7 dump file.

An alternative would be to maintain a test stream of real or synthetic messages, but it would have to be continuously
updated to store (say) the last 7 days of messages, so that it overlaps with the validation period, which is by default
the last 7 days.

Or you could perform waveform validation live, but this would mean a 7 day validation period would take 7 days to run,
and you'd have to run this separately from the main Emap validation process.

Things you could check in validation:

 - Are all the data points evenly spaced, with no unexpected gaps? Especially gaps when switching from the end of one array to the beginning of the next.
 - Does the actual data frequency match what is expected?
 - Is there any duplicate data? (same machine giving multiple overlapping data series)
 - Do machines switch from patient to patient suspiciously quickly?
 - Does machine total utilisation % look believable, or preferably match it to known utilisation data?
 - Can all waveform data be unambiguously assigned to a hospital visit?
 - Are all patients with waveform data in places where you'd expect them to be, eg. ICU? No transfers mid stream, etc.
 - Do the waveforms themselves look plausible? (Don't know if there's a simple check that can be performed here)



### Emap user queries

In conjunction with the DB design process, we will need to write some SQL queries that retrieve waveform data in a useful way,
to check that they run in reasonable time and are relatively easy to write.
Since the DB design will likely be optimised for space/time efficiency rather than ease of writing queries, we may need to write
some example R/python code to process the retrieved data into something the user would expect (unpacking the arrays mainly)


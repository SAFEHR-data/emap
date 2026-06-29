# Hibernate Id field generation strategy

Done as part of performance improvement work in March 2026.

## Decisions

* Switched from AUTO to SEQUENCE generation strategy for the primary key field
all Hibernate entity classes.

* This requires a database sequence to be created at DB init time,
which was achieved with *documentation* telling the user to do it manually.

## Reasoning

AUTO strategy on Postgres leads to using a database
sequence called `hibernate_sequence` with an increment of `1`. This means
that hibernate has to do a round-trip to the database for every single database row
it wishes to create so it can allocate a unique id value.
This results in `select nextval(...)` being the most called SQL query by Hibernate,
which accounts for a few % of overall running time.

SEQUENCE strategy defaults to an INCREMENT (allocation size) value of 50, which means the sequence
has to be called much less often. For each call, Hibernate knows it can use all values
between the current value and current value + 49 before it has to ask again for a value.
I also gave it a new name (`emap_id_sequence`) to avoid it accidentally using any stale
`hibernate_sequence` objects that might still exist (and be configured to an increment of 1).

This caused a problem, in that there seems to be a bug/feature of Hibernate in that it only
automatically creates sequences if that sequence doesn't already exist in *any* schema in the database.
Since we differentiate different instances of Emap by schemas on the same database, this means that
it will only get created on the first instance you run.

I tried a few things like adding `SequenceGenerator` and/or `SequenceStyleGenerator` to TemporalCore,
but it didn't help.

In the end I decided that manually creating the sequence isn't that big a deal and have added this
step to the deployment documentation.

# Future work

We should probably initialise our database using a database migration tool such as Liquibase.
Getting Hibernate to do the exact thing in SQL that you want it to do can has been a problem before
(notably when trying to make the SQL Server version).

# Emap-Star

This repository describes the structure of the EMAP-Star database.

# Design considerations and conventions

## Intro

The entire database is modelled using plain Java objects which use Hibernate annotations to map these to the
relational database. We have chosen a relational structure for ease of use and enabling indexes to improve query speed.

## Package structure

- We are using a parent maven package (`Emap Star Schema`) with two child-packages: 
  `Emap Star Annotations` and `Emap Star Database Definition`
  - Inform Annotations defines an annotation preprocessor which allows audit classes to be written during compilation
  - Inform-DB defines the Hibernate entities and uses the annotation preprocessor

## Patient identity

- UCLH patients can have two types of identifiers: the medical record number (MRN, AKA hospital number) and the NHS number.
  These identifiers are tracked in the `mrn` table. 
- Sets of patient identifiers can be merged (e.g. a patient who has an existing MRN is mistakenly given a new MRN), 
  the `mrn_to_live` table allows the "live" identifiers for a patient to be tracked. 

## Temporal validity (time travel)

- EMAP should only update existing data if the information is different and the information is more recent than the current data.
- There are two types of temporal validity which are tracked in an audit table (e.g. `hospital_visit` has a `hospital_visit_audit` table).
  - `valid_from` and `valid_until` are used for the "what did the doctor/electronic patient record know" at a specific time
  - `stored_from` and `stored_until` are used for the "what did Emap know" at a specific time (e.g. time of processing).
- The majority of non-audit tables extend the `TemporalCore` class and have a `valid_from` and `stored_from` 
  to ensure that the live data can be updated with more recent data.
  - This class also requires implementing a copy constructor and creating an audit entity for ease of use within Emap Core. 

## Automated documentation

- The Java docs for our Hibernate entities have a `\brief ` section.
  This section is used for automated generation of our end-user documentation. 
    ```java
    /**
    * \brief Unique identifier in EMAP for this department record.
    *
    * This is the primary key for the department table.
    */
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long departmentId;  
    ```


## DB conventions

- Every table has a primary key ID field named after the name of the table with "_id" added to the end.
    
    ```
                             Table "star.mrn"
        Column     |           Type           | Collation | Nullable | Default
    ---------------+--------------------------+-----------+----------+---------
    mrn_id         | bigint                   |           | not null |
    ...
    ```
  - In Java, these are defined as `Long` types, which default to null, before auto generation. `long` would default to 0.
    ```java
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long mrnId;
    ```
- Foreign keys use the same name as the primary key
  ```java
  @ManyToOne
  @JoinColumn(name = "mrnId", nullable = false)
  private Mrn mrnId;
  ```
- Timestamps (date and times) are timezone aware (and automated testing enforces this) and named in the form `<something>Datetime`
- Dates should only have date information and be named in the form `<something>Date`
- `timestamp with time zone` is Postgres-specific so should not be specified manually. Hibernate does the right thing in this case.
  ```java
  private Instant admissionDatetime;
  private LocalDate addedDate;
  ```

-- Tables as temporary instance of OMOP+.
-- Adapted from HL7 segment definitions.
-- Matthew Gillman 27/09/18
-- On command line:
-- createdb INFORM_SCRATCH
-- psql -f create_dummy_uds.sql INFORM_SCRATCH
-- psql INFORM_SCRATCH
-- psql 


-- The following table is based on the PID segment.
-- HL7 version 2.7
-- not all fields have been included, e.g. driver's licence number


-- NB check all following against UCLH-specific docs.


DROP TABLE IF EXISTS PERSON_SCRATCH;

CREATE TABLE PERSON_SCRATCH (
	-----UNID SERIAL PRIMARY KEY, -- Postgres equiv of AUTOINCREMENT. Use bigserial instead if you anticipate the use of more than 2^31 identifiers over the lifetime of the table.
	HospitalNumber char(8), 
	--set_ID	int, -- Optional. starts at 1 for first occurrence of segment, 2 for 2nd, etc
	-- patient_ID	, -- Withdrawn in 2.7
	-----patient_ID_list	varchar(300) NOT NULL, -- Required. 300 is a guess. List of 1+ identifiers containing ID number, assigning authority, etc
	-- alternate_patient_ID	, -- Withdrawn
	PatientFullName	varchar(200) NOT NULL, -- Required. 200 is a guess XPN Family name ^ Given Name ^ 2nd etc names ^ suffix ^ prefix ^ ...^ name type code
	-- Mother's maiden name	, -- Optional. 
	DateOfBirth	timestamp, --varchar(24),	-- Optional. DTM. See http://www.hl7.eu/refactored/dtDTM.html
	Sex	char(1),	-- Optional. F,M,O,U,A or N.
	-- patient_alias	,	-- Withdrawn in v2.7
	-- race	,	-- optional. The values are US-centric.
	PatientAddress	varchar(200),	-- optional. The 200 is a guess at maximum length.
	-- county_code	,	-- Withdrawn
	-- home_phone_number	,	-- Backward compatibility
	-- business_phone_number	,	-- Backward compatibility
	--- primary_language	,	-- Optional. PID-15
	-- nationality	,	-- Withdrawn as of v2.7
	PatientDeathDate	timestamp, --varchar(24),	-- Optional. PID-29
	----patient_death_indicator	char(1),	-- Optional. PID-30.  Y = deceased, N = not deceased
	----identity_unknown_indicator char(1), 	-- Optional. PID-31 Y = patient's/person's identity unknown. Else N	
	LastUpdated	timestamp -- varchar(24),	-- Optional. DTM. PID-33)
);

-- The next table is based on the PV1 segment definition.
-- "The PV1 segment is used by Registration/Patient Administration applications
-- to communicate information on an account or visit-specific basis. The default 
-- is to send account level data. To use this segment for visit level data PV1-51
-- - Visit Indicator must be valued to "V"."
--
-- A note on the location fields:
-- "The facility ID, the optional fourth component of each patient location field,
-- is a HD data type that is uniquely associated with the healthcare facility containing
-- the location. A given institution, or group of intercommunicating institutions,
-- should establish a list of facilities that may be potential assignors of patient
-- locations. The list will be one of the institution's master dictionary lists. Since
-- third parties other than the assignors of patient locations may send or receive HL7
-- messages containing patient locations, the facility ID in the patient location may
-- not be the same as that implied by the sending and receiving systems identified in
-- the MSH. The facility ID must be unique across facilities at a given site. This
-- field is required for HL7 implementations that have more than a single healthcare
--  facility with bed locations, since the same
-- <point of care> ^ <room> ^ <bed> combination may exist at more than one facility."

DROP TABLE IF EXISTS PATIENT_VISIT;

-- We have patientID as secondary key.
CREATE TABLE PATIENT_VISIT (
	VISITID SERIAL PRIMARY KEY, -- Postgres equiv of AUTOINCREMENT. Use bigserial instead if you anticipate the use of more than 2^31 identifiers over the lifetime of the table.
	HospitalNumber char(8), --patient_ID_list	varchar(300) NOT NULL, -- Required. 300 is a guess. List of 1+ identifiers containing ID number, assigning authority, etc
	---set_ID	int,	-- Set ID - PV-1. Optional
	PatientClass char(1) NOT NULL, -- Required. PV1-2. Atos: S,I,P,O

	-- This field contains the patient's initial assigned location or the location to which the patient is being moved.
	----PatientLocation	varchar(200), -- Optional. PV1-3. <Point of Care (HD)> ^ <Room (HD)> ^ <Bed (HD)> ^ <Facility (HD)> ^ ....
	
	--admission_type	char(1), -- Optional. PV1-4. e.g. A accident, E emergency, N newborn (i.e. birth), etc.
	-- preadmit_number	, -- Optional PV1-5
	-----prior_location	varchar(200),	-- Opt. PV1-6
	-----attending_doctor	varchar(200),	-- opt.
	-----referring_doctor	varchar(200),	 -- opt

	-- The treatment or type of surgery that the patient is scheduled to receive
	-- e.g. MED Medical Service, SUR Surgical Service. It is a required field with trigger events A01 (admit/visit notification),
	-- A02 (transfer a patient), A14 (pending admit), A15 (pending transfer). 
	HospitalService	char(3),	 -- opt. 
	
	----temp_location	varchar(200),	-- opt.
	--preadmit_test_indicator	varchar(200), -- opt
	ReadmissionIndicator	char(1),	 -- opt We suggest using "R" for readmission or else null.
	-- some skipped over...

	-- Ashish: PV1-19 is optional and "Carecast does not populate it for most of the messages"
	--visit_number	int, -- opt PV1-19 This field contains the unique number assigned to each patient visit. Our PK instead?
	
	--discharge_disposition	varchar(200),	 -- opt PV1-36 e.g. discharged to home
	--discharged_to_location	varchar(200), -- opt PV1-37 <Discharge to Location (CWE)> ^ <Effective Date (DTM)>
	-- bed_status	,	 -- withdrawn. PV1-40

	-- This field indicates the point of care, room, bed, healthcare facility ID, and bed status to which the patient may be moved.
	---pending_location	varchar(200), 	-- Opt. PV1-42
	
	-- prior_temp_location	,
	AdmissionDate	timestamp, -- Opt. PV1-44. DTM
	DischargeDate	timestamp, -- Opt. PV1-45. DTM
	----alternate_vist_id	int, -- opt, PV1-50
	----visit_indicator	char(1)	-- opt, PV1-51. A Account level (default), V Visit level
	--- Service Episode Description
	--- Service Episode Identifier
	
	LastUpdated timestamp -- HL7 MessageDateTime 
	--NB is this the same as the LastUpdated in person table?
);


DROP TABLE IF EXISTS BEDVISIT;

CREATE TABLE BEDVISIT (
	BED_VISIT_ID BIGSERIAL PRIMARY KEY,
	patient_visit_id INTEGER NOT NULL, -- primary key of patient_visit table
	location varchar(30), -- i.e. an individual bed in the hospital.
	start_time timestamp,
	end_time timestamp
);



-- Now we create a little table to store the latest IDS UNID successfully
-- processed in the UDS. We can query this to see what UNID we need to starts
-- with next time we query the IDS. 
DROP TABLE IF EXISTS LAST_UNID_PROCESSED;

CREATE TABLE LAST_UNID_PROCESSED (
	LATEST INT PRIMARY KEY
);

-- Add mrn

INSERT INTO public.mrn (mrn_id, mrn, nhs_number, source_system, stored_from, research_opt_out) VALUES
    (1001, '40800000', '9999999999', 'EPIC', '2010-09-01 11:04:04Z', true),
    -- 1002 merged with 1003, 1003 surviving
    (1002, '60600000', '1111111111', 'caboodle', '2010-09-03 10:05:04Z', false),
    (1003, '30700000', null, 'EPIC', '2010-09-10 16:01:05Z', false),
-- change patient identifiers testing
    -- previous MRN matches
    (1004, null, '222222222', 'another', '2010-09-10 17:02:08Z', false),
    (1005, '50100010', '222222222', 'EPIC', '2010-09-10 18:02:08Z', false),
    (1006, '50100012', null, 'another', '2010-09-10 18:02:08Z', false),
    -- surviving MRN matches
    (1007, '51111111', null, 'EPIC', '2010-09-10 19:02:08Z', false),
    (1008, null, '111222223', 'another', '2010-09-10 19:02:08Z', false),
    -- unrelated MRNs
    (1009, null, '997372627', 'another', '2010-09-10 19:02:08Z', false),
    (1010, '707070700',  null, 'EPIC', '2010-09-10 19:02:08Z', false);

-- Add mrn_to_live

INSERT INTO public.mrn_to_live (mrn_to_live_id, stored_from, valid_from, live_mrn_id, mrn_id) VALUES
    (2001, '2010-09-01 11:04:04Z', '2010-09-01 11:04:04Z', 1001, 1001),
    (2002, '2010-09-03 10:05:04Z', '2010-09-03 11:04:04Z', 1003, 1002),
    (2003, '2010-09-10 16:01:05Z', '2010-09-03 13:06:05Z', 1003, 1003),
    (2004, '2010-09-10 17:02:08Z', '2010-09-05 14:05:04Z', 1005, 1004),
    (2005, '2010-09-10 17:02:08Z', '2010-09-05 14:05:04Z', 1005, 1005),
    (2006, '2010-09-10 17:02:08Z', '2010-09-05 14:05:04Z', 1005, 1006),
    (2007, '2010-09-10 17:02:08Z', '2010-09-05 14:05:04Z', 1007, 1007),
    (2008, '2010-09-10 17:02:08Z', '2010-09-05 14:05:04Z', 1008, 1008),
    (2009, '2010-09-10 17:02:08Z', '2010-09-05 14:05:04Z', 1009, 1009),
    (2010, '2010-09-10 17:02:08Z', '2010-09-05 14:05:04Z', 1010, 1010);


-- Add to core_demographic
INSERT INTO public.core_demographic (
    core_demographic_id, stored_from, valid_from, alive, date_of_birth, date_of_death,
    datetime_of_birth, datetime_of_death, firstname, home_postcode, lastname, middlename, mrn_id, sex
    ) VALUES
        (3001, '2012-09-17 13:25:00Z', '2010-09-14 15:27:00Z', true, '1970-05-05', null,
         '1970-05-05 04:10:00Z', null, 'Sesame', 'W12 0HG', 'Snaps', null, 1003, 'F'),
        (3002, '2010-09-01 11:04:04Z', '2011-02-11 10:00:52Z', true, '1980-01-01', null,
         '1980-01-01 00:00:00Z', null, 'lemon', null, 'zest', 'middle', 1001, 'F'),
        (3003, '2010-06-17 13:25:00Z', '2010-02-16 10:00:52Z', false, '1972-06-14', '2010-05-05',
         '1972-06-14 13:00:00Z', '2010-05-05 00:00:00Z', 'Terry', 'T5 1TT', 'AGBTESTD', null, 1002, 'M');

INSERT INTO public.hospital_visit (
    hospital_visit_id, stored_from, valid_from, admission_datetime, arrival_method, discharge_destination, discharge_disposition,
    discharge_datetime, encounter, patient_class, presentation_datetime, source_system, mrn_id
    ) VALUES
        (4001, '2012-09-17 13:25:00Z', '2010-09-14 15:27:00Z', null, 'Public trans', null, null,
         null, '123412341234', 'INPATIENT', '2012-09-17 13:25:00Z', 'EPIC', 1001),
        (4002, '2010-09-03 10:05:04Z', '2010-09-03 11:04:04Z', '2010-09-03 11:04:04Z', 'Ambulance', null, null,
         null, '1234567890', 'EMERGENCY', '2012-09-17 13:15:00Z', 'EPIC', 1002),
        (4003, '2010-06-17 13:25:00Z', '2010-02-16 10:00:52Z', null, null, null, null,
         null, '0999999999', null, null, 'WinPath', 1004);

-- locations

INSERT INTO public.department (department_id, hl7string, name, internal_id, rptGrpNine, isWardOrFlowArea, isCoreInpatientArea) VALUES
(11001, 'ACUN', 'EGA E03 ACU NURSERY', 2, 'ACUN', true, true),
(11002, 'MEDSURG', 'EMH MED SURG', 1, 'ACUN', true, true);


INSERT INTO public.department_state
(department_state_id, status, stored_from, stored_until, valid_from, valid_until, department_id, speciality) VALUES
(12001, 'Active', '2022-09-17 14:00:00Z', null, '2018-02-08 00:00:00Z', null, 11001, 'Dental - Oral Medicine'),
(12202, 'Deleted and Hidden', '2012-09-17 14:00:00Z', null, '2021-04-23 09:00:00Z', null, 11002, 'Maternity - Well Baby');

INSERT INTO public.room(room_id, hl7string, name, department_id) VALUES
(13001, 'E03ACUN BY12', 'BY12', 11001),
(13002, 'MED/SURG', 'Med/Surg', 11002);

INSERT INTO public.room_state
    (room_state_id, csn, is_ready, status, stored_from, stored_until, valid_from, valid_until, room_id) VALUES
(14001, 1158, true, 'Active', '2012-09-17 14:00:00Z', null, '2016-02-09 00:00:00Z', null,  13001),
(14002, 274, true, 'Deleted and Hidden', '2012-09-17 14:00:00Z', null, '2010-02-04 00:00:00Z', null,  13002);

INSERT INTO public.bed (bed_id, hl7string, room_id) VALUES
(15001, 'BY12-C49', 13001),
(15002, 'Med/Surg', 13002);

INSERT INTO public.bed_state
    (bed_state_id, csn, is_in_census, pool_bed_count, status, is_bunk,
     stored_from, stored_until, valid_from, valid_until, bed_id) VALUES
(16001, 4417, true, null, 'Active', false, '2012-09-17 14:00:00Z', null, '2016-02-09 00:00:00Z', null, 15001),
(16002, 11, false, 1, 'Active', false, '2012-09-17 14:00:00Z', null, '2011-05-02 00:00:00Z', null, 15002);

INSERT INTO public.location (location_id, location_string, department_id, room_id, bed_id) VALUES
(105001, 'T42E^T42E BY03^BY03-17', null, null, null),
(105002, 'T11E^T11E BY02^BY02-17', null, null, null),
(105003, 'T06C^T06C SR41^SR41-41', null, null, null),
(105004, 'T11E^T11E BY02^BY02-25', null, null, null),
(105005, 'ACUN^E03ACUN BY12^BY12-C49', 11001, 13001, 15001),
(105006, 'MEDSURG^MED/SURG^Med/Surg', 11002, 13002, 15002),
(105007, 'ACUN^null^null', 11001, null, null);


INSERT INTO public.location_visit (
    location_visit_id, stored_from, valid_from, admission_datetime, inferred_admission, inferred_discharge,
    discharge_datetime, hospital_visit_id, location_id) VALUES
    (106001, '2012-09-10 13:25:00Z', '2010-09-14 15:27:00Z', '2010-09-10 12:00:00Z', false, false,
     '2010-09-14 15:27:00Z', 4001, 105004),
    (106002, '2012-09-17 13:27:00Z', '2010-09-14 15:27:00Z', '2010-09-14 15:27:00Z', false, false,
     null, 4001, 105001),
    (106003, '2012-09-17 13:28:00Z', '2010-09-14 16:30:00Z', '2010-09-16 01:00:00Z', false, false,
     '2010-09-16 10:00:00Z',  4002, 105003),
    (106004, '2010-09-03 10:05:04Z', '2010-09-03 11:04:04Z', '2010-09-03 11:04:04Z', false, false,
     null, 4003, 105002);

INSERT INTO public.visit_observation_type (
    visit_observation_type_id, primary_data_type, source_observation_type, interface_id, id_in_application, name, stored_from, valid_from)
    VALUES (107001, null, 'flowsheet', '5', null, 'R HPSC IDG SW PRESENT', '2012-09-17 14:00:00Z', '2020-01-22 14:04:00Z'),
           (107002, null, 'flowsheet', null, '5', 'R HPSC IDG SW PRESENT', '2012-09-17 14:00:00Z', '2020-01-22 14:04:00Z'),
           (107003, null, 'flowsheet', '8', '8', null, '2012-09-17 14:00:00Z', '2020-01-22 14:04:00Z'),
           (107004, null, 'flowsheet', '10', '10', null, '2012-09-17 14:00:00Z', '2020-01-22 14:04:00Z'),
           (107005, null, 'flowsheet', '6466', '6466', null, '2012-09-17 14:00:00Z', '2020-01-22 14:04:00Z'),
           (107006, null, 'flowsheet', '28315', '28315', null, '2012-09-17 14:00:00Z', '2020-01-22 14:04:00Z');

INSERT INTO public.visit_observation (
    visit_observation_id, hospital_visit_id, stored_from, valid_from, comment, observation_datetime,
    unit, value_as_real, value_as_text, visit_observation_type_id, source_system)
    VALUES (108001, 4001, '2012-09-17 14:00:00Z', '2020-01-22 14:04:00Z', null, '2020-01-22 14:03:00Z',
            null, null, '140/90', 107001, 'EPIC'),
           (108002, 4001, '2012-09-17 14:00:00Z', '2020-01-22 14:04:00Z', null, '2020-01-22 14:03:00Z',
            null, 50.0, null, 107003, 'EPIC'),
           (108003, 4001, '2012-09-17 14:00:00Z', '2020-01-22 14:04:00Z', null, '2020-01-22 14:03:00Z',
            null, null, 'you should delete me', 107006, 'EPIC');


INSERT INTO public.lab_sample (
    lab_sample_id, stored_from, valid_from, collection_method, external_lab_number, receipt_at_lab_datetime,
    sample_collection_datetime, sample_site, specimen_type, mrn_id)
    VALUES (109001, '2022-02-02 14:00:00Z', '2020-01-01 14:04:00Z', null, '22U113534', '2020-01-01 14:04:00Z',
            '2020-01-01 10:04:00Z', null, 'swab', 1001);

--------
-- SDEs (forms)
--------

-- Form metadata
INSERT INTO public.form_definition (
    form_definition_id, stored_from, valid_from, internal_id, name)
VALUES
    (200001, '2019-05-05 00:00:00Z', '2019-05-05 00:00:00Z', '2056', 'UCLH ADVANCED TEP')
;
-- Form question metadata (SDE metadata)
INSERT INTO public.form_question (
    form_question_id, stored_from, valid_from, internal_id,
    concept_abbrev_name, concept_name,
    internal_value_type)
VALUES
(220001, '2019-05-05 00:00:00Z', '2019-05-05 00:00:00Z', 'UCLH#1205', 'ICU Discussion', 'ICU DISCUSSION', 'Boolean'),
(220002, '2019-05-05 00:00:00Z', '2019-05-05 00:00:00Z', 'UCLH#1209', NULL, 'INVASIVE MONITORING', 'Boolean'),
(220003, '2019-05-05 00:00:00Z', '2019-05-05 00:00:00Z', 'UCLH#1210', NULL, 'INOTROPIC SUPPORT', 'Boolean'),
(220004, '2019-05-05 00:00:00Z', '2019-05-05 00:00:00Z', 'UCLH#1211', NULL, 'RRT', 'Boolean'),
(220005, '2019-05-05 00:00:00Z', '2019-05-05 00:00:00Z', 'UCLH#1213', NULL, 'NIV OUTSIDE CCU', 'Boolean'),
(220006, '2019-05-05 00:00:00Z', '2019-05-05 00:00:00Z', 'UCLH#1218', NULL, 'IV ABX', 'Boolean'),
(220007, '2019-05-05 00:00:00Z', '2019-05-05 00:00:00Z', 'UCLH#1219', NULL, 'IV FLUIDS', 'Boolean'),
(220016, '2019-05-05 00:00:00Z', '2019-05-05 00:00:00Z', 'UCLH#4480', 'Facemask Oxygen', 'WARD LEVEL FACEMASK OXYGEN', 'Boolean'),
(220021, '2019-05-05 00:00:00Z', '2019-05-05 00:00:00Z', 'UCLH#4499', NULL, 'PATIENT DISCUSSION DETAILS', 'String'),
(220023, '2019-05-05 00:00:00Z', '2019-05-05 00:00:00Z', 'UCLH#4501', NULL, 'DISCUSSION DETAILS LPA/RELATIVES/OTHERS', 'String')
;

-- Instances of a answered form
INSERT INTO public.form (
    form_id, stored_from, valid_from, first_filed_datetime,
    form_definition_id, hospital_visit_id, internal_id, note_id, mrn_id)
VALUES
    --(210001, '2020-01-01 13:00:00Z', '2020-01-01 10:32:00Z', '2020-01-01 10:32:00Z', 'fred1', 200001, 4001, NULL, NULL),
    --(210002, '2020-01-01 15:00:00Z', '2020-01-01 11:38:00Z', '2020-01-01 11:38:00Z', 'carol2', 200001, 4001, NULL, NULL)
    --cur_val_utc_dttm cur_value_user_id RECORD_ID_VARCHAR
       -- internal_id = firstfiled_mrn_smartformid
(210001, '2022-01-31 04:00:00Z', '2022-01-25 01:46:30Z', '2022-01-25 01:46:30Z', 200001, 4001, '2022-01-25T01:46:30Z_40800000_2056', 987654321, NULL),
(210002, '2022-01-31 04:00:00Z', '2022-01-25 10:02:43Z', '2022-01-25 10:02:43Z', 200001, 4001, '2022-01-25T10:02:43Z_40800000_2056', 987654322, NULL),
(210003, '2022-01-31 04:00:00Z', '2022-01-25 10:27:48Z', '2022-01-25 10:27:48Z', 200001, 4001, '2022-01-25T10:27:48Z_40800000_2056', 987654323, NULL),
(210004, '2022-01-31 04:00:00Z', '2022-01-25 12:19:03Z', '2022-01-25 12:19:03Z', 200001, 4001, '2022-01-25T12:19:03Z_40800000_2056', 987654324, NULL),
(210005, '2022-01-31 04:00:00Z', '2022-01-25 12:28:13Z', '2022-01-25 12:28:13Z', 200001, 4001, '2022-01-25T12:28:13Z_40800000_2056', 987654325, NULL),
(210006, '2022-01-31 04:00:00Z', '2022-01-25 13:06:39Z', '2022-01-25 13:06:39Z', 200001, 4001, '2022-01-25T13:06:39Z_40800000_2056', 987654326, NULL),
(210007, '2022-01-31 04:00:00Z', '2022-01-25 15:56:21Z', '2022-01-25 15:56:21Z', 200001, 4001, '2022-01-25T15:56:21Z_40800000_2056', 987654327, NULL),
(210008, '2022-01-31 04:00:00Z', '2022-01-25 20:10:47Z', '2022-01-25 20:10:47Z', 200001, 4001, '2022-01-25T20:10:47Z_40800000_2056', 987654328, NULL),
(210009, '2022-01-31 04:00:00Z', '2022-01-25 21:36:57Z', '2022-01-25 21:36:57Z', 200001, 4001, '2022-01-25T21:36:57Z_40800000_2056', 987654329, NULL),
(210010, '2022-01-31 04:00:00Z', '2022-01-25 21:39:28Z', '2022-01-25 21:39:28Z', 200001, 4001, '2022-01-25T21:39:28Z_40800000_2056', 987654330, NULL),
(210011, '2022-01-31 04:00:00Z', '2022-01-25 23:04:11Z', '2022-01-25 23:04:11Z', 200001, 4001, '2022-01-25T23:04:11Z_40800000_2056', 987654331, NULL)
;

-- Instances of an answered question (SDE)
INSERT INTO public.form_answer (
    form_answer_id, stored_from, valid_from, context, value_as_text, value_as_boolean, value_as_datetime, internal_id, form_id, form_question_id)
VALUES
    --(230001, '2020-01-01 13:00:00Z', '2020-01-01 10:32:00Z', 'Sometimes', NULL, NULL, 210001, 220001)
(230001, '2022-01-31 04:00:00Z', '2022-01-25 01:46:30Z', 'NOTE', '0', FALSE, NULL, '10001', 210001, 220002),
(230002, '2022-01-31 04:00:00Z', '2022-01-25 01:46:30Z', 'NOTE', '0', FALSE, NULL, '10002', 210001, 220005),
(230003, '2022-01-31 04:00:00Z', '2022-01-25 01:46:30Z', 'NOTE', '1', TRUE, NULL, '10003', 210001, 220006),
(230004, '2022-01-31 04:00:00Z', '2022-01-25 01:46:30Z', 'NOTE', '1', TRUE, NULL, '10004', 210001, 220007),
(230012, '2022-01-31 04:00:00Z', '2022-01-25 01:46:30Z', 'NOTE', '1', TRUE, NULL, '10012', 210001, 220016),
(230021, '2022-01-31 04:00:00Z', '2022-01-25 10:02:43Z', 'NOTE', '0', FALSE, NULL, '10021', 210002, 220002),
(230022, '2022-01-31 04:00:00Z', '2022-01-25 10:02:43Z', 'NOTE', '0', FALSE, NULL, '10022', 210002, 220005),
(230023, '2022-01-31 04:00:00Z', '2022-01-25 10:02:43Z', 'NOTE', '1', TRUE, NULL, '10023', 210002, 220006),
(230024, '2022-01-31 04:00:00Z', '2022-01-25 10:02:43Z', 'NOTE', '1', TRUE, NULL, '10024', 210002, 220007),
(230032, '2022-01-31 04:00:00Z', '2022-01-25 10:02:43Z', 'NOTE', '1', TRUE, NULL, '10032', 210002, 220016),
(230037, '2022-01-31 04:00:00Z', '2022-01-25 10:02:43Z', 'NOTE', 'REDACTED', NULL, NULL, '10037', 210002, 220023),
(230042, '2022-01-31 04:00:00Z', '2022-01-25 10:27:48Z', 'NOTE', '1', TRUE, NULL, '10042', 210003, 220002),
(230043, '2022-01-31 04:00:00Z', '2022-01-25 10:27:48Z', 'NOTE', '1', TRUE, NULL, '10043', 210003, 220003),
(230044, '2022-01-31 04:00:00Z', '2022-01-25 10:27:48Z', 'NOTE', '1', TRUE, NULL, '10044', 210003, 220004),
(230048, '2022-01-31 04:00:00Z', '2022-01-25 10:27:48Z', 'NOTE', 'REDACTED', NULL, NULL, '10048', 210003, 220021),
(230050, '2022-01-31 04:00:00Z', '2022-01-25 12:19:03Z', 'NOTE', '0', FALSE, NULL, '10050', 210004, 220002),
(230051, '2022-01-31 04:00:00Z', '2022-01-25 12:19:03Z', 'NOTE', '0', FALSE, NULL, '10051', 210004, 220005),
(230052, '2022-01-31 04:00:00Z', '2022-01-25 12:19:03Z', 'NOTE', '1', TRUE, NULL, '10052', 210004, 220006),
(230053, '2022-01-31 04:00:00Z', '2022-01-25 12:19:03Z', 'NOTE', '1', TRUE, NULL, '10053', 210004, 220007),
(230061, '2022-01-31 04:00:00Z', '2022-01-25 12:19:03Z', 'NOTE', '1', TRUE, NULL, '10061', 210004, 220016),
(230065, '2022-01-31 04:00:00Z', '2022-01-25 12:19:03Z', 'NOTE', 'REDACTED', NULL, NULL, '10065', 210004, 220021),
(230067, '2022-01-31 04:00:00Z', '2022-01-25 12:19:03Z', 'NOTE', 'REDACTED', NULL, NULL, '10067', 210004, 220023),
(230072, '2022-01-31 04:00:00Z', '2022-01-25 12:28:13Z', 'NOTE', '0', FALSE, NULL, '10072', 210005, 220002),
(230073, '2022-01-31 04:00:00Z', '2022-01-25 12:28:13Z', 'NOTE', '1', TRUE, NULL, '10073', 210005, 220005),
(230074, '2022-01-31 04:00:00Z', '2022-01-25 12:28:13Z', 'NOTE', '1', TRUE, NULL, '10074', 210005, 220006),
(230075, '2022-01-31 04:00:00Z', '2022-01-25 12:28:13Z', 'NOTE', '1', TRUE, NULL, '10075', 210005, 220007),
(230084, '2022-01-31 04:00:00Z', '2022-01-25 12:28:13Z', 'NOTE', '1', TRUE, NULL, '10084', 210005, 220016),
(230088, '2022-01-31 04:00:00Z', '2022-01-25 12:28:13Z', 'NOTE', 'REDACTED', NULL, NULL, '10088', 210005, 220021),
(230094, '2022-01-31 04:00:00Z', '2022-01-25 13:06:39Z', 'NOTE', '1', TRUE, NULL, '10094', 210006, 220002),
(230095, '2022-01-31 04:00:00Z', '2022-01-25 13:06:39Z', 'NOTE', '1', TRUE, NULL, '10095', 210006, 220003),
(230096, '2022-01-31 04:00:00Z', '2022-01-25 13:06:39Z', 'NOTE', '1', TRUE, NULL, '10096', 210006, 220004),
(230100, '2022-01-31 04:00:00Z', '2022-01-25 13:06:39Z', 'NOTE', 'REDACTED', NULL, NULL, '10100', 210006, 220021),
(230102, '2022-01-31 04:00:00Z', '2022-01-25 15:56:21Z', 'NOTE', '1', TRUE, NULL, '10102', 210007, 220001),
(230103, '2022-01-31 04:00:00Z', '2022-01-25 15:56:21Z', 'NOTE', '1', TRUE, NULL, '10103', 210007, 220002),
(230104, '2022-01-31 04:00:00Z', '2022-01-25 15:56:21Z', 'NOTE', '1', TRUE, NULL, '10104', 210007, 220003),
(230105, '2022-01-31 04:00:00Z', '2022-01-25 15:56:21Z', 'NOTE', '0', FALSE, NULL, '10105', 210007, 220004),
(230106, '2022-01-31 04:00:00Z', '2022-01-25 15:56:21Z', 'NOTE', '0', FALSE, NULL, '10106', 210007, 220005),
(230107, '2022-01-31 04:00:00Z', '2022-01-25 15:56:21Z', 'NOTE', '1', TRUE, NULL, '10107', 210007, 220006),
(230108, '2022-01-31 04:00:00Z', '2022-01-25 15:56:21Z', 'NOTE', '1', TRUE, NULL, '10108', 210007, 220007),
(230116, '2022-01-31 04:00:00Z', '2022-01-25 15:56:21Z', 'NOTE', '1', TRUE, NULL, '10116', 210007, 220016),
(230129, '2022-01-31 04:00:00Z', '2022-01-25 20:10:47Z', 'NOTE', '0', FALSE, NULL, '10129', 210008, 220002),
(230130, '2022-01-31 04:00:00Z', '2022-01-25 20:10:47Z', 'NOTE', '1', TRUE, NULL, '10130', 210008, 220005),
(230131, '2022-01-31 04:00:00Z', '2022-01-25 20:10:47Z', 'NOTE', '1', TRUE, NULL, '10131', 210008, 220006),
(230132, '2022-01-31 04:00:00Z', '2022-01-25 20:10:47Z', 'NOTE', '1', TRUE, NULL, '10132', 210008, 220007),
(230140, '2022-01-31 04:00:00Z', '2022-01-25 20:10:47Z', 'NOTE', '1', TRUE, NULL, '10140', 210008, 220016),
(230144, '2022-01-31 04:00:00Z', '2022-01-25 20:10:47Z', 'NOTE', 'REDACTED', NULL, NULL, '10144', 210008, 220021),
(230150, '2022-01-31 04:00:00Z', '2022-01-25 21:36:57Z', 'NOTE', '0', FALSE, NULL, '10150', 210009, 220002),
(230151, '2022-01-31 04:00:00Z', '2022-01-25 21:36:57Z', 'NOTE', '1', TRUE, NULL, '10151', 210009, 220005),
(230152, '2022-01-31 04:00:00Z', '2022-01-25 21:36:57Z', 'NOTE', '1', TRUE, NULL, '10152', 210009, 220006),
(230153, '2022-01-31 04:00:00Z', '2022-01-25 21:36:57Z', 'NOTE', '1', TRUE, NULL, '10153', 210009, 220007),
(230161, '2022-01-31 04:00:00Z', '2022-01-25 21:36:57Z', 'NOTE', '1', TRUE, NULL, '10161', 210009, 220016),
(230165, '2022-01-31 04:00:00Z', '2022-01-25 21:36:57Z', 'NOTE', 'REDACTED', NULL, NULL, '10165', 210009, 220021),
(230171, '2022-01-31 04:00:00Z', '2022-01-25 21:39:28Z', 'NOTE', '0', FALSE, NULL, '10171', 210010, 220002),
(230172, '2022-01-31 04:00:00Z', '2022-01-25 21:39:28Z', 'NOTE', '0', FALSE, NULL, '10172', 210010, 220005),
(230173, '2022-01-31 04:00:00Z', '2022-01-25 21:39:28Z', 'NOTE', '1', TRUE, NULL, '10173', 210010, 220006),
(230174, '2022-01-31 04:00:00Z', '2022-01-25 21:39:28Z', 'NOTE', '1', TRUE, NULL, '10174', 210010, 220007),
(230182, '2022-01-31 04:00:00Z', '2022-01-25 21:39:28Z', 'NOTE', '1', TRUE, NULL, '10182', 210010, 220016),
(230186, '2022-01-31 04:00:00Z', '2022-01-25 21:39:28Z', 'NOTE', 'REDACTED', NULL, NULL, '10186', 210010, 220021),
(230187, '2022-01-31 04:00:00Z', '2022-01-25 21:39:28Z', 'NOTE', 'REDACTED', NULL, NULL, '10187', 210010, 220021),
(230188, '2022-01-31 04:00:00Z', '2022-01-25 21:39:28Z', 'NOTE', 'REDACTED', NULL, NULL, '10188', 210010, 220021),
(230190, '2022-01-31 04:00:00Z', '2022-01-25 21:39:28Z', 'NOTE', 'REDACTED', NULL, NULL, '10190', 210010, 220023),
(230194, '2022-01-31 04:00:00Z', '2022-01-25 23:04:11Z', 'NOTE', '0', FALSE, NULL, '10194', 210011, 220002),
(230195, '2022-01-31 04:00:00Z', '2022-01-25 23:04:11Z', 'NOTE', '0', FALSE, NULL, '10195', 210011, 220005),
(230196, '2022-01-31 04:00:00Z', '2022-01-25 23:04:11Z', 'NOTE', '1', TRUE, NULL, '10196', 210011, 220006),
(230197, '2022-01-31 04:00:00Z', '2022-01-25 23:04:11Z', 'NOTE', '1', TRUE, NULL, '10197', 210011, 220007),
(230205, '2022-01-31 04:00:00Z', '2022-01-25 23:04:11Z', 'NOTE', '1', TRUE, NULL, '10205', 210011, 220016),
(230209, '2022-01-31 04:00:00Z', '2022-01-25 23:04:11Z', 'NOTE', 'REDACTED', NULL, NULL, '10209', 210011, 220021)
;

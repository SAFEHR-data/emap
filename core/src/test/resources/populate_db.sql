-- Add mrn

INSERT INTO public.mrn (mrn_id, mrn, nhs_number, source_system, stored_from) VALUES
    (1001, '40800000', '9999999999', 'EPIC', '2010-09-01 11:04:04'),
    (1002, '60600000', '1111111111', 'caboodle', '2010-09-03 10:05:04'),
    (1003, '30700000', null, 'EPIC', '2010-09-10 16:01:05'),
-- change patient identifiers testing
    -- previous MRN matches
    (1004, null, '222222222', 'another', '2010-09-10 17:02:08'),
    (1005, '50100010', '222222222', 'EPIC', '2010-09-10 18:02:08'),
    (1006, '50100012', null, 'another', '2010-09-10 18:02:08'),
    -- surviving MRN matches
    (1007, '51111111', null, 'EPIC', '2010-09-10 19:02:08'),
    (1008, null, '111222223', 'another', '2010-09-10 19:02:08'),
    -- unrelated MRNs
    (1009, null, '997372627', 'another', '2010-09-10 19:02:08'),
    (1010, '707070700',  null, 'EPIC', '2010-09-10 19:02:08');

-- Add mrn_to_live

INSERT INTO public.mrn_to_live (mrn_to_live_id, stored_from, valid_from, live_mrn_id, mrn_id) VALUES
    (2001, '2010-09-01 11:04:04', '2010-09-01 11:04:04', 1001, 1001),
    (2002, '2010-09-03 10:05:04', '2010-09-03 11:04:04', 1003, 1002),
    (2003, '2010-09-10 16:01:05', '2010-09-03 13:06:05', 1003, 1003),
    (2004, '2010-09-10 17:02:08', '2010-09-05 14:05:04', 1005, 1004),
    (2005, '2010-09-10 17:02:08', '2010-09-05 14:05:04', 1005, 1005),
    (2006, '2010-09-10 17:02:08', '2010-09-05 14:05:04', 1005, 1006),
    (2007, '2010-09-10 17:02:08', '2010-09-05 14:05:04', 1007, 1007),
    (2008, '2010-09-10 17:02:08', '2010-09-05 14:05:04', 1008, 1008),
    (2009, '2010-09-10 17:02:08', '2010-09-05 14:05:04', 1009, 1009),
    (2010, '2010-09-10 17:02:08', '2010-09-05 14:05:04', 1010, 1010);


-- Add to core_demographic
INSERT INTO public.core_demographic (
    core_demographic_id, stored_from, valid_from, alive, date_of_birth, date_of_death,
    datetime_of_birth, datetime_of_death, firstname, home_postcode, lastname, middlename, mrn_id, sex
    ) VALUES
        (3001, '2012-09-17 13:25:00', '2010-09-14 15:27:00', true, '1970-05-05', null,
         '1970-05-05 04:10:00', null, 'Sesame', 'W12 0HG', 'Snaps', null, 1003, 'F'),
        (3002, '2010-09-01 11:04:04', '2011-02-11 10:00:52', true, '1980-01-01', null,
         '1980-01-01 00:00:00', null, 'lemon', null, 'zest', 'middle', 1001, 'F'),
        (3003, '2010-06-17 13:25:00', '2010-02-16 10:00:52', false, '1972-06-14', '2010-05-05',
         '1972-06-14 13:00:00', '2010-05-05 00:00:00', 'Terry', 'T5 1TT', 'AGBTESTD', null, 1002, 'M');

INSERT INTO public.hospital_visit (
    hospital_visit_id, stored_from, valid_from, admission_time, arrival_method, discharge_destination, discharge_disposition,
    discharge_time, encounter, patient_class, presentation_time, source_system, mrn_id
    ) VALUES
        (4001, '2012-09-17 13:25:00', '2010-09-14 15:27:00', null, 'Public trans', null, null,
         null, '123412341234', 'INPATIENT', '2012-09-17 13:25:00', 'EPIC', 1001),
        (4002, '2010-09-03 10:05:04', '2010-09-03 11:04:04', '2010-09-03 11:04:04', 'Ambulance', null, null,
         null, '1234567890', 'EMERGENCY', '2012-09-17 13:15:00', 'EPIC', 1002),
        (4003, '2010-06-17 13:25:00', '2010-02-16 10:00:52', null, null, null, null,
         null, '0999999999', null, null, 'WinPath', 1004);

-- locations

INSERT INTO public.department (department_id, hl7string, name, speciality) VALUES
(11001, 'ACUN', 'EGA E03 ACU NURSERY', 'Maternity - Well Baby'),
(11002, 'MEDSURG', 'EMH MED SURG', null);


INSERT INTO public.department_state
(department_state_id, status, stored_from, stored_until, valid_from, valid_until, department_id) VALUES
(12001, 'Active', '2012-09-17 14:00:00', null, '2018-02-08 00:00:00', null, 11001),
(12202, 'Deleted and Hidden', '2012-09-17 14:00:00', null, '2021-04-23 09:00:00', null, 11002);

INSERT INTO public.room(room_id, hl7string, name, department_id) VALUES
(13001, 'E03ACUN BY12', 'BY12', 11001),
(13002, 'MED/SURG', 'Med/Surg', 11002);

INSERT INTO public.room_state
    (room_state_id, csn, is_ready, status, stored_from, stored_until, valid_from, valid_until, room_id) VALUES
(14001, 1158, true, 'Active', '2012-09-17 14:00:00', null, '2016-02-09 00:00:00', null,  13001),
(14002, 274, true, 'Deleted and Hidden', '2012-09-17 14:00:00', null, '2010-02-04 00:00:00', null,  13002);

INSERT INTO public.bed (bed_id, hl7string, room_id) VALUES
(15001, 'BY12-C49', 13001),
(15002, 'Med/Surg', 13002);

INSERT INTO public.bed_state
    (bed_state_id, csn, is_in_census, pool_bed_count, status, is_bunk,
     stored_from, stored_until, valid_from, valid_until, bed_id) VALUES
(16001, 4417, true, null, 'Active', false, '2012-09-17 14:00:00', null, '2016-02-09 00:00:00', null, 15001),
(16002, 11, false, 1, 'Active', false, '2012-09-17 14:00:00', null, '2011-05-02 00:00:00', null, 15002);

INSERT INTO public.location (location_id, location_string, department_id, room_id, bed_id) VALUES
(105001, 'T42E^T42E BY03^BY03-17', null, null, null),
(105002, 'T11E^T11E BY02^BY02-17', null, null, null),
(105003, 'T06C^T06C SR41^SR41-41', null, null, null),
(105004, 'T11E^T11E BY02^BY02-25', null, null, null),
(105005, 'ACUN^E03ACUN BY12^BY12-C49', 11001, 13001, 15001),
(105006, 'MEDSURG^MED/SURG^Med/Surg', 11002, 13002, 15002),
(105007, 'ACUN^null^null', 11001, null, null);


INSERT INTO public.location_visit (
    location_visit_id, stored_from, valid_from, admission_time, inferred_admission, inferred_discharge,
    discharge_time, hospital_visit_id, location_id) VALUES
    (106001, '2012-09-10 13:25:00', '2010-09-14 15:27:00', '2010-09-10 12:00:00', false, false,
     '2010-09-14 15:27:00', 4001, 105004),
    (106002, '2012-09-17 13:27:00', '2010-09-14 15:27:00', '2010-09-14 15:27:00', false, false,
     null, 4001, 105001),
    (106003, '2012-09-17 13:28:00', '2010-09-14 16:30:00', '2010-09-16 01:00:00', false, false,
     '2010-09-16 10:00:00',  4002, 105003),
    (106004, '2010-09-03 10:05:04', '2010-09-03 11:04:04', '2010-09-03 11:04:04', false, false,
     null, 4003, 105002);

INSERT INTO public.visit_observation_type (
    visit_observation_type_id, primary_data_type, source_observation_type, interface_id, id_in_application, name, stored_from, valid_from)
    VALUES (107001, null, 'flowsheet', '5', null, 'R HPSC IDG SW PRESENT', '2012-09-17 14:00:00', '2020-01-22 14:04:00'),
           (107002, null, 'flowsheet', null, '5', 'R HPSC IDG SW PRESENT', '2012-09-17 14:00:00', '2020-01-22 14:04:00'),
           (107003, null, 'flowsheet', '8', '8', null, '2012-09-17 14:00:00', '2020-01-22 14:04:00'),
           (107004, null, 'flowsheet', '10', '10', null, '2012-09-17 14:00:00', '2020-01-22 14:04:00'),
           (107005, null, 'flowsheet', '6466', '6466', null, '2012-09-17 14:00:00', '2020-01-22 14:04:00'),
           (107006, null, 'flowsheet', '28315', '28315', null, '2012-09-17 14:00:00', '2020-01-22 14:04:00');

INSERT INTO public.visit_observation (
    visit_observation_id, hospital_visit_id, stored_from, valid_from, comment, observation_datetime,
    unit, value_as_real, value_as_text, visit_observation_type_id, source_system)
    VALUES (108001, 4001, '2012-09-17 14:00:00', '2020-01-22 14:04:00', null, '2020-01-22 14:03:00',
            null, null, '140/90', 107001, 'EPIC'),
           (108002, 4001, '2012-09-17 14:00:00', '2020-01-22 14:04:00', null, '2020-01-22 14:03:00',
            null, 50.0, null, 107003, 'EPIC'),
           (108003, 4001, '2012-09-17 14:00:00', '2020-01-22 14:04:00', null, '2020-01-22 14:03:00',
            null, null, 'you should delete me', 107006, 'EPIC');



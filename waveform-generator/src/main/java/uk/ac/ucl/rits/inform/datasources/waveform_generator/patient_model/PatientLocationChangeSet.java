package uk.ac.ucl.rits.inform.datasources.waveform_generator.patient_model;

import java.util.List;

public record PatientLocationChangeSet(
        List<PatientDetails> admitList,
        List<PatientDetails> dischargeList,
        List<PatientDetails> transferList) {
}

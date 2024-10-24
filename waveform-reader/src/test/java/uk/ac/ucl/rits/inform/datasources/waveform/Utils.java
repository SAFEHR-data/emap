package uk.ac.ucl.rits.inform.datasources.waveform;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class Utils {
    static String readHl7FromResource(String relativeResourceFilePath) throws IOException, URISyntaxException {
        ClassLoader classLoader = Utils.class.getClassLoader();
        URI uri = classLoader.getResource(relativeResourceFilePath).toURI();
        List<String> readAllLines = Files.readAllLines(Path.of(uri));
        return String.join("\r", readAllLines) + "\r";
    }
}

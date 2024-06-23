import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class MultipleHttpRequestToCSvDataflow {

  public static void main(String[] args) {
    // Define the base URL of the service
    final String baseUrl = "https://www.red.cl/restservice_v2/rest/conocerecorrido?codsint=";

    Pipeline pipeline = Pipeline.create();

    // Read codes from a text file (replace "codsint.txt" with your actual file path)
    PCollection<String> codsint = pipeline
        .apply("Read Codsint", TextIO.read().from("codsint.txt"));

    // Create a PCollection of KV pairs (codsint, response)
    PCollection<KV<String, String>> codsintResponse = codsint.apply(
        "Make HTTP Requests", ParDo.of(new MakeHttpRequestFn(baseUrl))
    );

    // Format KV pairs into CSV lines
    PCollection<String> csvLines = codsintResponse.apply(
        "Format to CSV", Map.withOutputFn(new FormatToCsvFn())
    );

    // Write CSV lines to a text file (replace "responses.csv" with your actual file path)
    csvLines.apply(
        "Write CSV", TextIO.write().to("responses.csv").withHeader("codsint,response")
    );

    pipeline.run();
  }

  static class MakeHttpRequestFn extends DoFn<String, KV<String, String>> {

    private final String baseUrl;

    public MakeHttpRequestFn(String baseUrl) {
      this.baseUrl = baseUrl;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws IOException {
      String codsint = c.element();
      String urlString = baseUrl + codsint;

      try {
        URL url = new URL(urlString);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");

        StringBuilder response = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
          String inputLine;
          while ((inputLine = reader.readLine()) != null) {
            response.append(inputLine);
          }
        }

        c.output(KV.of(codsint, response.toString()));
        connection.disconnect();
      } catch (IOException e) {
        c.reportError(new Throwable("Error processing codsint " + codsint + ": " + e.getMessage()));
      }
    }
  }

  static class FormatToCsvFn extends SimpleFunction<KV<String, String>, String> {

    @Override
    public String apply(KV<String, String> input) {
      return String.format("%s,%s", input.getKey(), input.getValue());
    }
  }
}

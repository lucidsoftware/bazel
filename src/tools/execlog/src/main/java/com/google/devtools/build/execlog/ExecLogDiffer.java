import com.google.common.annotations.VisibleForTesting;
import com.google.devtools.build.execlog.DifferOptions;
import com.google.devtools.build.lib.exec.Protos.SpawnExec;
import com.google.devtools.build.lib.exec.SpawnLogReconstructor;
import com.google.devtools.build.lib.util.io.MessageInputStream;
import com.google.devtools.build.lib.util.io.MessageInputStreamWrapper.BinaryInputStreamWrapper;
import com.google.devtools.build.lib.util.io.MessageInputStreamWrapper.JsonInputStreamWrapper;
import com.google.devtools.common.options.OptionsParser;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.util.List;

public final class ExecLogDiffer {
  private ExecLogDiffer() {}

  private static byte[] readFirstFourBytes(String path) throws IOException {
    try (InputStream in = new FileInputStream(path)) {
      return in.readNBytes(4);
    }
  }
  @VisibleForTesting
  static MessageInputStream<SpawnExec> getMessageInputStream(String path) throws IOException {
    byte[] b = readFirstFourBytes(path);
    if (b.length == 4
        && b[0] == 0x28
        && b[1] == (byte) 0xb5
        && b[2] == 0x2f
        && b[3] == (byte) 0xfd) {
      // Looks like a compact file (zstd-compressed).
      // This is definitely not a JSON file (the first byte is not '{') and definitely not a
      // binary file (the first byte would indicate the size of the first message, and the
      // second byte would indicate an invalid wire type).
      return new SpawnLogReconstructor(new FileInputStream(path));
    }
    if (b.length >= 2 && b[0] == '{' && b[1] == '\n') {
      // Looks like a JSON file.
      // This is definitely not a compact file (the first byte is not 0x28) and definitely not a
      // binary file (the first byte would indicate the size of the first message, and the
      // second byte would indicate a field with number 1 and wire type I32, which doesn't match
      // the proto definition).
      return new JsonInputStreamWrapper<>(
        new FileInputStream(path), SpawnExec.getDefaultInstance());
    }
    // Otherwise assume it's a binary file.
    return new BinaryInputStreamWrapper<>(
      new FileInputStream(path), SpawnExec.getDefaultInstance());
  }

  public static void main(String[] args) throws Exception {
    // Parse command line options
    OptionsParser op = OptionsParser.builder().optionsClasses(DifferOptions.class).build();
    op.parseAndExitUponError(args);

    // Get the options
    DifferOptions options = op.getOptions(DifferOptions.class);
    List<String> remainingArgs = op.getResidue();

    // Check for unexpected options
    if (!remainingArgs.isEmpty()) {
      System.err.println("Unexpected options: " + String.join(" ", remainingArgs));
      System.exit(1);
    }

    // Check for required options
    if (options.logPath == null || options.logPath.size() != 2) {
      System.err.println("Exactly two --log_path values are required for comparison.");
      System.exit(1);
    } 

    // Get the log paths, the output path, and the allTargets flag
    String logPath1 = options.logPath.get(0);
    String logPath2 = options.logPath.get(1);
    String outputPath = options.outputPath != null ? options.outputPath : null;
    boolean allTargets = options.allTargets;
  }
}
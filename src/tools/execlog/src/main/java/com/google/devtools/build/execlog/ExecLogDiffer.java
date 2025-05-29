import com.google.common.annotations.VisibleForTesting;
import com.google.devtools.build.lib.exec.Protos.SpawnExec;
import com.google.devtools.build.lib.exec.SpawnLogReconstructor;
import com.google.devtools.build.lib.util.io.MessageInputStream;
import com.google.devtools.build.lib.util.io.MessageInputStreamWrapper.BinaryInputStreamWrapper;
import com.google.devtools.build.lib.util.io.MessageInputStreamWrapper.JsonInputStreamWrapper;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;

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
  
  }
}
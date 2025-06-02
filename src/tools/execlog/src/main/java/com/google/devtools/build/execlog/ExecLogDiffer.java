package com.google.devtools.build.execlog;

import com.google.common.annotations.VisibleForTesting;
import com.google.devtools.build.execlog.DifferOptions;
import com.google.devtools.build.lib.exec.Protos.SpawnExec;
import com.google.devtools.build.lib.exec.Protos.EnvironmentVariable;
import com.google.devtools.build.lib.exec.Protos.Platform;
import com.google.devtools.build.lib.exec.Protos.File;
import com.google.devtools.build.lib.exec.Protos.Digest;
import com.google.devtools.build.lib.exec.SpawnLogReconstructor;
import com.google.devtools.build.lib.util.io.MessageInputStream;
import com.google.devtools.build.lib.util.io.MessageInputStreamWrapper.BinaryInputStreamWrapper;
import com.google.devtools.build.lib.util.io.MessageInputStreamWrapper.JsonInputStreamWrapper;
import com.google.devtools.common.options.OptionsParser;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Set;
import java.util.HashSet;
import java.time.LocalDate;

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

  // Represents necessary details of a SpawnExec object
  // Assumes immutability: attributes are set once in the constructor and not modified later
  // Relies on precomputed hashes for comparisons
  static class SpawnExecDetails {
    String targetLabel; // Label of the target for which this SpawnExec was created
    Inputs inputs; // Inputs for the SpawnExec
    Outputs outputs; // Outputs for the SpawnExec
    int inputHash; // Hash of the inputs
    int outputHash; // Hash of the outputs

    SpawnExecDetails(SpawnExec ex) {
      this.targetLabel = ex.getTargetLabel();
      this.inputs = new Inputs(ex);
      this.outputs = new Outputs(ex);
      this.inputHash = this.inputs.hashCode();
      this.outputHash = this.outputs.hashCode();
    }

    @Override
    public int hashCode() {
      return Objects.hash(inputs, outputs);
    }

    static class Inputs {
      List<String> commandArgs; // List of command line arguments for action
      List<EnvironmentVariable> environmentVariables; // List of input directory paths
      Platform platform; // Platform information
      List<File> files; // List of file details

      Inputs(SpawnExec ex) {
        this.commandArgs = ex.getCommandArgsList();
        this.environmentVariables = ex.getEnvironmentVariablesList();
        this.platform = ex.getPlatform();
        this.files = ex.getInputsList();
      }

      @Override
      public int hashCode() {
        return Objects.hash(commandArgs, environmentVariables, platform, files);
      }
    }

    static class Outputs {
      List<File> files; // List of output file details
    
      Outputs(SpawnExec ex) {
        this.files = ex.getActualOutputsList();
      }

      @Override
      public int hashCode() {
        return Objects.hash(files);
      }
    }
  }

  // Represents the final output report
  static class OutputReport {
    String timestamp; // Timestamp of the report generation
    String log1Path; // Path to the first log file
    String log2Path; // Path to the second log file
    Map<String, NonDeterministicTarget> nonDeterministicTargets; // Map of non-deterministic targets

    OutputReport(String timestamp, String log1Path, String log2Path) {
      this.timestamp = timestamp;
      this.log1Path = log1Path;
      this.log2Path = log2Path;
      this.nonDeterministicTargets = new HashMap<>();
    }

    public void addNonDeterministicTarget(String targetLabel, NonDeterministicTarget target) {
      if (target != null && targetLabel != null) {
        this.nonDeterministicTargets.put(targetLabel, target);
      }
    }
  }

  static class NonDeterministicTarget {
    String targetLabel; // Label of the non-deterministic target
    List<MismatchedAction> mismatchedActions; // List of mismatched actions

    NonDeterministicTarget(String targetLabel) {
      this.targetLabel = targetLabel != null ? targetLabel : "";
      this.mismatchedActions = new ArrayList<>();
    }

    public void addMismatchedAction(MismatchedAction action) {
      if (action != null) {
        this.mismatchedActions.add(action);
      }
    }
  }

  static class MismatchedAction {
    String type; // "OUTPUT", "INPUT", or "TARGET_NOT_FOUND"
    SpawnExecDetails details; // Details from the first log
    SpawnExecDetails details2; // Details from the second log
    List<Diff> diffs; // List of differences between the two details

    MismatchedAction(String type, SpawnExecDetails details2) {
      this.type = type;
      this.details = null;
      this.details2 = details2;
      this.diffs = new ArrayList<>();
    }

    MismatchedAction() {}

    static class Diff {
      String type; // e.g., "FILE_HASH", "FILE_PATH", "MISSING_FILE"
      String log1Value; // Value from the first log
      String log2Value; // Value from the second log

      Diff(String type, String log1Value, String log2Value) {
        this.type = type;
        this.log1Value = log1Value;
        this.log2Value = log2Value;
      }

      Diff() {}
    }

    public void addDetails(SpawnExecDetails details) {
        if (details != null) {
            this.details = details;
            computeAndSetDiffs();
        }
    }

    // Computes and sets the diffs for this mismatched action
    private void computeAndSetDiffs() {
        this.diffs = computeDiffs();
    }

    // Computes diffs between the two SpawnExecDetails
    private List<Diff> computeDiffs() {
        List<Diff> diffs = new ArrayList<>();
        if (type.equals("OUTPUT") && details != null && details2 != null) {
            // Compare files in outputs
            Set<String> outputPaths1 = new HashSet<>();
            for (File file : details.outputs.files) {
                outputPaths1.add(file.getPath());
            }

            Set<String> outputPaths2 = new HashSet<>();
            for (File file : details2.outputs.files) {
                outputPaths2.add(file.getPath());
            }

            // Find missing files in details
            for (String path : outputPaths1) {
                if (!outputPaths2.contains(path)) {
                    diffs.add(new Diff("MISSING_FILE", path, null));
                }
            }

            // Find missing files in details2
            for (String path : outputPaths2) {
                if (!outputPaths1.contains(path)) {
                    diffs.add(new Diff("MISSING_FILE", null, path));
                }
            }

            // Compare hashes for common files
            for (File file1 : details.outputs.files) {
                for (File file2 : details2.outputs.files) {
                    if (file1.getPath().equals(file2.getPath())) {
                        if (!file1.getDigest().getHash().equals(file2.getDigest().getHash())) {
                            diffs.add(new Diff("FILE_HASH", file1.getDigest().getHash(), file2.getDigest().getHash()));
                        }
                    }
                }
            }
        }
        return diffs;
    }
  }

  private static void addMismatchedAction(OutputReport report, String targetLabel, String actionType, SpawnExecDetails details2) {
    NonDeterministicTarget nonDetTarget = report.nonDeterministicTargets
        .computeIfAbsent(targetLabel, k -> new NonDeterministicTarget(targetLabel));
    MismatchedAction action = new MismatchedAction(actionType, details2);
    nonDetTarget.addMismatchedAction(action);
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

    // Initialize data structures for the target actions and output
    Map<String, Map<Integer, Integer>> targetActionsMap = new HashMap<>();
    Map<String, Map<String, SpawnExecDetails>> mismatchedActionsMap = new HashMap<>();
    OutputReport report = new OutputReport(LocalDate.now().toString(), logPath1, logPath2);

    // First pass: Read the first log and populate the target actions map
    try (MessageInputStream<SpawnExec> input1 = getMessageInputStream(logPath1)) {
      SpawnExec ex1;
      while ((ex1 = input1.read()) != null) {
        SpawnExecDetails details = new SpawnExecDetails(ex1);
        String targetLabel = details.targetLabel;
        int inputHash = details.inputHash;
        int outputHash = details.outputHash;

        // Add the action to the target actions map for that target label
        targetActionsMap
            .computeIfAbsent(targetLabel, k -> new HashMap<>())
            .put(inputHash, outputHash);
      }
    }

    // Second pass: Read the second log and compare with target actions from the first log
    try (MessageInputStream<SpawnExec> input2 = getMessageInputStream(logPath2)) {
      SpawnExec ex2;
      while ((ex2 = input2.read()) != null) {
        SpawnExecDetails details2 = new SpawnExecDetails(ex2);
        String targetLabel = details2.targetLabel;
        int inputHash2 = details2.inputHash;
        int outputHash2 = details2.outputHash;
  
        if (targetActionsMap.containsKey(targetLabel)) {
          // Check if the input hash from the second log matches any input hash in the first log for that target
          Map<Integer, Integer> actions = targetActionsMap.get(targetLabel);
          if (actions.containsKey(inputHash2)) {
            int outputHash1 = actions.get(inputHash2);
            if (outputHash1 != outputHash2) {
              // Mismatch found in outputs
              addMismatchedAction(report, targetLabel, "OUTPUT", details2);
            }
          } else {
            // Input hash from the second log does not exist in the first log for this target
            addMismatchedAction(report, targetLabel, "INPUT", details2);
          }
        } else {
          // Target label not found in the first log
          addMismatchedAction(report, targetLabel, "TARGET_NOT_FOUND", details2);
        }
      }
    }

    // Third pass: Reread the first log to get details for mismatched actions
    try (MessageInputStream<SpawnExec> input1 = getMessageInputStream(logPath1)) {
      SpawnExec ex1;
      while ((ex1 = input1.read()) != null) {
        SpawnExecDetails details = new SpawnExecDetails(ex1);
        String targetLabel = details.targetLabel;
        int inputHash = details.inputHash;
    
        if (report.nonDeterministicTargets.containsKey(targetLabel)) {
          NonDeterministicTarget nonDetTarget = report.nonDeterministicTargets.get(targetLabel);
    
          // Use a stream to find the matching MismatchedAction
          nonDetTarget.mismatchedActions.stream()
              .filter(action -> action.details2 != null && action.details2.inputHash == inputHash)
              .findFirst()
              .ifPresent(action -> action.addDetails(details));
        }
      }
    }
  }
}
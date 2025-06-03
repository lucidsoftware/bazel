package com.google.devtools.build.execlog;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.common.annotations.VisibleForTesting;
import com.google.devtools.build.execlog.DifferOptions;
import com.google.devtools.build.execlog.ExecLogParser;
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
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
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

  // Represents necessary details of a SpawnExec object
  // Assumes immutability: attributes are set once in the constructor and not modified later
  // Relies on precomputed hashes for comparisons
  static class SpawnExecDetails {
    final String targetLabel; // Label of the target for which this SpawnExec was created
    final Inputs inputs; // Inputs for the SpawnExec
    final Outputs outputs; // Outputs for the SpawnExec
    final int inputHash; // Hash of the inputs
    final int outputHash; // Hash of the outputs

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
      final List<String> commandArgs; // List of command line arguments for action
      final List<EnvironmentVariable> environmentVariables; // List of input directory paths
      final Platform platform; // Platform information
      final List<File> files; // List of file details

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
      final List<File> files; // List of output file details
    
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
      String path; // Path of the file if applicable
      String log1Value; // Value from the first log
      String log2Value; // Value from the second log

      Diff(String type, String log1Value, String log2Value, String path) {
        this.type = type;
        this.path = path != null ? path : "";
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
                    diffs.add(new Diff("MISSING_FILE", null, null, path));
                }
            }

            // Find missing files in details2
            for (String path : outputPaths2) {
                if (!outputPaths1.contains(path)) {
                    diffs.add(new Diff("MISSING_FILE", null, null, path));
                }
            }

            // Compare hashes for common files
            for (File file1 : details.outputs.files) {
                for (File file2 : details2.outputs.files) {
                    if (file1.getPath().equals(file2.getPath())) {
                        if (!file1.getDigest().getHash().equals(file2.getDigest().getHash())) {
                            diffs.add(new Diff("FILE_HASH", file1.getDigest().getHash(), file2.getDigest().getHash(), file1.getPath()));
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
    try (MessageInputStream<SpawnExec> input1 = ExecLogParser.getMessageInputStream(logPath1)) {
      SpawnExec ex1;
      while ((ex1 = input1.read()) != null) {
        SpawnExecDetails details = new SpawnExecDetails(ex1);
        String targetLabel = details.targetLabel;
        int inputHash = details.inputHash;
        int outputHash = details.outputHash;

        if (!allTargets && targetLabel.startsWith("@@")) {
          // Skip external dependencies if allTargets is false
          continue;
        }

        // Add the action to the target actions map for that target label
        targetActionsMap
            .computeIfAbsent(targetLabel, k -> new HashMap<>())
            .put(inputHash, outputHash);
      }
    }

    // Second pass: Read the second log and compare with target actions from the first log
    try (MessageInputStream<SpawnExec> input2 = ExecLogParser.getMessageInputStream(logPath2)) {
      SpawnExec ex2;
      while ((ex2 = input2.read()) != null) {
        SpawnExecDetails details2 = new SpawnExecDetails(ex2);
        String targetLabel = details2.targetLabel;
        int inputHash2 = details2.inputHash;
        int outputHash2 = details2.outputHash;

        if (!allTargets && targetLabel.startsWith("@@")) {
          // Skip external dependencies if allTargets is false
          continue;
        }
  
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
    try (MessageInputStream<SpawnExec> input1 = ExecLogParser.getMessageInputStream(logPath1)) {
      SpawnExec ex1;
      while ((ex1 = input1.read()) != null) {
        SpawnExecDetails details = new SpawnExecDetails(ex1);
        String targetLabel = details.targetLabel;
        int inputHash = details.inputHash;

        if (!allTargets && targetLabel.startsWith("@@")) {
          // Skip external dependencies if allTargets is false
          continue;
        }
    
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

    // Write the report as JSONL to the specified output path or stdout
    Gson gson = new Gson();

    // Determine output destination
    if (outputPath != null) {
      // Write to a file
      try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath, StandardCharsets.UTF_8))) {
        for (NonDeterministicTarget target : report.nonDeterministicTargets.values()) {
          // Serialize the NonDeterministicTarget object to a JSON string
          String jsonLine = gson.toJson(target);
          // Write the JSON string followed by a newline character
          writer.write(jsonLine);
          writer.newLine(); // Writes a system-dependent new line character
        }
      } catch (IOException e) {
        System.err.println("Error writing report to file: " + e.getMessage());
        System.exit(1);
      }
    } else {
      // Write to standard output (console)
      try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8))) {
        for (NonDeterministicTarget target : report.nonDeterministicTargets.values()) {
          String jsonLine = gson.toJson(target);
          writer.write(jsonLine);
          writer.newLine();
        }
      } catch (IOException e) {
        System.err.println("Error writing report to stdout: " + e.getMessage());
        System.exit(1);
      }
    }
  }
}
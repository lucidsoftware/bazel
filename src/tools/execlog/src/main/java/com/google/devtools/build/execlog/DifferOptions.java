// Copyright 2018 The Bazel Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.devtools.build.execlog;

import com.google.devtools.common.options.Option;
import com.google.devtools.common.options.OptionDocumentationCategory;
import com.google.devtools.common.options.OptionEffectTag;
import com.google.devtools.common.options.OptionsBase;
import java.util.List;

/** Options for execution log differ. */
public class DifferOptions extends OptionsBase {
  @Option(
      name = "log_path",
      defaultValue = "null", // Default value is null, allowing the check for size != 2
      category = "logging",
      documentationCategory = OptionDocumentationCategory.UNCATEGORIZED,
      effectTags = {OptionEffectTag.UNKNOWN},
      allowMultiple = true,
      help =
          "Exactly two log paths are required for comparison. The first log will be used as the"
              + " 'golden' standard, and the actions in the second log will be reordered to match"
              + " the first based on the first output's name. Actions unique to the second log"
              + " will appear at the end.")
  public List<String> logPath;

  @Option(
      name = "output_path",
      defaultValue = "null", // Default value is null, allowing check for empty list
      category = "logging",
      documentationCategory = OptionDocumentationCategory.UNCATEGORIZED,
      effectTags = {OptionEffectTag.UNKNOWN},
      // Note: ExecLogDiffer expects a single output path or null for stdout.
      // allowMultiple = true as was in original file.
      help =
          "Location where to put the output of the comparison. If left empty, the output will be"
              + " directed to stdout.")
  public String outputPath;

  @Option(
        name = "all_targets",
        defaultValue = "false",
        category = "logging",
        documentationCategory = OptionDocumentationCategory.UNCATEGORIZED,
        effectTags = {OptionEffectTag.UNKNOWN},
        help = "If set, checks for non-determinism in all targets, including external dependencies ('@@' targets)."
            + " If not set, only checks for non-determinism for targets inside the repository ('//' targets). "
  )
  public boolean allTargets;
}
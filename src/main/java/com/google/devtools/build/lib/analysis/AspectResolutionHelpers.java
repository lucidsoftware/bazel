// Copyright 2023 The Bazel Authors. All rights reserved.
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
package com.google.devtools.build.lib.analysis;

import static java.util.Collections.reverse;

import com.google.common.collect.ImmutableList;
import com.google.devtools.build.lib.analysis.AspectCollection.AspectCycleOnPathException;
import com.google.devtools.build.lib.analysis.starlark.StarlarkAspectPropagationContext;
import com.google.devtools.build.lib.cmdline.Label;
import com.google.devtools.build.lib.events.ExtendedEventHandler;
import com.google.devtools.build.lib.packages.AdvertisedProviderSet;
import com.google.devtools.build.lib.packages.Aspect;
import com.google.devtools.build.lib.packages.AspectClass;
import com.google.devtools.build.lib.packages.AspectDefinition;
import com.google.devtools.build.lib.packages.Attribute;
import com.google.devtools.build.lib.packages.Rule;
import com.google.devtools.build.lib.packages.RuleClass;
import com.google.devtools.build.lib.skyframe.toolchains.UnloadedToolchainContext;
import java.util.ArrayList;
import javax.annotation.Nullable;
import net.starlark.java.eval.EvalException;
import net.starlark.java.syntax.Location;

/** Helpers for aspect resolution. */
public final class AspectResolutionHelpers {
  private AspectResolutionHelpers() {}

  /**
   * Computes the set of aspects that could be applied to a dependency.
   *
   * <p>This is composed of two parts:
   *
   * <ol>
   *   <li>The aspects that are visible to this aspect being evaluated, if any. If another aspect is
   *       visible on the configured target, it should also be visible on the dependencies for
   *       consistency. This is the argument {@code aspectsPath}.
   *   <li>The aspects propagated by the attributes of this configured target / aspect.
   * </ol>
   *
   * <p>The presence of an aspect here does not necessarily mean that it will be available on a
   * dependency: it can still be filtered out because it requires a provider that the configured
   * target it should be attached to it doesn't advertise. This is taken into account in {@link
   * #computeAspectCollection}.
   */
  public static ImmutableList<Aspect> computePropagatingAspects(
      DependencyKind kind,
      ImmutableList<Aspect> aspectsPath,
      Rule rule,
      @Nullable ToolchainCollection<UnloadedToolchainContext> baseTargetToolchainContext) {
    if (DependencyKind.isBaseTargetToolchain(kind)) {
      return computePropagatingAspectsToToolchainDep(
          (DependencyKind.BaseTargetToolchainDependencyKind) kind,
          aspectsPath,
          baseTargetToolchainContext);
    }

    Attribute attribute = kind.getAttribute();
    if (attribute == null) {
      return ImmutableList.of();
    }
    var aspectsBuilder = new ImmutableList.Builder<Aspect>().addAll(attribute.getAspects(rule));
    collectPropagatingAspects(
        aspectsPath, attribute.getName(), kind.getOwningAspect(), aspectsBuilder);
    return aspectsBuilder.build();
  }

  /**
   * Compute the set of aspects propagating to the given {@link BaseTargetToolchainDependencyKind}
   * based on the {@code toolchains_aspects} of each aspect in the {@code aspectsPath}.
   */
  private static ImmutableList<Aspect> computePropagatingAspectsToToolchainDep(
      DependencyKind.BaseTargetToolchainDependencyKind kind,
      ImmutableList<Aspect> aspectsPath,
      @Nullable ToolchainCollection<UnloadedToolchainContext> baseTargetToolchainContext) {
    var toolchainContext = baseTargetToolchainContext.getToolchainContext(kind.getExecGroupName());
    var toolchainType =
        toolchainContext.requestedLabelToToolchainType().get(kind.getToolchainType());

    // Since the label of the toolchain type can be an alias, we need to get all the labels that
    // point to the same toolchain type to compare them against the toolchain types that the aspects
    // can propagate.
    var allToolchainTypelabels =
        toolchainContext.requestedLabelToToolchainType().asMultimap().inverse().get(toolchainType);

    var filteredAspectPath = new ArrayList<Aspect>();

    int aspectsCount = aspectsPath.size();
    for (int i = aspectsCount - 1; i >= 0; i--) {
      Aspect aspect = aspectsPath.get(i);
      if (allToolchainTypelabels.stream()
              .anyMatch(label -> aspect.getDefinition().canPropagateToToolchainType(label))
          || isAspectRequired(aspect, filteredAspectPath)) {
        // Adds the aspect if it propagates to the toolchain type or it is
        // required by an aspect already in the {@code filteredAspectPath}.
        filteredAspectPath.add(aspect);
      }
    }
    reverse(filteredAspectPath);

    return ImmutableList.copyOf(filteredAspectPath);
  }

  /**
   * Computes the way aspects should be computed for the direct dependencies.
   *
   * <p>This is done by filtering the aspects that can be propagated on any attribute according to
   * the providers advertised by direct dependencies and by creating the {@link AspectCollection}
   * that tells how to compute the final set of providers based on the interdependencies between the
   * propagating aspects.
   */
  public static AspectCollection computeAspectCollection(
      ImmutableList<Aspect> aspects,
      AdvertisedProviderSet advertisedProviders,
      Label targetLabel,
      RuleClass ruleClass,
      ImmutableList<String> tags,
      Location targetLocation,
      ExtendedEventHandler eventHandler)
      throws InconsistentAspectOrderException, InterruptedException, EvalException {

    var filteredAspectPath = new ArrayList<Aspect>();

    int aspectsCount = aspects.size();
    for (int i = aspectsCount - 1; i >= 0; i--) {
      Aspect aspect = aspects.get(i);
      if (AspectDefinition.satisfies(aspect, advertisedProviders)
          || isAspectRequired(aspect, filteredAspectPath)) {
        // Considers the aspect if the target satisfies its required providers or it is
        // required by an aspect already in the {@code filteredAspectPath}.
        if (evaluatePropagationPredicate(aspect, targetLabel, ruleClass, tags, eventHandler)) {
          // Only add the aspect if its propagation predicate is satisfied by the target.
          filteredAspectPath.add(aspect);
        }
      }
    }

    reverse(filteredAspectPath);
    return computeAspectCollectionNoAspectsFiltering(
        ImmutableList.copyOf(filteredAspectPath), targetLabel, targetLocation);
  }

  public static AspectCollection computeAspectCollectionNoAspectsFiltering(
      ImmutableList<Aspect> aspects, Label targetLabel, Location targetLocation)
      throws InconsistentAspectOrderException {
    try {
      return AspectCollection.create(aspects);
    } catch (AspectCycleOnPathException e) {
      throw new InconsistentAspectOrderException(targetLabel, targetLocation, e);
    }
  }

  private static boolean evaluatePropagationPredicate(
      Aspect aspect,
      Label label,
      RuleClass ruleClass,
      ImmutableList<String> tags,
      ExtendedEventHandler eventHandler)
      throws InterruptedException, EvalException {
    if (aspect.getDefinition().getPropagationPredicate() == null) {
      return true;
    }
    return aspect
        .getDefinition()
        .getPropagationPredicate()
        .evaluate(
            StarlarkAspectPropagationContext.createForPropagationPredicate(
                aspect, label, ruleClass, tags),
            eventHandler);
  }

  /**
   * Collects the aspects from {@code aspectsPath} that need to be propagated along the attribute
   * {@code attributeName}.
   *
   * <p>It can happen that some of the aspects cannot be propagated if the dependency doesn't have a
   * provider that's required by them. These will be filtered out after the rule class of the
   * dependency is known.
   */
  private static void collectPropagatingAspects(
      ImmutableList<Aspect> aspectsPath,
      String attributeName,
      @Nullable AspectClass aspectOwningAttribute,
      ImmutableList.Builder<Aspect> allFilteredAspects) {
    int aspectsNum = aspectsPath.size();
    var filteredAspectsPath = new ArrayList<Aspect>();

    // `aspectsPath` is ordered bottom up. Iterating backwards traverses top-down so the following
    // loop captures aspects that propagate along the given attribute and all their transitive
    // requirements.
    for (int i = aspectsNum - 1; i >= 0; i--) {
      Aspect aspect = aspectsPath.get(i);
      if (aspect.getAspectClass().equals(aspectOwningAttribute)) {
        // Do not propagate over the aspect's own attributes.
        continue;
      }

      if (aspect.getDefinition().propagateAlong(attributeName)
          || isAspectRequired(aspect, filteredAspectsPath)) {
        // Add the aspect if it can propagate over this {@code attributeName} based on its
        // attr_aspects or it is required by an aspect already in the {@code filteredAspectsPath}.
        filteredAspectsPath.add(aspect);
      }
    }
    // Reverse filteredAspectsPath to return it to the same order as the input aspectsPath.
    reverse(filteredAspectsPath);
    allFilteredAspects.addAll(filteredAspectsPath);
  }

  /** Checks if {@code aspect} is required by any {@link Aspect} in {@code aspectsPath}. */
  private static boolean isAspectRequired(Aspect aspect, Iterable<Aspect> aspectsPath) {
    for (Aspect existingAspect : aspectsPath) {
      if (existingAspect.getDefinition().requires(aspect)) {
        return true;
      }
    }
    return false;
  }
}

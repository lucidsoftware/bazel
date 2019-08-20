package com.google.devtools.build.lib.remote;

import static java.lang.String.format;

import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.devtools.build.lib.actions.ActionInput;
import com.google.devtools.build.lib.actions.ActionInputHelper;
import com.google.devtools.build.lib.actions.cache.VirtualActionInput;
import com.google.devtools.build.lib.remote.merkletree.MerkleTree;
import com.google.devtools.build.lib.remote.options.RemoteOptions;
import com.google.devtools.build.lib.remote.util.DigestUtil;
import com.google.devtools.build.lib.vfs.Path;
import com.google.protobuf.Message;
import java.io.IOException;
import java.util.Map;

public class GrpcRemoteExecutionCache extends RemoteCache {

  public GrpcRemoteExecutionCache(RemoteOptions options,
      DigestUtil digestUtil,
      GrpcRemoteCacheProtocol protocolImpl) {
    super(options, digestUtil, protocolImpl);
  }

  /**
   * Ensures that the tree structure of the inputs, the input files themselves, and the command are
   * available in the remote cache, such that the tree can be reassembled and executed on another
   * machine given the root digest.
   *
   * <p>The cache may check whether files or parts of the tree structure are already present, and do
   * not need to be uploaded again.
   *
   * <p>Note that this method is only required for remote execution, not for caching itself.
   * However, remote execution uses a cache to store input files, and that may be a separate
   * end-point from the executor itself, so the functionality lives here.
   */
  public void ensureInputsPresent(
      MerkleTree merkleTree, Map<Digest, Message> additionalInputs, Path execRoot)
      throws IOException, InterruptedException {
    ImmutableSet<Digest> missingDigests =
        protocolImpl.findMissingBlobs(Iterables.concat(merkleTree.getAllDigests(), additionalInputs.keySet()));
    ImmutableList.Builder<ListenableFuture<Void>> uploads =
        ImmutableList.builderWithExpectedSize(missingDigests.size());
    for (Digest missingDigest : missingDigests) {
      Directory node = merkleTree.getDirectoryByDigest(missingDigest);
      if (node != null) {
        uploads.add(protocolImpl.uploadBlob(missingDigest, node.toByteString()));
        continue;
      }

      ActionInput actionInput = merkleTree.getInputByDigest(missingDigest);
      if (actionInput instanceof VirtualActionInput) {
        uploads.add(protocolImpl.uploadBlob(missingDigest, ((VirtualActionInput) actionInput).getBytes()));
        continue;
      }

      if (actionInput != null) {
        uploads.add(protocolImpl.uploadFile(missingDigest,
            ActionInputHelper.toInputPath(actionInput, execRoot)));
        continue;
      }

      Message message = additionalInputs.get(missingDigest);
      if (message != null) {
        uploads.add(protocolImpl.uploadBlob(missingDigest, message.toByteString()));
        continue;
      }

      throw new IOException(
          format(
              "findMissingBlobs returned a missing digest that has not been requested: %s",
              missingDigest));
    }

    waitForUploads(uploads.build());
  }
}

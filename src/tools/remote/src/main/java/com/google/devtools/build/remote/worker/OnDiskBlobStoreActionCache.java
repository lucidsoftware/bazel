// Copyright 2019 The Bazel Authors. All rights reserved.
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
package com.google.devtools.build.remote.worker;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.FileNode;
import com.google.devtools.build.lib.actions.ExecException;
import com.google.devtools.build.lib.remote.RemoteCache;
import com.google.devtools.build.lib.remote.SimpleBlobStoreActionCache;
import com.google.devtools.build.lib.remote.common.SimpleBlobStore.ActionKey;
import com.google.devtools.build.lib.remote.disk.OnDiskBlobStore;
import com.google.devtools.build.lib.remote.options.RemoteOptions;
import com.google.devtools.build.lib.remote.util.DigestUtil;
import com.google.devtools.build.lib.remote.util.Utils;
import com.google.devtools.build.lib.util.io.FileOutErr;
import com.google.devtools.build.lib.vfs.Path;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.Collection;

/** A {@link SimpleBlobStoreActionCache} backed by an {@link OnDiskBlobStore}. */
class OnDiskBlobStoreActionCache extends RemoteCache {

  public OnDiskBlobStoreActionCache(
      RemoteOptions options,
      Path cacheDir,
      DigestUtil digestUtil) {
    super(options, digestUtil, new OnDiskBlobStore(cacheDir, /* verifyDownloadsChecksum= */ true));
  }

  public boolean containsKey(Digest digest) {
    return ((OnDiskBlobStore) blobStore).contains(digest);
  }

  void downloadTree(Digest rootDigest, Path rootLocation)
      throws IOException, InterruptedException {
    rootLocation.createDirectoryAndParents();
    Directory directory = Directory.parseFrom(Utils.getFromFuture(downloadBlob(rootDigest)));
    for (FileNode file : directory.getFilesList()) {
      Path dst = rootLocation.getRelative(file.getName());
      Utils.getFromFuture(downloadFile(dst, file.getDigest()));
      dst.setExecutable(file.getIsExecutable());
    }
    for (DirectoryNode child : directory.getDirectoriesList()) {
      downloadTree(child.getDigest(), rootLocation.getRelative(child.getName()));
    }
  }

  void uploadBlob(Digest digest, ByteString data) throws IOException, InterruptedException {
    Utils.getFromFuture(protocolImpl.uploadBlob(digest, data));
  }

  void uploadActionResult(ActionKey actionKey, ActionResult actionResult) throws IOException, InterruptedException {
    protocolImpl.uploadActionResult(actionKey, actionResult);
  }

  @Override
  public ActionResult uploadActionOutputs(
      Path execRoot,
      ActionKey actionKey,
      Action action,
      Command command,
      Collection<Path> files,
      FileOutErr outErr)
      throws ExecException, IOException, InterruptedException {
    return super.uploadActionOutputs(execRoot, actionKey, action, command, files, outErr);
  }

  DigestUtil getDigestUtil() {
    return digestUtil;
  }
}

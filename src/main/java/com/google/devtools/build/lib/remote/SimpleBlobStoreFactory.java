// Copyright 2016 The Bazel Authors. All rights reserved.
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

package com.google.devtools.build.lib.remote;

import static com.google.common.base.Preconditions.checkNotNull;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.google.auth.Credentials;
import com.google.devtools.build.lib.authandtls.AuthAndTLSOptions;
import com.google.devtools.build.lib.authandtls.AwsAuthUtils;
import com.google.devtools.build.lib.authandtls.GoogleAuthUtils;
import com.google.devtools.build.lib.remote.blobstore.OnDiskBlobStore;
import com.google.devtools.build.lib.remote.blobstore.SimpleBlobStore;
import com.google.devtools.build.lib.remote.blobstore.http.HttpBlobStore;
import com.google.devtools.build.lib.remote.blobstore.http.HttpCredentialsAdapter;
import com.google.devtools.build.lib.vfs.Path;
import com.google.devtools.build.lib.vfs.PathFragment;
import io.netty.channel.unix.DomainSocketAddress;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * A factory class for providing a {@link SimpleBlobStore} to be used with {@link
 * SimpleBlobStoreActionCache}. Currently implemented with REST or local.
 */
public final class SimpleBlobStoreFactory {

  private static final String S3_SERVICE = "s3";

  private SimpleBlobStoreFactory() {}

  public static SimpleBlobStore createRest(RemoteOptions options, AuthAndTLSOptions authAndTLSOptions) {
    try {
      URI uri = URI.create(options.remoteHttpCache);
      int timeoutMillis = (int) TimeUnit.SECONDS.toMillis(options.remoteTimeout);

      if (options.remoteCacheProxy != null) {
        if (options.remoteCacheProxy.startsWith("unix:")) {
          return HttpBlobStore.create(
            new DomainSocketAddress(options.remoteCacheProxy.replaceFirst("^unix:", "")),
              uri, timeoutMillis, options.remoteMaxConnections, createHttpCredentialsAdapter(options, authAndTLSOptions));
        } else {
          throw new Exception("Remote cache proxy unsupported: " + options.remoteCacheProxy);
        }
      } else {
        return HttpBlobStore.create(uri, timeoutMillis, options.remoteMaxConnections, createHttpCredentialsAdapter(options, authAndTLSOptions));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static HttpCredentialsAdapter createHttpCredentialsAdapter(final RemoteOptions options, final AuthAndTLSOptions authAndTLSOptions) throws IOException {
    final Credentials googlAuth = GoogleAuthUtils.newCredentials(authAndTLSOptions);
    final AWSCredentialsProvider awsAuth = AwsAuthUtils.newCredentials(authAndTLSOptions);

    if (googlAuth != null && awsAuth != null) {
      throw new IOException("Both google and AWS credentials provided for remote caching. Only one should be used");
    } else if (googlAuth != null) {
      return HttpCredentialsAdapter.fromGoogleCredentials(googlAuth);
    } else if (awsAuth != null) {
      return HttpCredentialsAdapter.fromAwsCredentails(options.awsS3Region, options.remoteHttpCache, S3_SERVICE, awsAuth);
    } else {
      return null;
    }
  }

  public static SimpleBlobStore createDiskCache(Path workingDirectory, PathFragment diskCachePath)
      throws IOException {
    Path cacheDir = workingDirectory.getRelative(checkNotNull(diskCachePath));
    if (!cacheDir.exists()) {
      cacheDir.createDirectoryAndParents();
    }
    return new OnDiskBlobStore(cacheDir);
  }

  public static SimpleBlobStore create(
          RemoteOptions options, @Nullable AuthAndTLSOptions authAndTLSOptions, @Nullable Path workingDirectory)
      throws IOException {
    if (isRestUrlOptions(options)) {
      return createRest(options, authAndTLSOptions);
    }
    if (workingDirectory != null && isDiskCache(options)) {
      return createDiskCache(workingDirectory, options.diskCache);
    }
    throw new IllegalArgumentException(
        "Unrecognized concurrent map RemoteOptions: must specify "
            + "either Rest URL, or local cache options.");
  }

  public static boolean isRemoteCacheOptions(RemoteOptions options) {
    return isRestUrlOptions(options) || isDiskCache(options);
  }

  public static boolean isDiskCache(RemoteOptions options) {
    return options.diskCache != null && !options.diskCache.isEmpty();
  }

  static boolean isRestUrlOptions(RemoteOptions options) {
    return options.remoteHttpCache != null;
  }
}

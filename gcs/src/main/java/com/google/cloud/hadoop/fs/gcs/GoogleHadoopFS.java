/*
 * Copyright 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.fs.gcs;

import com.google.cloud.hadoop.util.InvocationIdContext;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 * GoogleHadoopFS provides a YARN compatible Abstract File System on top of Google Cloud Storage
 * (GCS).
 *
 * <p>It is implemented as a thin abstraction layer on top of GoogleHadoopFileSystem, but will soon
 * be refactored to share a common base.
 */
public class GoogleHadoopFS extends AbstractFileSystem {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  // Wrapped GoogleHadoopFileSystem instance
  private final GoogleHadoopFileSystem ghfs;

  public GoogleHadoopFS(URI uri, Configuration conf) throws URISyntaxException, IOException {
    this(new GoogleHadoopFileSystem(), uri, conf);
  }

  public GoogleHadoopFS(GoogleHadoopFileSystem ghfs, URI uri, Configuration conf)
      throws URISyntaxException, IOException {
    // AbstractFileSystem requires authority based AbstractFileSystems to have valid ports.
    // true == GoogleHadoopFS requires authorities in URIs.
    // 0 == the fake port passed to AbstractFileSystem.
    super(uri, ghfs.getScheme(), true, 0);
    this.ghfs = ghfs;
    ghfs.initialize(uri, conf);
  }

  @Override
  public FSDataOutputStream createInternal(
      Path file,
      EnumSet<CreateFlag> flag,
      FsPermission absolutePermission,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress,
      ChecksumOpt checksumOpt,
      boolean createParent)
      throws IOException {
    logger.atFiner().log(
        "createInternal(file: %s, flag: %s, absolutePermission: %s, bufferSize: %d, "
            + "replication: %b, blockSize: %d, progress: %s, checksumOpt: %s, createParent: %b)",
        file,
        flag,
        absolutePermission,
        bufferSize,
        replication,
        blockSize,
        progress,
        checksumOpt,
        createParent);
    if (!createParent) {
      // TODO: don't ignore 'createParent' flag
      logger.atFine().log(
          "%s: Ignoring createParent=false. Creating parents anyways.",
          InvocationIdContext.getInvocationId());
    }
    // AbstractFileSystems rely on permission to not overwrite.
    boolean overwriteFile = true;
    return ghfs.create(
        file, absolutePermission, overwriteFile, bufferSize, replication, blockSize, progress);
  }

  @Override
  public int getUriDefaultPort() {
    int defaultPort = ghfs.getDefaultPort();
    logger.atFiner().log(
        "%s: getUriDefaultPort(): %d", InvocationIdContext.getInvocationId(), defaultPort);
    return defaultPort;
  }

  /**
   * This is overridden to use GoogleHadoopFileSystem's URI, because AbstractFileSystem appends the
   * default port to the authority.
   */
  @Override
  public URI getUri() {
    return ghfs.getUri();
  }

  /** Follow HDFS conventions except allow for ':' in paths. */
  @Override
  public boolean isValidName(String src) {
    StringTokenizer tokens = new StringTokenizer(src, Path.SEPARATOR);
    while (tokens.hasMoreTokens()) {
      String element = tokens.nextToken();
      if (element.equals("..") || element.equals(".")) {
        return false;
      }
    }
    return true;
  }

  /** Only accept valid AbstractFileSystem and GoogleHadoopFileSystem Paths. */
  @Override
  public void checkPath(Path path) {
    super.checkPath(path);
    ghfs.checkPath(path);
  }

  // TODO: Implement GoogleHadoopFileSystem.getServerDefaults(Path)
  @SuppressWarnings("deprecation")
  @Override
  public FsServerDefaults getServerDefaults() throws IOException {
    logger.atFiner().log("%s: getServerDefaults()", InvocationIdContext.getInvocationId());
    return ghfs.getServerDefaults();
  }

  @Override
  public void mkdir(Path dir, FsPermission permission, boolean createParent) throws IOException {
    logger.atFiner().log(
        "%s: mkdir(dir: %s, permission: %s, createParent %b)",
        InvocationIdContext.getInvocationId(), dir, permission, createParent);
    if (!createParent) {
      logger.atFine().log(
          "%s: Ignoring createParent=false. Creating parents anyways.",
          InvocationIdContext.getInvocationId());
    }
    ghfs.mkdirs(dir, permission);
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    logger.atFiner().log(
        "%s: delete(path: %s, recursive: %b)", InvocationIdContext.getInvocationId(), f, recursive);
    return ghfs.delete(f, recursive);
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    logger.atFiner().log(
        "%s: open(path: %s, bufferSize: %d)", InvocationIdContext.getInvocationId(), f, bufferSize);
    return ghfs.open(f, bufferSize);
  }

  @Override
  public boolean setReplication(Path f, short replication) throws IOException {
    logger.atFiner().log(
        "%s: setReplication(path: %s, replication: %d)",
        InvocationIdContext.getInvocationId(), f, replication);
    return ghfs.setReplication(f, replication);
  }

  @Override
  public void renameInternal(Path src, Path dst) throws IOException {
    logger.atFiner().log(
        "%s: renameInternal(src: %s, dst: %s)", InvocationIdContext.getInvocationId(), src, dst);
    ghfs.renameInternal(src, dst);
  }

  @Override
  public void setPermission(Path f, FsPermission permission) throws IOException {
    logger.atFiner().log(
        "%s: setPermission(path: %s, permission: %s)",
        InvocationIdContext.getInvocationId(), f, permission);
    ghfs.setPermission(f, permission);
  }

  @Override
  public void setOwner(Path f, String username, String groupname) throws IOException {
    logger.atFiner().log(
        "%s: setOwner(path: %s, username: %s, groupname: %s)",
        InvocationIdContext.getInvocationId(), f, username, groupname);
    ghfs.setOwner(f, username, groupname);
  }

  @Override
  public void setTimes(Path f, long mtime, long atime) throws IOException {
    logger.atFiner().log(
        "%s: setTimes(path: %s, mtime: %d, atime: %d)",
        InvocationIdContext.getInvocationId(), f, mtime, atime);
    ghfs.setTimes(f, mtime, atime);
  }

  @Override
  public FileChecksum getFileChecksum(Path f) throws IOException {
    logger.atFiner().log("%s: getFileChecksum(path: %s)", InvocationIdContext.getInvocationId(), f);
    return ghfs.getFileChecksum(f);
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    logger.atFiner().log("%s: getFileStatus(path: %s)", InvocationIdContext.getInvocationId(), f);
    return ghfs.getFileStatus(f);
  }

  @Override
  public BlockLocation[] getFileBlockLocations(Path f, long start, long len) throws IOException {
    logger.atFiner().log(
        "%s: getFileBlockLocations(path: %s, start: %d, len: %d)",
        InvocationIdContext.getInvocationId(), f, start, len);
    return ghfs.getFileBlockLocations(f, start, len);
  }

  @Override
  public FsStatus getFsStatus() throws IOException {
    logger.atFiner().log("%s: getStatus()", InvocationIdContext.getInvocationId());
    return ghfs.getStatus();
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    logger.atFiner().log("%s: listStatus(path: %s)", InvocationIdContext.getInvocationId(), f);
    return ghfs.listStatus(f);
  }

  @Override
  public void setVerifyChecksum(boolean verifyChecksum) {
    logger.atFiner().log(
        "%s: setVerifyChecksum(verifyChecksum: %b)",
        InvocationIdContext.getInvocationId(), verifyChecksum);
    ghfs.setVerifyChecksum(verifyChecksum);
  }
}

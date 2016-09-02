/* -*- mode: Java; c-basic-offset: 2; indent-tabs-mode: nil; coding: utf-8-unix -*-
 *
 * Copyright (c) 2016 Edugility LLC. All rights reserved.
 */
package com.edugility.s3loader;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import java.net.URL;

import java.util.Collections;
import java.util.Enumeration;
import java.util.Objects;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.amazonaws.AmazonClientException;

import com.amazonaws.services.s3.AmazonS3;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;

public class S3ClassLoader extends ClassLoader {

  static {
    registerAsParallelCapable();
  }
  
  private static final Pattern dotPattern = Pattern.compile("\\.");

  /**
   * <h2>Thread Safety</h2>
   *
   * <p>This object <a
   * href="https://forums.aws.amazon.com/message.jspa?messageID=191643#191643">is
   * safe</a> for concurrent use by multiple threads.</p>
   */
  private final AmazonS3 client;

  private final String bucketName;

  public S3ClassLoader(final ClassLoader parent,
                       final AmazonS3 client,
                       final String bucketName) {
    super(parent);
    Objects.requireNonNull(client, "client == null");
    Objects.requireNonNull(bucketName, "bucketName == null");
    this.client = client;
    this.bucketName = bucketName;
  }

  @Override
  protected Class<?> findClass(final String name) throws ClassNotFoundException {
    if (name == null) {
      throw new ClassNotFoundException("null", new IllegalArgumentException("name == null"));
    } else if (name.isEmpty()) {
      throw new ClassNotFoundException(name, new IllegalArgumentException("name.isEmpty()"));
    } else if (!Character.isJavaIdentifierStart(name.charAt(0))) {
      throw new ClassNotFoundException(name, new IllegalArgumentException("!Character.isJavaIdentifierStart(name.charAt(0)): " + name));
    }

    Class<?> returnValue = null;
    
    assert this.client != null;

    final String objectKey = this.toObjectKey(name);
    if (objectKey == null) {
      throw new ClassNotFoundException(name, new IllegalStateException("toObjectKey(\"" + name + "\") == null"));
    }

    try (final S3Object s3Object = this.client.getObject(this.bucketName, objectKey)) {
      if (s3Object == null) {
        throw new ClassNotFoundException(name);
      } else {
        final ObjectMetadata metadata = s3Object.getObjectMetadata();
        if (metadata != null) {
          final long sizeInBytes = metadata.getInstanceLength();
          if (sizeInBytes > Integer.MAX_VALUE) {
            throw new ClassNotFoundException(name, new IllegalStateException("s3Object.getObjectMetadata().getInstanceLength() > Integer.MAX_VALUE (" + Integer.MAX_VALUE + "): " + sizeInBytes));
          } else if (sizeInBytes <= 0) {
            throw new ClassNotFoundException(name, new IllegalStateException("s3Object.getObjectMetadata().getInstanceLength() <= 0: " + sizeInBytes));
          } else {
            final int bytesLength = (int)sizeInBytes;
            final byte[] bytes = new byte[bytesLength];
            try (final InputStream inputStream = s3Object.getObjectContent()) {
              if (inputStream != null) {
                int offset = 0;
                while (offset < bytesLength) {
                  final int numberOfBytesReadOnThisPass = inputStream.read(bytes, offset, bytesLength - offset);
                  if (numberOfBytesReadOnThisPass < 0) {
                    throw new EOFException();
                  } else {
                    offset += numberOfBytesReadOnThisPass;
                  }
                }              
              }
            }
            returnValue = this.defineClass(name, bytes, 0, bytesLength);
          }
        }
      }
    } catch (final AmazonClientException | IOException e) {
      throw new ClassNotFoundException(name, e);
    }
    return returnValue;
  }

  @Override
  public InputStream getResourceAsStream(final String name) {
    InputStream returnValue = null;
    final ClassLoader parent = this.getParent();
    if (parent != null) {
      returnValue = parent.getResourceAsStream(name);
    }
    if (name != null && returnValue == null) {
      assert this.client != null;
      assert this.bucketName != null;
      try (final S3Object s3Object = this.client.getObject(this.bucketName, name)) {
        if (s3Object != null) {
          returnValue = s3Object.getObjectContent();
        }
      } catch (final AmazonClientException | IOException e) {
        // TODO: log
      }
    }
    return returnValue;
  }

  @Override
  protected URL findResource(final String name) {
    URL returnValue = null;
    if (name != null) {
      assert this.client != null;
      assert this.bucketName != null;
      try {
        returnValue = this.client.getUrl(this.bucketName, name);
      } catch (final AmazonClientException e) {
        // TODO: log
      }
    }
    return returnValue;
  }

  @Override
  protected Enumeration<URL> findResources(final String name) throws IOException {
    Enumeration<URL> returnValue = null;
    if (name != null) {      
      final URL resource = this.findResource(name);
      if (resource != null) {
        returnValue = Collections.enumeration(Collections.singleton(resource));
      }
    }
    if (returnValue == null) {
      returnValue = Collections.emptyEnumeration();
    }
    return returnValue;
  }

  protected String toObjectKey(final String className) {
    String returnValue = null;
    if (className != null) {
      final Matcher matcher = dotPattern.matcher(className);
      assert matcher != null;
      returnValue = matcher.replaceAll("/");
      assert returnValue != null;
      returnValue = new StringBuilder(returnValue).append(".class").toString();
    }
    return returnValue;
  }
  
}

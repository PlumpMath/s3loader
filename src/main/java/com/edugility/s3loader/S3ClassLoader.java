/* -*- mode: Java; c-basic-offset: 2; indent-tabs-mode: nil; coding: utf-8-unix -*-
 *
 * Copyright (c) 2016 Edugility LLC. All rights reserved.
 */
package com.edugility.s3loader;

import java.net.URL;

import java.security.CodeSource;

import java.security.cert.Certificate;

import java.util.Objects;

import com.amazonaws.AmazonClientException;

import com.amazonaws.services.s3.AmazonS3;

import com.amazonaws.services.s3.model.GetObjectRequest;

/**
 * A {@linkplain ClassLoader#registerAsParallelCapable()
 * parallel-capable} {@link AbstractS3ClassLoader} that {@linkplain
 * #findClass(String) loads <code>Class</code>es} from a single <a
 * href="https://aws.amazon.com/s3">Amazon Simple Storage Service</a>
 * <a
 * href="http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingBucket.html">bucket</a>.
 *
 * @author <a href="http://about.me/lairdnelson"
 * target="_parent">Laird Nelson</a>
 *
 * @see AbstractS3ClassLoader
 *
 * @see <a href="https://aws.amazon.com/s3">Amazon Simple Storage
 * Service overview</a>
 */
public class S3ClassLoader extends AbstractS3ClassLoader {

  /**
   * Static initializer; calls the {@link
   * ClassLoader#registerAsParallelCapable()} method.
   */
  static {
    ClassLoader.registerAsParallelCapable();
  }

  /**
   * The name of the <a
   * href="http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingBucket.html">bucket</a>
   * used to house Java class data.
   *
   * <p>This field is never {@code null}.</p>
   *
   * @see #S3ClassLoader(ClassLoader, AmazonS3, String, boolean)
   */
  protected final String bucketName;

  /**
   * The {@link URL} of the <a
   * href="http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingBucket.html">bucket</a>
   * used to house Java class data.
   *
   * <p>This field may be {@code null} in rare cases.</p>
   *
   * <p>This field is set to the return value of the {@link
   * AmazonS3#getUrl(String, String)} method invoked with the value of
   * the {@link #bucketName} field and {@code null} as its two
   * parameters.</p>
   *
   * @see #bucketName
   *
   * @see #S3ClassLoader(ClassLoader, AmazonS3, String, boolean)
   *
   * @see AmazonS3#getUrl(String, String)
   */
  protected final URL bucketUrl;

  /**
   * Indicates how the {@link
   * GetObjectRequest#setRequesterPays(boolean)} method should be
   * called when {@linkplain AmazonS3#getObject(GetObjectRequest)
   * requesting} data from this {@link S3ClassLoader}'s {@linkplain
   * #bucketName affiliated bucket}.
   *
   * @see #S3ClassLoader(ClassLoader, AmazonS3, String, boolean)
   *
   * @see GetObjectRequest#setRequesterPays(boolean)
   */
  protected final boolean requesterPays;


  /*
   * Constructors.
   */
  

  /**
   * Creates a new {@link S3ClassLoader}.
   *
   * @param parent the {@link ClassLoader} to which class loading
   * requests will be initially delegated; may be {@code null}
   *
   * @param client the {@link AmazonS3} implementation to use to
   * communicate with <a href="https://aws.amazon.com/s3/">Amazon's
   * Simple Storage Service</a>; must not be {@code null}
   *
   * @param bucketName the name of the <a
   * href="http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingBucket.html">bucket</a>
   * from which Java {@link Class} instances will be assembled; must
   * not be {@code null}
   *
   * @param requesterPays indicates how the {@link
   * GetObjectRequest#setRequesterPays(boolean)} method should be
   * called when {@linkplain AmazonS3#getObject(GetObjectRequest)
   * requesting} data from the <a
   * href="http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingBucket.html">bucket</a>
   * identified by the {@code bucketName} parameter
   *
   * @exception NullPointerException if either {@code client} or
   * {@code bucketName} is {@code null}
   *
   * @see AbstractS3ClassLoader#AbstractS3ClassLoader(ClassLoader,
   * AmazonS3)
   *
   * @see #bucketUrl
   */
  public S3ClassLoader(final ClassLoader parent,
                       final AmazonS3 client,
                       final String bucketName,
                       final boolean requesterPays) {
    super(parent, client);
    Objects.requireNonNull(bucketName, "bucketName == null");
    this.bucketName = bucketName;
    this.requesterPays = requesterPays;
    URL bucketUrl = null;
    try {
      bucketUrl = client.getUrl(bucketName, null /* no key on purpose */);
    } catch (final AmazonClientException ignore) {

    } finally {
      this.bucketUrl = bucketUrl;
    }
  }


  /*
   * Instance methods.
   */
  

  /**
   * {@linkplain GetObjectRequest#GetObjectRequest(String, String)
   * Creates} a new {@link GetObjectRequest} with this {@link
   * S3ClassLoader}'s {@linkplain #bucketName affiliated bucket name},
   * the supplied {@code className} and this {@link S3ClassLoader}'s
   * {@linkplain #requesterPays "requester pays" status} and returns
   * it.
   *
   * <p>This method never returns {@code null}.</p>
   *
   * <p>Overrides of this method are permitted to return {@code null},
   * which will typically cause a {@link ClassNotFoundException} to be
   * thrown by the {@link #findClass(String)} method.</p>
   *
   * @param className the name of a Java class to load as supplied by
   * the user; may be {@code null}
   *
   * @return a non-{@code null} {@link GetObjectRequest}
   *
   * @see #findClass(String)
   *
   * @see GetObjectRequest
   *
   * @see AmazonS3#getObject(GetObjectRequest)
   */
  @Override
  protected GetObjectRequest classNameToGetObjectRequest(final String className) {
    final GetObjectRequest request = new GetObjectRequest(this.bucketName, className);
    request.setRequesterPays(this.requesterPays);
    return request;
  }

  /**
   * {@linkplain GetObjectRequest#GetObjectRequest(String, String)
   * Creates} a new {@link GetObjectRequest} with this {@link
   * S3ClassLoader}'s {@linkplain #bucketName affiliated bucket name},
   * the supplied {@code resourceName} and this {@link
   * S3ClassLoader}'s {@linkplain #requesterPays "requester pays"
   * status} and returns it.
   *
   * <p>This method never returns {@code null}.</p>
   *
   * <p>Overrides of this method are permitted to return {@code null},
   * which will typically cause {@code null} to be returned by the
   * {@link #findResource(String)} method.</p>
   *
   * @param resourceName the name of a resource to find as supplied by
   * the user; may be {@code null}
   *
   * @return a non-{@code null} {@link GetObjectRequest}
   *
   * @see #findResource(String)
   *
   * @see GetObjectRequest
   *
   * @see AmazonS3#getObject(GetObjectRequest)
   */
  @Override
  protected GetObjectRequest resourceNameToGetObjectRequest(final String resourceName) {
    final GetObjectRequest request = new GetObjectRequest(this.bucketName, resourceName);
    request.setRequesterPays(this.requesterPays);
    return request;
  }

  /**
   * Overrides the {@link
   * AbstractS3ClassLoader#getCodeSource(GetObjectRequest)} method to
   * more efficiently return a {@link CodeSource} for the supplied
   * {@link GetObjectRequest}.
   *
   * <p>This method may return {@code null}.</p>
   *
   * <p>Overrides of this method are permitted to return {@code
   * null}.</p>
   *
   * @param request the {@link GetObjectRequest} for which a suitable
   * {@link CodeSource} should be returned; may be {@code null} in
   * which case {@code null} will be returned
   *
   * @return a {@link CodeSource} for the supplied {@link
   * GetObjectRequest}, or {@code null}
   *
   * @see CodeSource
   *
   * @see #defineClass(String, byte[], int, int, CodeSource)
   *
   * @see AmazonS3#getUrl(String, String)
   */
  @Override
  protected CodeSource getCodeSource(final GetObjectRequest request) {
    if (this.bucketUrl == null) {
      return null;
    } else {
      return new CodeSource(this.bucketUrl, (Certificate[])null /* no certificates */);
    }
  }
  
}

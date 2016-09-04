/* -*- mode: Java; c-basic-offset: 2; indent-tabs-mode: nil; coding: utf-8-unix -*-
 *
 * Copyright (c) 2016 Edugility LLC. All rights reserved.
 */
package com.edugility.s3loader;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import java.net.URL;

import java.security.CodeSource;
import java.security.SecureClassLoader;

import java.security.cert.Certificate;

import java.util.Collections;
import java.util.Enumeration;
import java.util.Objects;

import com.amazonaws.AmazonClientException;

import com.amazonaws.services.s3.AmazonS3;

import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;

/**
 * A {@linkplain ClassLoader#registerAsParallelCapable()
 * parallel-capable} {@link SecureClassLoader} that loads classes from
 * the <a href="https://aws.amazon.com/s3/">Amazon Simple Storage
 * Service</a>.
 *
 * <h2>Thread Safety</h2>
 *
 * <p>This class is safe for concurrent use by multiple threads.</p>
 *
 * @author <a href="http://about.me/lairdnelson"
 * target="_parent">Laird Nelson</a>
 *
 * @see AmazonS3
 *
 * @see <a href="https://aws.amazon.com/s3/">Amazon Simple Storage
 * Service overview</a>
 */
public abstract class AbstractS3ClassLoader extends SecureClassLoader {

  /**
   * Static initializer; calls the {@link
   * ClassLoader#registerAsParallelCapable()} method.
   */
  static {
    ClassLoader.registerAsParallelCapable();
  }
  
  /**
   * The {@link AmazonS3} implementation to use to communicate with <a
   * href="https://aws.amazon.com/s3/">Amazon's Simple Storage
   * Service</a>.
   *
   * <p>This field is never {@code null}.</p>
   *
   * <h2>Thread Safety</h2>
   *
   * <p>This object <a
   * href="https://forums.aws.amazon.com/message.jspa?messageID=191643#191643">is
   * safe</a> for concurrent use by multiple threads.</p>
   *
   * @see #AbstractS3ClassLoader(ClassLoader, AmazonS3)
   */
  protected final AmazonS3 client;


  /*
   * Constructors.
   */


  /**
   * Creates a new {@link AbstractS3ClassLoader}.
   *
   * @param parent the {@link ClassLoader} to which class loading
   * operations will be initially delegated; may be {@code null}
   *
   * @param client the {@link AmazonS3} implementation to use to
   * communicate with <a href="https://aws.amazon.com/s3/">Amazon's
   * Simple Storage Service</a>; must not be {@code null}
   *
   * @exception NullPointerException if {@code client} is {@code null}
   */
  protected AbstractS3ClassLoader(final ClassLoader parent,
                                  final AmazonS3 client) {
    super(parent);
    Objects.requireNonNull(client, "client == null");
    this.client = client;
  }


  /*
   * Instance methods.
   */
  

  /**
   * Locates a binary object defining a Java {@link Class} in the <a
   * href="https://aws.amazon.com/aws">Amazon Simple Storage
   * Service</a>, instantiates the resulting {@link Class} object and
   * returns it.
   *
   * <p>This method never returns {@code null}.</p>
   *
   * @param name a valid class name; must not be {@code null}; must
   * not be {@linkplain String#isEmpty() empty}; must {@linkplain
   * Character#isJavaIdentifierStart(char) start with a
   * <code>char</code> that is a valid Java identifier starting
   * character}
   *
   * @return a {@link Class} object; never {@code null}
   *
   * @exception ClassNotFoundException if a {@link Class} object could
   * not be found for the supplied {@code name} for any reason
   *
   * @see #loadClass(String)
   *
   * @see #classNameToGetObjectRequest(String)
   *
   * @see #getCodeSource(GetObjectRequest)
   *
   * @see #defineClass(String, byte[], int, int, CodeSource)
   *
   * @see AmazonS3#getObject(GetObjectRequest)
   */
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

    final GetObjectRequest request = this.classNameToGetObjectRequest(name);
    if (request == null) {
      throw new ClassNotFoundException(name, new IllegalStateException("classNameToGetObjectRequest(\"" + name + "\") == null"));
    }

    final CodeSource codeSource = this.getCodeSource(request);
    
    try (final S3Object s3Object = this.client.getObject(request)) {
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
            returnValue = this.defineClass(name, bytes, 0, bytesLength, codeSource);
          }
        }
      }
    } catch (final AmazonClientException | IOException e) {
      throw new ClassNotFoundException(name, e);
    }
    return returnValue;
  }

  /**
   * Finds the resource identified by the supplied {@code name} and
   * returns an open {@link InputStream} that can be used to read its
   * contents, or {@code null} if the resource could not be located.
   *
   * <p>This method may return {@code null}.</p>
   *
   * <p>This method first calls the {@linkplain #getParent() parent
   * <code>ClassLoader</code>}'s {@link #getResourceAsStream(String)}
   * method, if the {@linkplain #getParent() parent
   * <code>ClassLoader</code>} is non-{@code null}.  If the return
   * value of that method invocation is non-{@code null}, then it is
   * returned.</p>
   *
   * @param name the name of the resource to find; may be {@code null}
   * in which case {@code null} will be returned
   *
   * @return an open {@link InputStream}, or {@code null}
   *
   * @see #resourceNameToGetObjectRequest(String)
   *
   * @see AmazonS3#getObject(GetObjectRequest)
   *
   * @see S3Object#getObjectContent()
   */
  @Override
  public InputStream getResourceAsStream(final String name) {
    InputStream returnValue = null;
    final ClassLoader parent = this.getParent();
    if (parent != null) {
      returnValue = parent.getResourceAsStream(name);
    }
    if (name != null && returnValue == null) {
      final GetObjectRequest request = this.resourceNameToGetObjectRequest(name);
      if (request != null) {
        try (final S3Object s3Object = this.client.getObject(request)) {
          if (s3Object != null) {
            returnValue = s3Object.getObjectContent();
          }
        } catch (final AmazonClientException | IOException e) {
          // TODO: log
        }
      }
    }
    return returnValue;
  }

  /**
   * Finds the resource identified by the supplied {@code name} and
   * returns a {@link URL} to it, or {@code null} if the resource
   * could not be located.
   *
   * <p>This method may return {@code null}.</p>
   *
   * @param name the name of the resource to locate; may be {@code
   * null} in which case {@code null} will be returned
   *
   * @return a {@link URL} to the resource, or {@code null}
   *
   * @see #resourceNameToGetObjectRequest(String)
   *
   * @see GetObjectRequest#getBucketName()
   *
   * @see GetObjectRequest#getKey()
   *
   * @see AmazonS3#getUrl(String, String)
   */
  @Override
  protected URL findResource(final String name) {
    URL returnValue = null;
    if (name != null) {
      final GetObjectRequest request = this.resourceNameToGetObjectRequest(name);
      if (request != null) {
        final String bucketName = request.getBucketName();
        if (bucketName != null) {
          final String key = request.getKey();
          try {
            returnValue = this.client.getUrl(bucketName, key);
          } catch (final AmazonClientException e) {
            // TODO: log
          }
        }
      }
    }
    return returnValue;
  }

  /**
   * Returns an {@link Enumeration} of {@link URL} instances
   * representing all resources that this {@link
   * AbstractS3ClassLoader} can find with the supplied {@code name}.
   *
   * <p>This method never returns {@code null}.</p>
   *
   * <p>Overrides of this method must not return {@code null}.</p>
   *
   * <p>The default implementation of this method calls the {@link
   * AmazonS3#getUrl(String, String)} method with the return values of
   * the {@link GetObjectRequest#getBucketName()} and {@link
   * GetObjectRequest#getKey()} methods as invoked on the {@link
   * GetObjectRequest} resulting from the {@link
   * #resourceNameToGetObjectRequest(String)} method as parameters,
   * and {@linkplain Collections#singleton(Object) wraps} the single
   * resulting {@link URL} in an {@link
   * Collections#enumeration(Collection) Enumeration} before returning
   * it.</p>
   *
   * @param name the name identifying resources to find; may be {@code
   * null} in which case an {@linkplain Collections#emptyEnumeration()
   * empty <code>Enumeration</code>} will be returned
   *
   * @return a non-{@code null} {@link Enumeration} of {@link URL}s to
   * resources
   *
   * @exception IOException if an error occurs
   *
   * @see #resourceNameToGetObjectRequest(String)
   *
   * @see GetObjectRequest#getBucketName()
   *
   * @see GetObjectRequest#getKey()
   *
   * @see AmazonS3#getUrl(String, String)
   */
  @Override
  protected Enumeration<URL> findResources(final String name) throws IOException {
    Enumeration<URL> returnValue = null;
    if (name != null) {
      final GetObjectRequest request = this.resourceNameToGetObjectRequest(name);
      if (request != null) {
        final String bucketName = request.getBucketName();
        if (bucketName != null) {
          final String key = request.getKey();
          URL resource = null;
          try {
            resource = this.client.getUrl(bucketName, key);
          } catch (final AmazonClientException e) {
            throw new IOException(e);
          }
          if (resource != null) {
            returnValue = Collections.enumeration(Collections.singleton(resource));
          }
        }
      }
    }
    if (returnValue == null) {
      returnValue = Collections.emptyEnumeration();
    }
    return returnValue;
  }

  /**
   * Returns a {@link CodeSource} suitably representing the supplied
   * {@link GetObjectRequest}, or {@code null} if such a {@link
   * CodeSource} could not be returned for any reason.
   *
   * <p>This method may return {@code null}.</p>
   *
   * <p>Overrides of this method are permitted to return {@code
   * null}.</p>
   *
   * <p>The default implementation returns a {@link CodeSource}
   * constructed with a {@link URL} representing the bucket identified
   * by the return value of the {@link
   * GetObjectRequest#getBucketName()} method as invoked on the
   * supplied {@link GetObjectRequest}.</p>
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
  protected CodeSource getCodeSource(final GetObjectRequest request) {
    final CodeSource returnValue;
    if (request == null) {
      returnValue = null;
    } else {
      URL codeSourceUrl = null;
      try {
        codeSourceUrl = this.client.getUrl(request.getBucketName(), null /* no key */);
      } catch (final AmazonClientException e) {
        // TODO: log
      }
      returnValue = new CodeSource(codeSourceUrl, (Certificate[])null /* no certificates */);
    }
    return returnValue;
  }

  /**
   * Returns a {@link GetObjectRequest} representing a {@linkplain
   * #findClass(String) class loading request} for the class
   * identified by the supplied {@code className}.
   *
   * <p>Implementations of this method are permitted to return {@code
   * null}, which will typically cause a {@link
   * ClassNotFoundException} to be thrown by the {@link
   * #findClass(String)} method.</p>
   *
   * @param className a Java class name; may be {@code null}
   *
   * @return the {@link GetObjectRequest} representing the {@linkplain
   * #findClass(String) class loading request} for the supplied {@code
   * className}, or {@code null}
   *
   * @see #findClass(String)
   *
   * @see GetObjectRequest
   *
   * @see AmazonS3#getObject(GetObjectRequest)
   */
  protected abstract GetObjectRequest classNameToGetObjectRequest(final String className);

  /**
   * Returns a {@link GetObjectRequest} representing a {@linkplain
   * #findResource(String) resource finding request} for the resource
   * identified by the supplied {@code resourceName}.
   *
   * <p>Implementations of this method are permitted to return {@code
   * null}, which will typically cause {@code null} to be returned by
   * the {@link #findResource(String)} method.</p>
   *
   * @param resourceName a resource name; may be {@code null}
   *
   * @return the {@link GetObjectRequest} representing the {@linkplain
   * #findResource(String) resource finding request} for the supplied
   * {@code resourceName}, or {@code null}
   */
  protected abstract GetObjectRequest resourceNameToGetObjectRequest(final String resourceName);
  
}

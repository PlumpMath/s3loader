/* -*- mode: Java; c-basic-offset: 2; indent-tabs-mode: nil; coding: utf-8-unix -*-
 *
 * Copyright (c) 2016 Edugility LLC. All rights reserved.
 */
package com.edugility.s3loader;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class TestS3ClassLoader {

  private static Logger amazonLogger;

  private static AmazonS3 client;

  private static String goodClassName;

  private static String badClassName;

  private ClassLoader loader;
  
  public TestS3ClassLoader() {
    super();
  }
  
  @BeforeClass
  public static void setUpSingletons() {
    final boolean termsAccepted = Boolean.getBoolean(TestS3ClassLoader.class.getName() + ".usageTermsAccepted");
    if (!termsAccepted) {
      fail("Please arrange for a boolean System property named " + TestS3ClassLoader.class.getName() + ".usageTermsAccepted\n" +
           "to be set to true to continue. This indicates that you are aware of the fact\n" +
           "that you may incur charges as a result of running the tests present in the\n" +
           TestS3ClassLoader.class.getName() + " class.");
    }
    amazonLogger = Logger.getLogger("com.amazonaws");
    assertNotNull(amazonLogger);
    amazonLogger.setLevel(Level.FINE);
    final Handler handler = new ConsoleHandler();
    amazonLogger.addHandler(handler);
    handler.setLevel(Level.FINE);
    amazonLogger.setUseParentHandlers(false);
    client = new AmazonS3Client();
  }  

  @Before
  public void setUpClassLoader() {
    final String bucketName = System.getProperty(S3ClassLoader.class.getName() + ".bucketName");
    if (bucketName == null || bucketName.isEmpty()) {
      fail("Please arrange for a System property named " + S3ClassLoader.class.getName() + ".bucketName to be set to the name of an Amazon S3 bucket containing valid Java class bytes indexed under their class names.");
    }
    this.loader = new S3ClassLoader(this.getClass().getClassLoader(), client, bucketName, true);
  }
  
  @Test(expected = ClassNotFoundException.class)
  public void testBadClassLoad() throws ClassNotFoundException {
    final String badClassName = System.getProperty(this.getClass().getName() + ".badClassName");
    if (badClassName == null || badClassName.isEmpty()) {
      fail("Please arrange for a System property named " + this.getClass().getName() + ".badClassName to be set to the name of a Java class known to *not* exist in your Amazon S3 bucket.");
    }
    final Class<?> bogusClass = this.loader.loadClass(badClassName);
  }

  @Test
  public void testGoodClassLoad() throws ClassNotFoundException {
    final String goodClassName = System.getProperty(this.getClass().getName() + ".goodClassName");
    if (goodClassName == null || goodClassName.isEmpty()) {
      fail("Please arrange for a System property named " + this.getClass().getName() + ".goodClassName to be set to the name of a valid Java class known to exist in your Amazon S3 bucket.");
    }
    final Class<?> goodClass = this.loader.loadClass(goodClassName);
    assertNotNull(goodClass);
    assertEquals(goodClassName, goodClass.getName());
  }

  
}

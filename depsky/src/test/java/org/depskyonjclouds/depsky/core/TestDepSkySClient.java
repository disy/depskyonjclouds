package org.depskyonjclouds.depsky.core;

import static org.testng.Assert.assertEquals;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.Random;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import depskys.core.DepSkyDataUnit;
import depskys.core.IDepSkyClient;
import depskys.core.IDepSkyClient.DepSkyClientFactory;
import depskys.core.exceptions.DepSkyException;

/**
 * 
 * @author Andreas Rain, University of Konstanz
 *
 */
public class TestDepSkySClient {
    
  private IDepSkyClient mClient;
  private String mConfigPath;
  
  @BeforeClass
  public void beforeClass() {
      
      mConfigPath = new StringBuilder().append("src").append(File.separator).append("test")
      .append(File.separator).append("resources").append(File.separator).append(
      "account.props.yml").toString();
      
      // Using a different configuration path here.
      // It is wise not to use one with real credentials within the project,
      // if you don't want to upload it to a public repository.
      mClient = DepSkyClientFactory.create(new Random().nextInt(221412), mConfigPath);
  }
  
  @Test
  public void testWriteReadData() throws Exception{
      DepSkyDataUnit dataU = new DepSkyDataUnit("testcontainer", "testunit");
      dataU.setLastVersionNumber(-1);
      System.out.println(dataU);
      long acMil = System.currentTimeMillis();

      byte[] value = "Hello world!".getBytes("UTF-8");
      dataU = new DepSkyDataUnit("testcontainer", "testunit");
      mClient.write(dataU, value);
      long tempo = System.currentTimeMillis() - acMil;
      System.out.println("I'm finished write -> " + Long.toString(tempo) + " milis");
      
      // Let s try to read what we've written
      
      byte[] response = mClient.read(dataU);
      
      assertEquals(response, value);
      System.out.println(new String(response, "UTF-8"));

      value = "Hello world2!".getBytes("UTF-8");
      dataU = new DepSkyDataUnit("testcontainer", "testunit");
      mClient.write(dataU, value);
      tempo = System.currentTimeMillis() - acMil;
      System.out.println("I'm finished write -> " + Long.toString(tempo) + " milis");
      
      // Let s try to read what we've written
      
      response = mClient.read(dataU);
      
      assertEquals(response, value);
      System.out.println(new String(response, "UTF-8"));

      value = "Hello world3!".getBytes("UTF-8");
      dataU = new DepSkyDataUnit("testcontainer", "testunit");
      mClient.write(dataU, value);
      tempo = System.currentTimeMillis() - acMil;
      System.out.println("I'm finished write -> " + Long.toString(tempo) + " milis");

      // Let s try to read what we've written
      
      response = mClient.read(dataU);
      
      assertEquals(response, value);
      System.out.println(new String(response, "UTF-8"));

      value = "Hello world4!".getBytes("UTF-8");
      dataU = new DepSkyDataUnit("testcontainer", "testunit");
      mClient.write(dataU, value);
      tempo = System.currentTimeMillis() - acMil;
      System.out.println("I'm finished write -> " + Long.toString(tempo) + " milis");
      
      // Let s try to read what we've written
      
      response = mClient.read(dataU);
      
      assertEquals(response, value);
      System.out.println(new String(response, "UTF-8"));
      
  }
  
  // Currently not working, because of problems with the jclouds blob binding with files.
  @Test (enabled=false)
  public void testDiffSizesWritesAndReads() throws UnsupportedEncodingException, DepSkyException, InterruptedException{
      System.out.println("Testing different sizes");
      
      Random rand = new Random(42);

      for(int i = 1; i <= 1024*32; i *= 2){
          DepSkyDataUnit dataU = new DepSkyDataUnit("testcontainer", "testunit"+i);
          dataU.setLastVersionNumber(-1);
          long acMil = System.currentTimeMillis();
          System.out.println("asd payloadsize: " + (i * 1024));
          byte[] value = new byte[i * 1024];
          rand.nextBytes(value);
          mClient.write(dataU, value);
          
          // Let s try to read what we've written
          
          byte[] response = mClient.read(dataU);
          assertEquals(response, value);

          long tempo = System.currentTimeMillis() - acMil;
          System.out.println("I'm finished with " + i + "kB write/read in -> " + Long.toString(tempo) + " milis");
      }
  }

  // Currently not working, because of problems with the jclouds blob binding with files.
  @Test (enabled=false)
  public void testManyWritesAndReads() throws UnsupportedEncodingException, DepSkyException, InterruptedException{
      System.out.println("Testing many writes/reads");
      
      Random rand = new Random(42);

      for(int i = 1; i <= 1000; i++){
          long acMil = System.currentTimeMillis();
          DepSkyDataUnit dataU = new DepSkyDataUnit("testcontainer", "testunit"+i);
          dataU.setLastVersionNumber(-1);
          System.out.println("realInpPay: " + (4*1024));
          byte[] value = new byte[4*1024];
          rand.nextBytes(value);
          mClient.write(dataU, value);
          
          // Let s try to read what we've written
          
          byte[] response = mClient.read(dataU);
          assertEquals(response, value);

          long tempo = System.currentTimeMillis() - acMil;
          System.out.println("I'm finished with 4kB write/read in -> " + Long.toString(tempo) + " milis");
      }
  }

  @AfterClass
  public void afterClass() {
  }

}

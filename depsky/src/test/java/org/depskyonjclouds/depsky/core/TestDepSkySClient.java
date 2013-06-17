package org.depskyonjclouds.depsky.core;

import java.io.File;
import java.util.LinkedList;
import java.util.Random;

import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.AfterClass;

import depskys.core.DefaultClient;
import depskys.core.DepSkyDataUnit;
import depskys.core.IDepSkyClient;
import depskys.core.IDepSkyClient.DepSkyClientFactory;
import static org.testng.Assert.*;

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
      DepSkyDataUnit dataU = new DepSkyDataUnit("test");
      System.out.println(dataU);
      long acMil = System.currentTimeMillis();

      byte[] value = new byte[8096];
      new Random().nextBytes(value);
      
      byte[] hash = mClient.write(dataU, value);
      long tempo = System.currentTimeMillis() - acMil;
      System.out.println("I'm finished write -> " + Long.toString(tempo) + " milis");
      
      // Let s try to read what we ve written
      
      mClient.read(dataU);
      System.out.println(dataU);
      
  }

  @AfterClass
  public void afterClass() {
  }

}

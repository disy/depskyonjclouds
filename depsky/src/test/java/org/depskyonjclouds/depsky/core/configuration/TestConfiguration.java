package org.depskyonjclouds.depsky.core.configuration;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.yaml.snakeyaml.Yaml;

import depskys.core.configuration.Configuration;

/**
 * 
 * @author Andreas Rain, University of Konstanz
 *
 */
public class TestConfiguration {
  
  private Yaml yaml;
  private Configuration config;
  private String yamlPath;
  
  @BeforeClass
  public void beforeClass() {
      yamlPath = new StringBuilder().append("src").append(File.separator).append("main").append(File.separator).append("resources").append(File.separator).append("account.props.yml").toString();
  }
  
  @Test
  public void testConfigurationLoading() {
      InputStream in;
      try {
          in = new FileInputStream(new File(yamlPath));
          yaml = new Yaml();
          config = (Configuration) yaml.load(in);
      } catch (IOException e) {
          fail();
      }
  }
  
  @Test(dependsOnMethods = "testConfigurationLoading")
  public void testLoadedConfiguration(){
      assertNotNull(config);
      assertEquals(config.getClouds().size(), 5);
      System.out.println(config);
  }

}

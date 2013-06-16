package org.depskyonjclouds.depsky.core.configuration;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import static org.testng.Assert.*;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import depskys.core.configuration.Account;
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
      yamlPath = "src/main/resources/account.props.yml";
  }
  
  @Test
  public void testConfigurationLoading() {
      InputStream in;
      try {
          in = Files.newInputStream(Paths.get(yamlPath));
          Constructor constructor = new Constructor(Configuration.class);
          TypeDescription configDescription = new TypeDescription(Configuration.class);
          configDescription.putListPropertyType("clouds", Account.class);
          constructor.addTypeDescription(configDescription);
          yaml = new Yaml(constructor);
          config = yaml.loadAs(in, Configuration.class);
      } catch (IOException e) {
          fail();
      }
  }
  
  @Test(dependsOnMethods = "testConfigurationLoading")
  public void testLoadedConfiguration(){
      assertNotNull(config);
      assertEquals(config.getAccounts().size(), 5);
  }

}

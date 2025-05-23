package br.com.sankhya.acoesgrautec.services;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import br.com.sankhya.modelcore.util.SWRepositoryUtils;

public class PropertiesService {
	public static Properties getApplicationProperties() throws Exception {
		return getPropertiesFile("/personalizacao/application.properties");
	}
	
	public static Properties getApiProperties() throws Exception{
		return getPropertiesFile("/personalizacao/api.properties");
	}
	
	
	private static Properties getPropertiesFile(String path) throws Exception {
		Properties props = new Properties();
		InputStream targetStream = new FileInputStream(SWRepositoryUtils.getFile(path));
		props.load(targetStream);
		return props;
	}

}

package br.com.sankhya.acoesgrautec.jobs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;

import javax.net.ssl.HttpsURLConnection;

import br.com.sankhya.acoesgrautec.util.EnviromentUtils;
import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;

public class Teste2 implements AcaoRotinaJava{

	@Override
	public void doAction(ContextoAcao contexto) throws Exception {
		
		String consulta = contexto.getParam("Pametro").toString();
		
		EnviromentUtils.updateQueryConnection(consulta);
		
	}
	
	

}

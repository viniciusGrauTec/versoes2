package br.com.sankhya.acoesgrautec.jobs;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import javax.net.ssl.HttpsURLConnection;

public class Teste {

	public static void main(String[] args) throws Exception{
		


		BufferedReader reader;
		String line;
		StringBuilder responseContent = new StringBuilder();
		//String key = preferenciaSenha();
		
		// Preparando a requisição
		URL obj = new URL("https://api.acadweb.com.br/testegrautboavistasankhya/financeiro/titulos?quantidade=2&situacao=A");
		HttpURLConnection https = (HttpURLConnection) obj.openConnection();
		
		System.out.println("URL: " + "https://api.acadweb.com.br/testegrautboavistasankhya/financeiro/titulos?quantidade=2&situacao=A");
		System.out.println("https: " + https);
		
		https.setRequestMethod("GET");
		https.setConnectTimeout(30000);
		https.setRequestProperty("User-Agent",
				"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0)");
		https.setRequestProperty("Content-Type",
				"application/json; charset=UTF-8");
		https.setRequestProperty("Authorization", "Bearer " + "2|VFBUMOCUNitomQYMrwWY7dCaTLts1Lsab3Bktpf5");
		https.setDoOutput(true);
		https.setDoInput(true);
		
		int status = https.getResponseCode();
	
		if (status >= 300) {
			reader = new BufferedReader(new InputStreamReader(
					https.getErrorStream()));
			while ((line = reader.readLine()) != null) {
				responseContent.append(line);
			}
			reader.close();
		} else {
			reader = new BufferedReader(new InputStreamReader(
					https.getInputStream()));
			while ((line = reader.readLine()) != null) {
				responseContent.append(line);
			}
			reader.close();
		}
		System.out.println("Output from Server .... \n" + status);
		String response = responseContent.toString();
	
		https.disconnect();
		
		//return new String[] {Integer.toString(status), response};

	

	}

}

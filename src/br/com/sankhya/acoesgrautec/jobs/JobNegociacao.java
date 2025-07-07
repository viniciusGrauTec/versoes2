package br.com.sankhya.acoesgrautec.jobs;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.cuckoo.core.ScheduledAction;
import org.cuckoo.core.ScheduledActionContext;

import br.com.sankhya.acoesgrautec.services.SkwServicoCompras;
import br.com.sankhya.jape.core.JapeSession;
import br.com.sankhya.jape.core.JapeSession.SessionHandle;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.modelcore.util.DynamicEntityNames;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class JobNegociacao implements ScheduledAction {

	@Override
	public void onTime(ScheduledActionContext arg0) {
		
		SimpleDateFormat formatoOriginal = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat formatoDesejado = new SimpleDateFormat("dd/MM/yyyy");
		
		SkwServicoCompras sc = null;

		SessionHandle hnd = null;

		hnd = JapeSession.open();

		System.out.println("Entrou aqui JOBTransferencia");

		// parametroVO.asString("VALOR");
		String domain = "http://127.0.0.1:8501";
		try {
			JapeWrapper usuarioDAO = JapeFactory
					.dao(DynamicEntityNames.USUARIO);
			DynamicVO usuarioVO;

			usuarioVO = usuarioDAO.findByPK(new BigDecimal(0));

			String md5 = usuarioVO.getProperty("INTERNO").toString();
			String nomeUsu = usuarioVO.getProperty("NOMEUSU").toString();

			sc = new SkwServicoCompras(domain, nomeUsu, md5);

			System.out.println("Passou da instancia da api");
			
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		
		try {

			String[] response = apiGet("https://api.acadweb.com.br/testegrautboavistasankhya" + "/financeiro/acordos?quantidade=1");

			System.out.println("Teste: " + response[1]);

			JsonParser parser = new JsonParser();
			JsonArray jsonArray = parser.parse(response[1]).getAsJsonArray();

	        // Iterar sobre os objetos do array
	        for (JsonElement element : jsonArray) {
	            JsonObject jsonObject = element.getAsJsonObject();
	            String alunoId = jsonObject.get("aluno_id").getAsString();
	            String acordoId = jsonObject.get("acordo_id").getAsString();
	            String acordoData = jsonObject.get("acordo_data").getAsString();
	            int acordoNumeroParcelas = jsonObject.get("acordo_numero_parcelas").getAsInt();
	            String acordoJuros = jsonObject.get("acordo_juros").getAsString();
	            String acordoMulta = jsonObject.get("acordo_multa").getAsString();
	            String acordoDesconto = jsonObject.get("acordo_desconto").getAsString();
	            String acordoOutrosAcrescimos = jsonObject.get("acordo_outros_acrescimos").getAsString();
	            String acordoOutrosDescontos = jsonObject.get("acordo_outros_descontos").getAsString();
	            String dataAtualizacao = jsonObject.get("data_atualizacao").getAsString();

	            // Para as parcelas
	            JsonArray parcelasArray = jsonObject.getAsJsonArray("parcelas");
	            for (JsonElement parcelaElement : parcelasArray) {
	                JsonObject parcelaObject = parcelaElement.getAsJsonObject();
	                String parcelaTituloId = parcelaObject.get("parcela_titulo_id").getAsString();
	                String parcelaTituloVencimento = parcelaObject.get("parcela_titulo_vencimento").getAsString();
	                String parcelaTituloValor = parcelaObject.get("parcela_titulo_valor").getAsString();
	                String parcelaTituloTipo = parcelaObject.get("parcela_titulo_tipo").getAsString();

	                System.out.println("Aluno ID: " + alunoId);
	                System.out.println("Acordo ID: " + acordoId);
	                System.out.println("Parcela Titulo ID: " + parcelaTituloId);
	                System.out.println("Parcela Titulo Vencimento: " + parcelaTituloVencimento);
	                System.out.println("Parcela Titulo Valor: " + parcelaTituloValor);
	                System.out.println("Parcela Titulo Tipo: " + parcelaTituloTipo);
	                
	                Date data = formatoOriginal.parse(parcelaTituloVencimento);
					String dataVencFormatada = formatoDesejado.format(data);
	                
	                sc.parcelarTitulo(dataVencFormatada, "0", acordoNumeroParcelas, "4");
	            }
	            	
	        }

		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	public String[] apiGet(String ur) throws Exception {

		BufferedReader reader;
		String line;
		StringBuilder responseContent = new StringBuilder();
		// String key = preferenciaSenha();

		// Preparando a requisição
		URL obj = new URL(ur);
		HttpURLConnection https = (HttpURLConnection) obj.openConnection();

		System.out.println("Entrou na API");
		System.out.println("URL: " + ur);
		System.out.println("https: " + https);

		https.setRequestMethod("GET");
		// https.setConnectTimeout(50000);
		https.setRequestProperty("User-Agent",
				"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0)");
		https.setRequestProperty("Content-Type",
				"application/json; charset=UTF-8");
		https.setRequestProperty("Authorization", "Bearer "
				//+ "1|XQ90vCZbgXyL7grdAgHOMIL5C43Rt6RuaHJnQIMx");
				+ "2|VFBUMOCUNitomQYMrwWY7dCaTLts1Lsab3Bktpf5");
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

		return new String[] { Integer.toString(status), response };

	}

}

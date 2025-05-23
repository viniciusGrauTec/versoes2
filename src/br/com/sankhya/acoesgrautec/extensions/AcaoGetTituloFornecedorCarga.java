package br.com.sankhya.acoesgrautec.extensions;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.activiti.engine.impl.util.json.JSONArray;
import org.cuckoo.core.ScheduledAction;
import org.cuckoo.core.ScheduledActionContext;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import br.com.sankhya.acoesgrautec.util.EnviromentUtils;
import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.extensions.actionbutton.Registro;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

public class AcaoGetTituloFornecedorCarga implements AcaoRotinaJava, ScheduledAction{

	private List<String> selectsParaInsertLog = new ArrayList<String>();
	private EnviromentUtils util = new EnviromentUtils();

	@Override
	public void doAction(ContextoAcao contexto) throws Exception {

		Registro[] linhas = contexto.getLinhas();
		Registro registro = linhas[0];

		String url = (String) registro.getCampo("URL");
		String token = (String) registro.getCampo("TOKEN");
		BigDecimal codEmp = (BigDecimal) registro.getCampo("CODEMP");

		String dataInicio = contexto.getParam("DTINICIO").toString().substring(0, 10);
		String dataFim = contexto.getParam("DTFIM").toString().substring(0, 10);
		String tituloAberto = (String) contexto.getParam("TITABERTO");
		String idForn = (String) contexto.getParam("IDFORN");

		String profissionalizante = Optional.ofNullable(registro.getCampo("PROFISSIONAL")).orElse("N").toString();
		String tecnico = (String) Optional.ofNullable(registro.getCampo("TECNICO")).orElse("N");

		String tipoEmpresa = "";

		if(profissionalizante.equalsIgnoreCase("S")){
			tipoEmpresa = "P";
		}else if(tecnico.equalsIgnoreCase("S")){
			tipoEmpresa = "T";
		}else{
			tipoEmpresa = "N";
		}

		try {

			// Financeiro
			List<Object[]> listInfFinanceiro = retornarInformacoesFinanceiro();
			Map<String, BigDecimal> mapaInfFinanceiro = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfFinanceiro) {
				BigDecimal nuFin = (BigDecimal) obj[0];
				BigDecimal codEmpObj = (BigDecimal) obj[1];
				String idExternoObj = (String) obj[2];

				if (mapaInfFinanceiro.get(codEmpObj + "###" + idExternoObj) == null) {
					mapaInfFinanceiro.put(codEmpObj + "###" + idExternoObj,
							nuFin);
				}
			}

			// Nro Banco
			Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco = new HashMap<BigDecimal, BigDecimal>();
			for (Object[] obj : listInfFinanceiro) {
				BigDecimal nuFin = (BigDecimal) obj[0];
				BigDecimal nuBco = (BigDecimal) obj[5];
				if (mapaInfFinanceiroBanco.get(nuFin) == null) {
					mapaInfFinanceiroBanco.put(nuFin, nuBco);
				}

			}

			// NuFin Baixados
			Map<BigDecimal, String> mapaInfFinanceiroBaixado = new HashMap<BigDecimal, String>();
			for (Object[] obj : listInfFinanceiro) {
				BigDecimal nuFin = (BigDecimal) obj[0];
				String baixado = (String) obj[3];
				if (mapaInfFinanceiroBaixado.get(nuFin) == null) {
					mapaInfFinanceiroBaixado.put(nuFin, baixado);
				}

			}

			// Parceiros
			List<Object[]> listInfParceiro = retornarInformacoesParceiros();
			Map<String, BigDecimal> mapaInfParceiros = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfParceiro) {
				BigDecimal codParc = (BigDecimal) obj[0];
				String idAcad = (String) obj[1];
				BigDecimal codemp = (BigDecimal) obj[2];

				if (mapaInfParceiros.get(idAcad + "###" + codemp) == null) {
					mapaInfParceiros.put(idAcad + "###" + codemp, codParc);
				}
			}

			// CenCus
			List<Object[]> listInfCenCus = retornarInformacoesCenCus();
			Map<String, BigDecimal> mapaInfCenCus = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfCenCus) {
				BigDecimal codCenCus = (BigDecimal) obj[0];
				String idExterno = (String) obj[1];
				String flag = (String) obj[2];

				if (mapaInfCenCus.get(idExterno + "###" + flag) == null) {
					mapaInfCenCus.put(idExterno + "###" + flag, codCenCus);
				}
			}

			// CenCus por empresa
			List<Object[]> listInfCenCusEmpresa = retornarInformacoesCenCusEmpresa();
			Map<String, BigDecimal> mapaInfCenCusEmp = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfCenCusEmpresa) {
				BigDecimal codCenCus = (BigDecimal) obj[0];
				String idExterno = (String) obj[1];
				BigDecimal codemp = (BigDecimal) obj[2];

				if (mapaInfCenCusEmp.get(idExterno + "###" + codemp) == null) {
					mapaInfCenCusEmp.put(idExterno + "###" + codemp, codCenCus);
				}
			}

			// Natureza
			List<Object[]> listInfNatureza = retornarInformacoesNatureza();
			Map<String, BigDecimal> mapaInfNatureza = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfNatureza) {
				BigDecimal natureza = (BigDecimal) obj[0];
				String idExternoObj = (String) obj[1];
				String flag = (String) obj[2];

				if (mapaInfNatureza.get(idExternoObj + "###" + flag) == null) {
					mapaInfNatureza.put(idExternoObj + "###" + flag, natureza);
				}
			}

			// Natureza por empresa
			List<Object[]> listInfNaturezaEmpresa = retornarInformacoesNaturezaEmpresa();
			Map<String, BigDecimal> mapaInfNaturezaEmp = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfNaturezaEmpresa) {
				BigDecimal natureza = (BigDecimal) obj[0];
				String idExternoObj = (String) obj[1];
				BigDecimal codemp = (BigDecimal) obj[2];

				if (mapaInfNaturezaEmp.get(idExternoObj + "###" + codemp) == null) {
					mapaInfNaturezaEmp.put(idExternoObj + "###" + codemp,
							natureza);
				}
			}

			// RecDesp
			List<Object[]> listInfRecDesp = retornarInformacoesRecDesp();
			Map<String, String> mapaInfRecDesp = new HashMap<String, String>();
			for (Object[] obj : listInfRecDesp) {
				String recDesp = (String) obj[0];
				String idExternoObj = (String) obj[1];

				if (mapaInfRecDesp.get(idExternoObj) == null) {
					mapaInfRecDesp.put(idExternoObj, recDesp);
				}
			}

			// Banco
			List<Object[]> listInfBancoConta = retornarInformacoesBancoConta();
			Map<String, BigDecimal> mapaInfBanco = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfBancoConta) {
				Long codEmpObj = (Long) obj[1];
				BigDecimal codBcoObj = (BigDecimal) obj[3];

				if (mapaInfBanco.get(codEmpObj.toString()) == null) {
					mapaInfBanco.put(codEmpObj.toString(), codBcoObj);
				}
			}

			// Conta
			Map<String, BigDecimal> mapaInfConta = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfBancoConta) {
				BigDecimal codCtabCointObj = (BigDecimal) obj[0];
				Long codEmpObj = (Long) obj[1];

				if (mapaInfConta.get(codEmpObj.toString()) == null) {
					mapaInfConta.put(codEmpObj.toString(), codCtabCointObj);
				}
			}

			processDateRange(tituloAberto.trim(), tipoEmpresa, mapaInfNaturezaEmp, mapaInfCenCusEmp,
					mapaInfFinanceiroBanco, mapaInfFinanceiroBaixado,
					mapaInfNatureza, mapaInfBanco, mapaInfConta,
					mapaInfFinanceiro, mapaInfCenCus,
					mapaInfParceiros, url, token, codEmp, dataInicio, dataFim, idForn);

			contexto.setMensagemRetorno("Periodo Processado!");

		}catch(Exception e){
			e.printStackTrace();
			contexto.mostraErro(e.getMessage());
		}finally{


			if(selectsParaInsertLog.size() > 0){

				StringBuilder msgError = new StringBuilder();

				System.out.println("Entrou na lista do finally: " + selectsParaInsertLog.size());

				//int idInicial = util.getMaxNumLog();

				int qtdInsert = selectsParaInsertLog.size();

				int i = 1;
				for (String sqlInsert : selectsParaInsertLog) {
					String sql = sqlInsert;
					int nuFin = util.getMaxNumLog();
					sql = sql.replace("<#NUMUNICO#>", String.valueOf(nuFin));
					msgError.append(sql);

					if (i < qtdInsert) {
						msgError.append(" \nUNION ALL ");
					}
					i++;
				}

				System.out.println("Consulta de log: \n" + msgError);
				insertLogList(msgError.toString(), codEmp);

			}


		}

	}

	@Override
	public void onTime(ScheduledActionContext arg0) {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal codEmp = BigDecimal.ZERO;

		String url = "";
		String token = "";
		String tipoEmpresa = "";


		System.out.println("Iniciou a JobGetTituloFornecedor");

		try {

			// Financeiro
			List<Object[]> listInfFinanceiro = retornarInformacoesFinanceiro();
			Map<String, BigDecimal> mapaInfFinanceiro = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfFinanceiro) {
				BigDecimal nuFin = (BigDecimal) obj[0];
				BigDecimal codEmpObj = (BigDecimal) obj[1];
				String idExternoObj = (String) obj[2];

				if (mapaInfFinanceiro.get(codEmpObj + "###" + idExternoObj) == null) {
					mapaInfFinanceiro.put(codEmpObj + "###" + idExternoObj,
							nuFin);
				}
			}

			// Nro Banco
			Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco = new HashMap<BigDecimal, BigDecimal>();
			for (Object[] obj : listInfFinanceiro) {
				BigDecimal nuFin = (BigDecimal) obj[0];
				BigDecimal nuBco = (BigDecimal) obj[5];
				if (mapaInfFinanceiroBanco.get(nuFin) == null) {
					mapaInfFinanceiroBanco.put(nuFin, nuBco);
				}

			}

			// NuFin Baixados
			Map<BigDecimal, String> mapaInfFinanceiroBaixado = new HashMap<BigDecimal, String>();
			for (Object[] obj : listInfFinanceiro) {
				BigDecimal nuFin = (BigDecimal) obj[0];
				String baixado = (String) obj[3];
				if (mapaInfFinanceiroBaixado.get(nuFin) == null) {
					mapaInfFinanceiroBaixado.put(nuFin, baixado);
				}

			}

			// Parceiros
			List<Object[]> listInfParceiro = retornarInformacoesParceiros();
			Map<String, BigDecimal> mapaInfParceiros = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfParceiro) {
				BigDecimal codParc = (BigDecimal) obj[0];
				String idAcad = (String) obj[1];
				BigDecimal codemp = (BigDecimal) obj[2];

				if (mapaInfParceiros.get(idAcad + "###" + codemp) == null) {
					mapaInfParceiros.put(idAcad + "###" + codemp, codParc);
				}
			}

			// CenCus
			List<Object[]> listInfCenCus = retornarInformacoesCenCus();
			Map<String, BigDecimal> mapaInfCenCus = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfCenCus) {
				BigDecimal codCenCus = (BigDecimal) obj[0];
				String idExterno = (String) obj[1];
				String flag = (String) obj[2];

				if (mapaInfCenCus.get(idExterno + "###" + flag) == null) {
					mapaInfCenCus.put(idExterno + "###" + flag, codCenCus);
				}
			}

			// CenCus por empresa
			List<Object[]> listInfCenCusEmpresa = retornarInformacoesCenCusEmpresa();
			Map<String, BigDecimal> mapaInfCenCusEmp = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfCenCusEmpresa) {
				BigDecimal codCenCus = (BigDecimal) obj[0];
				String idExterno = (String) obj[1];
				BigDecimal codemp = (BigDecimal) obj[2];

				if (mapaInfCenCusEmp.get(idExterno + "###" + codemp) == null) {
					mapaInfCenCusEmp.put(idExterno + "###" + codemp, codCenCus);
				}
			}

			// Natureza
			List<Object[]> listInfNatureza = retornarInformacoesNatureza();
			Map<String, BigDecimal> mapaInfNatureza = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfNatureza) {
				BigDecimal natureza = (BigDecimal) obj[0];
				String idExternoObj = (String) obj[1];
				String flag = (String) obj[2];

				if (mapaInfNatureza.get(idExternoObj + "###" + flag) == null) {
					mapaInfNatureza.put(idExternoObj + "###" + flag, natureza);
				}
			}

			// Natureza por empresa
			List<Object[]> listInfNaturezaEmpresa = retornarInformacoesNaturezaEmpresa();
			Map<String, BigDecimal> mapaInfNaturezaEmp = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfNaturezaEmpresa) {
				BigDecimal natureza = (BigDecimal) obj[0];
				String idExternoObj = (String) obj[1];
				BigDecimal codemp = (BigDecimal) obj[2];

				if (mapaInfNaturezaEmp.get(idExternoObj + "###" + codemp) == null) {
					mapaInfNaturezaEmp.put(idExternoObj + "###" + codemp,
							natureza);
				}
			}

			// RecDesp
			List<Object[]> listInfRecDesp = retornarInformacoesRecDesp();
			Map<String, String> mapaInfRecDesp = new HashMap<String, String>();
			for (Object[] obj : listInfRecDesp) {
				String recDesp = (String) obj[0];
				String idExternoObj = (String) obj[1];

				if (mapaInfRecDesp.get(idExternoObj) == null) {
					mapaInfRecDesp.put(idExternoObj, recDesp);
				}
			}

			// Banco
			List<Object[]> listInfBancoConta = retornarInformacoesBancoConta();
			Map<String, BigDecimal> mapaInfBanco = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfBancoConta) {
				Long codEmpObj = (Long) obj[1];
				BigDecimal codBcoObj = (BigDecimal) obj[3];

				if (mapaInfBanco.get(codEmpObj.toString()) == null) {
					mapaInfBanco.put(codEmpObj.toString(), codBcoObj);
				}
			}

			// Conta
			Map<String, BigDecimal> mapaInfConta = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfBancoConta) {
				BigDecimal codCtabCointObj = (BigDecimal) obj[0];
				Long codEmpObj = (Long) obj[1];

				if (mapaInfConta.get(codEmpObj.toString()) == null) {
					mapaInfConta.put(codEmpObj.toString(), codCtabCointObj);
				}
			}

			jdbc.openSession();

			String query = "SELECT CODEMP, URL, TOKEN, INTEGRACAO, " +
					"CASE WHEN PROFISSIONAL = 'S' THEN 'P' WHEN TECNICO = 'S' THEN 'T' ELSE 'N' END AS TIPEMP " +
					"FROM AD_LINKSINTEGRACAO";

			pstmt = jdbc.getPreparedStatement(query);

			rs = pstmt.executeQuery();

			while (rs.next()) {

				codEmp = rs.getBigDecimal("CODEMP");
				url = rs.getString("URL");
				token = rs.getString("TOKEN");
				tipoEmpresa = rs.getString("TIPEMP");
				String statusIntegracao = rs.getString("INTEGRACAO");

				// Verifica se a integração está ativa para esta empresa
				if (!"S".equals(statusIntegracao)) {
					System.out.println("Integração desativada para a empresa " + codEmp + " - pulando processamento");
					continue; // Pula para a próxima iteração do loop
				}

				iterarEndpoint(tipoEmpresa.trim(), mapaInfNaturezaEmp, mapaInfCenCusEmp,    //433
						mapaInfFinanceiroBanco, mapaInfFinanceiroBaixado,
						mapaInfNatureza, mapaInfBanco, mapaInfConta,
						mapaInfFinanceiro, mapaInfCenCus,
						mapaInfParceiros, url, token, codEmp);
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			jdbc.closeSession();



			if(selectsParaInsertLog.size() > 0){

				StringBuilder msgError = new StringBuilder();

				System.out.println("Entrou na lista do finally: " + selectsParaInsertLog.size());

				//int idInicial = util.getMaxNumLog();

				int qtdInsert = selectsParaInsertLog.size();

				int i = 1;
				for (String sqlInsert : selectsParaInsertLog) {
					String sql = sqlInsert;
					int nuFin = 0;

					try {
						nuFin = util.getMaxNumLog();
					} catch (Exception e) {
						e.printStackTrace();
					}

					sql = sql.replace("<#NUMUNICO#>", String.valueOf(nuFin));
					msgError.append(sql);

					if (i < qtdInsert) {
						msgError.append(" \nUNION ALL ");
					}
					i++;
				}

				System.out.println("Consulta de log: \n" + msgError);

				try {
					insertLogList(msgError.toString(), codEmp);
				} catch (Exception e) {
					e.printStackTrace();
				}

				msgError = null;
				this.selectsParaInsertLog = new ArrayList<String>();

			}

			System.out.println("Finalizou a JobGetTituloFornecedor");
		}
	}

	/**
	 * Versão otimizada do método apiGet com melhor tratamento de erros e recursos
	 */
	public String[] apiGet2(String ur, String token) throws Exception {
		BufferedReader reader;
		StringBuilder responseContent = new StringBuilder();
		String encodedUrl = ur.replace(" ", "%20");
		URL obj = new URL(encodedUrl);
		HttpURLConnection https = (HttpURLConnection)obj.openConnection();
		System.out.println("Entrou na API");
		System.out.println("URL: " + encodedUrl);
		System.out.println("Token Enviado: [" + token + "]");
		https.setRequestMethod("GET");
		https.setRequestProperty("User-Agent",
				"Mozilla/5.0 (Windows NT 10.0; Win64; x64)");
		https.setRequestProperty("Content-Type",
				"application/json; charset=UTF-8");
		https.setRequestProperty("Accept", "application/json");
		https.setRequestProperty("Authorization", "Bearer " + token);
		https.setDoInput(true);
		int status = https.getResponseCode();
		if (status >= 300) {
			reader = new BufferedReader(new InputStreamReader(
					https.getErrorStream()));
		} else {
			reader = new BufferedReader(new InputStreamReader(
					https.getInputStream()));
		}
		String line;
		while ((line = reader.readLine()) != null)
			responseContent.append(line);
		reader.close();
		System.out.println("Output from Server .... \n" + status);
		String response = responseContent.toString();
		https.disconnect();
		return new String[] { Integer.toString(status), response };
	}

	public void updateNumFin() throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		jdbc.openSession();

		String sqlUpdate = "UPDATE TGFNUM SET ULTCOD = NVL(ULTCOD, 0) + 1  WHERE ARQUIVO = 'TGFFIN'";

		pstmt = jdbc.getPreparedStatement(sqlUpdate);
		pstmt.executeUpdate();

		try {
			if (pstmt != null) {
				pstmt.close();
			}
			if (jdbc != null) {
				jdbc.closeSession();
			}
		} catch (Exception se) {
			se.printStackTrace();
		}

	}

	public BigDecimal getMaxNumFin() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal id = BigDecimal.ZERO;

		try {

			updateNumFin();

			jdbc.openSession();

			// String sqlNota = "SELECT SEQ_TGFFIN_NUFIN.NEXTVAL FROM DUAL";
			String sqlNota = "SELECT MAX(ULTCOD) AS ULTCOD FROM TGFNUM WHERE ARQUIVO = 'TGFFIN'";

			pstmt = jdbc.getPreparedStatement(sqlNota);

			rs = pstmt.executeQuery();

			if (rs.next()) {

				id = rs.getBigDecimal("ULTCOD");
			}
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}

		return id;
	}

	//esse metodo foi alterado
//	public void processDateRange(String tituloAberto,
//	        String tipoEmpresa,
//	        Map<String, BigDecimal> mapaInfNaturezaEmp,
//	        Map<String, BigDecimal> mapaInfCenCusEmp,
//	        Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco,
//	        Map<BigDecimal, String> mapaInfFinanceiroBaixado,
//	        Map<String, BigDecimal> mapaInfNatureza,
//	        Map<String, BigDecimal> mapaInfBanco,
//	        Map<String, BigDecimal> mapaInfConta,
//	        Map<String, BigDecimal> mapaInfFinanceiro,
//	        Map<String, BigDecimal> mapaInfCenCus,
//	        Map<String, BigDecimal> mapaInfParceiros,
//	        String url,
//	        String token,
//	        BigDecimal codEmp,
//	        String dataInicio,
//	        String dataFim,
//	        String idForn) throws Exception {
//
//	    try {
//	        // Validação do período de datas
//	        LocalDate inicio = LocalDate.parse(dataInicio);
//	        LocalDate fim = LocalDate.parse(dataFim);
//
//	        System.out.println("Iniciando consulta de títulos para o período: " + dataInicio + " até " + dataFim);
//
//	        // Construção da URL base com os parâmetros comuns
//	        StringBuilder urlBuilder = new StringBuilder(url)
//	            .append("/financeiro/clientes/titulos-pagar?")
//	            .append("quantidade=0")
//	            .append("&dataInicial=").append(dataInicio).append(" 00:00:00")
//	            .append("&dataFinal=").append(dataFim).append(" 23:59:59");
//
//	        // Adiciona parâmetro de situação (A = Aberto) se necessário
//	        if (tituloAberto.equalsIgnoreCase("S")) {
//	            urlBuilder.append("&situacao=A");
//	        }
//
//	        // Adiciona o parâmetro do fornecedor se estiver presente
//	        if (idForn != null && !idForn.isEmpty()) {
//	            String fornecedorEncoded = URLEncoder.encode(idForn, "UTF-8");
//	            urlBuilder.append("&fornecedor=").append(fornecedorEncoded);
//	            System.out.println("Adicionando fornecedor à requisição: " + idForn);
//	        }
//
//	        // Realiza uma única chamada à API para todo o período
//	        String[] response = apiGet(urlBuilder.toString(), token);
//
//	        // Processa a resposta
//	        int status = Integer.parseInt(response[0]);
//	        System.out.println("Status da requisição: " + status);
//
//	        String responseString = response[1];
//	        System.out.println("Response string títulos: " + responseString);
//
//	        // Processa os dados recebidos usando o método de leitura JSON existente
//	        leituraJSON(tipoEmpresa,
//	                   mapaInfNaturezaEmp,
//	                   mapaInfCenCusEmp,
//	                   mapaInfFinanceiroBanco,
//	                   mapaInfFinanceiroBaixado,
//	                   mapaInfNatureza,
//	                   mapaInfBanco,
//	                   mapaInfConta,
//	                   mapaInfFinanceiro,
//	                   mapaInfCenCus,
//	                   mapaInfParceiros,
//	                   response,
//	                   url,
//	                   token,
//	                   codEmp);
//
//	    } catch (Exception e) {
//	        System.err.println("Erro ao processar títulos para o período " +
//	                          dataInicio + " até " + dataFim + ": " + e.getMessage());
//	        e.printStackTrace();
//	        throw e;
//	    }
//	}


	public void processDateRange(
			String tituloAberto,
			String tipoEmpresa,
			Map<String, BigDecimal> mapaInfNaturezaEmp,
			Map<String, BigDecimal> mapaInfCenCusEmp,
			Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco,
			Map<BigDecimal, String> mapaInfFinanceiroBaixado,
			Map<String, BigDecimal> mapaInfNatureza,
			Map<String, BigDecimal> mapaInfBanco,
			Map<String, BigDecimal> mapaInfConta,
			Map<String, BigDecimal> mapaInfFinanceiro,
			Map<String, BigDecimal> mapaInfCenCus,
			Map<String, BigDecimal> mapaInfParceiros,
			String url,
			String token,
			BigDecimal codEmp,
			String dataInicio,
			String dataFim,
			String idForn) throws Exception {

		try {
			// Preparar as datas
			String dataInicialCompleta = dataInicio + " 00:00:00";
			String dataFinalCompleta = dataFim + " 23:59:59";

			// Codificar os parâmetros
			String dataInicialEncoded = URLEncoder.encode(dataInicialCompleta, "UTF-8");
			String dataFinalEncoded = URLEncoder.encode(dataFinalCompleta, "UTF-8");

			System.out.println("Iniciando consulta de títulos para o período: " + dataInicio + " até " + dataFim);

			// Lista para armazenar todos os registros
			JSONArray todosRegistros = new JSONArray();
			int pagina = 1;
			boolean temMaisRegistros = true;

			while (temMaisRegistros) {
				// Construir a URL para a página atual
				StringBuilder urlBuilder = new StringBuilder();
				urlBuilder.append(url.trim())
						.append("/financeiro/clientes/titulos-pagar")
						.append("?pagina=").append(pagina)
						.append("&quantidade=100")
						.append("&dataInicial=").append(dataInicialEncoded)
						.append("&dataFinal=").append(dataFinalEncoded);

				// Adiciona parâmetro de situação (A = Aberto) se necessário
				if (tituloAberto.equalsIgnoreCase("S")) {
					urlBuilder.append("&situacao=A");
				}

				// Adiciona o parâmetro do fornecedor se estiver presente
				if (idForn != null && !idForn.isEmpty()) {
					String fornecedorEncoded = URLEncoder.encode(idForn, "UTF-8");
					urlBuilder.append("&fornecedor=").append(fornecedorEncoded);
					System.out.println("Adicionando fornecedor à requisição: " + idForn);
				}

				String urlCompleta = urlBuilder.toString();
				System.out.println("URL para títulos (página " + pagina + "): " + urlCompleta);

				// Fazer a requisição
				String[] response = apiGet2(urlCompleta, token);
				int status = Integer.parseInt(response[0]);

				if (status == 200) {
					JSONArray paginaAtual = new JSONArray(response[1]);

					// Adicionar registros ao array acumulado
					for (int i = 0; i < paginaAtual.length(); i++) {
						todosRegistros.put(paginaAtual.getJSONObject(i));
					}

					// Verificar se é a última página
					if (paginaAtual.length() < 100) {
						temMaisRegistros = false;
					} else {
						pagina++;
					}

					System.out.println("Página " + pagina + ": " + paginaAtual.length() +
							" registros. Total acumulado: " + todosRegistros.length());
				} else {
					throw new Exception(String.format(
							"Erro na requisição de títulos. Status: %d. Resposta: %s. URL: %s",
							status, response[1], urlCompleta
					));
				}
			}

			// Criar um array de resposta com todos os registros acumulados
			String[] responseArray = new String[]{
					String.valueOf(200),
					todosRegistros.toString()
			};

			System.out.println("Total de registros de títulos acumulados: " + todosRegistros.length());

			// Processar todos os registros acumulados
			leituraJSON(
					tipoEmpresa,
					mapaInfNaturezaEmp,
					mapaInfCenCusEmp,
					mapaInfFinanceiroBanco,
					mapaInfFinanceiroBaixado,
					mapaInfNatureza,
					mapaInfBanco,
					mapaInfConta,
					mapaInfFinanceiro,
					mapaInfCenCus,
					mapaInfParceiros,
					responseArray,
					url,
					token,
					codEmp
			);

		} catch (Exception e) {
			System.err.println("Erro ao processar títulos para o período " +
					dataInicio + " até " + dataFim + ": " + e.getMessage());
			e.printStackTrace();
			throw e;
		}
	}

	public void iterarEndpoint(String tipoEmpresa,
							   Map<String, BigDecimal> mapaInfNaturezaEmp,
							   Map<String, BigDecimal> mapaInfCenCusEmp,
							   Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco,
							   Map<BigDecimal, String> mapaInfFinanceiroBaixado,
							   Map<String, BigDecimal> mapaInfNatureza,
							   Map<String, BigDecimal> mapaInfBanco,
							   Map<String, BigDecimal> mapaInfConta,
							   Map<String, BigDecimal> mapaInfFinanceiro,
							   Map<String, BigDecimal> mapaInfCenCus,
							   Map<String, BigDecimal> mapaInfParceiros,
							   String url, String token,
							   BigDecimal codEmp) throws Exception {

		Date dataAtual = new Date();

		SimpleDateFormat formato = new SimpleDateFormat("yyyy-MM-dd");

		String dataFormatada = formato.format(dataAtual);

		try {

			System.out.println("While de iteracao");

			String[] response = apiGet2(url
					+ "/financeiro/clientes/titulos-pagar?quantidade=0"
					+ "&dataInicial=" + dataFormatada + " 00:00:00" + "&dataFinal="
					+ dataFormatada + " 23:59:59", token);

			int status = Integer.parseInt(response[0]);

			System.out.println("Status teste: " + status);

			String responseString = response[1];
			System.out.println("response string alunos: " + responseString);

			leituraJSON(tipoEmpresa, mapaInfNaturezaEmp, mapaInfCenCusEmp,   //870
					mapaInfFinanceiroBanco, mapaInfFinanceiroBaixado,
					mapaInfNatureza, mapaInfBanco, mapaInfConta,
					mapaInfFinanceiro, mapaInfCenCus, mapaInfParceiros,
					response, url, token, codEmp);

		}

		catch (Exception e) {
			e.printStackTrace();
		}
	}


	//debug para erro de this is not a java array
	public void leituraJSON(String tipoEmpresa,
							Map<String, BigDecimal> mapaInfNaturezaEmp,
							Map<String, BigDecimal> mapaInfCenCusEmp,
							Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco,
							Map<BigDecimal, String> mapaInfFinanceiroBaixado,
							Map<String, BigDecimal> mapaInfNatureza,
							Map<String, BigDecimal> mapaInfBanco,
							Map<String, BigDecimal> mapaInfConta,
							Map<String, BigDecimal> mapaInfFinanceiro,
							Map<String, BigDecimal> mapaInfCenCus,
							Map<String, BigDecimal> mapaInfParceiros,
							String [] response, String url,String token, BigDecimal codemp) throws Exception {
		System.out.println("Inicio leitura do JSON - JobGetTituloFornecedor");

		BigDecimal codparc = BigDecimal.ZERO;

		Date dataAtual = new Date();
		SimpleDateFormat formato = new SimpleDateFormat("yyyy-MM-dd");

		Calendar calendar = Calendar.getInstance();
		calendar.setTime(dataAtual);
		calendar.add(Calendar.DAY_OF_MONTH, -1);

		Date dataUmDiaAtras = calendar.getTime();

		String dataUmDiaFormatada = formato.format(dataUmDiaAtras);
		String dataAtualFormatada = formato.format(dataAtual);

		System.out.println("data um dia atras forn titulo: " + dataUmDiaFormatada);
		System.out.println("data normal forn titulo: " + dataAtualFormatada);

		StringBuilder consulta = new StringBuilder();

		EnviromentUtils util = new EnviromentUtils();



		try {

			JsonParser parser = new JsonParser();

			System.out.println("Response length: " + response.length);
			System.out.println("Response content: " + response[1]);

			// Adicione estas instruções de debug logo após criar o parser
			System.out.println("Tipo de resposta: " + parser.parse(response[1]).getClass().getName());
			System.out.println("Conteúdo bruto da resposta: ");
			System.out.println(response[1]);

			try {
				// Tente analisar a resposta de maneira diferente
				JsonElement element = parser.parse(response[1]);

				if (element.isJsonArray()) {
					JsonArray jsonArray = element.getAsJsonArray();
					System.out.println("Analisado com sucesso como array JSON com " + jsonArray.size() + " elementos");

					// Continue com o processamento do array
					if (response[0].equalsIgnoreCase("200")) {
						// Seu código existente para processar o array...
					}
				} else if (element.isJsonObject()) {
					JsonObject jsonObject = element.getAsJsonObject();
					System.out.println("A resposta é um objeto JSON, não um array");
					// Se os dados reais estiverem dentro de um objeto, você pode precisar extraí-los
					if (jsonObject.has("data") && jsonObject.get("data").isJsonArray()) {
						System.out.println("Array de dados encontrado dentro do objeto JSON");
						JsonArray jsonArray = jsonObject.get("data").getAsJsonArray();

						// Continue com o processamento do array extraído
						if (response[0].equalsIgnoreCase("200")) {
							// Seu código existente para processar o array...
						}
					} else {
						System.out.println("Nenhum array 'data' encontrado no objeto JSON");
					}
				} else {
					System.out.println("A resposta não é nem um array JSON nem um objeto JSON");
				}
			} catch (Exception e) {
				System.out.println("Exceção ao tentar analisar o JSON: " + e.getMessage());
				e.printStackTrace();
			}


			//fim do debug

			System.out.println("Response length: " + response.length);

			System.out.println("Response content: " + response[1]);

			JsonArray jsonArray = parser.parse(response[1]).getAsJsonArray();

			if (response[0].equalsIgnoreCase("200")) {
				System.out.println("API response code: " + response[0]);

				int count = 0;
				int total = jsonArray.size();
				int qtdInsert = 0;

				List<String> selectsParaInsert = new ArrayList<String>();   //922

				for (JsonElement jsonElement : jsonArray) {
					System.out.println("comecou a leitura do JSON");
					JsonObject JSON = jsonElement.getAsJsonObject();

					String fornecedorId = JSON.get("fornecedor_id")
							.getAsString();

					System.out.println("FornecedorId: " + fornecedorId);

					codparc = Optional
							.ofNullable(
									mapaInfParceiros.get(fornecedorId + "###"
											+ codemp)).orElse(BigDecimal.ZERO);

					String idFin = JSON.get("titulo_id").getAsString();

					String taxaId = JSON.get("taxa_id").getAsString();

					String dtVenc = JSON.get("titulo_vencimento").getAsString();

					String vlrDesdob = JSON.get("titulo_valor").getAsString();

					String tituloSituacao = "";

					if (!JSON.get("titulo_situacao").isJsonNull()) {

						tituloSituacao = JSON.get("titulo_situacao")
								.getAsString();
					}

					String tituloObservacao = "";
					if (!JSON.get("titulo_observacao").isJsonNull()) {

						tituloObservacao = JSON.get("titulo_observacao")
								.getAsString();
					}


					String cursoId = "";

					if (!JSON.get("curso_id").isJsonNull()) {

						cursoId = JSON.get("curso_id").getAsString();
					}

					String dtPedido = JSON.get("data_atualizacao")
							.getAsString();

					String recDesp = "";

					// Formatando o DATA DO PEDIDO
					SimpleDateFormat formatoHoraMs = new SimpleDateFormat(
							"yyyy-MM-dd HH:mm:ss.SSS");
					Date dataHora = formatoHoraMs.parse(dtPedido);
					SimpleDateFormat formatoHora = new SimpleDateFormat(
							"dd/MM/yyyy");
					String dtPedidoFormatado = formatoHora.format(dataHora);

					// Formatando o DATA DE VENCIMENTO
					SimpleDateFormat formatoEntrada = new SimpleDateFormat(
							"yyyy-MM-dd");
					Date data = formatoEntrada.parse(dtVenc);
					SimpleDateFormat formatoSaida = new SimpleDateFormat(
							"dd/MM/yyyy");
					String dataVencFormatada = formatoSaida.format(data);

					BigDecimal codCenCus = Optional.ofNullable(mapaInfCenCus.get(taxaId + "###" + tipoEmpresa))
							.orElse(BigDecimal.ZERO);

					System.out.println("CodCenCus: " + codCenCus);
					System.out.println("Taxa id: " + taxaId);
					System.out.println("CodParc: " + codparc);

					if (!tituloSituacao.equalsIgnoreCase("X")) {

						if (codparc.compareTo(BigDecimal.ZERO) != 0) {
							System.out
									.println("Entrou no parceiro: " + codparc);

							BigDecimal validarNufin = Optional.ofNullable(
									mapaInfFinanceiro.get(codemp + "###"
											+ idFin)).orElse(BigDecimal.ZERO);

							if (validarNufin.compareTo(BigDecimal.ZERO) == 0) {
								System.out.println("Entrou no financeiro");

								BigDecimal codConta = mapaInfConta.get(codemp
										.toString());// getCodConta(codemp);

								BigDecimal codBanco = mapaInfBanco.get(codemp
										.toString());// getCodBanco(codemp);

								System.out.println("Size Nat: " + mapaInfNatureza.size());

								BigDecimal natureza = Optional.ofNullable(mapaInfNatureza.get(taxaId + "###" + tipoEmpresa))
										.orElse(BigDecimal.ZERO);

								System.out.println("Natureza: " + natureza);

								recDesp = "-1";

								BigDecimal vlrDesdobBigDecimal = new BigDecimal(vlrDesdob);

								if (natureza.compareTo(BigDecimal.ZERO) != 0
										&& codCenCus.compareTo(BigDecimal.ZERO) != 0) {

									String sqlInsert = " SELECT  <#NUFIN#>, NULL, 0, 'F', -1, "
											+ ""
											+ codemp
											+ " , "
											+ codCenCus
											+ ", "
											+ natureza
											+ ",  "
											+ BigDecimal.valueOf(1300)
											+ ", "
											+ "(SELECT MAX(DHALTER)    FROM TGFTOP   WHERE CODTIPOPER = "
											+ BigDecimal.valueOf(1300)
											+ "), 0, "
											+ "(SELECT MAX(DHALTER) FROM TGFTOP WHERE CODTIPOPER = 0), "
											+ codparc
											+ ", "
											+ BigDecimal.valueOf(4)
											+ ", "
											+ vlrDesdobBigDecimal
											+ " ,0, 0, "
											+ codBanco
											+ ", "
											+ codConta
											+ ", '"
											+ dtPedidoFormatado
											+ "' "
											+ ",SYSDATE, SYSDATE, "
											+ "'"
											+ dataVencFormatada
											+ "', SYSDATE, "
											+ "'"
											+ dataVencFormatada
											+ "' , 1 ,  1 , "
											+ "'"
											+ tituloObservacao
											+ "' ,'I' ,  'N' ,  'N' ,  'N' ,  'N' ,  'N' ,  'N' ,  'N' ,  'S' ,  'S' ,  0,  0, 0,  0, 0,  0, 0,  0, 0,  0, 0,  0, 0,  0, 0,  0, 0,  0, 0,  0, 0,  0, 0,  0, 0,  0, 0,  0, "
											+ "'" + idFin + "', null FROM DUAL";

									consulta.append(sqlInsert);

									selectsParaInsert.add(sqlInsert);
									qtdInsert++;

									if (count < total - 1) {
										consulta.append("\nUNION ALL");
									}

									System.out.println("Financeiro cadastrado");

								}else{

									selectsParaInsertLog.add("SELECT <#NUMUNICO#>, 'Sem \"de para\" para a Taxa ID: "+ taxaId+"', SYSDATE, 'Aviso', "+codemp+", '' FROM DUAL");

									//util.inserirLog("Erro de de para/parceiro: " + taxaId, "Aviso", codparc.toString(), codemp);


								}

							} else {
								System.out.println("Financeiro " + idFin
										+ " ja cadastrado para o parceiro: "
										+ codparc);
							}

						} else {
							System.out.println("Sem Parceiro");
						}

					} else if(tituloSituacao.equalsIgnoreCase("X")){
						System.out.println("Cancelado");

						BigDecimal validarNufin = Optional.ofNullable(
										mapaInfFinanceiro.get(codemp + "###" + idFin))
								.orElse(BigDecimal.ZERO);

						if (validarNufin.compareTo(BigDecimal.ZERO) != 0) {
							if ("S".equalsIgnoreCase(mapaInfFinanceiroBaixado
									.get(validarNufin))) {

								BigDecimal nubco = mapaInfFinanceiroBanco
										.get(validarNufin);

								updateFinExtorno(validarNufin, codemp);
								deleteTgfMbc(nubco, codemp);
								deleteTgfFin(validarNufin, codemp);

							} else {
								deleteTgfFin(validarNufin, codemp);
							}
						}

					}

					// insertFinanceiro(codemp);
					count++;
				}

				// Apenas se encontrar registro eleg�vel
				if (qtdInsert > 0) {
					// Capturar o tgfnum
					BigDecimal nuFinInicial = util.getMaxNumFin(false);

					// Atualizar o nufin adicionando a quantidade de lista
					util.updateNumFinByQtd(qtdInsert);
					System.out.println("nuFinInicial: " + nuFinInicial);

					// remontar a lista para inserir
					StringBuilder sqlInsertFin = new StringBuilder();
					int i = 1;
					for (String sqlInsert : selectsParaInsert) {

						String sql = sqlInsert;

						int nuFin = nuFinInicial.intValue() + i;
						sql = sql.replace("<#NUFIN#>", String.valueOf(nuFin));
						sqlInsertFin.append(sql);

						if (i < qtdInsert) {
							sqlInsertFin.append(" \nUNION ALL ");
						}
						i++;

					}

					System.out.println("Consulta Ap�s Tratamento: "
							+ sqlInsertFin);

					// gravar o financeiro
					insertFinByList(sqlInsertFin, codemp);

					// updateFlagAlunoIntegrado(aluno);
				}
			} else {

				selectsParaInsertLog.add("SELECT <#NUMUNICO#>, 'Api Retornou Status Diferente de Sucesso, Status Retornado: "+response[0]+"', SYSDATE, 'Erro', "+codemp+", '' FROM DUAL");

				//util.inserirLog("Retorno da API diferente de 200, favor resivar: " + response[0], "Aviso", "", codemp);
			}


		}catch(Exception e){

			StringWriter sw = new StringWriter();
			String stackTraceAsString = sw.toString();

			selectsParaInsertLog.add("SELECT <#NUMUNICO#>, 'Erro ao integrar financeiro, Mensagem de erro: "
					+ e.getMessage()+"', SYSDATE, 'Erro', "+codemp+", '' FROM DUAL");

			//util.inserirLog("Ocorreu um erro no cadastro financeiro do credo: "+ stackTraceAsString, "erro", "", codemp);

			e.printStackTrace();

		}
		finally{

			System.out.println("Fim leitura do JSON - JobGetTituloFornecedor");
		}


	}

	public BigDecimal getNatureza(String idExterno) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal id = BigDecimal.ZERO;

		try {

			jdbc.openSession();

			String sqlNota = "SELECT CODNAT FROM AD_NATACAD WHERE IDEXTERNO = '" + idExterno +"'"
					+" union "
					+ "SELECT 0 FROM DUAL WHERE NOT EXISTS (SELECT CODNAT FROM AD_NATACAD WHERE IDEXTERNO = '" + idExterno+"')" ;

			pstmt = jdbc.getPreparedStatement(sqlNota);

			rs = pstmt.executeQuery();

			if (rs.next()) {

				id = rs.getBigDecimal("CODNAT");

			}

		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}

		if(id.compareTo(BigDecimal.ZERO) == 0){
			id = BigDecimal.valueOf(30701010);
		}

		return id;
	}

	public boolean validarFin(String idFin, BigDecimal codemp) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		int count = 0;

		try {

			jdbc.openSession();

			String sqlNota = "SELECT COUNT(0) AS COUNT FROM TGFFIN WHERE AD_IDEXTERNO = ? AND CODEMP = " + codemp;

			pstmt = jdbc.getPreparedStatement(sqlNota);
			pstmt.setString(1, idFin);

			rs = pstmt.executeQuery();

			if (rs.next()) {

				count = rs.getInt("COUNT");

			}

		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}

		if (count > 0) {
			return false;
		} else {
			return true;
		}
	}

	public BigDecimal getCodCenCus(String idCurso) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal id = BigDecimal.ZERO;

		try {

			jdbc.openSession();

			String sqlNota = "select codcencus from tsicus where ad_idexterno = ? "
					+ "	union     "
					+ "	select 0 from dual where not exists (select codcencus from tsicus where ad_idexterno = ?)";

			pstmt = jdbc.getPreparedStatement(sqlNota);
			pstmt.setString(1, idCurso);
			pstmt.setString(2, idCurso);

			rs = pstmt.executeQuery();

			if (rs.next()) {

				id = rs.getBigDecimal("codcencus");

			}

		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}

		return id;
	}

	public BigDecimal insertFinanceiro(BigDecimal codemp, BigDecimal codCenCus,
									   BigDecimal codNat, BigDecimal codTipOper, BigDecimal codparc,
									   BigDecimal codTipTit, BigDecimal vlrDesdbo, String dtVenc,
									   String dtPedido, String idExterno, String idAluno,
									   BigDecimal codConta, BigDecimal codBanco, String recDesp, String obs) throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement ps = null;

		BigDecimal nufin = getMaxNumFin();

		try	{
			System.out.println("Deu inicio ao insertTGFFIN()");

			jdbc.openSession();
			String sqlUpdate = "INSERT INTO TGFFIN "
					+ "        (NUFIN, "
					+ "         NUNOTA, "
					+ "         NUMNOTA, "
					+ "         ORIGEM, "
					+ "         RECDESP, "
					+ "         CODEMP, "
					+ "         CODCENCUS, "
					+ "         CODNAT, "
					+ "         CODTIPOPER, "
					+ "         DHTIPOPER, "
					+ "         CODTIPOPERBAIXA, "
					+ "         DHTIPOPERBAIXA, "
					+ "         CODPARC, "
					+ "         CODTIPTIT, "
					+ "         VLRDESDOB, "
					+ "         VLRDESC, "
					+ "         VLRBAIXA, "
					+ "         CODBCO, "
					+ "         CODCTABCOINT, "
					+ "         DTNEG, "
					+ "         DHMOV, "
					+ "         DTALTER, "
					+ "         DTVENC, "
					+ "         DTPRAZO, "
					+ "         DTVENCINIC, "
					+ "         TIPJURO, "
					+ "         TIPMULTA, "
					+ "         HISTORICO, "
					+ "         TIPMARCCHEQ, "
					+ "         AUTORIZADO, "
					+ "         BLOQVAR, "
					+ "         INSSRETIDO, "
					+ "         ISSRETIDO, "
					+ "         PROVISAO, "
					+ "         RATEADO, "
					+ "         TIMBLOQUEADA, "
					+ "         IRFRETIDO, "
					+ "         TIMTXADMGERALU, "
					+ "         VLRDESCEMBUT, "
					+ "         VLRINSS, "
					+ "         VLRIRF, "
					+ "         VLRISS, "
					+ "         VLRJURO, "
					+ "         VLRJUROEMBUT, "
					+ "         VLRJUROLIB, "
					+ "         VLRJURONEGOC, "
					+ "         VLRMOEDA, "
					+ "         VLRMOEDABAIXA, "
					+ "         VLRMULTA, "
					+ "         VLRMULTAEMBUT, "
					+ "         VLRMULTALIB, "
					+ "         VLRMULTANEGOC, "
					+ "         VLRPROV, "
					+ "         VLRVARCAMBIAL, "
					+ "         VLRVENDOR, "
					+ "         ALIQICMS, "
					+ "         BASEICMS, "
					+ "         CARTAODESC, "
					+ "         CODMOEDA, "
					+ "         CODPROJ, "
					+ "         CODVEICULO, "
					+ "         CODVEND, "
					+ "         DESPCART, "
					+ "         NUMCONTRATO, "
					+ "         ORDEMCARGA, "
					+ "         CODUSU,"
					+ "         AD_IDEXTERNO,"
					+ "         AD_IDALUNO) "
					+ "        VALUES (?, "
					+ "               NULL, "
					+ "               0, "
					+ "               'F', "
					+ "               -1, "
					+ "               "+codemp+" , " // AS CODEMP
					+ "               "+codCenCus+" , " // AS CODCENCUS
					+ "               "+codNat+" , " // AS CODNAT
					+ "               "+codTipOper+" , " // AS CODTIPOPER
					+ "               (SELECT MAX(DHALTER) "
					+ "                  FROM TGFTOP "
					+ "                 WHERE CODTIPOPER = "+codTipOper+"), "
					+ "               0, "
					+ "               (SELECT MAX(DHALTER) FROM TGFTOP WHERE CODTIPOPER = 0), "
					+ "               "+codparc+" , " // AS CODPARC
					+ "               "+codTipTit+" , " // AS CODTIPTIT
					+ "               "+vlrDesdbo+" , " // AS VLRDESDOB
					+ "               0, "
					+ "               0, "
					+ "               "+codBanco+", " // AS CODBCO
					+ "               "+codConta+", " // AS CODCTABCOINT
					+ "               '"+dtPedido+"' , " // AS DTNEG
					+ "               SYSDATE, "
					+ "               SYSDATE, "
					+ "               '"+dtVenc+"' , " // AS DTVENC
					+ "               SYSDATE, " // AS PRAZO
					+ "               '"+dtVenc+"' , " // AS DTVENCINIC
					+ "               1 , " // AS TIPJURO
					+ "               1 , " // AS TIPMULTA
					+ "               '"+obs+"' , " // AS HISTORICO
					+ "               'I' , " // AS TIPMARCCHEQ
					+ "               'N' , " // AS AUTORIZADO
					+ "               'N' , " // AS BLOQVAR
					+ "               'N' , " // AS INSSRETIDO
					+ "               'N' , " // AS ISSRETIDO
					+ "               'N' , " // AS PROVISAO
					+ "               'N' , " // AS RATEADO
					+ "               'N' , " // AS TIMBLOQUEADA
					+ "               'S' , " // AS IRFRETIDO
					+ "               'S' , " // AS TIMTXADMGERALU
					+ "               0, " + "               0, "
					+ "               0, " + "               0, "
					+ "               0, " + "               0, "
					+ "               0, " + "               0, "
					+ "               0, " + "               0, "
					+ "               0, " + "               0, "
					+ "               0, " + "               0, "
					+ "               0, " + "               0, "
					+ "               0, " + "               0, "
					+ "               0, " + "               0, "
					+ "               0, " + "               0, "
					+ "               0, " + "               0, "
					+ "               0, " + "               0, "
					+ "               0, " + "               0,"
					+ "               '"+idExterno+"'," + "     '"+idAluno+"')";

			ps = jdbc.getPreparedStatement(sqlUpdate);
			ps.setBigDecimal(1, nufin);


			ps.executeUpdate();


		}catch(Exception e){
			e.printStackTrace();
			throw new Exception(e.getCause());
		}finally{
			if(ps != null){
				ps.close();
			}
			jdbc.closeSession();
			System.out.println("Deu Fim ao insertTGFFIN()");
		}

		return nufin;

	}

	public BigDecimal getCodCenCusPeloCusto(String idTaxa) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal id = BigDecimal.ZERO;

		try {

			jdbc.openSession();

			String sqlNota = "SELECT CODCENCUS FROM AD_NATACAD WHERE IDEXTERNO = " + idTaxa;

			pstmt = jdbc.getPreparedStatement(sqlNota);

			rs = pstmt.executeQuery();

			if (rs.next()) {

				id = rs.getBigDecimal("CODCENCUS");

			}

		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}

		return id;
	}

	public boolean getRecDesp(String idExterno) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		int id = 0;

		try {

			jdbc.openSession();

			String sqlNota = "SELECT SUBSTR(CODNAT, 0, 1) NAT "
					+ "FROM AD_NATACAD where idexterno = " + idExterno + " "
					+ "union SELECT '0' FROM DUAL "
					+ "WHERE NOT EXISTS (SELECT SUBSTR(CODNAT, 0, 1) NAT FROM AD_NATACAD where idexterno = " + idExterno+")";

			pstmt = jdbc.getPreparedStatement(sqlNota);

			rs = pstmt.executeQuery();

			if (rs.next()) {

				id = Integer.parseInt(rs.getString("NAT"));

			}

		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}

		if(id > 1){
			return true;
		}else{
			return false;
		}
	}

	public BigDecimal getCodBanco(BigDecimal codEmp) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal id = BigDecimal.ZERO;

		try {

			jdbc.openSession();

			String sqlNota = "select CODBCO " + "from ad_infobankbaixa "
					+ "WHERE CODEMP = ? " + "AND IDEXTERNO IS NULL";

			pstmt = jdbc.getPreparedStatement(sqlNota);
			pstmt.setBigDecimal(1, codEmp);

			rs = pstmt.executeQuery();

			if (rs.next()) {

				id = rs.getBigDecimal("CODBCO");

			}

		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}

		return id;
	}

	public BigDecimal getCodConta(BigDecimal codEmp) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal id = BigDecimal.ZERO;

		try {

			jdbc.openSession();

			String sqlNota = "select CODCTABCOINT " + "from ad_infobankbaixa "
					+ "WHERE CODEMP = ? " + "AND IDEXTERNO IS NULL";

			pstmt = jdbc.getPreparedStatement(sqlNota);
			pstmt.setBigDecimal(1, codEmp);

			rs = pstmt.executeQuery();

			if (rs.next()) {

				id = rs.getBigDecimal("CODCTABCOINT");

			}

		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}

		return id;
	}


	public void updateFlagFornIntegrado(String idAluno, BigDecimal codemp) throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		try {

			jdbc.openSession();

			String sqlUpdate = "UPDATE AD_IDFORNACAD SET INTEGRADOFIN = 'S' WHERE IDACADWEB = '"+idAluno+"' AND CODEMP = " + codemp;

			pstmt = jdbc.getPreparedStatement(sqlUpdate);
			pstmt.executeUpdate();



		} catch (Exception se) {
			se.printStackTrace();
		}finally{
			if (pstmt != null) {
				pstmt.close();
			}
			if (jdbc != null) {
				jdbc.closeSession();
			}
		}

	}

	public void updateResetarForn() throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		try {
			System.out.println("Entrou no UPDATE da flag dos alunos");
			jdbc.openSession();

			String sqlUpdate = "UPDATE AD_IDFORNACAD SET INTEGRADOFIN = 'N'";

			pstmt = jdbc.getPreparedStatement(sqlUpdate);
			pstmt.executeUpdate();

		} catch (Exception se) {
			se.printStackTrace();
		}finally{
			if (pstmt != null) {
				pstmt.close();
			}
			if (jdbc != null) {
				jdbc.closeSession();
			}
		}

	}

	public List<Object[]> retornarInformacoesFinanceiro() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		List<Object[]> listRet = new ArrayList<>();
		try {
			jdbc.openSession();
			String sql = "	SELECT 	CODEMP, NUFIN, AD_IDEXTERNO, (CASE WHEN DHBAIXA IS NOT NULL THEN 'S' ELSE 'N' END) BAIXADO, VLRDESDOB, NUBCO ";
			sql += "		FROM  	TGFFIN ";
			sql += "		WHERE  	RECDESP = -1 ";
			sql += "		    AND PROVISAO = 'N' ";
			pstmt = jdbc.getPreparedStatement(sql);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				Object[] ret = new Object[6];
				ret[0] = rs.getBigDecimal("NUFIN");
				ret[1] = rs.getBigDecimal("CODEMP");
				ret[2] = rs.getString("AD_IDEXTERNO");
				ret[3] = rs.getString("BAIXADO");
				ret[4] = rs.getBigDecimal("VLRDESDOB");
				ret[5] = rs.getBigDecimal("NUBCO");

				listRet.add(ret);
			}

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}

		return listRet;
	}

	private List<Object[]> retornarInformacoesParceiros() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		List<Object[]> listRet = new ArrayList<>();

		try {
			jdbc.openSession();
			String sql = "	SELECT CODPARC, IDACADWEB, CODEMP FROM AD_IDFORNACAD";

			pstmt = jdbc.getPreparedStatement(sql);
			rs = pstmt.executeQuery();

			while (rs.next()) {
				Object[] ret = new Object[3];
				ret[0] = rs.getBigDecimal("CODPARC");
				ret[1] = rs.getString("IDACADWEB");
				ret[2] = rs.getBigDecimal("CODEMP");

				listRet.add(ret);
			}

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}

		return listRet;
	}

	public List<Object[]> retornarInformacoesCenCus() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		List<Object[]> listRet = new ArrayList<>();
		try {
			jdbc.openSession();
			String sql = "SELECT CODCENCUS, IDEXTERNO, CASE WHEN PROFISSIONAL = 'S' THEN 'P' WHEN TECNICO = 'S' THEN 'T' ELSE 'N' END AS FLAG "
					+ " FROM AD_NATACAD ";

			pstmt = jdbc.getPreparedStatement(sql);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				Object[] ret = new Object[3];
				ret[0] = rs.getBigDecimal("CODCENCUS");
				ret[1] = rs.getString("IDEXTERNO");
				ret[2] = rs.getString("FLAG");

				listRet.add(ret);
			}

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}

		return listRet;
	}


	public List<Object[]> retornarInformacoesCenCusEmpresa() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		List<Object[]> listRet = new ArrayList<>();
		try {
			jdbc.openSession();
			String sql = "SELECT CODCENCUS, IDEXTERNO, CODEMP FROM AD_NATACAD WHERE AD_IDEXTERNO IS NOT NULL AND CODEMP IS NOT NULL";

			pstmt = jdbc.getPreparedStatement(sql);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				Object[] ret = new Object[3];
				ret[0] = rs.getBigDecimal("CODCENCUS");
				ret[1] = rs.getString("IDEXTERNO");
				ret[2] = rs.getBigDecimal("CODEMP");

				listRet.add(ret);
			}

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}

		return listRet;
	}


	public List<Object[]> retornarInformacoesNatureza() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		List<Object[]> listRet = new ArrayList<>();
		try {
			jdbc.openSession();
			String sql = "SELECT CODNAT, IDEXTERNO, CODEMP, "
					+ "CASE WHEN PROFISSIONAL = 'S' THEN 'P' WHEN TECNICO = 'S' THEN 'T' ELSE 'N' END AS FLAG  FROM AD_NATACAD";

			pstmt = jdbc.getPreparedStatement(sql);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				Object[] ret = new Object[3];
				ret[0] = rs.getBigDecimal("CODNAT");
				ret[1] = rs.getString("IDEXTERNO");
				ret[2] = rs.getString("FLAG");

				listRet.add(ret);
			}

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}

		return listRet;
	}


	public List<Object[]> retornarInformacoesNaturezaEmpresa() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		List<Object[]> listRet = new ArrayList<>();
		try {
			jdbc.openSession();
			String sql = "SELECT CODNAT, IDEXTERNO, CODEMP FROM AD_NATACAD WHERE CODEMP IS NOT NULL";

			pstmt = jdbc.getPreparedStatement(sql);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				Object[] ret = new Object[3];
				ret[0] = rs.getBigDecimal("CODNAT");
				ret[1] = rs.getString("IDEXTERNO");
				ret[2] = rs.getBigDecimal("CODEMP");

				listRet.add(ret);
			}

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}

		return listRet;
	}

	public List<Object[]> retornarInformacoesRecDesp() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		List<Object[]> listRet = new ArrayList<>();
		try {
			jdbc.openSession();
			String sql = "SELECT CASE WHEN SUBSTR(CODNAT, 0, 1) > 1 THEN '-1' ELSE '1' END AS NAT, idexterno "
					+ "FROM AD_NATACAD where idexterno IS NOT NULL "
					+ " union SELECT '0', '0' FROM DUAL "
					+ " WHERE NOT EXISTS (SELECT SUBSTR(CODNAT, 0, 1) NAT FROM AD_NATACAD where idexterno IS NOT NULL)";

			pstmt = jdbc.getPreparedStatement(sql);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				Object[] ret = new Object[2];
				ret[0] = rs.getString("NAT");
				ret[1] = rs.getString("idexterno");

				listRet.add(ret);
			}

		} catch (SQLException e) {
			e.printStackTrace();
			throw new Exception(
					"Erro Ao Executar Metodo retornarInformacoesBancoConta");
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}

		return listRet;
	}

	public List<Object[]> retornarInformacoesBancoConta() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		List<Object[]> listRet = new ArrayList<>();
		try {
			jdbc.openSession();
			String sql = "	SELECT 	CODCTABCOINT, CODEMP, IDEXTERNO, CODBCO ";
			sql += "		FROM  	ad_infobankbaixa WHERE IDEXTERNO IS NULL";
			pstmt = jdbc.getPreparedStatement(sql);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				Object[] ret = new Object[4];
				ret[0] = rs.getBigDecimal("CODCTABCOINT");
				ret[1] = rs.getLong("CODEMP");
				ret[2] = rs.getLong("IDEXTERNO");
				ret[3] = rs.getBigDecimal("CODBCO");

				listRet.add(ret);
			}

		} catch (SQLException e) {
			e.printStackTrace();
			throw new Exception(
					"Erro Ao Executar Metodo retornarInformacoesBancoConta");
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}

		return listRet;
	}


	public void updateFinExtorno(BigDecimal nufin, BigDecimal codemp) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		try {

			jdbc.openSession();

			String sqlNota = "UPDATE TGFFIN SET "
					+ "VLRBAIXA = 0, "
					+ "DHBAIXA = NULL, "
					+ "NUBCO = NULL, "
					+ "CODTIPOPERBAIXA = 0, "
					+ "DHTIPOPERBAIXA = (SELECT MAX(DHALTER) FROM TGFTOP WHERE CODTIPOPER = 0), "
					+ "CODUSUBAIXA = NULL  " + "WHERE NUFIN = " + nufin;

			pstmt = jdbc.getPreparedStatement(sqlNota);

			pstmt.executeUpdate();

			System.out.println("Passou do update");
		} catch (SQLException e) {
			selectsParaInsertLog.add("SELECT <#NUMUNICO#>, 'Erro ao Extornar Titulo "+nufin+": "+e.getMessage()+"', SYSDATE, 'Erro', "+codemp+", '' FROM DUAL");

			e.printStackTrace();
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}

	public void deleteTgfMbc(BigDecimal nubco, BigDecimal codemp) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		try {

			jdbc.openSession();

			String sqlNota = "DELETE FROM TGFMBC WHERE NUBCO = " + nubco;

			pstmt = jdbc.getPreparedStatement(sqlNota);

			pstmt.executeUpdate();

			System.out.println("Passou do update");
		} catch (SQLException e) {
			e.printStackTrace();
			selectsParaInsertLog.add("SELECT <#NUMUNICO#>, 'Erro ao Excluir Movimentacao Bancaria "+nubco+": "+e.getMessage()+"', SYSDATE, 'Erro', "+codemp+", '' FROM DUAL");

		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}

	public void deleteTgfFin(BigDecimal nufin, BigDecimal codemp) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		try {

			jdbc.openSession();

			String sqlNota = "DELETE FROM TGFFIN WHERE NUFIN = " + nufin;

			pstmt = jdbc.getPreparedStatement(sqlNota);

			pstmt.executeUpdate();

			System.out.println("Passou do update");
		} catch (SQLException e) {
			e.printStackTrace();
			selectsParaInsertLog.add("SELECT <#NUMUNICO#>, 'Erro ao Excluir Titulo "+nufin+": "+e.getMessage()+"', SYSDATE, 'Erro', "+codemp+", '' FROM DUAL");

		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}

	public void updateNumFinByQtd(int qtdAdd) throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		try {

			jdbc.openSession();

			String sqlUpdate = "UPDATE TGFNUM SET ULTCOD = (NVL(ULTCOD, 0) + "
					+ qtdAdd + ")  WHERE ARQUIVO = 'TGFFIN'";

			pstmt = jdbc.getPreparedStatement(sqlUpdate);
			pstmt.executeUpdate();

		} catch (Exception se) {
			se.printStackTrace();
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			if (jdbc != null) {
				jdbc.closeSession();
			}
		}

	}

	public void insertFinByList(StringBuilder listInsert, BigDecimal codemp) throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		EnviromentUtils util = new EnviromentUtils();

		try {

			jdbc.openSession();

			String sqlUpdate = "INSERT INTO TGFFIN " + "        (NUFIN, "
					+ "         NUNOTA, " + "         NUMNOTA, "
					+ "         ORIGEM, " + "         RECDESP, "
					+ "         CODEMP, " + "         CODCENCUS, "
					+ "         CODNAT, " + "         CODTIPOPER, "
					+ "         DHTIPOPER, " + "         CODTIPOPERBAIXA, "
					+ "         DHTIPOPERBAIXA, " + "         CODPARC, "
					+ "         CODTIPTIT, " + "         VLRDESDOB, "
					+ "         VLRDESC, " + "         VLRBAIXA, "
					+ "         CODBCO, " + "         CODCTABCOINT, "
					+ "         DTNEG, " + "         DHMOV, "
					+ "         DTALTER, " + "         DTVENC, "
					+ "         DTPRAZO, " + "         DTVENCINIC, "
					+ "         TIPJURO, " + "         TIPMULTA, "
					+ "         HISTORICO, " + "         TIPMARCCHEQ, "
					+ "         AUTORIZADO, " + "         BLOQVAR, "
					+ "         INSSRETIDO, " + "         ISSRETIDO, "
					+ "         PROVISAO, " + "         RATEADO, "
					+ "         TIMBLOQUEADA, " + "         IRFRETIDO, "
					+ "         TIMTXADMGERALU, " + "         VLRDESCEMBUT, "
					+ "         VLRINSS, " + "         VLRIRF, "
					+ "         VLRISS, " + "         VLRJURO, "
					+ "         VLRJUROEMBUT, " + "         VLRJUROLIB, "
					+ "         VLRJURONEGOC, " + "         VLRMOEDA, "
					+ "         VLRMOEDABAIXA, " + "         VLRMULTA, "
					+ "         VLRMULTAEMBUT, " + "         VLRMULTALIB, "
					+ "         VLRMULTANEGOC, " + "         VLRPROV, "
					+ "         VLRVARCAMBIAL, " + "         VLRVENDOR, "
					+ "         ALIQICMS, " + "         BASEICMS, "
					+ "         CARTAODESC, " + "         CODMOEDA, "
					+ "         CODPROJ, " + "         CODVEICULO, "
					+ "         CODVEND, " + "         DESPCART, "
					+ "         NUMCONTRATO, " + "         ORDEMCARGA, "
					+ "         CODUSU," + "         AD_IDEXTERNO,"
					+ "         AD_IDALUNO) " + listInsert.toString();

			pstmt = jdbc.getPreparedStatement(sqlUpdate);
			pstmt.executeUpdate();

		} catch (Exception se) {
			se.printStackTrace();
			selectsParaInsertLog.add("SELECT <#NUMUNICO#>, 'Erro ao integrar financeiro, Mensagem de erro: "
					+ se.getMessage().replace("'", "\"")+"', SYSDATE, 'Erro', "+codemp+", '' FROM DUAL");

			/*try {
				util.inserirLog(
						"Erro ao integrar financeiro, Mensagem de erro: "
								+ se.getMessage(), "Erro", "", codemp);
			} catch (Exception e1) {
				e1.printStackTrace();
			}*/

		} finally {

			try {
				if (pstmt != null) {
					pstmt.close();
				}
				if (jdbc != null) {
					jdbc.closeSession();
				}
			} catch (Exception se) {
				se.printStackTrace();
			}

		}

	}

	public void insertLogList(String listInsert, BigDecimal codemp) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		try {
			jdbc.openSession();

			String sqlUpdate = "INSERT INTO AD_LOGINTEGRACAO (NUMUNICO, DESCRICAO, DTHORA, "
					+ "	STATUS, CODEMP, MATRICULA_IDFORN) " + listInsert;

			pstmt = jdbc.getPreparedStatement(sqlUpdate);
			//pstmt.setString(1, listInsert);
			pstmt.executeUpdate();
		} catch (Exception se) {
			se.printStackTrace();

		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}

}
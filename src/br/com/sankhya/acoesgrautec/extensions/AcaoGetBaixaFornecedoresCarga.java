package br.com.sankhya.acoesgrautec.extensions;

import br.com.sankhya.acoesgrautec.services.SkwServicoCompras;
import br.com.sankhya.acoesgrautec.util.EnviromentUtils;
import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.extensions.actionbutton.Registro;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.core.JapeSession;
import br.com.sankhya.jape.core.JapeSession.SessionHandle;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.cuckoo.core.ScheduledAction;
import org.cuckoo.core.ScheduledActionContext;

public class AcaoGetBaixaFornecedoresCarga implements AcaoRotinaJava, ScheduledAction {
	
	private List<String> selectsParaInsert = new ArrayList<String>();
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
		String idForn = (String) contexto.getParam("IDFORN");
		
		
		try{
			//contexto.setMensagemRetorno("Data inicio: " + dataInicio + "\nData Fim: " + dataFim);
			
			// Banco
			List<Object[]> listInfBancoConta = retornarInformacoesBancoConta();
			Map<String, BigDecimal> mapaInfBanco = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfBancoConta) {
				Long codEmpObj = (Long) obj[1];
				String idExternoObj = (String) obj[2];
				BigDecimal codBcoObj = (BigDecimal) obj[3];

				if (mapaInfBanco.get(codEmpObj + "###" + idExternoObj) == null) {
					mapaInfBanco.put(codEmpObj + "###" + idExternoObj, codBcoObj);
				}
			}

			// Conta
			Map<String, BigDecimal> mapaInfConta = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfBancoConta) {
				BigDecimal codCtabCointObj = (BigDecimal) obj[0];
				Long codEmpObj = (Long) obj[1];
				String idExternoObj = (String) obj[2];

				if (mapaInfConta.get(codEmpObj + "###" + idExternoObj) == null) {
					mapaInfConta.put(codEmpObj + "###" + idExternoObj,
							codCtabCointObj);
				}
			}

			// Financeiro
			List<Object[]> listInfFinanceiro = retornarInformacoesFinanceiro();
			Map<String, BigDecimal> mapaInfFinanceiro = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfFinanceiro) {
				BigDecimal nuFin = (BigDecimal) obj[0];
				BigDecimal codEmpObj = (BigDecimal) obj[1];
				String idExternoObj = (String) obj[2];

				if (mapaInfFinanceiro.get(codEmpObj + "###" + idExternoObj) == null) {
					mapaInfFinanceiro.put(codEmpObj + "###" + idExternoObj, nuFin);
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

			// Valor Desdobramento
			Map<BigDecimal, BigDecimal> mapaInfFinanceiroValor = new HashMap<BigDecimal, BigDecimal>();
			for (Object[] obj : listInfFinanceiro) {
				BigDecimal nuFin = (BigDecimal) obj[0];
				BigDecimal vlrDesdob = (BigDecimal) obj[4];
				if (mapaInfFinanceiroValor.get(nuFin) == null) {
					mapaInfFinanceiroValor.put(nuFin, vlrDesdob);
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

			// Tipo de Titulo
			List<Object[]> listInfTipoTitulo = retornarInformacoesTipoTitulo();
			Map<String, BigDecimal> mapaInfTipoTitulo = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfTipoTitulo) {
				BigDecimal codTipTit = (BigDecimal) obj[0];
				Long codEmpObj = (Long) obj[1];
				String idExternoObj = (String) obj[2];

				if (mapaInfTipoTitulo.get(codEmpObj + "###" + idExternoObj) == null) {
					mapaInfTipoTitulo.put(codEmpObj + "###" + idExternoObj,
							codTipTit);
				}
			}

			// Tipo de Titulo Taxa
			Map<String, BigDecimal> mapaInfTipoTituloTaxa = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfTipoTitulo) {
				BigDecimal taxa = (BigDecimal) obj[3];
				Long codEmpObj = (Long) obj[1];
				String idExternoObj = (String) obj[2];

				if (mapaInfTipoTituloTaxa.get(codEmpObj + "###" + idExternoObj) == null) {
					mapaInfTipoTituloTaxa.put(codEmpObj + "###" + idExternoObj,
							taxa);
				}
			}

			// Menor Movimenta��o Banc�ria Por Conta
			List<Object[]> listInfMenorDataMovBancariaPorConta = retornarInformacoesMenorDataMovBancariaPorConta();
			Map<Long, Date> mapaInfMenorDataMovBancariaPorConta = new HashMap<Long, Date>();
			for (Object[] obj : listInfMenorDataMovBancariaPorConta) {
				Long codCtabCointObj = (Long) obj[0];
				Date dtMinRef = (Date) obj[1];

				if (mapaInfMenorDataMovBancariaPorConta.get(codCtabCointObj) == null) {
					mapaInfMenorDataMovBancariaPorConta.put(codCtabCointObj,
							dtMinRef);
				}
			}
			

			iterarEndpoint(url, token, codEmp, mapaInfTipoTituloTaxa, mapaInfBanco,
					mapaInfConta, mapaInfFinanceiro, mapaInfTipoTitulo,
					mapaInfMenorDataMovBancariaPorConta, mapaInfFinanceiroBaixado,
					mapaInfFinanceiroValor, mapaInfFinanceiroBanco, dataInicio, dataFim, idForn);
			
			contexto.setMensagemRetorno("Periodo Processado!");
			
		}catch(Exception e){
			e.printStackTrace();
			contexto.mostraErro(e.getMessage());
		}finally{

			if(selectsParaInsert.size() > 0){
				
				StringBuilder msgError = new StringBuilder();
				
				System.out.println("Entrou na lista do finally: " + selectsParaInsert.size());
				
				//int idInicial = util.getMaxNumLog();
				
				int qtdInsert = selectsParaInsert.size();
				
				int i = 1;
				for (String sqlInsert : selectsParaInsert) {
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
		try {
			
			// Banco
			List<Object[]> listInfBancoConta = retornarInformacoesBancoConta();
			Map<String, BigDecimal> mapaInfBanco = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfBancoConta) {
				Long codEmpObj = (Long) obj[1];
				String idExternoObj = (String) obj[2];
				BigDecimal codBcoObj = (BigDecimal) obj[3];

				if (mapaInfBanco.get(codEmpObj + "###" + idExternoObj) == null) {
					mapaInfBanco.put(codEmpObj + "###" + idExternoObj,
							codBcoObj);
				}
			}

			// Conta
			Map<String, BigDecimal> mapaInfConta = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfBancoConta) {
				BigDecimal codCtabCointObj = (BigDecimal) obj[0];
				Long codEmpObj = (Long) obj[1];
				String idExternoObj = (String) obj[2];

				if (mapaInfConta.get(codEmpObj + "###" + idExternoObj) == null) {
					mapaInfConta.put(codEmpObj + "###" + idExternoObj,
							codCtabCointObj);
				}
			}

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

			// NuFin Baixados
			Map<BigDecimal, String> mapaInfFinanceiroBaixado = new HashMap<BigDecimal, String>();
			for (Object[] obj : listInfFinanceiro) {
				BigDecimal nuFin = (BigDecimal) obj[0];
				String baixado = (String) obj[3];
				if (mapaInfFinanceiroBaixado.get(nuFin) == null) {
					mapaInfFinanceiroBaixado.put(nuFin, baixado);
				}

			}

			// Valor Desdobramento
			Map<BigDecimal, BigDecimal> mapaInfFinanceiroValor = new HashMap<BigDecimal, BigDecimal>();
			for (Object[] obj : listInfFinanceiro) {
				BigDecimal nuFin = (BigDecimal) obj[0];
				BigDecimal vlrDesdob = (BigDecimal) obj[4];
				if (mapaInfFinanceiroValor.get(nuFin) == null) {
					mapaInfFinanceiroValor.put(nuFin, vlrDesdob);
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

			// Tipo de Titulo
			List<Object[]> listInfTipoTitulo = retornarInformacoesTipoTitulo();
			Map<String, BigDecimal> mapaInfTipoTitulo = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfTipoTitulo) {
				BigDecimal codTipTit = (BigDecimal) obj[0];
				Long codEmpObj = (Long) obj[1];
				String idExternoObj = (String) obj[2];

				if (mapaInfTipoTitulo.get(codEmpObj + "###" + idExternoObj) == null) {
					mapaInfTipoTitulo.put(codEmpObj + "###" + idExternoObj,
							codTipTit);
				}
			}

			// Tipo de Titulo Taxa
			Map<String, BigDecimal> mapaInfTipoTituloTaxa = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfTipoTitulo) {
				BigDecimal taxa = (BigDecimal) obj[3];
				Long codEmpObj = (Long) obj[1];
				String idExternoObj = (String) obj[2];

				if (mapaInfTipoTituloTaxa.get(codEmpObj + "###" + idExternoObj) == null) {
					mapaInfTipoTituloTaxa.put(codEmpObj + "###" + idExternoObj,
							taxa);
				}
			}

			// Menor Movimenta��o Banc�ria Por Conta
			List<Object[]> listInfMenorDataMovBancariaPorConta = retornarInformacoesMenorDataMovBancariaPorConta();
			Map<Long, Date> mapaInfMenorDataMovBancariaPorConta = new HashMap<Long, Date>();
			for (Object[] obj : listInfMenorDataMovBancariaPorConta) {
				Long codCtabCointObj = (Long) obj[0];
				Date dtMinRef = (Date) obj[1];

				if (mapaInfMenorDataMovBancariaPorConta.get(codCtabCointObj) == null) {
					mapaInfMenorDataMovBancariaPorConta.put(codCtabCointObj,
							dtMinRef);
				}
			}
			
			jdbc.openSession();

			String query = "SELECT CODEMP, URL, TOKEN FROM AD_LINKSINTEGRACAO";
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
			pstmt = jdbc.getPreparedStatement(query);

			rs = pstmt.executeQuery();
			while (rs.next()) {
				codEmp = rs.getBigDecimal("CODEMP");

				url = rs.getString("URL");
				token = rs.getString("TOKEN");

				iterarEndpoint(url, token, codEmp, mapaInfTipoTituloTaxa, mapaInfBanco,
						mapaInfConta, mapaInfFinanceiro,
						mapaInfTipoTitulo, mapaInfMenorDataMovBancariaPorConta,
						mapaInfFinanceiroBaixado, mapaInfFinanceiroValor,
						mapaInfFinanceiroBanco);
			}
		} catch (Exception e) {
			e.printStackTrace();
			try {
				insertLogIntegracao(
						"Erro ao integrar Baixas, Mensagem de erro: "
								+ e.getMessage(), "Erro");
			} catch (Exception e1) {
				e1.printStackTrace();
			}
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
			
			if(selectsParaInsert.size() > 0){
				
				StringBuilder msgError = new StringBuilder();
				
				System.out.println("Entrou na lista do finally: " + selectsParaInsert.size());
				
				//int idInicial = util.getMaxNumLog();
				
				int qtdInsert = selectsParaInsert.size();
				
				int i = 1;
				for (String sqlInsert : selectsParaInsert) {
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
				this.selectsParaInsert = new ArrayList<String>();
				
			}
		}
	}
	

	public void iterarEndpoint(String url, String token, BigDecimal codemp,
			Map<String, BigDecimal> mapaInfTipoTituloTaxa, 
			Map<String, BigDecimal> mapaInfBanco,
			Map<String, BigDecimal> mapaInfConta,
			Map<String, BigDecimal> mapaInfFinanceiro,
			Map<String, BigDecimal> mapaInfTipoTitulo,
			Map<Long, Date> mapaInfMenorDataMovBancariaPorConta,
			Map<BigDecimal, String> mapaInfFinanceiroBaixado,
			Map<BigDecimal, BigDecimal> mapaInfFinanceiroValor,
			Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco,
			String dataInicio, String dataFim, String idForn) throws Exception {

		//Date dataAtual = new Date();

		//dataFormatada = "2024-10-14";

		try {

			 // Convertendo as Strings para LocalDate
	        LocalDate inicio = LocalDate.parse(dataInicio);
	        LocalDate fim = LocalDate.parse(dataFim);

	        // Loop para percorrer o intervalo de dias
	        LocalDate atual = inicio;
	        
	        String[] response = null;
	        
			while (!atual.isAfter(fim)) {
				
				String dataAtual = atual.toString();
				//System.out.println("Data atual da itera��o: " + dataAtual);
				
				if(idForn != null && !idForn.isEmpty()){

					response = apiGet(url + "/financeiro" + "/clientes"
							+ "/titulos-pagar-baixa" + "?quantidade=0"
							+ "&dataInicial=" + dataAtual + " 00:00:00&dataFinal="
							+ dataAtual + " 23:59:59"
							+ "&fornecedor=" + idForn
							, token);

				}else{

					response = apiGet(url + "/financeiro" + "/clientes"
							+ "/titulos-pagar-baixa" + "?quantidade=0"
							+ "&dataInicial=" + dataAtual + " 00:00:00&dataFinal="
							+ dataAtual + " 23:59:59", token);

				}
				
				int status = Integer.parseInt(response[0]);

				System.out.println("Status teste: " + status);

				String responseString = response[1];
				System.out.println("response string baixas: " + responseString);

				efetuarBaixa(response, url, token, codemp,
						mapaInfTipoTituloTaxa, mapaInfBanco, mapaInfConta,
						mapaInfFinanceiro, mapaInfTipoTitulo,
						mapaInfMenorDataMovBancariaPorConta,
						mapaInfFinanceiroBaixado, mapaInfFinanceiroValor,
						mapaInfFinanceiroBanco);

				// Incrementa para o pr�ximo dia
				atual = atual.plusDays(1);
			}
	
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void iterarEndpoint(String url, String token, BigDecimal codemp,
			Map<String, BigDecimal> mapaInfTipoTituloTaxa, 
			Map<String, BigDecimal> mapaInfBanco,
			Map<String, BigDecimal> mapaInfConta,
			Map<String, BigDecimal> mapaInfFinanceiro,
			Map<String, BigDecimal> mapaInfTipoTitulo,
			Map<Long, Date> mapaInfMenorDataMovBancariaPorConta,
			Map<BigDecimal, String> mapaInfFinanceiroBaixado,
			Map<BigDecimal, BigDecimal> mapaInfFinanceiroValor,
			Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco) throws Exception {
		
		Date dataAtual = new Date();

		SimpleDateFormat formato = new SimpleDateFormat("yyyy-MM-dd");

		String dataFormatada = formato.format(dataAtual);
		
		try {

			String[] response = apiGet(url + "/financeiro" + "/clientes"
					+ "/titulos-pagar-baixa" + "?quantidade=0"
					+ "&dataInicial=" + dataFormatada + " 00:00:00&dataFinal="
					+ dataFormatada + " 23:59:59", token);

			int status = Integer.parseInt(response[0]);

			System.out.println("Status teste: " + status);

			String responseString = response[1];
			System.out.println("response string baixas: " + responseString);

			efetuarBaixa(response, url, token, codemp, mapaInfTipoTituloTaxa,
					mapaInfBanco, mapaInfConta, mapaInfFinanceiro,
					mapaInfTipoTitulo, mapaInfMenorDataMovBancariaPorConta,
					mapaInfFinanceiroBaixado, mapaInfFinanceiroValor,
					mapaInfFinanceiroBanco);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void efetuarBaixa(String[] response, String url, String token, BigDecimal codemp,
			Map<String, BigDecimal> mapaInfTipoTituloTaxa,
			Map<String, BigDecimal> mapaInfBanco,
			Map<String, BigDecimal> mapaInfConta,
			Map<String, BigDecimal> mapaInfFinanceiro,
			Map<String, BigDecimal> mapaInfTipoTitulo,
			Map<Long, Date> mapaInfMenorDataMovBancariaPorConta,
			Map<BigDecimal, String> mapaInfFinanceiroBaixado,
			Map<BigDecimal, BigDecimal> mapaInfFinanceiroValor,
			Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco)
			throws Exception {
		System.out.println("Entrou no job baixa");

		boolean movBanc = false;

		SimpleDateFormat formatoOriginal = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat formatoDesejado = new SimpleDateFormat("dd/MM/yyyy");

		Date dataAtual = new Date();

		String dataAtualFormatada = formatoOriginal.format(dataAtual);

		BigDecimal codTipTit = BigDecimal.ZERO;
		BigDecimal codBanco = BigDecimal.ZERO;
		BigDecimal codConta = BigDecimal.ZERO;
		BigDecimal nubco = BigDecimal.ZERO;
		
		BigDecimal nufin = BigDecimal.ZERO;
		
		String dataEstorno = "";

		SkwServicoCompras sc = null;

		JapeSession.SessionHandle hnd = null;

		hnd = JapeSession.open();

		System.out.println("Entrou aqui JOBBaixas");

		String domain = "http://127.0.0.1:8501";
		
		String formaDePagamento = "";
		
		int count = 0;
		
		EnviromentUtils util = new EnviromentUtils();
		
		try {

				System.out.println("Teste: " + response[1]);
				
				String response2 = response[1];
				
				if(response[0].equalsIgnoreCase("200")){
					JsonParser parser = new JsonParser();
					JsonArray jsonArray = parser.parse(response[1])
							.getAsJsonArray();

					for (JsonElement jsonElement : jsonArray) {

						JsonObject jsonObject = jsonElement.getAsJsonObject();
						
						String tituloId = jsonObject.get("titulo_id").getAsString();

						BigDecimal vlrBaixa = new BigDecimal(jsonObject.get(
								"baixa_valor").getAsString());

						BigDecimal vlrJuros = Optional.ofNullable(jsonObject.get("baixa_juros"))
							    .filter(element -> !element.isJsonNull())
							    .map(JsonElement::getAsString)
							    .map(BigDecimal::new)
							    .orElse(BigDecimal.ZERO);

						BigDecimal vlrMulta = Optional.ofNullable(jsonObject.get("baixa_multa"))
						    .filter(element -> !element.isJsonNull())
						    .map(JsonElement::getAsString)
						    .map(BigDecimal::new)
						    .orElse(BigDecimal.ZERO);
	
						BigDecimal vlrDesconto = Optional.ofNullable(jsonObject.get("baixa_desconto"))
						    .filter(element -> !element.isJsonNull())
						    .map(JsonElement::getAsString)
						    .map(BigDecimal::new)
						    .orElse(BigDecimal.ZERO);
	
						BigDecimal vlrOutrosAcrescimos = Optional.ofNullable(jsonObject.get("baixa_outros_acrescimos"))
							.filter(element -> !element.isJsonNull())
							.map(JsonElement::getAsString)
							.map(BigDecimal::new)
							.orElse(BigDecimal.ZERO);

						String dataBaixa = jsonObject.get("baixa_data")
								.getAsString();

						String baixaId = jsonObject.get("baixa_id").getAsString();

						Date data = formatoOriginal.parse(dataBaixa);

						String dataBaixaFormatada = formatoDesejado.format(data);

						nufin = mapaInfFinanceiro.get(codemp + "###" + tituloId);
						
						if (jsonObject.has("baixa_estorno_data")) {
							if (!jsonObject.get("baixa_estorno_data").isJsonNull()) {
								System.out.println("Entrou no if de estorno");

								dataEstorno = jsonObject.get("baixa_estorno_data")
										.getAsString();
							}else{
								dataEstorno = null;
							}
						}
						
						String idExterno = jsonObject.get("local_pagamento_id")
								.getAsString();
						codBanco = mapaInfBanco.get(codemp + "###" + idExterno);
						
						System.out.println("Banco: " + codBanco);
						
						codConta = mapaInfConta.get(codemp + "###" + idExterno);
						
						System.out.println("Conta: " + codConta);

						
						String[] response1 = apiGet(url + "/financeiro"
								+ "/clientes"
								+ "/titulos-pagar-baixa-forma-pagamento"
								+ "?baixa=" + baixaId, token);

						JsonParser parser2 = new JsonParser();
						JsonArray jsonArray2 = parser2.parse(response1[1])
								.getAsJsonArray();

						JsonObject jsonObject2 = jsonArray2.get(0)
								.getAsJsonObject();

						String baixaPagamentoVlr = jsonObject2.get(
								"forma_pagamento_valor").getAsString();

						String baixaFormaPagamentoId = jsonObject2.get(
								"forma_pagamento_id").getAsString();
						
						formaDePagamento = jsonObject2.get(
								"forma_pagamento_id").getAsString().trim();

						codTipTit = Optional.ofNullable(mapaInfTipoTitulo.get(codemp
								+ "###"
								+ jsonObject2.get(
										"forma_pagamento_id").getAsString())).orElse(BigDecimal.ZERO);
						
						BigDecimal taxaCartao = Optional.ofNullable(mapaInfTipoTituloTaxa.get(codemp
								+ "###"
								+ jsonObject2.get(
										"forma_pagamento_id").getAsString())).orElse(BigDecimal.ZERO);
						

						if(taxaCartao.compareTo(BigDecimal.ZERO) != 0){
							vlrBaixa = vlrBaixa.subtract( vlrBaixa.multiply(taxaCartao).divide(BigDecimal.valueOf(100)) );
						}
						
						System.out.println("estorno: " + dataEstorno);
						System.out.println("Data estorno: "
								+ jsonObject.get("baixa_estorno_data"));
						if (nufin != null && nufin.compareTo(BigDecimal.ZERO) != 0) {
							
							System.out.println("Achou nufin pra baixar: " + nufin);
							
							Date dtMinMovConta = mapaInfMenorDataMovBancariaPorConta
									.get(Long.parseLong(codConta.toString()));
							
							System.out.println("dtMinMovConta: " + dtMinMovConta);
							

							if (dataEstorno == null) {
								
								if(dtMinMovConta != null){
									
									if (data.equals(dtMinMovConta) || data.after(dtMinMovConta)) {
										
										if(codTipTit != null && codTipTit.compareTo(BigDecimal.ZERO) != 0){
											
												if ("N".equalsIgnoreCase(mapaInfFinanceiroBaixado
														.get(nufin))) {
													
													System.out.println("Chegou no update");
													
													if (vlrBaixa
															.compareTo(mapaInfFinanceiroValor
																	.get(nufin)) == 0) {
														System.out
																.println("Entrou no if do valor");
														updateFin(codTipTit, nufin, codBanco,
																codConta, vlrDesconto,
																vlrJuros, vlrMulta, vlrOutrosAcrescimos,
																codemp);
													} else {
														System.out
																.println("Entrou no else do valor");
														updateFinComVlrBaixa(codTipTit, nufin,
																codBanco, codConta, vlrBaixa,vlrDesconto,
																vlrJuros, vlrMulta, vlrOutrosAcrescimos,
																codemp);
													}
													System.out.println("vlrDesconto: "
															+ vlrDesconto);
													System.out.println("vlrJuros: " + vlrJuros);
													System.out.println("vlrMulta: " + vlrMulta);

													nubco = insertMovBancaria(codConta,
															vlrBaixa, nufin, dataBaixaFormatada, codemp);

													movBanc = true;

													System.out
															.println("Passou da mov bancaria: "
																	+ nubco);

													System.out.println("vlrBaixa: " + vlrBaixa);

													updateBaixa(nufin, nubco, vlrBaixa,
															dataBaixaFormatada, codemp);

													/*insertLogIntegracao(
															"Baixa Efetuada Com Sucesso Para o Financeiro: "
																	+ nufin, "Sucesso");*/
												} else {
													System.out.println("Financeiro " + nufin
															+ " j� baixado");
												}
											
										}else{

											selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Sem \"de para\" de Tipo de Titulo "
													+ "Configurado Para o Metodo de Pagamento: "+formaDePagamento+"' , SYSDATE, 'Aviso', "+codemp+", '' FROM DUAL");
											
										}
										
										
									} else {
										
										selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Baixa Para o Titulo "
														+ nufin
														+ " N�o Efetuada Pois a Data Minima de Movimenta��o Bancaria "
														+ "Para a Conta " + codConta
														+ " � Superior a Data de Baixa: "
														+ dataBaixaFormatada+"' , SYSDATE, 'Aviso', "+codemp+", '' FROM DUAL");
										
										
										/*util.inserirLog("Baixa Para o Titulo "
														+ nufin
														+ " N�o Efetuada Pois a Data Minima de Movimenta��o Bancaria "
														+ "Para a Conta " + codConta
														+ " � Superior a Data de Baixa: "
														+ dataBaixaFormatada, "Aviso", "", codemp);*/
									}
									
								}else{

									selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Data Minima de Inje��o de Saldo N�o Localizada Para a Conta: "+codConta+"' , SYSDATE, 'Aviso', "+codemp+", '' FROM DUAL");
									
								}
								
							} else if ("S".equalsIgnoreCase(mapaInfFinanceiroBaixado
									.get(nufin))) {
								nubco = mapaInfFinanceiroBanco.get(nufin);
								updateFinExtorno(nufin, codemp);
								deleteTgfMbc(nubco, codemp);
								deleteTgfFin(nufin, codemp);
								/*insertLogIntegracao(
										"Estorno Efetuado com sucesso",
										"Sucesso");*/
							}
							
						} else {
							System.out
									.println("N�o foi possivel encontrar financeiro com id externo "
											+ tituloId);
						}
					}
				}else{
					
					selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Resposta da API invalida ou vazia: codigo: " + response[0] 
							+ "\nResposta: " + response[1]+"' , SYSDATE, 'Erro', "+codemp+", '' FROM DUAL");
					
					/*util.inserirLog("Resposta da API invalida ou vazia: codigo: " + response[0] 
							+ "\nResposta: " + response[1], "Aviso", "", codemp);*/
				}
				
			
		} catch (Exception e) {
			e.printStackTrace();
			if (movBanc) {
				updateFinExtorno(nufin, codemp);
				deleteTgfMbc(nubco, codemp);
				System.out.println("Apagou mov bank");
			}
			try {
				
				selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Mensagem de erro nas Baixas: " + e.getMessage()+"' , SYSDATE, 'Erro', "+codemp+", '' FROM DUAL");
				
				/*util.inserirLog(
						"Mensagem de erro nas Baixas: " + e.getMessage(),
						"Erro", "", codemp);*/
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
	}

	public String[] apiGet(String ur, String token) throws Exception {

		BufferedReader reader;
		String line;
		StringBuilder responseContent = new StringBuilder();
		// String key = preferenciaSenha();

		// Preparando a requisi��o
		URL obj = new URL(ur);
		HttpURLConnection https = (HttpURLConnection) obj.openConnection();

		System.out.println("Entrou na API");
		System.out.println("URL: " + ur);
		System.out.println("https: " + https);
		System.out.println("token: " + token);

		https.setRequestMethod("GET");
		// https.setConnectTimeout(50000);
		https.setRequestProperty("User-Agent",
				"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0)");
		https.setRequestProperty("Content-Type",
				"application/json; charset=UTF-8");
		https.setRequestProperty("Authorization", "Bearer " + token);
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
		
		System.out.println("response teste: " + response);
		
		https.disconnect();

		return new String[] { Integer.toString(status), response };

	}

	public BigDecimal getNufin(String idTitulo) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal id = BigDecimal.ZERO;
		try {
			jdbc.openSession();

			String sqlNota = "SELECT NUFIN FROM TGFFIN WHERE AD_IDEXTERNO = ?";

			pstmt = jdbc.getPreparedStatement(sqlNota);
			pstmt.setString(1, idTitulo.trim());

			rs = pstmt.executeQuery();
			if (rs.next()) {
				id = rs.getBigDecimal("NUFIN");
				if (id == null) {
					id = BigDecimal.ZERO;
				}
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
		return id;
	}

	public BigDecimal getNubco(BigDecimal nufin) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal id = BigDecimal.ZERO;
		try {
			jdbc.openSession();

			String sqlNota = "SELECT NUBCO FROM TGFFIN WHERE NUFIN = " + nufin;

			pstmt = jdbc.getPreparedStatement(sqlNota);

			rs = pstmt.executeQuery();
			if (rs.next()) {
				id = rs.getBigDecimal("NUBCO");
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
		return id;
	}

	public boolean validarBaixa(BigDecimal nufin) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		int count = 0;
		try {
			jdbc.openSession();

			String sqlNota = "SELECT COUNT(0) AS COUNT FROM TGFFIN WHERE NUFIN = ? AND DHBAIXA IS NOT NULL AND VLRBAIXA IS NOT NULL AND CODUSUBAIXA IS NOT NULL";

			pstmt = jdbc.getPreparedStatement(sqlNota);
			pstmt.setBigDecimal(1, nufin);

			rs = pstmt.executeQuery();
			if (rs.next()) {
				count = rs.getInt("COUNT");
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
		if (count > 0) {
			return false;
		}
		return true;
	}

	public boolean validarDataMinMovBancaria(BigDecimal codConta,
			String dataBaixa) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		int count = 0;
		try {
			jdbc.openSession();

			String sqlNota = "SELECT COUNT(0) AS COUNT FROM (SELECT MIN(REFERENCIA) DTREF \t    FROM TGFSBC \t   WHERE CODCTABCOINT = "
					+

					codConta
					+ ") "
					+ "\tWHERE DTREF > TO_DATE('"
					+ dataBaixa
					+ "', 'DD/MM/YYYY')";

			pstmt = jdbc.getPreparedStatement(sqlNota);

			rs = pstmt.executeQuery();
			if (rs.next()) {
				count = rs.getInt("COUNT");
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
		if (count > 0) {
			return false;
		}
		return true;
	}

	public BigDecimal getTipTit(String idExterno, BigDecimal codEmp)
			throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal id = BigDecimal.ZERO;
		try {
			jdbc.openSession();

			String sqlNota = "SELECT CODTIPTIT FROM AD_TIPTITINTEGRACAO WHERE CODEMP = ? AND IDEXTERNO = ?";

			pstmt = jdbc.getPreparedStatement(sqlNota);
			pstmt.setBigDecimal(1, codEmp);
			pstmt.setString(2, idExterno.trim());

			rs = pstmt.executeQuery();
			if (rs.next()) {
				id = rs.getBigDecimal("CODTIPTIT");
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
		return id;
	}

	public BigDecimal getCodBanco(String idExterno, BigDecimal codEmp)
			throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal id = BigDecimal.ZERO;
		try {
			jdbc.openSession();

			String sqlNota = "select CODBCO from ad_infobankbaixa WHERE CODEMP = ? AND IDEXTERNO = ?";

			pstmt = jdbc.getPreparedStatement(sqlNota);
			pstmt.setBigDecimal(1, codEmp);
			pstmt.setString(2, idExterno.trim());

			rs = pstmt.executeQuery();
			if (rs.next()) {
				id = rs.getBigDecimal("CODBCO");
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
		return id;
	}

	public BigDecimal getCodConta(String idExterno, BigDecimal codEmp)
			throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal id = BigDecimal.ZERO;
		try {
			jdbc.openSession();

			String sqlNota = "select CODCTABCOINT from ad_infobankbaixa WHERE CODEMP = ? AND IDEXTERNO = ?";

			pstmt = jdbc.getPreparedStatement(sqlNota);
			pstmt.setBigDecimal(1, codEmp);
			pstmt.setString(2, idExterno.trim());

			rs = pstmt.executeQuery();
			if (rs.next()) {
				id = rs.getBigDecimal("CODCTABCOINT");
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
		return id;
	}

	public BigDecimal getVlrDesdob(BigDecimal nufin) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal vlrDesdob = BigDecimal.ZERO;
		try {
			jdbc.openSession();

			String sqlNota = "select VLRDESDOB FROM TGFFIN WHERE NUFIN = "
					+ nufin;

			pstmt = jdbc.getPreparedStatement(sqlNota);

			rs = pstmt.executeQuery();
			if (rs.next()) {
				vlrDesdob = rs.getBigDecimal("VLRDESDOB");
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
		return vlrDesdob;
	}

	public void updateFin(BigDecimal codtiptit, BigDecimal nufin,
			BigDecimal codBanco, BigDecimal codConta, BigDecimal vlrDesconto,
			BigDecimal vlrJuros, BigDecimal vlrMulta,
			BigDecimal vlrOutrosAcrescimos, BigDecimal codemp) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		try {

			jdbc.openSession();

			String sqlNota = "UPDATE TGFFIN SET " + "CODTIPTIT = ?, "
					+ "CODBCO = ?, " + "CODCTABCOINT = ?, ";
					
						sqlNota += "AD_VLRDESCINT = "+vlrDesconto+", ";
						sqlNota += "VLRINSS = 0, " + "VLRIRF = 0, " + "VLRISS = 0, ";
						sqlNota += "AD_VLRMULTAINT = "+vlrMulta+", ";
						sqlNota += "AD_VLRJUROSINT = "+vlrJuros+", AD_OUTACRESCIMOS = " + vlrOutrosAcrescimos;
						sqlNota += ", TIPJURO = null, ";
						sqlNota += "TIPMULTA = null";
					
					sqlNota += " WHERE nufin = ?";

			pstmt = jdbc.getPreparedStatement(sqlNota);
			pstmt.setBigDecimal(1, codtiptit);
			pstmt.setBigDecimal(2, codBanco);
			pstmt.setBigDecimal(3, codConta);
			pstmt.setBigDecimal(4, nufin);

			pstmt.executeUpdate();

			System.out.println("Passou do update");
		} catch (SQLException e) {
			e.printStackTrace();
			selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro Ao Atualizar Financeiro Para baixa: "+e.getMessage()+"' , SYSDATE, 'Erro', "+codemp+", '' FROM DUAL");
			
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

			selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro Ao Deletar Mov. Bancaria: "+e.getMessage()+"' , SYSDATE, 'Erro', "+codemp+", '' FROM DUAL");
			
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

			selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro Ao Excluir Titulo: "+e.getMessage()+"' , SYSDATE, 'Erro', "+codemp+", '' FROM DUAL");
			
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}

	public void updateFinExtorno(BigDecimal nufin, BigDecimal codemp) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		try {
			jdbc.openSession();

			String sqlNota = "UPDATE TGFFIN SET VLRBAIXA = 0, DHBAIXA = NULL, NUBCO = NULL, CODTIPOPERBAIXA = 0, DHTIPOPERBAIXA = (SELECT MAX(DHALTER) FROM TGFTOP WHERE CODTIPOPER = 0), CODUSUBAIXA = NULL  WHERE NUFIN = "
					+

					nufin;

			pstmt = jdbc.getPreparedStatement(sqlNota);

			pstmt.executeUpdate();

			System.out.println("Passou do update");
		} catch (SQLException e) {
			e.printStackTrace();

			selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro Ao Extornar Titulo: "+e.getMessage()+"' , SYSDATE, 'Erro', "+codemp+", '' FROM DUAL");
			
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}

	public void updateFinComVlrBaixa(BigDecimal codtiptit, BigDecimal nufin,
			BigDecimal codBanco, BigDecimal codConta, BigDecimal vlrBaixa,
			BigDecimal vlrDesconto, BigDecimal vlrJuros, BigDecimal vlrMulta,
			BigDecimal outrosAcrescimos, BigDecimal codemp)
			throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		try {
			jdbc.openSession();

			String sqlNota = "UPDATE TGFFIN SET CODTIPTIT = ?, CODBCO = ?, CODCTABCOINT = ?, AD_VLRDESCINT = "+vlrDesconto+", VLRINSS = 0, VLRIRF = 0, VLRISS = 0, AD_VLRJUROSINT = "+vlrJuros+", AD_VLRMULTAINT = "+vlrMulta+", TIPJURO = null, AD_VLRORIG = VLRDESDOB, VLRDESDOB = "
					+ vlrBaixa + "," + "TIPMULTA = null, AD_OUTACRESCIMOS = "+outrosAcrescimos+" WHERE nufin = ?";

			pstmt = jdbc.getPreparedStatement(sqlNota);
			pstmt.setBigDecimal(1, codtiptit);
			pstmt.setBigDecimal(2, codBanco);
			pstmt.setBigDecimal(3, codConta);
			pstmt.setBigDecimal(4, nufin);

			pstmt.executeUpdate();

			System.out.println("Passou do update");
		} catch (SQLException e) {
			e.printStackTrace();

			selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro Ao Atualizar Titulo Para Baixa: "+e.getMessage()+"' , SYSDATE, 'Erro', "+codemp+", '' FROM DUAL");
			
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}

	public void updateBaixa(BigDecimal nufin, BigDecimal nubco,
			BigDecimal vlrDesdob, String dataBaixaFormatada,
			BigDecimal codemp) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		try {
			jdbc.openSession();

			String sqlNota = "UPDATE TGFFIN SET VLRBAIXA = "
					+

					vlrDesdob
					+ ", "
					+ "DHBAIXA = '"
					+ dataBaixaFormatada
					+ "', "
					+ "NUBCO = "
					+ nubco
					+ ", "
					+ "CODTIPOPERBAIXA = 1500, "
					+ "DHTIPOPERBAIXA = (SELECT MAX(DHALTER) FROM TGFTOP WHERE CODTIPOPER = 1500), "
					+ "CODUSUBAIXA = 0  " + "WHERE NUFIN = " + nufin;

			pstmt = jdbc.getPreparedStatement(sqlNota);

			pstmt.executeUpdate();

			System.out.println("Passou do update");
		} catch (SQLException e) {
			e.printStackTrace();

			selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro Ao Baixar Titulo: "+e.getMessage()+"' , SYSDATE, 'Erro', "+codemp+", '' FROM DUAL");
			
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}

	public void insertLogIntegracao(String descricao, String status)
			throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		jdbc.openSession();

		String sqlUpdate = "INSERT INTO AD_LOGINTEGRACAO (NUMUNICO, DESCRICAO, DTHORA, STATUS)VALUES (((SELECT NVL(MAX(NUMUNICO), 0) + 1 FROM AD_LOGINTEGRACAO)), ?, SYSDATE, ?)";

		pstmt = jdbc.getPreparedStatement(sqlUpdate);
		pstmt.setString(1, descricao);
		pstmt.setString(2, status);
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

	public BigDecimal insertMovBancaria(BigDecimal contaBancaria,
			BigDecimal vlrDesdob, BigDecimal nufin, String dataBaixaFormatada, 
			BigDecimal codemp)
			throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		
		EnviromentUtils util = new EnviromentUtils();
		
		BigDecimal nubco = util.getMaxNumMbc();

		jdbc.openSession();
		
		try{
			
			String sqlUpdate = "INSERT INTO TGFMBC (NUBCO, CODLANC, DTLANC, CODTIPOPER, DHTIPOPER, DTCONTAB, HISTORICO, CODCTABCOINT, NUMDOC, VLRLANC, TALAO, PREDATA, CONCILIADO, DHCONCILIACAO, ORIGMOV, NUMTRANSF, RECDESP, DTALTER, DTINCLUSAO, CODUSU, VLRMOEDA, SALDO, CODCTABCOCONTRA, NUBCOCP, CODPDV )  VALUES ("
					+
					nubco
					+ ", "
					+ "1, "
					+ "'"
					+ dataBaixaFormatada
					+ "'"
					+ ", "
					+ "1500, "
					+ "(SELECT MAX(DHALTER) FROM TGFTOP WHERE CODTIPOPER = 1500), "
					+ "NULL, "
					+ "(SELECT HISTORICO FROM TGFFIN WHERE NUFIN = "
					+ nufin
					+ "), "
					+

					contaBancaria
					+ ", "
					+ "0, "
					+

					vlrDesdob
					+ ", "
					+ "NULL, "
					+ "'"
					+ dataBaixaFormatada
					+ "', "
					+ "'N', "
					+ "NULL, "
					+ "'F', "
					+ "NULL, "
					+ "1, "
					+ "SYSDATE, "
					+ "SYSDATE, "
					+ "0, "
					+ "0, "
					+ vlrDesdob
					+ ", "
					+ "NULL,  "
					+ "NULL, " + "NULL) ";

			pstmt = jdbc.getPreparedStatement(sqlUpdate);
			pstmt.executeUpdate();
			
		}catch(Exception e){
			e.printStackTrace();

			selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro Ao Inserir Mov. Bancaria: "+e.getMessage().replace("'", "\"")+"' , SYSDATE, 'Erro', "+codemp+", '' FROM DUAL");
			
		}finally{
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
		
		return nubco;
	}

	public BigDecimal getMaxNumMbc() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal id = BigDecimal.ZERO;
		try {
			updateNumMbc();

			jdbc.openSession();

			String sqlNota = "SELECT MAX(ULTCOD) AS ULTCOD FROM TGFNUM WHERE ARQUIVO = 'TGFMBC'";

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

	public void updateNumMbc() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		jdbc.openSession();

		String sqlUpdate = "UPDATE TGFNUM SET ULTCOD = NVL(ULTCOD, 0) + 1  WHERE ARQUIVO = 'TGFMBC'";

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
			throw se;
		}
	}
	
	public void updateFlagTituloProcessado(String idTitulo) throws Exception {
		
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		
		try {
			
			jdbc.openSession();
			
			String sqlUpdate = "UPDATE TGFFIN SET AD_PROCESSADO = 'S' WHERE AD_IDEXTERNO = '" + idTitulo + "'";
			
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
	
	public void updateResetarTitulo(BigDecimal codEmp) throws Exception {
		
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		
		try {
			
			jdbc.openSession();
			
			String sqlUpdate = "UPDATE TGFFIN SET AD_PROCESSADO = 'N' WHERE AD_IDEXTERNO IS NOT NULL AND DHBAIXA IS NULL AND CODEMP = " + codEmp + " AND AD_IDALUNO IS NULL";
			
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
	
	public List<Object[]> retornarInformacoesBancoConta() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		List<Object[]> listRet = new ArrayList<>();
		try {
			jdbc.openSession();
			String sql = "	SELECT 	CODCTABCOINT, CODEMP, IDEXTERNO, CODBCO ";
			sql += "		FROM  	ad_infobankbaixa ";
			pstmt = jdbc.getPreparedStatement(sql);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				Object[] ret = new Object[4];
				ret[0] = rs.getBigDecimal("CODCTABCOINT");
				ret[1] = rs.getLong("CODEMP");
				ret[2] = rs.getString("IDEXTERNO");
				ret[3] = rs.getBigDecimal("CODBCO");

				listRet.add(ret);
			}

		} catch (SQLException e) {
			e.printStackTrace();
			throw new Exception("Erro Ao Executar Metodo retornarInformacoesBancoConta");
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
	

	public List<Object[]> retornarInformacoesTipoTitulo() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		List<Object[]> listRet = new ArrayList<>();
		try {
			jdbc.openSession();
			String sql = "	SELECT 	CODEMP, CODTIPTIT, IDEXTERNO, TAXACART ";
			sql += "		FROM  	AD_TIPTITINTEGRACAO ";
			pstmt = jdbc.getPreparedStatement(sql);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				Object[] ret = new Object[4];
				ret[0] = rs.getBigDecimal("CODTIPTIT");
				ret[1] = rs.getLong("CODEMP");
				ret[2] = rs.getString("IDEXTERNO");
				ret[3] = rs.getBigDecimal("TAXACART");

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
	

	public List<Object[]> retornarInformacoesMenorDataMovBancariaPorConta()
			throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		List<Object[]> listRet = new ArrayList<>();
		try {
			jdbc.openSession();
			String sql = "	SELECT 	CODCTABCOINT, MIN(REFERENCIA) DTREF ";
			sql += "		FROM  	TGFSBC ";
			sql += "	    GROUP BY CODCTABCOINT ";
			pstmt = jdbc.getPreparedStatement(sql);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				Object[] ret = new Object[2];
				ret[0] = rs.getLong("CODCTABCOINT");
				ret[1] = rs.getDate("DTREF");

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

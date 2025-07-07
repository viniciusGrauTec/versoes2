package br.com.sankhya.acoesgrautec.jobs;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.cuckoo.core.ScheduledAction;
import org.cuckoo.core.ScheduledActionContext;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

public class JobGetTituloFornecedor implements ScheduledAction{

	@Override
	public void onTime(ScheduledActionContext arg0) {
		// TODO Auto-generated method stub
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal codEmp = BigDecimal.ZERO;

		String url = "";
		String token = "";

		
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
					mapaInfFinanceiro.put(codEmpObj + "###" + idExternoObj, nuFin);
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

				if (mapaInfCenCus.get(idExterno) == null) {
					mapaInfCenCus.put(idExterno, codCenCus);
				}
			}

			// Natureza
			List<Object[]> listInfNatureza = retornarInformacoesNatureza();
			Map<String, BigDecimal> mapaInfNatureza = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfNatureza) {
				BigDecimal natureza = (BigDecimal) obj[0];
				String idExternoObj = (String) obj[1];

				if (mapaInfNatureza.get(idExternoObj) == null) {
					mapaInfNatureza.put(idExternoObj, natureza);
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
			
			String query = "SELECT CODEMP, URL, TOKEN FROM AD_LINKSINTEGRACAO";
//			String query3 = "SELECT CODEMP, URL, TOKEN FROM AD_LINKSINTEGRACAO WHERE CODEMP = 3";
//			String query4 = "SELECT CODEMP, URL, TOKEN FROM AD_LINKSINTEGRACAO WHERE CODEMP = 4";

			pstmt = jdbc.getPreparedStatement(query);

			rs = pstmt.executeQuery();

			while (rs.next()) {
				
				codEmp = rs.getBigDecimal("CODEMP");
				url = rs.getString("URL");
				token = rs.getString("TOKEN");
				iterarEndpoint(mapaInfFinanceiroBanco, mapaInfFinanceiroBaixado, mapaInfNatureza, mapaInfBanco, mapaInfConta, mapaInfFinanceiro, mapaInfCenCus, mapaInfParceiros, url, token, codEmp);
				
			}

		} catch (Exception e) {
			e.printStackTrace();
			try {
				insertLogIntegracao("Erro ao integrar financeiro, Mensagem de erro: "+ e.getMessage(), "Erro");
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
			System.out.println("Finalizou a JobGetTituloFornecedor");
		}
	}
	
	public void insertLogIntegracao(String descricao, String status)
			throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		String descFormatada = descricao;
		
		jdbc.openSession();
		
		if(descricao.length() > 4000){
			
		descFormatada = descricao.substring(0,4000);
		}else{
			descFormatada = descricao;
		}

		String sqlUpdate = "INSERT INTO AD_LOGINTEGRACAO (NUMUNICO, DESCRICAO, DTHORA, STATUS)"
				+ "VALUES (((SELECT NVL(MAX(NUMUNICO), 0) + 1 FROM AD_LOGINTEGRACAO)), ?, SYSDATE, ?)";

		pstmt = jdbc.getPreparedStatement(sqlUpdate);
		pstmt.setString(1, descFormatada);
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
	
	public String[] apiGet(String ur, String token) throws Exception {

		BufferedReader reader;
		String line;
		StringBuilder responseContent = new StringBuilder();
		// String key = preferenciaSenha();

		// Preparando a requisiï¿½ï¿½o
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
		https.setRequestProperty("Authorization", "Bearer "	+ token);
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
	
	public void iterarEndpoint(Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco,
			Map<BigDecimal, String> mapaInfFinanceiroBaixado,
			Map<String, BigDecimal> mapaInfNatureza,
			Map<String, BigDecimal> mapaInfBanco,
			Map<String, BigDecimal> mapaInfConta,
			Map<String, BigDecimal> mapaInfFinanceiro,
			Map<String, BigDecimal> mapaInfCenCus, 
			Map<String, BigDecimal> mapaInfParceiros, 
			String url, String token,
			BigDecimal codEmp) throws Exception {
		// int pagina = 1;

		Date dataAtual = new Date();

		SimpleDateFormat formato = new SimpleDateFormat("yyyy-MM-dd");

		String dataFormatada = formato.format(dataAtual);
		//dataFormatada = "2024-10-14";

		int paginaInicio = 1;
		int paginaFim = 2;
		/*
		 * int paginaAtual = getPagina();
		 * 
		 * if(paginaAtual == 0){ paginaInicio = 1; paginaFim = 30; }else{
		 * paginaInicio = paginaAtual; paginaFim = paginaAtual + 30; }
		 */
		
		try {
			// for (;;)
			// {
			System.out.println("While de iteração");

			String [] response = apiGet(url+ "/financeiro/clientes/titulos-pagar?quantidade=0"
					+"&dataInicial="+dataFormatada+" 00:00:00"
					+ "&dataFinal="+dataFormatada+" 23:59:59"
					,token);

			int status = Integer.parseInt(response[0]);

			System.out.println("Status teste: " + status);
			System.out.println("pagina: " + paginaInicio);

			String responseString = response[1];
			System.out.println("response string alunos: " + responseString);

			// if ((responseString.equals("[]")) || (paginaInicio == paginaFim))
			// {
			// System.out.println("Entrou no if da quebra");
			// /* if(responseString.equals("[]")){
			// insertUltPagina(paginaInicio);
			// }else{
			// insertUltPagina(paginaFim);
			// }*/
			//
			// break;
			// }
			leituraJSON(mapaInfFinanceiroBanco, mapaInfFinanceiroBaixado, 
					mapaInfNatureza, mapaInfBanco, mapaInfConta, 
					mapaInfFinanceiro, mapaInfCenCus, mapaInfParceiros, 
					response, url, token, codEmp);
			// paginaInicio++;
		}
		// }
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void leituraJSON(Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco,
			Map<BigDecimal, String> mapaInfFinanceiroBaixado,
			Map<String, BigDecimal> mapaInfNatureza,
			Map<String, BigDecimal> mapaInfBanco,
			Map<String, BigDecimal> mapaInfConta,
			Map<String, BigDecimal> mapaInfFinanceiro,
			Map<String, BigDecimal> mapaInfCenCus, 
			Map<String, BigDecimal> mapaInfParceiros, 
			String [] response, String url,String token, BigDecimal codemp) throws Exception {
		System.out.println("Inicio leitura do JSON - JobGetTituloFornecedor");
		
//		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
//		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
//		PreparedStatement pstmt = null;
//		ResultSet rs = null;
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
		
		try {
			// jdbc.openSession();
			// /*,ad_idfornecedor*/
			// String sqlP =
			// "select * from AD_IDFORNACAD where NVL(INTEGRADOFIN, 'N') = 'N' AND CODEMP = "+codemp+" AND ROWNUM <= 300";
			//
			// pstmt = jdbc.getPreparedStatement(sqlP);
			//
			// rs = pstmt.executeQuery();

			// while (rs.next()) {

			JsonParser parser = new JsonParser();

			JsonArray jsonArray = parser.parse(response[1]).getAsJsonArray();

			if (response[0].equalsIgnoreCase("200")) {
				System.out.println("API response code: " + response[0]);

				int count = 0;
				int total = jsonArray.size();
				int qtdInsert = 0;

				List<String> selectsParaInsert = new ArrayList<String>();

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
					// String taxaDescricao =
					// JSON.get("taxa_descricao").getAsString();
					// String taxaCategoria =
					// JSON.get("titulo_categoria").getAsString();
					// String taxaCategoriaDesc =
					// JSON.get("taxa_categoria_descricao").getAsString();

					String dtVenc = JSON.get("titulo_vencimento").getAsString();

					String vlrDesdob = JSON.get("titulo_valor").getAsString();

					// String tituloMesRef =
					// JSON.get("titulo_mes_ref").getAsString();
					// String tituloAnoRef =
					// JSON.get("titulo_ano_ref").getAsString();
					String tituloSituacao = "";// =
												// JSON.get("titulo_situacao").getAsString();

					if (!JSON.get("titulo_situacao").isJsonNull()) {

						tituloSituacao = JSON.get("titulo_situacao")
								.getAsString();
					}

					String tituloObservacao = "";
					if (!JSON.get("titulo_observacao").isJsonNull()) {

						tituloObservacao = JSON.get("titulo_observacao")
								.getAsString();
					}

					System.out.println("teste campo curso ir: "
							+ JSON.get("curso_id").isJsonNull());
					String cursoId = "";
					if (!JSON.get("curso_id").isJsonNull()) {

						cursoId = JSON.get("curso_id").getAsString();
					}

					String dtPedido = JSON.get("data_atualizacao")
							.getAsString();

					System.out.println("Leu todos os campo de um JSON");
					String recDesp = "";
					BigDecimal codCenCus = BigDecimal.ZERO;

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

					System.out.println("Formatou as datas");

					codCenCus = Optional.ofNullable(mapaInfCenCus.get(taxaId))
							.orElse(BigDecimal.ZERO);

					System.out.println("CodCenCus: " + codCenCus);
					System.out.println("Taxa id: " + taxaId);
					System.out.println("CodParc: " + codparc);

					if (!tituloSituacao.equalsIgnoreCase("X")) {

						if (codparc.compareTo(BigDecimal.ZERO) != 0
								&& codCenCus.compareTo(BigDecimal.ZERO) != 0) {
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

								BigDecimal natureza = Optional.ofNullable(
										mapaInfNatureza.get(taxaId)).orElse(
										BigDecimal.ZERO);

								/*
								 * codConta = new BigDecimal("1"); codBanco =
								 * new BigDecimal("341");
								 */

								recDesp = "-1";

								/*
								 * if(getRecDesp(taxaId)){ recDesp = "-1";
								 * }else{ recDesp = "1"; }
								 */

								BigDecimal vlrDesdobBigDecimal = new BigDecimal(
										vlrDesdob);

								if (natureza.compareTo(BigDecimal.ZERO) != 0) {
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

									// BigDecimal nufin =
									// insertFinanceiro(codemp, /* codemp */
									// codCenCus, /* codCenCus */
									// getNatureza(taxaId), /* codNat */
									// BigDecimal.valueOf(1300), /* codTipOper
									// */
									// codparc, /* codparc */
									// BigDecimal.valueOf(4), /* codtiptit */
									// vlrDesdobBigDecimal, /* vlrDesdob */
									// dataVencFormatada, /* dtvenc */
									// // "25/11/2023", /* dtvenc */
									// dtPedidoFormatado, /* dtPedido */
									// // "22/11/2023", /* dtPedido */
									// idFin, "", codConta, codBanco, recDesp,
									// tituloObservacao);
									System.out.println("Financeiro cadastrado");
									/*
									 * insertLogIntegracao(
									 * "Financeiro com Id Externo: " + idFin +
									 * " Criado Com Sucesso, numero unico interno: "
									 * + nufin, "Sucesso");
									 */

								}

							} else {
								System.out.println("Financeiro " + idFin
										+ " ja cadastrado para o parceiro: "
										+ codparc);
							}

						} else {
							System.out.println("Erro de de para/parceiro");
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
								updateFinExtorno(validarNufin);
								deleteTgfMbc(nubco);
								deleteTgfFin(validarNufin);

							} else {
								deleteTgfFin(validarNufin);
							}
						}

					}

					// insertFinanceiro(codemp);
					count++;
				}

				// Apenas se encontrar registro elegível
				if (qtdInsert > 0) {
					// Capturar o tgfnum
					BigDecimal nuFinInicial = getMaxNumFin();

					// Atualizar o nufin adicionando a quantidade de lista
					updateNumFinByQtd(qtdInsert);
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

					System.out.println("Consulta Após Tratamento: "
							+ sqlInsertFin);

					// gravar o financeiro
					insertFinByList(sqlInsertFin);

					// updateFlagAlunoIntegrado(aluno);
				}
			} else {
				// insertLogIntegracao("Response Code Diferente de 200","Response Code: "
				// + response[0]);
			}

			// updateFlagFornIntegrado(idFornecedor, codemp);

			/*
			 * }
			 * 
			 * if(count == 0){ updateResetarForn(); }
			 */

		}catch(Exception e){
			
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			e.printStackTrace(pw);
			String stackTraceAsString = sw.toString();
			insertLogIntegracao("Ocorreu um erro na funï¿½ï¿½o LeituraJSON(); "+ stackTraceAsString,"erro");
			e.printStackTrace();
		}
		finally{
			
//			if(rs != null){
//				rs.close();
//			}
//			if(pstmt != null){
//				pstmt.close();
//			}
//			jdbc.closeSession();
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
			String sql = "SELECT CODCENCUS, IDEXTERNO FROM AD_NATACAD WHERE IDEXTERNO IS NOT NULL";

			pstmt = jdbc.getPreparedStatement(sql);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				Object[] ret = new Object[2];
				ret[0] = rs.getBigDecimal("CODCENCUS");
				ret[1] = rs.getString("IDEXTERNO");

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
			String sql = "SELECT CODNAT, IDEXTERNO FROM AD_NATACAD";

			pstmt = jdbc.getPreparedStatement(sql);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				Object[] ret = new Object[2];
				ret[0] = rs.getBigDecimal("CODNAT");
				ret[1] = rs.getString("IDEXTERNO");

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
	

	public void updateFinExtorno(BigDecimal nufin) throws Exception {
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
			e.printStackTrace();
			throw e;
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}
	

	public void deleteTgfMbc(BigDecimal nubco) throws Exception {
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
			throw e;
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}
	
	public void deleteTgfFin(BigDecimal nufin) throws Exception {
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
			throw e;
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
	

	public void insertFinByList(StringBuilder listInsert) throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

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
			try {
				insertLogIntegracao(
						"Erro ao integrar financeiro, Mensagem de erro: "
								+ se.getMessage(), "Erro");
			} catch (Exception e1) {
				e1.printStackTrace();
			}
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

}
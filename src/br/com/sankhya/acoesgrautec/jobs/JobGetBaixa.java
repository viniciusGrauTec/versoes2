package br.com.sankhya.acoesgrautec.jobs;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.cuckoo.core.ScheduledAction;
import org.cuckoo.core.ScheduledActionContext;

import br.com.sankhya.acoesgrautec.services.SkwServicoCompras;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.core.JapeSession;
import br.com.sankhya.jape.core.JapeSession.SessionHandle;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.modelcore.util.DynamicEntityNames;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class JobGetBaixa implements ScheduledAction {

	@Override
	public void onTime(ScheduledActionContext arg0) {

		
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		 BigDecimal codEmp = BigDecimal.ZERO;
		 BigDecimal idCarga = BigDecimal.ZERO;
		 
		 String url = "";
		 String token = "";
		 String matricula = "";
		 
		 int count = 0;
		 System.out.println("Iniciou baixa carga empresa 4");
		try {

			jdbc.openSession();

//			String query3 = "SELECT CODEMP, URL, TOKEN FROM AD_LINKSINTEGRACAO WHERE CODEMP = 3";
			String query3 = "SELECT LINK.CODEMP, URL, TOKEN, IDCARGA, MATRICULA FROM AD_LINKSINTEGRACAO LINK INNER JOIN AD_CARGAALUNOS CARGA ON CARGA.CODEMP = LINK.CODEMP WHERE LINK.CODEMP = 3 AND NVL(CARGA.INTEGRADO_BAIXA, 'N') = 'N'";
			String query4 = "SELECT LINK.CODEMP, URL, TOKEN, IDCARGA, MATRICULA FROM AD_LINKSINTEGRACAO LINK INNER JOIN AD_CARGAALUNOS CARGA ON CARGA.CODEMP = LINK.CODEMP WHERE LINK.CODEMP = 4 AND NVL(CARGA.INTEGRADO_BAIXA, 'N') = 'N'";

			pstmt = jdbc.getPreparedStatement(query4);

			rs = pstmt.executeQuery();

			while (rs.next()) {
				count++;
				
				codEmp = rs.getBigDecimal("CODEMP");
			    idCarga = rs.getBigDecimal("IDCARGA");
			        
			    url = rs.getString("URL");
			    token = rs.getString("TOKEN");
			    matricula = rs.getString("MATRICULA");

			    iterarEndpoint(url, token, codEmp, matricula);
				updateCarga(idCarga);
			}
			System.out.println("Chegou ao final da baixa carga empresa 4");
			
			/*if(count == 0){
				resetCarga(codEmp);
			}*/

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
		}
	}
	
	public void iterarEndpoint(String url, String token, BigDecimal codEmp,
			String matricula) throws Exception {
		// int pagina = 1;

		Date dataAtual = new Date();

		SimpleDateFormat formato = new SimpleDateFormat("yyyy-MM-dd");

		String dataFormatada = formato.format(dataAtual);

		int paginaInicio = 1;
		int paginaFim = 30;
		/*
		 * int paginaAtual = getPagina();
		 * 
		 * if(paginaAtual == 0){ paginaInicio = 1; paginaFim = 30; }else{
		 * paginaInicio = paginaAtual; paginaFim = paginaAtual + 30; }
		 */

		try {
			for (;;) {
				//System.out.println("While de iteração");
				
				String[] response = apiGet(url + "/financeiro" + "/baixas"
						// + "?quantidade=1"
						// + "?dataInicial="+dataAtualFormatada+" 00:00:00"
						+ "?matricula=" + matricula
						+ "&pagina="+ paginaInicio
						//+ "&vencimentoInicial=2024-08-04 00:00:00&vencimentoFinal=2024-08-05 23:59:59"
						//+ "&dataInicial="+dataUmDiaFormatada+" 00:00:00&dataFinal="+dataAtualFormatada+" 23:59:59"
						, token);

				int status = Integer.parseInt(response[0]);

				System.out.println("Status teste: " + status);
				System.out.println("pagina: " + paginaInicio);

				String responseString = response[1];
				System.out.println("response string alunos: " + responseString);

				if ((responseString.equals("[]"))
						|| (paginaInicio == paginaFim)) {
					
					System.out.println("Entrou no if da quebra");
					/*
					 * if(responseString.equals("[]")){
					 * insertUltPagina(paginaInicio); }else{
					 * insertUltPagina(paginaFim); }
					 */

					break;
				}
				efetuarBaixa(response, url, token, codEmp, matricula);
				paginaInicio++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void efetuarBaixa(String[] response, String url, String token, BigDecimal codemp, String matricula) throws Exception {

		System.out.println("Entrou no job baixa");
		
		boolean movBanc = false;

		SimpleDateFormat formatoOriginal = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat formatoDesejado = new SimpleDateFormat("dd/MM/yyyy");

		Date dataAtual = new Date();
	    SimpleDateFormat formato = new SimpleDateFormat("yyyy-MM-dd");

	    Calendar calendar = Calendar.getInstance();
	    calendar.setTime(dataAtual);
	    calendar.add(Calendar.DAY_OF_MONTH, -1); 

	    Date dataUmDiaAtras = calendar.getTime();
	    
	    String dataUmDiaFormatada = formato.format(dataUmDiaAtras);
	    String dataAtualFormatada = formato.format(dataAtual);
	    
	    System.out.println("data um dia atras: " + dataUmDiaFormatada);
	    System.out.println("data normal: " + dataAtualFormatada);

		BigDecimal codTipTit = BigDecimal.ZERO;
		BigDecimal codBanco = BigDecimal.ZERO;
		BigDecimal codConta = BigDecimal.ZERO;
		BigDecimal nubco = BigDecimal.ZERO;

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		String dataEstorno = "";
		

		// String codUsu,"123",
		// String nuFin,
		// String codEmp,
		// String vlrDesdob,
		// String codContaBaixa,
		// int codLancamento 2
		// String codTopBaixa "1000"

		SkwServicoCompras sc = null;

		SessionHandle hnd = null;

		hnd = JapeSession.open();

		System.out.println("Entrou aqui JOBBaixas");

		// parametroVO.asString("VALOR");
		String domain = "http://127.0.0.1:8501";
		/*
		 * try { JapeWrapper usuarioDAO = JapeFactory
		 * .dao(DynamicEntityNames.USUARIO); DynamicVO usuarioVO;
		 * 
		 * usuarioVO = usuarioDAO.findByPK(new BigDecimal(0));
		 * 
		 * String md5 = usuarioVO.getProperty("INTERNO").toString(); String
		 * nomeUsu = usuarioVO.getProperty("NOMEUSU").toString();
		 * 
		 * sc = new SkwServicoCompras(domain, nomeUsu, md5);
		 * 
		 * System.out.println("Passou da instancia da api");
		 * 
		 * } catch (Exception e1) { e1.printStackTrace(); }
		 */
		
		int count = 0;
		
		BigDecimal nufin = BigDecimal.ZERO;
		
		try {

			jdbc.openSession();

			//String sqlP = "SELECT ID_EXTERNO, CODPARC, CODCENCUS FROM AD_ALUNOS WHERE CODEMP = 3 AND NVL(INTEGRADO_BAIXA, 'N') = 'N' AND ROWNUM <= 200";
			// + "WHERE ID_EXTERNO = '"+"STB210040"+"'";
			
			String sqlP = "SELECT ID_EXTERNO, CODPARC FROM AD_ALUNOS WHERE ID_EXTERNO = '" + matricula + "'";//NVL(INTEGRADO_BAIXA, 'N') = 'N' AND CODEMP = "+codemp+" AND ROWNUM <= 300";
			
			pstmt = jdbc.getPreparedStatement(sqlP);

			rs = pstmt.executeQuery();

			while (rs.next()) {
				count++;
				//String aluno = rs.getString("ID_EXTERNO");
				String aluno = rs.getString("ID_EXTERNO");

				/*String[] response = apiGet(url + "/financeiro" + "/baixas"
				// + "?quantidade=1"
				// + "?dataInicial="+dataAtualFormatada+" 00:00:00"
						+ "?matricula=" + aluno
						//+ "&vencimentoInicial=2024-08-04 00:00:00&vencimentoFinal=2024-08-05 23:59:59"
						+ "&dataInicial="+dataUmDiaFormatada+" 00:00:00&dataFinal="+dataAtualFormatada+" 23:59:59"
						, token);*/

				System.out.println("Teste: " + response[1]);

				JsonParser parser = new JsonParser();
				JsonArray jsonArray = parser.parse(response[1])
						.getAsJsonArray();

				for (JsonElement jsonElement : jsonArray) {
					JsonObject jsonObject = jsonElement.getAsJsonObject();

					System.out.println("Titulo ID: "
							+ jsonObject.get("titulo_id").getAsInt());
					System.out.println("Valor da Baixa: "
							+ jsonObject.get("baixa_valor").getAsString());

					String tituloId = jsonObject.get("titulo_id").getAsString();
					BigDecimal vlrBaixa = new BigDecimal(jsonObject.get(
							"baixa_valor").getAsString());
					
					BigDecimal vlrJuros = new BigDecimal(jsonObject.get(
							"baixa_juros").getAsString());
					
					BigDecimal vlrMulta = new BigDecimal(jsonObject.get(
							"baixa_multa").getAsString());
					
					BigDecimal vlrDesconto = new BigDecimal(jsonObject.get(
							"baixa_desconto").getAsString());
					
					BigDecimal vlrOutrosAcrescimos = new BigDecimal(jsonObject.get(
							"baixa_outros_acrescimos").getAsString());

					String dataBaixa = jsonObject.get("baixa_data")
							.getAsString();

					Date data = formatoOriginal.parse(dataBaixa);

					String dataBaixaFormatada = formatoDesejado.format(data);

					nufin = getNufin(tituloId);

					if (jsonObject.has("baixa_estorno_data")
							&& !jsonObject.get("baixa_estorno_data")
									.isJsonNull()) {

						System.out.println("Entrou no if de estorno");

						dataEstorno = jsonObject.get("baixa_estorno_data")
								.getAsString();
					} else {
						System.out.println("Entrou no else de estorno");
						dataEstorno = null;
					}

					String idExterno = jsonObject.get("local_pagamento_id")
							.getAsString();

					codBanco = getCodBanco(idExterno, codemp);
					codConta = getCodConta(idExterno, codemp);

					JsonArray formas_de_pagamento = jsonObject
							.getAsJsonArray("formas_de_pagamento");

					for (JsonElement formas_de_pagamentoElement : formas_de_pagamento) {

						JsonObject formas_de_pagamentoObject = formas_de_pagamentoElement
								.getAsJsonObject();

						System.out.println("Forma de pagamento: "
								+ formas_de_pagamentoObject.get(
										"forma_pagamento_id").getAsString());

						// codTipTit = getTipTit("Dinheiro");
						codTipTit = getTipTit(
								formas_de_pagamentoObject.get(
										"forma_pagamento_id").getAsString(),
								codemp);

					}

					System.out.println("estorno: " + dataEstorno);
					System.out.println("Data estorno: "
							+ jsonObject.get("baixa_estorno_data"));

					if (nufin.compareTo(BigDecimal.ZERO) != 0) {
						if (validarDataMinMovBancaria(codConta,
								dataBaixaFormatada)) {
							if (dataEstorno == null) {
								if (validarBaixa(nufin) && nufin.compareTo(BigDecimal.valueOf(113328)) != 0) {

									System.out.println("Chegou no update");
									if (vlrBaixa.compareTo(getVlrDesdob(nufin)) == 0) {
										System.out
												.println("Entrou no if do valor");
										updateFin(codTipTit, nufin, codBanco,
												codConta, vlrDesconto, vlrJuros, vlrMulta, vlrOutrosAcrescimos);
									} else {
										System.out
												.println("Entrou no else do valor");
										updateFinComVlrBaixa(codTipTit, nufin,
												codBanco, codConta, vlrBaixa,
												vlrDesconto, vlrJuros, vlrMulta, vlrOutrosAcrescimos);
									}
									
									System.out.println("vlrDesconto: " + vlrDesconto);
									System.out.println("vlrJuros: " + vlrJuros);
									System.out.println("vlrMulta: " + vlrMulta);
									
									/*updateFin(codTipTit, nufin, codBanco,
											codConta, vlrDesconto, vlrJuros, vlrMulta);*/
									
									nubco = insertMovBancaria(
											codConta, vlrBaixa, nufin,
											dataBaixaFormatada);
									

									System.out
											.println("Passou da mov bancaria: "
													+ nubco);
									
									System.out.println("vlrBaixa: " + vlrBaixa);
									
									updateBaixa(nufin, nubco, vlrBaixa,
											dataBaixaFormatada);
									
									movBanc = true;

									/*insertLogIntegracao(
											"Baixa Efetuada Com Sucesso Para o Financeiro: "
													+ nufin, "Sucesso");*/
								} else {
									System.out.println("Financeiro " + nufin
											+ " já baixado");
								}
							} else {
								if (!validarBaixa(nufin)) {
									
									nubco = getNubco(nufin);
									updateFinExtorno(nufin);
									deleteTgfMbc(nubco);
									/*insertLogIntegracao(
											"Estorno Efetuado com sucesso",
											"Sucesso");*/

								} 
							}
						} else {
							/*insertLogIntegracao("Baixa Para o Titulo " + nufin
									+ " Não Efetuada Pois a Data Minima de Movimentação Bancaria "
									+ "Para a Conta " +codConta+ " é Superior a Data de Baixa: " + dataBaixaFormatada, "Aviso");*/
						}
					} else {
						System.out
								.println("Não foi possivel encontrar financeiro com id externo "
										+ tituloId);
					}

				}
				
				//updateFlagAlunoIntegrado(aluno);
				
			}
			
			
			/*if(count == 0){
				updateResetarAlunos();
			}*/
			
		} catch (Exception e) {
			e.printStackTrace();
			
			if(movBanc){
				updateFinExtorno(nufin);
				deleteTgfMbc(nubco);
				System.out.println("Apagou mov bank");
			}
			
			try {
				insertLogIntegracao(
						"Mensagem de erro nas Baixas: " + e.getMessage(),
						"Erro");
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

		}

	}

	public String[] apiGet(String ur, String token) throws Exception {

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

			String sqlNota = "SELECT COUNT(0) AS COUNT " + "FROM TGFFIN "
					+ "WHERE NUFIN = ? " + "AND DHBAIXA IS NOT NULL "
					+ "AND VLRBAIXA IS NOT NULL "
					+ "AND CODUSUBAIXA IS NOT NULL";

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
		} else {
			return true;
		}

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

			String sqlNota = "SELECT COUNT(0) AS COUNT FROM (SELECT MIN(REFERENCIA) DTREF "
					+ "	    FROM TGFSBC "
					+ "	   WHERE CODCTABCOINT = "
					+ codConta
					+ ") "
					+ "	WHERE DTREF > TO_DATE('"
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
		} else {
			return true;
		}

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

			String sqlNota = "SELECT CODTIPTIT " + "FROM AD_TIPTITINTEGRACAO "
					+ "WHERE CODEMP = ? " + "AND IDEXTERNO = ?";

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

			String sqlNota = "select CODBCO " + "from ad_infobankbaixa "
					+ "WHERE CODEMP = ? " + "AND IDEXTERNO = ?";

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

			String sqlNota = "select CODCTABCOINT " + "from ad_infobankbaixa "
					+ "WHERE CODEMP = ? " + "AND IDEXTERNO = ?";

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
			BigDecimal codBanco, BigDecimal codConta, 
			BigDecimal vlrDesconto, BigDecimal vlrJuros, BigDecimal vlrMulta,
			BigDecimal vlrOutrosAcrescimos) throws Exception {
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

	public void updateFinComVlrBaixa(BigDecimal codtiptit, BigDecimal nufin,
			BigDecimal codBanco, BigDecimal codConta, BigDecimal vlrBaixa,
			BigDecimal vlrDesconto, BigDecimal vlrJuros, BigDecimal vlrMulta,
			BigDecimal vlrOutrosAcrescimos)
			throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		try {

			jdbc.openSession();

			String sqlNota = "UPDATE TGFFIN SET " + "CODTIPTIT = ?, "
					+ "CODBCO = ?, " + "CODCTABCOINT = ?, " + "AD_VLRDESCINT = "+vlrDesconto+", "
					+ "VLRINSS = 0, " + "VLRIRF = 0, " + "VLRISS = 0, "
					+ "AD_VLRJUROSINT = "+vlrJuros+", " + "AD_VLRMULTAINT = "+vlrMulta+", "
					+ "TIPJURO = null, AD_VLRORIG = VLRDESDOB, "
					+ "VLRDESDOB = " + vlrBaixa + ", " + "TIPMULTA = null, AD_OUTACRESCIMOS = " + vlrOutrosAcrescimos
					+ " WHERE nufin = ?";

			pstmt = jdbc.getPreparedStatement(sqlNota);
			pstmt.setBigDecimal(1, codtiptit);
			pstmt.setBigDecimal(2, codBanco);
			pstmt.setBigDecimal(3, codConta);
			pstmt.setBigDecimal(4, nufin);

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

	public void updateBaixa(BigDecimal nufin, BigDecimal nubco,
			BigDecimal vlrDesdob, String dataBaixaFormatada) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		try {

			jdbc.openSession();

			String sqlNota = "UPDATE TGFFIN SET "
					+ "VLRBAIXA = "
					+ vlrDesdob
					+ ", "
					+ "DHBAIXA = '"
					+ dataBaixaFormatada
					+ "', "
					+ "NUBCO = "
					+ nubco
					+ ", "
					+ "CODTIPOPERBAIXA = 1400, "
					+ "DHTIPOPERBAIXA = (SELECT MAX(DHALTER) FROM TGFTOP WHERE CODTIPOPER = 1400), "
					+ "CODUSUBAIXA = 0  " + "WHERE NUFIN = " + nufin;

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

	public void insertLogIntegracao(String descricao, String status)
			throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		
		try {
		
		jdbc.openSession();

		String sqlUpdate = "INSERT INTO AD_LOGINTEGRACAO (NUMUNICO, DESCRICAO, DTHORA, STATUS)"
				+ "VALUES (((SELECT NVL(MAX(NUMUNICO), 0) + 1 FROM AD_LOGINTEGRACAO)), ?, SYSDATE, ?)";

		pstmt = jdbc.getPreparedStatement(sqlUpdate);
		pstmt.setString(1, descricao);
		pstmt.setString(2, status);
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

	public BigDecimal insertMovBancaria(BigDecimal contaBancaria,
			BigDecimal vlrDesdob, BigDecimal nufin, String dataBaixaFormatada)
			throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		BigDecimal nubco = getMaxNumMbc();
		
		try {
		
		jdbc.openSession();

		String sqlUpdate = "INSERT INTO TGFMBC " + "(NUBCO, " + "CODLANC, "
				+ "DTLANC, " + "CODTIPOPER, " + "DHTIPOPER, " + "DTCONTAB, "
				+ "HISTORICO, " + "CODCTABCOINT, " + "NUMDOC, " + "VLRLANC, "
				+ "TALAO, " + "PREDATA, " + "CONCILIADO, " + "DHCONCILIACAO, "
				+ "ORIGMOV, " + "NUMTRANSF, " + "RECDESP, " + "DTALTER, "
				+ "DTINCLUSAO, " + "CODUSU, " + "VLRMOEDA, " + "SALDO, "
				+ "CODCTABCOCONTRA, " + "NUBCOCP, " + "CODPDV ) " + " VALUES ("
				+ nubco
				+ ", " // pk
				+ "1, "
				+ "'"
				+ dataBaixaFormatada
				+ "'"
				+ ", " // dtneg
				+ "1400, " // top baixa
				+ "(SELECT MAX(DHALTER) FROM TGFTOP WHERE CODTIPOPER = 1400), " // dhtop
				+ "NULL, "
				+ "(SELECT HISTORICO FROM TGFFIN WHERE NUFIN = "
				+ nufin
				+ "), " // historico
				+ ""
				+ contaBancaria
				+ ", " // conta bancaria (CODCTABCOINT)
				+ "0, " // NUMNOTA
				+ ""
				+ vlrDesdob
				+ ", " // vlrdesdob
				+ "NULL, "
				+ "'"+dataBaixaFormatada+"', " // dtneg2
				+ "'N', "
				+ "NULL, "
				+ "'F', "
				+ "NULL, "
				+ "1, "
				+ "SYSDATE, "
				+ "SYSDATE, " + "0, " // Usuario
				+ "0, " + "" + vlrDesdob + ", " // vlrDesdob
				+ "NULL,  " + "NULL, " + "NULL) ";

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

			// String sqlNota = "SELECT SEQ_TGFFIN_NUFIN.NEXTVAL FROM DUAL";
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
		
		try {
		
		jdbc.openSession();

		String sqlUpdate = "UPDATE TGFNUM SET ULTCOD = NVL(ULTCOD, 0) + 1  WHERE ARQUIVO = 'TGFMBC'";

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
	
	public void updateFlagAlunoIntegrado(String idAluno) throws Exception {
		
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		
		try {
			
			jdbc.openSession();
			
			String sqlUpdate = "UPDATE AD_ALUNOS SET INTEGRADO_BAIXA = 'S' WHERE ID_EXTERNO = '"+idAluno+"'";
			
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
	
	public void updateResetarAlunos() throws Exception {
		
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		
		try {
			System.out.println("Entrou no UPDATE da flag dos alunos baixa");
			jdbc.openSession();
			
			String sqlUpdate = "UPDATE AD_ALUNOS SET INTEGRADO_BAIXA = 'N'";
			
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
	
	private void updateCarga(BigDecimal idCarga) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		try
		{
			jdbc.openSession();
			
			String sqlUpd = "UPDATE AD_CARGAALUNOS SET INTEGRADO_BAIXA = 'S' WHERE IDCARGA = " + idCarga;
			
			pstmt = jdbc.getPreparedStatement(sqlUpd);
			pstmt.executeUpdate();
		}
		catch (SQLException e)
		{
			e.printStackTrace();
		}
		finally
		{
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}
	
	private void resetCarga(BigDecimal codEmp) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		try
		{
			jdbc.openSession();
			
			String sqlUpd = "UPDATE AD_CARGAALUNOS SET INTEGRADO_BAIXA = 'N' WHERE CODEMP = " + codEmp + " AND INTEGRADO_BAIXA = 'S'";
			
			pstmt = jdbc.getPreparedStatement(sqlUpd);
			pstmt.executeUpdate();
		}
		catch (SQLException e)
		{
			e.printStackTrace();
		}
		finally
		{
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}
	
}

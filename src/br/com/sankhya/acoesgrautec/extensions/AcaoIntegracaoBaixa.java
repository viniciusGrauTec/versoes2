package br.com.sankhya.acoesgrautec.extensions;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import br.com.sankhya.acoesgrautec.services.SkwServicoCompras;
import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.core.JapeSession;
import br.com.sankhya.jape.core.JapeSession.SessionHandle;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.modelcore.util.DynamicEntityNames;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

public class AcaoIntegracaoBaixa implements AcaoRotinaJava {

	@Override
	public void doAction(ContextoAcao contexto) throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		String dtIni = contexto.getParam("DTINI").toString();
		String dtFin = contexto.getParam("DTFIN").toString();
		
		SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
		SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd");
        
		Date dateIni = inputFormat.parse(dtIni);
		Date dateFin = inputFormat.parse(dtFin);
		
        String DateFIni = outputFormat.format(dateIni);
        String DateFFin = outputFormat.format(dateFin);

		BigDecimal codEmp = BigDecimal.ZERO;

		String url = "";
		String token = "";

		try {

			jdbc.openSession();

			String query = "SELECT CODEMP, URL, TOKEN FROM AD_LINKSINTEGRACAO WHERE CODEMP = 3";

			pstmt = jdbc.getPreparedStatement(query);

			rs = pstmt.executeQuery();

			while (rs.next()) {

				codEmp = rs.getBigDecimal("CODEMP");

				url = rs.getString("URL");
				token = rs.getString("TOKEN");

				efetuarBaixa(url, token, codEmp, DateFIni, DateFFin);

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
		}
	}

	public void efetuarBaixa(String url, String token, BigDecimal codemp,String DateFIni,String DateFFin) throws Exception {

		System.out.println("Entrou no job baixa");
		
		boolean movBanc = false;

		SimpleDateFormat formatoOriginal = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat formatoDesejado = new SimpleDateFormat("dd/MM/yyyy");

		// Obter a data atual
		Date dataAtual = new Date();

		// Formatando a data e imprimindo
		String dataAtualFormatada = formatoOriginal.format(dataAtual);

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

		try {

			jdbc.openSession();

			String sqlP = "SELECT ID_EXTERNO, CODPARC FROM AD_ALUNOS WHERE CODEMP = 3";
			// + "WHERE ID_EXTERNO = '"+"STB210040"+"'";

			pstmt = jdbc.getPreparedStatement(sqlP);

			rs = pstmt.executeQuery();

			while (rs.next()) {

				String aluno = rs.getString("ID_EXTERNO");

				String[] response = apiGet(url + "/financeiro" + "/baixas"
				// + "?quantidade=1"
				// + "?dataInicial="+dataAtualFormatada+" 00:00:00"
						+ "?matricula=" + aluno
						+ "&dataInicial="+DateFIni+" 00:00:00&dataFinal="+DateFFin+" 23:59:59", token);

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
					
					

					String dataBaixa = jsonObject.get("baixa_data")
							.getAsString();

					Date data = formatoOriginal.parse(dataBaixa);

					String dataBaixaFormatada = formatoDesejado.format(data);

					BigDecimal nufin = getNufin(tituloId);

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
												codConta, vlrDesconto, vlrJuros, vlrMulta);
									} else {
										System.out
												.println("Entrou no else do valor");
										updateFinComVlrBaixa(codTipTit, nufin,
												codBanco, codConta, vlrBaixa);
									}
									
									System.out.println("vlrDesconto: " + vlrDesconto);
									System.out.println("vlrJuros: " + vlrJuros);
									System.out.println("vlrMulta: " + vlrMulta);
									
									/*updateFin(codTipTit, nufin, codBanco,
											codConta, vlrDesconto, vlrJuros, vlrMulta);*/
									
									nubco = insertMovBancaria(
											codConta, vlrBaixa, nufin,
											dataBaixaFormatada);
									
									movBanc = true;

									System.out
											.println("Passou da mov bancaria: "
													+ nubco);
									
									System.out.println("vlrBaixa: " + vlrBaixa);
									
									updateBaixa(nufin, nubco, vlrBaixa,
											dataBaixaFormatada);

									insertLogIntegracao(
											"Baixa Efetuada Com Sucesso Para o Financeiro: "
													+ nufin, "Sucesso");
								} else {
									System.out.println("Financeiro " + nufin
											+ " já baixado");
								}
							} else {
								if (!validarBaixa(nufin)) {
									
									nubco = getNubco(nufin);
									updateFinExtorno(nufin);
									deleteTgfMbc(nubco);
									insertLogIntegracao(
											"Estorno Efetuado com sucesso",
											"Sucesso");

								} 
							}
						} else {
							insertLogIntegracao("Baixa Para o Titulo " + nufin
									+ " Não Efetuada Pois a Data Minima de Movimentação Bancaria "
									+ "Para a Conta " +codConta+ " é Superior a Data de Baixa: " + dataBaixaFormatada, "Aviso");
						}
					} else {
						System.out
								.println("Não foi possivel encontrar financeiro com id externo "
										+ tituloId);
					}

				}
			}
			System.out.println("Chegou ao final da baixa");

		} catch (Exception e) {
			e.printStackTrace();
			
			if(movBanc){
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
			BigDecimal vlrDesconto, BigDecimal vlrJuros, BigDecimal vlrMulta) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		
		try {

			jdbc.openSession();

			String sqlNota = "UPDATE TGFFIN SET " + "CODTIPTIT = ?, "
					+ "CODBCO = ?, " + "CODCTABCOINT = ?, ";
					
					if(vlrDesconto.compareTo(BigDecimal.ZERO) == 0){
						sqlNota += "VLRDESC = 0, ";
					}else{
						sqlNota += "VLRDESC = "+vlrDesconto+", ";
					}
			
					
			sqlNota += "VLRINSS = 0, " + "VLRIRF = 0, " + "VLRISS = 0, ";
					
					if(vlrMulta.compareTo(BigDecimal.ZERO) == 0){
						sqlNota += "VLRMULTA = 0, ";
					}else{
						sqlNota += "VLRMULTA = "+vlrMulta+", ";
					}
					
					if(vlrJuros.compareTo(BigDecimal.ZERO) == 0){
						sqlNota += "VLRJURO = 0, ";
					}else{
						sqlNota += "VLRJURO = "+vlrJuros+", ";
					}
			
					if(vlrJuros.compareTo(BigDecimal.ZERO) == 0){
						sqlNota += "TIPJURO = null, ";
					}else{
						sqlNota += "TIPJURO = null, ";
					}
					
					if(vlrMulta.compareTo(BigDecimal.ZERO) == 0){
						sqlNota += "TIPMULTA = null";
					}else{
						sqlNota += "TIPMULTA = null";
					}
					
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
			BigDecimal codBanco, BigDecimal codConta, BigDecimal vlrBaixa)
			throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		try {

			jdbc.openSession();

			String sqlNota = "UPDATE TGFFIN SET " + "CODTIPTIT = ?, "
					+ "CODBCO = ?, " + "CODCTABCOINT = ?, " + "VLRDESC = 0, "
					+ "VLRINSS = 0, " + "VLRIRF = 0, " + "VLRISS = 0, "
					+ "VLRJURO = 0, " + "VLRMULTA = 0, "
					+ "TIPJURO = null, AD_VLRORIG = VLRDESDOB, "
					+ "VLRDESDOB = " + vlrBaixa + "," + "TIPMULTA = null"
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

		jdbc.openSession();

		String sqlUpdate = "INSERT INTO AD_LOGINTEGRACAO (NUMUNICO, DESCRICAO, DTHORA, STATUS)"
				+ "VALUES (((SELECT NVL(MAX(NUMUNICO), 0) + 1 FROM AD_LOGINTEGRACAO)), ?, SYSDATE, ?)";

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
			BigDecimal vlrDesdob, BigDecimal nufin, String dataBaixaFormatada)
			throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		BigDecimal nubco = getMaxNumMbc();

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
		}

	}

}

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
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.cuckoo.core.ScheduledAction;
import org.cuckoo.core.ScheduledActionContext;

import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class JobGetTitulos implements ScheduledAction {

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

		System.out.println("Iniciou o financeiro dos alunos Empresas");

		try {

			// Alunos
			List<Object[]> listInfAlunos = retornarInformacoesAlunos();
			Map<String, BigDecimal> mapaInfAlunos = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfAlunos) {
				BigDecimal codParc = (BigDecimal) obj[0];
				String idExternoObj = (String) obj[1];

				if (mapaInfAlunos.get(idExternoObj) == null) {
					mapaInfAlunos.put(idExternoObj, codParc);
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
				String cpf_cnpj = (String) obj[1];

				if (mapaInfParceiros.get(cpf_cnpj) == null) {
					mapaInfParceiros.put(cpf_cnpj, codParc);
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

			// CenCus Pelo Aluno
			Map<String, BigDecimal> mapaInfCenCusAluno = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfAlunos) {
				String idExternoObj = (String) obj[1];
				BigDecimal codCenCus = (BigDecimal) obj[2];

				if (mapaInfCenCusAluno.get(idExternoObj) == null) {
					mapaInfCenCusAluno.put(idExternoObj, codCenCus);
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
			//String query3 = "SELECT CODEMP, URL, TOKEN FROM AD_LINKSINTEGRACAO WHERE CODEMP = 3";
			//String query4 = "SELECT CODEMP, URL, TOKEN FROM AD_LINKSINTEGRACAO WHERE CODEMP = 4";
			// String query3 =
			// "SELECT LINK.CODEMP, URL, TOKEN, IDCARGA, MATRICULA FROM AD_LINKSINTEGRACAO LINK INNER JOIN AD_CARGAALUNOS CARGA ON CARGA.CODEMP = LINK.CODEMP WHERE LINK.CODEMP = 3 AND NVL(CARGA.INTEGRADO_FIN, 'N') = 'N'";
			//String query4 = "SELECT LINK.CODEMP, URL, TOKEN, IDCARGA, MATRICULA FROM AD_LINKSINTEGRACAO LINK INNER JOIN AD_CARGAALUNOS CARGA ON CARGA.CODEMP = LINK.CODEMP WHERE LINK.CODEMP = 4 AND NVL(CARGA.INTEGRADO_FIN, 'N') = 'N'";

			pstmt = jdbc.getPreparedStatement(query);

			rs = pstmt.executeQuery();

			while (rs.next()) {
				count++;
				codEmp = rs.getBigDecimal("CODEMP");
				// idCarga = rs.getBigDecimal("IDCARGA");

				url = rs.getString("URL");
				token = rs.getString("TOKEN");
				// matricula = rs.getString("MATRICULA");

				iterarEndpoint(mapaInfFinanceiroBaixado, mapaInfFinanceiroBanco, mapaInfFinanceiro, mapaInfRecDesp, mapaInfConta,
						mapaInfBanco, mapaInfNatureza, mapaInfCenCus,
						mapaInfCenCusAluno, mapaInfAlunos, url, token, codEmp,
						matricula);
				// updateCarga(idCarga);

			}

			/*
			 * if(count == 0){ resetCarga(codEmp); }
			 */

			System.out
					.println("Finalizou o financeiro dos alunos Empresas");

		} catch (Exception e) {
			e.printStackTrace();
			try {
				insertLogIntegracao(
						"Erro ao integrar financeiro, Mensagem de erro: "
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

	public void iterarEndpoint(Map<BigDecimal, String> mapaInfFinanceiroBaixado,
			Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco,
			Map<String, BigDecimal> mapaInfFinanceiro,
			Map<String, String> mapaInfRecDesp,
			Map<String, BigDecimal> mapaInfConta,
			Map<String, BigDecimal> mapaInfBanco,
			Map<String, BigDecimal> mapaInfNatureza,
			Map<String, BigDecimal> mapaInfCenCus,
			Map<String, BigDecimal> mapaInfCenCusAluno,
			Map<String, BigDecimal> mapaInfAlunos, String url, String token,
			BigDecimal codEmp, String matricula) throws Exception {
		// int pagina = 1;

		Date dataAtual = new Date();

		SimpleDateFormat formato = new SimpleDateFormat("yyyy-MM-dd");

		String dataFormatada = formato.format(dataAtual);
		//dataFormatada = "2024-09-13";

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

			String[] response = apiGet(url
					+ "/financeiro"
					+ "/titulos?"
					// + "situacao=A"
					+ "quantidade=0"
					// + "&dataInicial=2023-04-19 00:00:00"
					// + "&dataFinal=2023-10-24 00:00:00"
					// + "matricula=" + matricula
					// + "&pagina=" + paginaInicio
					// +
					// "&vencimentoInicial=2024-08-05 00:00:00&vencimentoFinal=2024-08-08 23:59:59"
					+ "&dataInicial=" + dataFormatada + " 00:00:00&dataFinal="
					+ dataFormatada + " 23:59:59", token);

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
			cadastrarFinanceiro(mapaInfFinanceiroBaixado, mapaInfFinanceiroBanco, mapaInfFinanceiro, mapaInfRecDesp,
					mapaInfConta, mapaInfBanco, mapaInfNatureza, mapaInfCenCus,
					mapaInfCenCusAluno, mapaInfAlunos, response, url, token,
					codEmp, matricula);
			// paginaInicio++;
		}
		// }
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void cadastrarFinanceiro(Map<BigDecimal, String> mapaInfFinanceiroBaixado,
			Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco,
			Map<String, BigDecimal> mapaInfFinanceiro,
			Map<String, String> mapaInfRecDesp,
			Map<String, BigDecimal> mapaInfConta,
			Map<String, BigDecimal> mapaInfBanco,
			Map<String, BigDecimal> mapaInfNatureza,
			Map<String, BigDecimal> mapaInfCenCus,
			Map<String, BigDecimal> mapaInfCenCusAluno,
			Map<String, BigDecimal> mapaInfAlunos, String[] response,
			String url, String token, BigDecimal codemp, String matricula)
			throws Exception {

		SimpleDateFormat formatoEntrada = new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss.SSS");
		SimpleDateFormat formatoOriginal = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat formatoDesejado = new SimpleDateFormat("dd/MM/yyyy");

		System.out.println("Entrou no job");

		// int count = 0;

		Date dataAtual = new Date();

		SimpleDateFormat formato = new SimpleDateFormat("yyyy-MM-dd");

		String dataFormatada = formato.format(dataAtual);

		StringBuilder consulta = new StringBuilder();

		try {


			String responseString = response[1];
			String responseStatus = response[0];

			if (responseStatus.equalsIgnoreCase("200")) {

				JsonParser parser = new JsonParser();
				JsonArray jsonArray = parser.parse(responseString)
						.getAsJsonArray();
				int count = 0;
				int total = jsonArray.size();
				int qtdInsert = 0;

				List<String> selectsParaInsert = new ArrayList<String>();

				for (JsonElement jsonElement : jsonArray) {
					JsonObject jsonObject = jsonElement.getAsJsonObject();

					String idFin = jsonObject.get("titulo_id").getAsString();

					BigDecimal vlrDesdob = new BigDecimal(jsonObject.get(
							"titulo_valor").getAsDouble());

					String dtVenc = jsonObject.get("titulo_vencimento")
							.getAsString();

					String idCurso = jsonObject.get("curso_id").getAsString();

					String taxaId = jsonObject.get("taxa_id").getAsString();

					String dtPedidoOrig = jsonObject.get("data_atualizacao")
							.getAsString();

					Date dataPedido = formatoEntrada.parse(dtPedidoOrig);

					String dtPedido = formatoDesejado.format(dataPedido);

					System.out.println("Data Pedido: " + dtPedido);

					Date data = formatoOriginal.parse(dtVenc);
					String dataVencFormatada = formatoDesejado.format(data);

					String idAluno = jsonObject.get("aluno_id").getAsString();

					BigDecimal codparc = Optional.ofNullable(
							mapaInfAlunos.get(idAluno)).orElse(BigDecimal.ZERO);

					String situacao_titulo = jsonObject.get("titulo_situacao")
							.getAsString();
					
					System.out.println("Aluno: " + idAluno);
					
					if(!situacao_titulo.equalsIgnoreCase("X")){
						

						if (vlrDesdob.compareTo(new BigDecimal("5")) > 0
								&& codparc.compareTo(BigDecimal.ZERO) != 0) {
							
							BigDecimal codCenCus = Optional.ofNullable(
									mapaInfCenCusAluno.get(idAluno)).orElse(
									BigDecimal.ZERO);// getCodCenCusPeloCusto(taxaId);

							if (codCenCus.compareTo(BigDecimal.ZERO) == 0) {
								codCenCus = Optional.ofNullable(
										mapaInfCenCus.get(taxaId)).orElse(
										BigDecimal.ZERO);
							}

							System.out.println("CodCenCus: " + codCenCus);
							System.out.println("Taxa id: " + taxaId);

							System.out.println("CodParc: " + codparc);

							if (validarDataLimite(dtPedido)
									&& codCenCus.compareTo(BigDecimal.ZERO) != 0) {

								if (codparc.compareTo(BigDecimal.ZERO) != 0) {
									System.out.println("Entrou no parceiro: "
											+ codparc);

									BigDecimal validarNufin = Optional.ofNullable(
											mapaInfFinanceiro.get(codemp + "###"
													+ idFin)).orElse(
											BigDecimal.ZERO);

									if (validarNufin.compareTo(BigDecimal.ZERO) == 0) {
										System.out.println("Entrou no financeiro");

										BigDecimal codConta = mapaInfConta
												.get(codemp.toString());// getCodConta(codemp);

										BigDecimal codBanco = mapaInfBanco
												.get(codemp.toString());// getCodBanco(codemp);

										String recDesp = mapaInfRecDesp.get(taxaId);
										
										BigDecimal natureza = Optional.ofNullable(mapaInfNatureza.get(taxaId)).orElse(BigDecimal.ZERO);
										
										/*
										 * (getRecDesp(taxaId)){ recDesp = "-1";
										 * }else{ recDesp = "1"; }
										 */
										if (recDesp != null && !recDesp.isEmpty() && natureza.compareTo(BigDecimal.ZERO) != 0) {

											String sqlInsert = " SELECT <#NUFIN#>, NULL, 0, 'F', "
													+ recDesp
													+ ", "
													+ codemp
													+ " , "
													+ codCenCus
													+ " , "
													+ natureza
													+ " ,  "
													+ BigDecimal.valueOf(1300)
													+ " ,  (SELECT MAX(DHALTER) FROM TGFTOP WHERE CODTIPOPER = "
													+ BigDecimal.valueOf(1300)
													+ "), 0, (SELECT MAX(DHALTER) FROM TGFTOP WHERE CODTIPOPER = 0), "
													+ codparc
													+ " , "
													+ BigDecimal.valueOf(4)
													+ " , "
													+ vlrDesdob
													+ " , 0, 0, "
													+ codBanco
													+ ", "
													+ codConta
													+ ", '"
													+ dtPedido
													+ "' , SYSDATE, SYSDATE, '"
													+ dataVencFormatada
													+ "', SYSDATE, '"
													+ dataVencFormatada
													+ "' , 1 , 1 , null , 'I' , 'N' , 'N' , 'N' , 'N' , 'N' , 'N' , 'N' , 'S' , 'S' , 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, '"
													+ idFin
													+ "', '"
													+ idAluno
													+ "' FROM DUAL ";

											consulta.append(sqlInsert);

											selectsParaInsert.add(sqlInsert);
											qtdInsert++;

											if (count < total - 1) {
												consulta.append("\nUNION ALL");
											}

											// BigDecimal nufin = insertFin(codemp,
											// /* codemp */
											// codCenCus, /* codCenCus */
											// mapaInfNatureza.get(taxaId), /*
											// codNat */
											// BigDecimal.valueOf(1300), /*
											// codTipOper */
											// codparc, /* codparc */
											// BigDecimal.valueOf(4), /* codtiptit
											// */
											// vlrDesdob, /* vlrDesdob */
											// dataVencFormatada, /* dtvenc */
											// // "25/11/2023", /* dtvenc */
											// dtPedido, /* dtPedido */
											// // "22/11/2023", /* dtPedido */
											// idFin, idAluno, codConta, codBanco,
											// recDesp);

											System.out
													.println("Financeiro cadastrado");
										} else {
											System.out
													.println("Sem \"de para\" para a Taxa ID: "
															+ taxaId);
										}

										/*
										 * insertLogIntegracao(
										 * "Financeiro com Id Externo: " + idFin +
										 * " Criado Com Sucesso, numero unico interno: "
										 * + nufin, "Sucesso");
										 */
									} else {
										System.out
												.println("Financeiro "
														+ idFin
														+ " ja cadastrado para o parceiro: "
														+ codparc);
									}

								} else {
									/*
									 * insertLogIntegracao("Aluno com Id : " +
									 * idAluno + " não Encontrado", "Aviso");
									 */
								}

							} else {
								System.out
										.println("Data pedido inferior a data limite / Natureza ou Centro de Resultado não encontrados");
							}

						}
						
					}else if(situacao_titulo.equalsIgnoreCase("X")){
						System.out.println("Cancelado");
						
						BigDecimal validarNufin = Optional.ofNullable(
								mapaInfFinanceiro.get(codemp + "###"
										+ idFin)).orElse(
								BigDecimal.ZERO);
						
						if(validarNufin.compareTo(BigDecimal.ZERO) != 0){
							if ("S".equalsIgnoreCase(mapaInfFinanceiroBaixado
									.get(validarNufin))) {

								BigDecimal nubco = mapaInfFinanceiroBanco.get(validarNufin);
								updateFinExtorno(validarNufin);
								deleteTgfMbc(nubco);
								deleteTgfFin(validarNufin);

							}else{
								deleteTgfFin(validarNufin);
							}
						}
						
					}
					
					count++;
				}

				System.out.println("Consulta Antes tratamento: " + consulta);

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
				insertLogIntegracao(
						"Status retornado pela API diferente de 200, "
								+ "por favor verificar, Status retornado: "
								+ responseStatus, "Aviso");
			}

			// }

			/*
			 * if(count == 0){ updateResetarAlunos(); }
			 */

		} catch (Exception e) {
			e.printStackTrace();
			try {
				insertLogIntegracao(
						"Erro ao integrar financeiro, Mensagem de erro: "
								+ e.getMessage(), "Erro");
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
		// + "2|VFBUMOCUNitomQYMrwWY7dCaTLts1Lsab3Bktpf5");
				+ token);
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

	public BigDecimal insertFin(BigDecimal codemp, BigDecimal codCenCus,
			BigDecimal codNat, BigDecimal codTipOper, BigDecimal codparc,
			BigDecimal codTipTit, BigDecimal vlrDesdbo, String dtVenc,
			String dtPedido, String idExterno, String idAluno,
			BigDecimal codConta, BigDecimal codBanco, String recDesp)
			throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		BigDecimal nufin = getMaxNumFin();

		System.out.println("Teste financeiro: " + nufin);

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
					+ "         AD_IDALUNO) " + "        VALUES (?, "
					+ "               NULL, " + "               0, "
					+ "               'F', " + "               "
					+ recDesp
					+ ", "
					+ "               "
					+ codemp
					+ " , " // AS CODEMP
					+ "               "
					+ codCenCus
					+ " , " // AS CODCENCUS
					+ "               "
					+ codNat
					+ " , " // AS CODNAT
					+ "               "
					+ codTipOper
					+ " , " // AS CODTIPOPER
					+ "               (SELECT MAX(DHALTER) "
					+ "                  FROM TGFTOP "
					+ "                 WHERE CODTIPOPER = "
					+ codTipOper
					+ "), "
					+ "               0, "
					+ "               (SELECT MAX(DHALTER) FROM TGFTOP WHERE CODTIPOPER = 0), "
					+ "               "
					+ codparc
					+ " , " // AS CODPARC
					+ "               "
					+ codTipTit
					+ " , " // AS CODTIPTIT
					+ "               "
					+ vlrDesdbo
					+ " , " // AS VLRDESDOB
					+ "               0, "
					+ "               0, "
					+ "               "
					+ codBanco
					+ ", " // AS CODBCO
					+ "               "
					+ codConta
					+ ", " // AS CODCTABCOINT
					+ "               '"
					+ dtPedido
					+ "' , " // AS DTNEG
					+ "               SYSDATE, "
					+ "               SYSDATE, "
					+ "               '"
					+ dtVenc
					+ "' , " // AS DTVENC
					+ "               SYSDATE, " // AS PRAZO
					+ "               '"
					+ dtVenc
					+ "' , " // AS DTVENCINIC
					+ "               1 , " // AS TIPJURO
					+ "               1 , " // AS TIPMULTA
					+ "               null , " // AS HISTORICO
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
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0,"
					+ "               '"
					+ idExterno
					+ "',"
					+ "     '"
					+ idAluno + "')";

			pstmt = jdbc.getPreparedStatement(sqlUpdate);
			pstmt.setBigDecimal(1, nufin);
			/*
			 * pstmt.setBigDecimal(2, codemp); pstmt.setBigDecimal(3,
			 * codCenCus); pstmt.setBigDecimal(4, codNat);
			 * pstmt.setBigDecimal(5, codTipOper); pstmt.setBigDecimal(6,
			 * codTipOper); pstmt.setBigDecimal(7, codparc);
			 * pstmt.setBigDecimal(8, codTipTit); pstmt.setBigDecimal(9,
			 * vlrDesdbo); pstmt.setString(10, dtPedido); pstmt.setString(11,
			 * dtVenc); pstmt.setString(12, dtVenc); pstmt.setString(13,
			 * idExterno); pstmt.setString(14, idAluno);
			 */

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

		return nufin;

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

	public BigDecimal getCodParc(String idAluno) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal id = BigDecimal.ZERO;

		try {

			jdbc.openSession();

			String sqlNota = "SELECT NVL((SELECT CODPARC FROM AD_ALUNOS WHERE ID_EXTERNO = ?), 0) AS ID FROM DUAL";

			pstmt = jdbc.getPreparedStatement(sqlNota);
			pstmt.setString(1, idAluno);

			rs = pstmt.executeQuery();

			if (rs.next()) {

				id = rs.getBigDecimal("ID");

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

	public BigDecimal getNatureza(String idExterno) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal id = BigDecimal.ZERO;

		try {

			jdbc.openSession();

			String sqlNota = "SELECT CODNAT FROM AD_NATACAD WHERE IDEXTERNO = '"
					+ idExterno
					+ "'"
					+ " union "
					+ "SELECT 0 FROM DUAL WHERE NOT EXISTS (SELECT CODNAT FROM AD_NATACAD WHERE IDEXTERNO = '"
					+ idExterno + "')";

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

		if (id.compareTo(BigDecimal.ZERO) == 0) {
			id = BigDecimal.valueOf(10101002);
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

	public BigDecimal getCodCenCusPeloCusto(String idTaxa) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal id = BigDecimal.ZERO;

		try {

			jdbc.openSession();

			String sqlNota = "SELECT CODCENCUS FROM AD_NATACAD WHERE IDEXTERNO = "
					+ idTaxa;

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

	public boolean validarFin(String idFin, BigDecimal codemp) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		int count = 0;

		try {

			jdbc.openSession();

			String sqlNota = "SELECT COUNT(0) AS COUNT FROM TGFFIN WHERE AD_IDEXTERNO = ? AND CODEMP = "
					+ codemp;

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

	public boolean validarDataLimite(String data) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		int count = 0;

		try {

			jdbc.openSession();

			String sqlNota = "SELECT COUNT(0) AS COUNT FROM DUAL WHERE TO_DATE('"
					+ data
					+ "') < ADD_MONTHS(SYSDATE, 12 * -(SELECT NVL(INTEIRO,0) FROM TSIPAR WHERE CHAVE = 'LIMINFANO')) ";

			pstmt = jdbc.getPreparedStatement(sqlNota);

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

	public BigDecimal getAluno(BigDecimal codparc) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal id = BigDecimal.ZERO;

		try {

			jdbc.openSession();

			String sqlNota = "SELECT ID_EXTERNO FROM AD_ALUNOS WHERE CODPARC = ?";

			pstmt = jdbc.getPreparedStatement(sqlNota);
			pstmt.setBigDecimal(1, codparc);

			rs = pstmt.executeQuery();

			if (rs.next()) {

				id = rs.getBigDecimal("ID_EXTERNO");

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
					+ "FROM AD_NATACAD where idexterno = "
					+ idExterno
					+ " "
					+ "union SELECT '0' FROM DUAL "
					+ "WHERE NOT EXISTS (SELECT SUBSTR(CODNAT, 0, 1) NAT FROM AD_NATACAD where idexterno = "
					+ idExterno + ")";

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

		if (id > 1) {
			return true;
		} else {
			return false;
		}
	}

	public void updateNumFin() throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		try {

			jdbc.openSession();

			String sqlUpdate = "UPDATE TGFNUM SET ULTCOD = NVL(ULTCOD, 0) + 1  WHERE ARQUIVO = 'TGFFIN'";

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

	public void updateFlagAlunoIntegrado(String idAluno) throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		try {

			jdbc.openSession();

			String sqlUpdate = "UPDATE AD_ALUNOS SET INTEGRADO = 'S' WHERE ID_EXTERNO = '"
					+ idAluno + "'";

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

	public void updateResetarAlunos() throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		try {
			System.out.println("Entrou no UPDATE da flag dos alunos");
			jdbc.openSession();

			String sqlUpdate = "UPDATE AD_ALUNOS SET INTEGRADO = 'N'";

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
		} finally {
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
		try {
			jdbc.openSession();

			String sqlUpd = "UPDATE AD_CARGAALUNOS SET INTEGRADO_FIN = 'S' WHERE IDCARGA = "
					+ idCarga;

			pstmt = jdbc.getPreparedStatement(sqlUpd);
			pstmt.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
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
		try {
			jdbc.openSession();

			String sqlUpd = "UPDATE AD_CARGAALUNOS SET INTEGRADO_FIN = 'N' WHERE CODEMP = "
					+ codEmp + " AND INTEGRADO_FIN = 'S'";

			pstmt = jdbc.getPreparedStatement(sqlUpd);
			pstmt.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}

	private List<Object[]> retornarInformacoesParceiros() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		List<Object[]> listRet = new ArrayList<>();

		try {
			jdbc.openSession();
			String sql = "	SELECT 	CODPARC, CGC_CPF";
			sql += "		FROM  	TGFPAR ";

			pstmt = jdbc.getPreparedStatement(sql);
			rs = pstmt.executeQuery();

			while (rs.next()) {
				Object[] ret = new Object[2];
				ret[0] = rs.getBigDecimal("CODPARC");
				ret[1] = rs.getString("CGC_CPF");

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

	public List<Object[]> retornarInformacoesAlunos() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		List<Object[]> listRet = new ArrayList<>();
		try {
			jdbc.openSession();
			String sql = "	SELECT 	CODPARC, ID_EXTERNO, CODCENCUS ";
			sql += "		FROM  	AD_ALUNOS ";
			pstmt = jdbc.getPreparedStatement(sql);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				Object[] ret = new Object[3];
				ret[0] = rs.getBigDecimal("CODPARC");
				ret[1] = rs.getString("ID_EXTERNO");
				ret[2] = rs.getBigDecimal("CODCENCUS");

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
			String sql = "SELECT CODCENCUS, IDEXTERNO FROM AD_NATACAD WHERE AD_IDEXTERNO IS NOT NULL";

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
			sql += "		WHERE  	RECDESP = 1 ";
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
}

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
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.cuckoo.core.ScheduledAction;
import org.cuckoo.core.ScheduledActionContext;

import br.com.sankhya.acoesgrautec.util.EnviromentUtils;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class JobGetBaixaMap implements ScheduledAction {

	@Override
	public void onTime(ScheduledActionContext arg0) {

		System.out
				.println("/*************** Inicio - JobGetBaixaMap *****************/ ");
		long tempoAnterior = System.currentTimeMillis();
		long tempoInicio = System.currentTimeMillis();

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

		try {

			// Carregar os mapas das listas - Apenas para consultas únicas por
			// execução para evitar múltiplos acessos

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
			
			// Id de Baixa
			List<Object[]> listInfIdBaixa = retornarInformacoesIdBaixa();
			Map<BigDecimal, String> mapaInfIdBaixa = new HashMap<BigDecimal, String>();
			for (Object[] obj : listInfIdBaixa) {
				BigDecimal nuFin = (BigDecimal) obj[0];
				String idBaixa = (String) obj[1];
				
				if (mapaInfIdBaixa.get(nuFin) == null) {
					mapaInfIdBaixa.put(nuFin, idBaixa);
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

			// Menor Movimentação Bancária Por Conta
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

			// Depois rever para ativar todos
			String query = "SELECT CODEMP, URL, TOKEN FROM AD_LINKSINTEGRACAO";
			//String query4 = "SELECT CODEMP, URL, TOKEN FROM AD_LINKSINTEGRACAO WHERE CODEMP = 3";
			//String query3 = "SELECT LINK.CODEMP, URL, TOKEN, IDCARGA, MATRICULA FROM AD_LINKSINTEGRACAO LINK INNER JOIN AD_CARGAALUNOS CARGA ON CARGA.CODEMP = LINK.CODEMP WHERE LINK.CODEMP = 3 ";
			//String query4 = "SELECT LINK.CODEMP, URL, TOKEN, IDCARGA, MATRICULA FROM AD_LINKSINTEGRACAO LINK INNER JOIN AD_CARGAALUNOS CARGA ON CARGA.CODEMP = LINK.CODEMP WHERE LINK.CODEMP = 4 AND NVL(CARGA.INTEGRADO_BAIXA, 'N') = 'N'";

			pstmt = jdbc.getPreparedStatement(query);

			rs = pstmt.executeQuery();

			tempoAnterior = printLogDebug(tempoAnterior,
					"Consulta para capturar o link de integração: AD_LINKSINTEGRACAO");

			while (rs.next()) {
				count++;
				
				System.out.println("Contagem: " + count);

				codEmp = rs.getBigDecimal("CODEMP");
				//idCarga = rs.getBigDecimal("IDCARGA");

				url = rs.getString("URL");
				token = rs.getString("TOKEN");
				//matricula = rs.getString("MATRICULA");

				iterarEndpoint(url, token, codEmp, matricula, mapaInfIdBaixa,
						mapaInfTipoTituloTaxa, 
						mapaInfBanco,
						mapaInfConta,
						mapaInfAlunos,
						mapaInfFinanceiro,
						mapaInfTipoTitulo,
						mapaInfMenorDataMovBancariaPorConta,
						mapaInfFinanceiroBaixado,
						mapaInfFinanceiroValor,
						mapaInfFinanceiroBanco,
						tempoAnterior);

				tempoAnterior = printLogDebug(tempoAnterior,
						"onTime - efetuarBaixa da empresa(" + codEmp + ")");

				//updateCarga(idCarga);

				tempoAnterior = printLogDebug(tempoAnterior,
						"onTime - updateCarga da empresa(" + codEmp + ")");
			}
			System.out.println("Chegou ao final da baixa");
			if (count == 0) {
				//resetCarga(codEmp);
				tempoAnterior = printLogDebug(tempoAnterior, "resetCarga");
			}

			System.out
					.println("/*************** Fim - JobGetBaixaMap *****************/");

			printLogDebug(tempoInicio, "Tempo Total: ");

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
	
	public void iterarEndpoint(String url, String token, BigDecimal codemp,
			String matricula, Map<BigDecimal, String> mapaInfIdBaixa,
			Map<String, BigDecimal> mapaInfTipoTituloTaxa, 
			Map<String, BigDecimal> mapaInfBanco,
			Map<String, BigDecimal> mapaInfConta,
			Map<String, BigDecimal> mapaInfAlunos,
			Map<String, BigDecimal> mapaInfFinanceiro,
			Map<String, BigDecimal> mapaInfTipoTitulo,
			Map<Long, Date> mapaInfMenorDataMovBancariaPorConta,
			Map<BigDecimal, String> mapaInfFinanceiroBaixado,
			Map<BigDecimal, BigDecimal> mapaInfFinanceiroValor,
			Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco,
			long tempoAnterior) throws Exception {

		Date dataAtual = new Date();

		SimpleDateFormat formato = new SimpleDateFormat("yyyy-MM-dd");

		String dataFormatada = formato.format(dataAtual);
		//dataFormatada = "2024-08-26";

		int paginaInicio = 1;
		int paginaFim = 30;


		try {
				
				String[] response = apiGet(url + "/financeiro" + "/baixas" 
						 + "?quantidade=0"
						// + "?dataInicial="+dataAtualFormatada+" 00:00:00"
						//+ "?matricula=" + matricula
						//+ "&pagina="+ paginaInicio
						//+ "&vencimentoInicial=2024-08-04 00:00:00&vencimentoFinal=2024-08-05 23:59:59"
						+ "&dataInicial="+dataFormatada+" 00:00:00&dataFinal="+dataFormatada+" 23:59:59"
						, token);

				int status = Integer.parseInt(response[0]);

				System.out.println("Status teste: " + status);
				System.out.println("pagina: " + paginaInicio);

				String responseString = response[1];
				System.out.println("response string baixas: " + responseString);

				efetuarBaixa(response, url, token, codemp,
						mapaInfIdBaixa,
						mapaInfTipoTituloTaxa,
						mapaInfBanco,
						mapaInfConta,
						mapaInfAlunos,
						mapaInfFinanceiro,
						mapaInfTipoTitulo,
						mapaInfMenorDataMovBancariaPorConta,
						mapaInfFinanceiroBaixado,
						mapaInfFinanceiroValor,
						mapaInfFinanceiroBanco);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void efetuarBaixa(String[] response, String url, String token, BigDecimal codemp,
			Map<BigDecimal, String> mapaInfIdBaixa,
			Map<String, BigDecimal> mapaInfTipoTituloTaxa,
			Map<String, BigDecimal> mapaInfBanco,
			Map<String, BigDecimal> mapaInfConta,
			Map<String, BigDecimal> mapaInfAlunos,
			Map<String, BigDecimal> mapaInfFinanceiro,
			Map<String, BigDecimal> mapaInfTipoTitulo,
			Map<Long, Date> mapaInfMenorDataMovBancariaPorConta,
			Map<BigDecimal, String> mapaInfFinanceiroBaixado,
			Map<BigDecimal, BigDecimal> mapaInfFinanceiroValor,
			Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco) throws Exception {

		System.out.println("Entrou no job baixa");
		
		EnviromentUtils util = new EnviromentUtils();
		
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

		String dataEstorno = "";

		BigDecimal nufin = BigDecimal.ZERO;
		
		String idAluno = "";
		
		Map<BigDecimal, String> mapIdBaixaAtual = new HashMap<BigDecimal, String>();

		try {

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
					
					idAluno = jsonObject.get("aluno_id").getAsString().trim();
					
					System.out.println("IdAluno: " + idAluno);
					
					BigDecimal codParc = mapaInfAlunos.get(idAluno);
					
					if (codParc != null) {
						
						String tituloId = jsonObject.get("titulo_id").getAsString();
						String baixaId = jsonObject.get("baixa_id").getAsString();

						BigDecimal vlrBaixa = new BigDecimal(jsonObject.get(
								"baixa_valor").getAsString());

						BigDecimal vlrJuros = new BigDecimal(jsonObject.get(
								"baixa_juros").getAsString());

						BigDecimal vlrMulta = new BigDecimal(jsonObject.get(
								"baixa_multa").getAsString());

						BigDecimal vlrDesconto = new BigDecimal(jsonObject.get(
								"baixa_desconto").getAsString());

						BigDecimal vlrOutrosAcrescimos = new BigDecimal(jsonObject
								.get("baixa_outros_acrescimos").getAsString());

						String dataBaixa = jsonObject.get("baixa_data")
								.getAsString();

						Date data = formatoOriginal.parse(dataBaixa);

						String dataBaixaFormatada = formatoDesejado.format(data);

						nufin = mapaInfFinanceiro.get(codemp + "###" + tituloId);

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

						// codBanco = getCodBanco(idExterno, codemp);
						// Buscar pelo mapa
						codBanco = mapaInfBanco.get(codemp + "###" + idExterno);
						
						System.out.println("Banco: " + codBanco);
						
						codConta = mapaInfConta.get(codemp + "###" + idExterno);
						
						System.out.println("Conta: " + codConta);
						
						JsonArray formas_de_pagamento = jsonObject
								.getAsJsonArray("formas_de_pagamento");
						
						System.out.println("quantidade de formar de pagamento: " + formas_de_pagamento.size());
						
						BigDecimal taxaCartao = BigDecimal.ZERO;
						
						

						if (nufin != null && nufin.compareTo(BigDecimal.ZERO) != 0 && formas_de_pagamento.size() == 1) {
							
							for (JsonElement formas_de_pagamentoElement : formas_de_pagamento) {

								JsonObject formas_de_pagamentoObject = formas_de_pagamentoElement
										.getAsJsonObject();

								System.out.println("Forma de pagamento: "
										+ formas_de_pagamentoObject.get(
												"forma_pagamento_id").getAsString());
								
								System.out.println("codemp: " + codemp);

								codTipTit = mapaInfTipoTitulo.get(codemp
										+ "###"
										+ formas_de_pagamentoObject.get(
												"forma_pagamento_id").getAsString().trim());
								
								System.out.println("Tipo de Titulo Achado: " + codTipTit);
								
								taxaCartao = Optional.ofNullable(mapaInfTipoTituloTaxa.get(codemp
										+ "###"
										+ formas_de_pagamentoObject.get(
												"forma_pagamento_id").getAsString())).orElse(BigDecimal.ZERO);
							}
							
							if(taxaCartao.compareTo(BigDecimal.ZERO) != 0){
								vlrBaixa.subtract( vlrBaixa.multiply(taxaCartao).divide(BigDecimal.valueOf(100)) );
							}

							System.out.println("estorno: " + dataEstorno);
							System.out.println("Data estorno: "
									+ jsonObject.get("baixa_estorno_data"));
							
							Date dtMinMovConta = mapaInfMenorDataMovBancariaPorConta
									.get(Long.parseLong(codConta.toString()));
							
							System.out.println("dtMinMovConta: " + dtMinMovConta);

							if (data.after(dtMinMovConta) && codTipTit.compareTo(BigDecimal.ZERO) != 0) {
								if (dataEstorno == null) {

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
													vlrJuros, vlrMulta,
													vlrOutrosAcrescimos);
										} else {
											System.out
													.println("Entrou no else do valor");
											updateFinComVlrBaixa(codTipTit, nufin,
													codBanco, codConta, vlrBaixa,
													vlrDesconto, vlrJuros,
													vlrMulta, vlrOutrosAcrescimos, baixaId);
										}
										
										System.out.println("vlrDesconto: "
												+ vlrDesconto);
										System.out.println("vlrJuros: " + vlrJuros);
										System.out.println("vlrMulta: " + vlrMulta);

										nubco = insertMovBancaria(codConta,
												vlrBaixa, nufin, dataBaixaFormatada);

										System.out
												.println("Passou da mov bancaria: "
														+ nubco);

										System.out.println("vlrBaixa: " + vlrBaixa);

										updateBaixa(nufin, nubco, vlrBaixa,
												dataBaixaFormatada, baixaId);
										
										mapIdBaixaAtual.put(nufin, baixaId);
										
										mapaInfFinanceiroBaixado.put(nufin, "S");
										
										movBanc = true;

										/*
										 * insertLogIntegracao(
										 * "Baixa Efetuada Com Sucesso Para o Financeiro: "
										 * + nufin, "Sucesso");
										 */
									} else {
										
										System.out.println("Titulo ja baixado");
										
										String baixaIdExist = Optional.ofNullable(mapaInfIdBaixa.get(nufin)).orElse("");
										
										String baixaIdAtual = Optional.ofNullable(mapIdBaixaAtual.get(nufin)).orElse("");
										
										if((!baixaIdExist.isEmpty() 
												&& !baixaIdExist.equalsIgnoreCase(baixaId)
												&& !baixaIdExist.equalsIgnoreCase("N"))
												|| (!baixaIdAtual.isEmpty() 
														&& !baixaIdAtual.equalsIgnoreCase(baixaId))){
											
											System.out.println("Baixa Dupla");
											
											BigDecimal nufinDup = insertFin(nufin, vlrBaixa, codTipTit, codemp);
											
											updateFinComVlrBaixa(codTipTit, nufinDup,
													codBanco, codConta, vlrBaixa,
													vlrDesconto, vlrJuros,
													vlrMulta, vlrOutrosAcrescimos, baixaId);
											
											nubco = insertMovBancaria(codConta,
													vlrBaixa, nufinDup, dataBaixaFormatada);

											movBanc = true;

											updateBaixa(nufinDup, nubco, vlrBaixa,
													dataBaixaFormatada, baixaId);
											
											System.out.println("Fim baixa dupla");
											
										}
										
									}
								} else {
									if ("S".equalsIgnoreCase(mapaInfFinanceiroBaixado
											.get(nufin))) {

										nubco = mapaInfFinanceiroBanco.get(nufin);
										updateFinExtorno(nufin);
										deleteTgfMbc(nubco);
										deleteTgfFin(nufin);
										
										/*
										 * insertLogIntegracao(
										 * "Estorno Efetuado com sucesso",
										 * "Sucesso");
										 */

									}
								}
							} else {
								util.inserirLog("Baixa Para o Titulo: " +
										  nufin +
										  " Não Efetuada Pois a Data Minima de Movimentação Bancaria "
										  + "Para a Conta " +codConta+
										  " é Superior a Data de Baixa: " +
										  dataBaixaFormatada, "Aviso", idAluno, codemp);
							}
							
						} else if(nufin != null && nufin.compareTo(BigDecimal.ZERO) != 0 && formas_de_pagamento.size() > 1){
							
							System.out.println("entrou em mais de uma forma de pagamento");
							
							int countBaixa = 0;
							
							for (JsonElement formas_de_pagamentoElement : formas_de_pagamento) {

								JsonObject formas_de_pagamentoObject = formas_de_pagamentoElement
										.getAsJsonObject();
								
								System.out.println("Forma de pagamento: "
										+ formas_de_pagamentoObject.get(
												"forma_pagamento_id"));
								
								System.out.println("codemp: " + codemp);
								
								codTipTit = Optional.ofNullable(mapaInfTipoTitulo.get(codemp
										+ "###"
										+ formas_de_pagamentoObject.get(
												"forma_pagamento_id").getAsString().trim())).orElse(BigDecimal.ZERO);
								
								System.out.println("Tipo de titulo: " + codTipTit);
								
								taxaCartao = Optional.ofNullable(mapaInfTipoTituloTaxa.get(codemp
										+ "###"
										+ formas_de_pagamentoObject.get(
												"forma_pagamento_id").getAsString())).orElse(BigDecimal.ZERO);
								
								vlrBaixa = formas_de_pagamentoObject.get(
										"forma_pagamento_valor").getAsBigDecimal();
								
								if(taxaCartao.compareTo(BigDecimal.ZERO) != 0){
									vlrBaixa.subtract( vlrBaixa.multiply(taxaCartao).divide(BigDecimal.valueOf(100)) );
								}

								System.out.println("estorno: " + dataEstorno);
								System.out.println("Data estorno: "
										+ jsonObject.get("baixa_estorno_data"));
								
								Date dtMinMovConta = mapaInfMenorDataMovBancariaPorConta
										.get(Long.parseLong(codConta.toString()));
								
								System.out.println("dtMinMovConta: " + dtMinMovConta);
								
								if(countBaixa == 0){
									System.out.println("contagem 1");
									if (data.after(dtMinMovConta) && codTipTit.compareTo(BigDecimal.ZERO) != 0) {
										if (dataEstorno == null) {

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
															vlrJuros, vlrMulta,
															vlrOutrosAcrescimos);
												} else {
													System.out
															.println("Entrou no else do valor");
													updateFinComVlrBaixa(codTipTit, nufin,
															codBanco, codConta, vlrBaixa,
															vlrDesconto, vlrJuros,
															vlrMulta, vlrOutrosAcrescimos, baixaId);
												}

												System.out.println("vlrDesconto: "
														+ vlrDesconto);
												System.out.println("vlrJuros: " + vlrJuros);
												System.out.println("vlrMulta: " + vlrMulta);

												/*
												 * updateFin(codTipTit, nufin, codBanco,
												 * codConta, vlrDesconto, vlrJuros,
												 * vlrMulta);
												 */

												nubco = insertMovBancaria(codConta,
														vlrBaixa, nufin, dataBaixaFormatada);

												System.out
														.println("Passou da mov bancaria: "
																+ nubco);

												System.out.println("vlrBaixa: " + vlrBaixa);

												updateBaixaParcial(nufin, nubco, vlrBaixa,
														dataBaixaFormatada);

												movBanc = true;

												/*
												 * insertLogIntegracao(
												 * "Baixa Efetuada Com Sucesso Para o Financeiro: "
												 * + nufin, "Sucesso");
												 */
											} else {
												System.out.println("Financeiro " + nufin
														+ " já baixado");
											}
										} else {
											if ("S".equalsIgnoreCase(mapaInfFinanceiroBaixado
													.get(nufin))) {

												nubco = mapaInfFinanceiroBanco.get(nufin);
												updateFinExtorno(nufin);
												deleteTgfMbc(nubco);
												deleteTgfFin(nufin);
												
												/*
												 * insertLogIntegracao(
												 * "Estorno Efetuado com sucesso",
												 * "Sucesso");
												 */

											}
										}
									} else {
										
										util.inserirLog("Baixa Para o Titulo: " +
												  nufin +
												  " Não Efetuada Pois a Data Minima de Movimentação Bancaria "
												  + "Para a Conta " +codConta+
												  " é Superior a Data de Baixa: " +
												  dataBaixaFormatada, "Aviso", idAluno, codemp);
									}
									
								}else if (codTipTit != null && codTipTit.compareTo(BigDecimal.ZERO) != 0 && countBaixa > 0){
									System.out.println("contagem 2");
									BigDecimal nufinDup = insertFin(nufin, vlrBaixa, codTipTit, codemp);
									
									nubco = insertMovBancaria(codConta,
											vlrBaixa, nufinDup, dataBaixaFormatada);

									movBanc = true;
									System.out
											.println("Passou da mov bancaria duplicada: "
													+ nubco);

									System.out.println("vlrBaixa: " + vlrBaixa);

									updateBaixa(nufinDup, nubco, vlrBaixa,
											dataBaixaFormatada, baixaId);

									
								}
								
								countBaixa++;
							}
							
						}else {
							System.out
									.println("Não foi possivel encontrar financeiro com id externo "
											+ tituloId);
						}
						
					}
					
					movBanc = false;
					
					nubco = BigDecimal.ZERO;
					
				}

		} catch (Exception e) {
			e.printStackTrace();

			if (movBanc) {
				updateFinExtorno(nufin);
				deleteTgfMbc(nubco);
				System.out.println("Apagou mov bank");
			}

			try {
				util.inserirLog(
						"Mensagem de erro nas Baixas: " + e.getMessage(),
						"Erro", idAluno, codemp);
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		} finally {
			
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

	public void updateFin(BigDecimal codtiptit, BigDecimal nufin,
			BigDecimal codBanco, BigDecimal codConta, BigDecimal vlrDesconto,
			BigDecimal vlrJuros, BigDecimal vlrMulta,
			BigDecimal vlrOutrosAcrescimos) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		try {

			jdbc.openSession();

			String sqlNota = "UPDATE TGFFIN SET " + "CODTIPTIT = ?, "
					+ "CODBCO = ?, " + "CODCTABCOINT = ?, ";

			sqlNota += "AD_VLRDESCINT = " + vlrDesconto + ", ";
			sqlNota += "VLRINSS = 0, " + "VLRIRF = 0, " + "VLRISS = 0, ";
			sqlNota += "AD_VLRMULTAINT = " + vlrMulta + ", ";
			sqlNota += "AD_VLRJUROSINT = " + vlrJuros + ", AD_OUTACRESCIMOS = "
					+ vlrOutrosAcrescimos;
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
			BigDecimal vlrOutrosAcrescimos, String baixaId) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		try {

			jdbc.openSession();

			String sqlNota = "UPDATE TGFFIN SET " + "CODTIPTIT = ?, "
					+ "CODBCO = ?, " + "CODCTABCOINT = ?, "
					+ "AD_VLRDESCINT = " + vlrDesconto + ", " + "VLRINSS = 0, "
					+ "VLRIRF = 0, " + "VLRISS = 0, " + "AD_VLRJUROSINT = "
					+ vlrJuros + ", " + "AD_VLRMULTAINT = " + vlrMulta + ", "
					+ "TIPJURO = null, AD_VLRORIG = VLRDESDOB, "
					+ "VLRDESDOB = " + vlrBaixa + ", "
					+ "TIPMULTA = null, AD_OUTACRESCIMOS = "
					+ vlrOutrosAcrescimos +", AD_BAIXAID = "+baixaId+" WHERE nufin = ?";

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
			BigDecimal vlrDesdob, String dataBaixaFormatada, String baixaId) throws Exception {
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
					+ "CODUSUBAIXA = 0, AD_BAIXAID = "+baixaId+"  " + "WHERE NUFIN = " + nufin;

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
	
	public void updateBaixaParcial(BigDecimal nufin, BigDecimal nubco,
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
					+ "CODUSUBAIXA = 0, AD_BAIXAPARCIAL = 'S'  " + "WHERE NUFIN = " + nufin;
			
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
		} finally {
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
		
		EnviromentUtils util = new EnviromentUtils();

		BigDecimal nubco = util.getMaxNumMbc();

		try {

			jdbc.openSession();

			String sqlUpdate = "INSERT INTO TGFMBC " + "(NUBCO, " + "CODLANC, "
					+ "DTLANC, " + "CODTIPOPER, " + "DHTIPOPER, "
					+ "DTCONTAB, " + "HISTORICO, " + "CODCTABCOINT, "
					+ "NUMDOC, " + "VLRLANC, " + "TALAO, " + "PREDATA, "
					+ "CONCILIADO, " + "DHCONCILIACAO, " + "ORIGMOV, "
					+ "NUMTRANSF, " + "RECDESP, " + "DTALTER, "
					+ "DTINCLUSAO, " + "CODUSU, " + "VLRMOEDA, " + "SALDO, "
					+ "CODCTABCOCONTRA, " + "NUBCOCP, " + "CODPDV) "
					+ " VALUES ("
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
					+ "'"
					+ dataBaixaFormatada
					+ "', " // dtneg2
					+ "'N', "
					+ "NULL, "
					+ "'F', "
					+ "NULL, "
					+ "1, "
					+ "SYSDATE, " + "SYSDATE, " + "0, " // Usuario
					+ "0, " + "" + vlrDesdob + ", " // vlrDesdob
					+ "NULL,  " + "NULL, " + "NULL) ";

			pstmt = jdbc.getPreparedStatement(sqlUpdate);
			pstmt.executeUpdate();

		} catch (Exception se) {
			se.printStackTrace();
			throw se;
		} finally {
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

			String sqlUpdate = "UPDATE AD_ALUNOS SET INTEGRADO_BAIXA = 'S' WHERE ID_EXTERNO = '"
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
			System.out.println("Entrou no UPDATE da flag dos alunos baixa");
			jdbc.openSession();

			String sqlUpdate = "UPDATE AD_ALUNOS SET INTEGRADO_BAIXA = 'N'";

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

	private void updateCarga(BigDecimal idCarga) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		try {
			jdbc.openSession();

			String sqlUpd = "UPDATE AD_CARGAALUNOS SET INTEGRADO_BAIXA = 'S' WHERE IDCARGA = "
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

			String sqlUpd = "UPDATE AD_CARGAALUNOS SET INTEGRADO_BAIXA = 'N' WHERE CODEMP = "
					+ codEmp + " AND INTEGRADO_BAIXA = 'S'";

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

	/* Novos */

	public long printLogDebug(long tempoAnterior, String msgRetornoLog) {
		// Log Tempo
		boolean modoDebug = true;
		if (modoDebug) {
			long tempoAgora = System.currentTimeMillis();
			long diffInSeconds = tempoAgora - tempoAnterior;
			tempoAnterior = tempoAgora;
			System.out.println(msgRetornoLog + " : " + diffInSeconds);
		}

		return tempoAnterior;
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

	public List<Object[]> retornarInformacoesAlunos() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		List<Object[]> listRet = new ArrayList<>();
		try {
			jdbc.openSession();
			String sql = "	SELECT 	CODPARC, ID_EXTERNO ";
			sql += "		FROM  	AD_ALUNOS ";
			pstmt = jdbc.getPreparedStatement(sql);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				Object[] ret = new Object[2];
				ret[0] = rs.getBigDecimal("CODPARC");
				ret[1] = rs.getString("ID_EXTERNO");

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
				ret[2] = rs.getString("IDEXTERNO").trim();
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
	
	public BigDecimal insertFin(BigDecimal nufinOrig, 
			BigDecimal vlrDesdob, BigDecimal codTipTit,
			BigDecimal codemp)
			throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		
		EnviromentUtils util = new EnviromentUtils();
		
		System.out.println("Chegou no insert do financeiro segundo");
		
		BigDecimal nufin = util.getMaxNumFin(true);

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
					+ "         AD_IDALUNO, AD_NUFINORIG, AD_BAIXAPARCIAL) " + "        "
					
					+ " (SELECT "+nufin+", NULL, 0, 'F', recDesp ,codemp ,codCenCus ,codNat ,codTipOper ,(SELECT MAX(DHALTER) FROM TGFTOP WHERE CODTIPOPER = TGFFIN.codTipOper), 0, (SELECT MAX(DHALTER) FROM TGFTOP WHERE CODTIPOPER = 0), codparc ,"+codTipTit+", "+vlrDesdob+", 0, 0, CODBCO, CODCTABCOINT, DTNEG , SYSDATE, SYSDATE, DTVENC , SYSDATE, DTVENCINIC , 1 , 1 , null , 'I' , 'N' , 'N' , 'N' , 'N' , 'N' , 'N' , 'N' , 'S' , 'S' , 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, NULL, AD_IDALUNO, "+nufinOrig+", 'S' FROM TGFFIN WHERE NUFIN = "+nufinOrig+")";

			pstmt = jdbc.getPreparedStatement(sqlUpdate);

			pstmt.executeUpdate();

		} catch (Exception se) {
			se.printStackTrace();
			try {
				util.inserirLog(
						"Erro ao gerar financeiro parcial, Nufin Orig: "+nufin+"\nMensagem de erro: "
								+ se.getMessage(), "Erro", "", codemp);
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			throw se;
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
	
	public List<Object[]> retornarInformacoesIdBaixa() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		List<Object[]> listRet = new ArrayList<>();
		try {
			jdbc.openSession();
			
			String sql = "	SELECT 	NUFIN, NVL(AD_BAIXAID, 'N') as BAIXAID";
			sql += "		FROM  	TGFFIN ";
			sql += "		WHERE  	RECDESP = 1 ";
			sql += "		    AND PROVISAO = 'N' "
				+  "            AND DHBAIXA IS NOT NULL"
				+  "            AND AD_IDALUNO IS NOT NULL";
			
			pstmt = jdbc.getPreparedStatement(sql);
			rs = pstmt.executeQuery();
			
			while (rs.next()) {
				Object[] ret = new Object[2];
				ret[0] = rs.getBigDecimal("NUFIN");
				ret[1] = rs.getString("BAIXAID");
				
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
	
}

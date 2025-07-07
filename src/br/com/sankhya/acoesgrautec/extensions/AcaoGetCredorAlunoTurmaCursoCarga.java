package br.com.sankhya.acoesgrautec.extensions;

import br.com.sankhya.acoesgrautec.util.EnviromentUtils;
import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.extensions.actionbutton.Registro;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import br.com.sankhya.modelcore.util.SWRepositoryUtils;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;


import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.activiti.engine.impl.util.json.JSONArray;
import org.cuckoo.core.ScheduledAction;
import org.cuckoo.core.ScheduledActionContext;

public class AcaoGetCredorAlunoTurmaCursoCarga
  implements AcaoRotinaJava, ScheduledAction
 {
	
	private List<String> selectsParaInsert = new ArrayList<String>();
	private EnviromentUtils util = new EnviromentUtils();
	   static {
		   LogConfiguration.setPath(SWRepositoryUtils.getBaseFolder() + "/logAcao/logs");
	    }
	
	   @Override
	   public void doAction(ContextoAcao contexto) throws Exception {
	       String threadName = Thread.currentThread().getName();
	       Date startTime = new Date();

	       System.out.println("=== INÍCIO DO BOTÃO DE AÇÃO === Thread: " + threadName + " - Hora: " + startTime);
	       LogCatcher.logInfo("\n=== INÍCIO DO BOTÃO DE AÇÃO DO CADASTRO DE ALUNO=== Thread: " + threadName + " - Hora: " + startTime);

	       System.out.println("Iniciando processamento de dados");
	       LogCatcher.logInfo("Iniciando processamento de dados.");

	       Registro[] linhas = contexto.getLinhas();

	       if (linhas.length < 1) {
	           System.out.println("Ação cancelada: Nenhuma linha selecionada");
	           LogCatcher.logError("Ação interrompida: Nenhuma linha selecionada.");
	           contexto.mostraErro("É necessário selecionar pelo menos 1 linha.");
	           return;
	       }

	       int linhasParaProcessar = Math.min(linhas.length, 65);
	       System.out.println("Processando " + linhasParaProcessar + " registro(s)");
	       LogCatcher.logInfo("Processando " + linhasParaProcessar + " registro(s)");

	       EntityFacade entityFacade = null;
	       JdbcWrapper jdbc = null;

	       try {
	           entityFacade = EntityFacadeFactory.getDWFFacade();
	           jdbc = entityFacade.getJdbcWrapper();
	           jdbc.openSession();

	           System.out.println("Sessão JDBC aberta");
	           LogCatcher.logInfo("Sessão JDBC aberta com sucesso.");

	           System.out.println("Carregando informações de alunos...");
	           LogCatcher.logInfo("Carregando informações de alunos...");

	           Map<String, BigDecimal> mapaInfAlunos = retornarInformacoesAlunos().stream()
	               .collect(HashMap::new,
	                   (map, obj) -> {
	                       BigDecimal codParc = (BigDecimal) obj[0];
	                       String idExternoObj = (String) obj[1];
	                       BigDecimal codemp = (BigDecimal) obj[2];
	                       String chave = idExternoObj + "###" + codemp;
	                       map.putIfAbsent(chave, codParc);
	                   },
	                   HashMap::putAll);

	           System.out.println("Carregando informações de parceiros...");
	           LogCatcher.logInfo("Carregando informações de parceiros...");

	           Map<String, BigDecimal> mapaInfParceiros = retornarInformacoesParceiros().stream()
	               .collect(HashMap::new,
	                   (map, obj) -> {
	                       BigDecimal codParc = (BigDecimal) obj[0];
	                       String cpf_cnpj = (String) obj[1];
	                       map.putIfAbsent(cpf_cnpj, codParc);
	                   },
	                   HashMap::putAll);

	           String dataInicio = contexto.getParam("DTINICIO").toString().substring(0, 10);
	           String dataFim = contexto.getParam("DTFIM").toString().substring(0, 10);
	           String matricula = (String) contexto.getParam("Matricula");

	           System.out.println("Processando período de " + dataInicio + " até " + dataFim + " para matrícula: " + matricula);
	           LogCatcher.logInfo("Processando período de " + dataInicio + " até " + dataFim + " para matrícula: " + matricula);

	           for (int i = 0; i < linhasParaProcessar; i++) {
	               Registro registro = linhas[i];

	               String url = (String) registro.getCampo("URL");
	               String token = (String) registro.getCampo("TOKEN");
	               BigDecimal codEmp = (BigDecimal) registro.getCampo("CODEMP");

	               System.out.println("Processando registro " + (i + 1) + " de " + linhasParaProcessar + " - Empresa: " + codEmp);
	               LogCatcher.logInfo("Processando registro " + (i + 1) + " de " + linhasParaProcessar + " - Empresa: " + codEmp);

	               processDateRangeByMonths(mapaInfAlunos, mapaInfParceiros, url, token, codEmp, dataInicio, dataFim, matricula);
	           }

	           contexto.setMensagemRetorno("Período processado com sucesso para " + linhasParaProcessar + " registro(s)!");
	           System.out.println("Finalizou o processamento do período");
	           LogCatcher.logInfo("Finalizou o processamento do período");

	       } catch (Exception e) {
	           System.out.println("Erro na ação - Thread: " + threadName);
	           e.printStackTrace();

	           LogCatcher.logError("Erro na execução da ação - Thread: " + threadName + " - Erro: " + e.getMessage());
	           LogCatcher.logError(e);

	           try {
	               Registro primeiroRegistro = linhas[0];
	               String url = (String) primeiroRegistro.getCampo("URL");
	               String token = (String) primeiroRegistro.getCampo("TOKEN");

	               insertLogIntegracao("Erro ao processar período: " + e.getMessage(), "Erro", url, token);
	           } catch (Exception e1) {
	               e1.printStackTrace();
	               LogCatcher.logError("Erro secundário ao registrar falha na integração: " + e1.getMessage());
	           }

	           contexto.mostraErro("Ocorreu um erro durante o processamento: " + e.getMessage());

	       } finally {
	           if (jdbc != null) {
	               jdbc.closeSession();
	               System.out.println("Sessão JDBC encerrada");
	               LogCatcher.logInfo("Sessão JDBC encerrada.");
	           }

	           processarLogsPendentes();

	           Date endTime = new Date();
	           System.out.println("=== FIM DO BOTÃO DE AÇÃO === Thread: " + threadName + " - Hora: " + endTime);
	           LogCatcher.logInfo("\n=== FIM DO BOTÃO DE AÇÃO === Thread: " + threadName + " - Hora: " + endTime);
	       }
	   }


	
	
	/**
	 * Versão otimizada do onTime
	 */
	   @Override
	   public void onTime(ScheduledActionContext arg0) {
	       String threadName = Thread.currentThread().getName();
	       Date startTime = new Date();

	       System.out.println("=== INÍCIO DO JOB === Thread: " + threadName + " - Hora: " + startTime);
	       LogCatcher.logInfo("\n=== INÍCIO DO JOB DE CADASTRO DE ALUNO === Thread: " + threadName + " - Hora: " + startTime);

	       System.out.println("Iniciou cadastro dos alunos");
	       LogCatcher.logInfo("Iniciou cadastro dos alunos");

	       EntityFacade entityFacade = null;
	       JdbcWrapper jdbc = null;

	       try {
	           entityFacade = EntityFacadeFactory.getDWFFacade();
	           jdbc = entityFacade.getJdbcWrapper();
	           jdbc.openSession();

	           System.out.println("Iniciando carregamento de informações - Thread: " + threadName);
	           LogCatcher.logInfo("Iniciando carregamento de informações - Thread: " + threadName);

	           Map<String, BigDecimal> mapaInfAlunos = retornarInformacoesAlunos().stream()
	               .collect(HashMap::new, 
	                   (map, obj) -> {
	                       BigDecimal codParc = (BigDecimal) obj[0];
	                       String idExternoObj = (String) obj[1];
	                       BigDecimal codemp = (BigDecimal) obj[2];
	                       String chave = idExternoObj + "###" + codemp;
	                       map.putIfAbsent(chave, codParc);
	                   },
	                   HashMap::putAll);

	           Map<String, BigDecimal> mapaInfParceiros = retornarInformacoesParceiros().stream()
	               .collect(HashMap::new,
	                   (map, obj) -> {
	                       BigDecimal codParc = (BigDecimal) obj[0];
	                       String cpf_cnpj = (String) obj[1];
	                       map.putIfAbsent(cpf_cnpj, codParc);
	                   },
	                   HashMap::putAll);

	           System.out.println("Iniciando processamento das empresas - Thread: " + threadName);
	           LogCatcher.logInfo("Iniciando processamento das empresas - Thread: " + threadName);

	           processarEmpresas(jdbc, mapaInfAlunos, mapaInfParceiros, threadName);

	           System.out.println("Finalizou o cadastro dos alunos");
	           LogCatcher.logInfo("Finalizou o cadastro dos alunos");

	       } catch (Exception e) {
	           System.out.println("Erro no job - Thread: " + threadName);
	           e.printStackTrace();
	           LogCatcher.logError("Erro na execução do job - Thread: " + threadName + " - Erro: " + e.getMessage());
	           LogCatcher.logError(e);

	           try {
	               insertLogIntegracao(
	                   "Erro ao integrar Alunos e/ou Credor, Mensagem de erro: " + e.getMessage(), 
	                   "Erro", "", "");
	           } catch (Exception e1) {
	               e1.printStackTrace();
	               LogCatcher.logError("Erro secundário ao registrar falha no log de integração: " + e1.getMessage());
	           }

	       } finally {
	           if (jdbc != null) {
	               jdbc.closeSession();
	               System.out.println("Sessão JDBC encerrada");
	               LogCatcher.logInfo("Sessão JDBC encerrada");
	           }

	           processarLogsPendentes();

	           Date endTime = new Date();
	           System.out.println("=== FIM DO JOB === Thread: " + threadName + " - Hora: " + endTime);
	           LogCatcher.logInfo("\n=== FIM DO JOB DO CADASTRO DE ALUNO=== Thread: " + threadName + " - Hora: " + endTime);
	       }
	   }


		private void processarEmpresas(JdbcWrapper jdbc, Map<String, BigDecimal> mapaInfAlunos,
				Map<String, BigDecimal> mapaInfParceiros, String threadName) throws Exception {

			String queryEmp = "SELECT CODEMP, URL, TOKEN, INTEGRACAO FROM AD_LINKSINTEGRACAO";

			try (PreparedStatement pstmt = jdbc.getPreparedStatement(queryEmp); ResultSet rs = pstmt.executeQuery()) {

				while (rs.next()) {
					System.out.println("While principal");
					LogCatcher.logInfo("\nIniciando iteração no while principal de empresas do cadastro de aluno");

					BigDecimal codEmp = rs.getBigDecimal("CODEMP");
					String url = rs.getString("URL");
					String token = rs.getString("TOKEN");
					String statusIntegracao = rs.getString("INTEGRACAO");

					if (!"S".equals(statusIntegracao)) {
						System.out
								.println("Integração desativada para a empresa " + codEmp + " - pulando processamento");
						LogCatcher
								.logInfo("Integração desativada para a empresa " + codEmp + " - pulando processamento");
						continue;
					}

					System.out.println("Processando empresa " + codEmp + " na thread " + threadName);
					LogCatcher.logInfo("Processando empresa " + codEmp + " na thread " + threadName);

					iterarEndpoint(mapaInfAlunos, mapaInfParceiros, url, token, codEmp);

					System.out.println("Finalizou processamento da empresa " + codEmp + " na thread " + threadName);
					LogCatcher.logInfo("\nFinalizou processamento da empresa do cadastro de Aluno " + codEmp + " na thread " + threadName);
				}
			}
		}


	private void processarLogsPendentes() {
	    if (selectsParaInsert.isEmpty()) {
	        return;
	    }
	    
	    System.out.println("Entrou na lista do finally: " + selectsParaInsert.size());
	    
	    StringBuilder msgError = new StringBuilder();
	    int qtdInsert = selectsParaInsert.size();
	    
	    for (int i = 0; i < qtdInsert; i++) {
	        String sqlInsert = selectsParaInsert.get(i);
	        int nuFin;
	        
	        try {
	            nuFin = util.getMaxNumLog();
	        } catch (Exception e) {
	            e.printStackTrace();
	            continue;
	        }
	        
	        String sql = sqlInsert.replace("<#NUMUNICO#>", String.valueOf(nuFin));
	        msgError.append(sql);
	        
	        if (i < qtdInsert - 1) {
	            msgError.append(" \nUNION ALL ");
	        }
	    }
	    
	    System.out.println("Consulta de log: \n" + msgError);
	    LogCatcher.logInfo("Consulta de log pendente a ser executada: \n" + msgError);

	    try {
	        insertLogList(msgError.toString());
	    } catch (Exception e) {
	        e.printStackTrace();
	        LogCatcher.logError("Erro ao executar insertLogList: " + e.getMessage());
	    }

	    
	    // Limpa a lista
	    selectsParaInsert.clear();
	}
	
	/**
	 * Versão otimizada do onTime -FIM-
	 */
		
		

	
	//Retorna informações dos parceiros (credores) do banco de dados.
	private List<Object[]> retornarInformacoesParceiros() throws Exception {
	    EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	    JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
	    PreparedStatement pstmt = null;
	    ResultSet rs = null;
	    List<Object[]> listRet = new ArrayList<>();

	    try {
	        jdbc.openSession();
	        String sql = "SELECT CODPARC, CGC_CPF FROM TGFPAR";
	        
	        System.out.println("Executando query: " + sql);
	        LogCatcher.logInfo("Executando consulta de parceiros: " + sql);

	        pstmt = jdbc.getPreparedStatement(sql);
	        rs = pstmt.executeQuery();

	        while (rs.next()) {
	            Object[] ret = new Object[2];
	            ret[0] = rs.getBigDecimal("CODPARC");
	            ret[1] = rs.getString("CGC_CPF");

	            listRet.add(ret);
	        }

	        System.out.println("Parceiros carregados: " + listRet.size());
	        LogCatcher.logInfo("Parceiros carregados: " + listRet.size());

	    } catch (SQLException e) {
	        String msg = "Erro ao buscar parceiros no método retornarInformacoesParceiros: " + e.getMessage();
	        insertLogIntegracao(msg, "Erro", "", "");
	        LogCatcher.logError(msg);
	        LogCatcher.logError(e);
	        throw e;
	    } finally {
	        if (rs != null) rs.close();
	        if (pstmt != null) pstmt.close();
	        jdbc.closeSession();
	    }

	    return listRet;
	}


	
	//Retorna informações dos alunos do banco de dados.
	public List<Object[]> retornarInformacoesAlunos() throws Exception {
	    EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	    JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
	    PreparedStatement pstmt = null;
	    ResultSet rs = null;
	    List<Object[]> listRet = new ArrayList<>();

	    try {
	        jdbc.openSession();
	        String sql = "SELECT CODPARC, ID_EXTERNO, CODEMP FROM AD_ALUNOS";
	        
	        System.out.println("Executando query: " + sql);
	        LogCatcher.logInfo("Executando consulta de alunos: " + sql);

	        pstmt = jdbc.getPreparedStatement(sql);
	        rs = pstmt.executeQuery();

	        while (rs.next()) {
	            Object[] ret = new Object[3];
	            ret[0] = rs.getBigDecimal("CODPARC");
	            ret[1] = rs.getString("ID_EXTERNO");
	            ret[2] = rs.getBigDecimal("CODEMP");

	            listRet.add(ret);
	        }

	        System.out.println("Alunos carregados: " + listRet.size());
	        LogCatcher.logInfo("Alunos carregados: " + listRet.size());

	    } catch (SQLException e) {
	        String msg = "Erro ao buscar alunos no método retornarInformacoesAlunos: " + e.getMessage();
	        insertLogIntegracao(msg, "Erro", "", "");
	        LogCatcher.logError(msg);
	        LogCatcher.logError(e);
	        throw e;
	    } finally {
	        if (rs != null) rs.close();
	        if (pstmt != null) pstmt.close();
	        jdbc.closeSession();
	    }

	    return listRet;
	}

	
	
	
	/**
	 * Processa um intervalo de datas agrupando por meses
	 */
	public void processDateRangeByMonths(Map<String, BigDecimal> mapaInfAlunos,
	        Map<String, BigDecimal> mapaInfParceiros, String url, String token, BigDecimal codEmp, String dataInicio,
	        String dataFim, String matricula) throws Exception {

	    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
	    LocalDate inicio = LocalDate.parse(dataInicio, formatter);
	    LocalDate fim = LocalDate.parse(dataFim, formatter);

	    LocalDate periodoInicio = inicio;

	    while (!periodoInicio.isAfter(fim)) {
	        LocalDate periodoFim = periodoInicio.plusMonths(1).withDayOfMonth(1).minusDays(1);
	        if (periodoFim.isAfter(fim)) {
	            periodoFim = fim;
	        }

	        String periodoTexto = "Processando período: " + periodoInicio + " até " + periodoFim;
	        System.out.println(periodoTexto);
	        LogCatcher.logInfo(periodoTexto);

	        try {
	            processDateRange(mapaInfAlunos, mapaInfParceiros, url, token, codEmp,
	                    periodoInicio.format(formatter),
	                    periodoFim.format(formatter), matricula);
	        } catch (Exception e) {
	            String mensagemErro = "Erro ao processar período " + periodoInicio.format(formatter) +
	                                  " até " + periodoFim.format(formatter) + ". Erro: " + e.getMessage();

	            insertLogIntegracao(mensagemErro, "ERRO", "", "");
	            System.err.println(mensagemErro);
	            e.printStackTrace();
	            LogCatcher.logError(mensagemErro);
	            LogCatcher.logError(e);
	        }

	        periodoInicio = periodoInicio.plusMonths(1).withDayOfMonth(1);
	    }
	}


	/**
	iterarendpoint das requisicoes
	novo método iterarendpoint com URL encoder
	Processa um intervalo de datas, buscando e cadastrando alunos e credores.
	correção do processDateRange  onde : Se todas as páginas completas retornam exatamente 100 registros (inclusive a última), o sistema nunca irá parar de fazer requisições porque a condição paginaAtual.length() < 100 nunca se tornará verdadeira.
	*/
	public void processDateRange(
	        Map<String, BigDecimal> mapaInfAlunos,
	        Map<String, BigDecimal> mapaInfParceiros,
	        String url,
	        String token,
	        BigDecimal codEmp,
	        String dataInicio,
	        String dataFim,
	        String matricula) throws Exception {

	    try {
	        String dataInicialCompleta = dataInicio + " 00:00:00";
	        String dataFinalCompleta = dataFim + " 23:59:59";

	        String dataInicialEncoded = URLEncoder.encode(dataInicialCompleta, "UTF-8");
	        String dataFinalEncoded = URLEncoder.encode(dataFinalCompleta, "UTF-8");

	        JSONArray todosRegistros = new JSONArray();
	        int pagina = 1;
	        boolean temMaisRegistros = true;

	        while (temMaisRegistros) {
	            StringBuilder urlBuilder = new StringBuilder();
	            urlBuilder.append(url.trim())
	                    .append("/alunos")
	                    .append("?pagina=").append(pagina)
	                    .append("&quantidade=100")
	                    .append("&dataInicial=").append(dataInicialEncoded)
	                    .append("&dataFinal=").append(dataFinalEncoded);

	            if (matricula != null && !matricula.isEmpty()) {
	                String matriculaEncoded = URLEncoder.encode(matricula, "UTF-8");
	                urlBuilder.append("&matricula=").append(matriculaEncoded);
	            }

	            String urlCompleta = urlBuilder.toString();
	            System.out.println("URL para alunos (página " + pagina + "): " + urlCompleta);
	            LogCatcher.logInfo("URL para alunos (página " + pagina + "): " + urlCompleta);

	            String[] response = apiGet2(urlCompleta, token);
	            int status = Integer.parseInt(response[0]);

	            if (status == 200) {
	                JSONArray paginaAtual = new JSONArray(response[1]);

	                if (paginaAtual.length() == 0) {
	                    temMaisRegistros = false;
	                    System.out.println("Página " + pagina + " vazia. Finalizando coleta de dados.");
	                    LogCatcher.logInfo("Página " + pagina + " vazia. Finalizando coleta de dados.");
	                } else {
	                    for (int i = 0; i < paginaAtual.length(); i++) {
	                        todosRegistros.put(paginaAtual.getJSONObject(i));
	                    }

	                    if (paginaAtual.length() < 100) {
	                        temMaisRegistros = false;
	                        System.out.println("Última página encontrada com " + paginaAtual.length() + " registros.");
	                        LogCatcher.logInfo("Última página encontrada com " + paginaAtual.length() + " registros.");
	                    } else {
	                        pagina++;
	                        System.out.println("Página " + (pagina - 1) + " completa com 100 registros. Avançando para página " + pagina);
	                        LogCatcher.logInfo("Página " + (pagina - 1) + " completa. Avançando para página " + pagina);
	                    }

	                    System.out.println("Página " + (pagina - 1) + ": " + paginaAtual.length() +
	                            " registros. Total acumulado: " + todosRegistros.length());
	                    LogCatcher.logInfo("Total acumulado até agora: " + todosRegistros.length());
	                }
	            } else if (status == 404) {
	                temMaisRegistros = false;
	                System.out.println("Página " + pagina + " não encontrada (404). Finalizando.");
	                LogCatcher.logInfo("Página " + pagina + " não encontrada (404). Finalizando.");
	            } else {
	                String erro = String.format("Erro na requisição. Status: %d. Resposta: %s. URL: %s", status, response[1], urlCompleta);
	                LogCatcher.logError(erro);
	                throw new Exception(erro);
	            }
	        }

	        if (todosRegistros.length() == 0) {
	            System.out.println("Nenhum registro de aluno encontrado.");
	            LogCatcher.logInfo("Nenhum registro de aluno encontrado.");
	            return;
	        }

	        System.out.println("Total de registros acumulados: " + todosRegistros.length());
	        LogCatcher.logInfo("\nTotal final de registros acumulados de Alunos: " + todosRegistros.length());

	        cadastrarCredorAlunoCursoTurma(mapaInfAlunos, mapaInfParceiros, todosRegistros.toString(), codEmp);

	    } catch (Exception e) {
	        String erro = "Erro ao processar período " + dataInicio + " até " + dataFim + ": " + e.getMessage();
	        System.err.println(erro);
	        e.printStackTrace();
	        LogCatcher.logError(erro);
	        LogCatcher.logError(e);
	        throw e;
	    }
	}

	
	
	// Itera sobre o endpoint de alunos para processar e cadastrar informações.
	public void iterarEndpoint(Map<String, BigDecimal> mapaInfAlunos,
	        Map<String, BigDecimal> mapaInfParceiros, String url, String token,
	        BigDecimal codEmp) throws Exception {

	    Date dataAtual = new Date();
	    SimpleDateFormat formato = new SimpleDateFormat("yyyy-MM-dd");
	    String dataFormatada = formato.format(dataAtual);

	    try {
	        System.out.println("While de iteração");
	        LogCatcher.logInfo("\nIniciando iteração de endpoint para empresa " + codEmp);

	        String urlFinal = url + "/alunos" + "?dataInicial=" + dataFormatada + " 00:00:00&dataFinal=" +
	                dataFormatada + " 23:59:59&quantidade=0";

	        String[] response = apiGet2(urlFinal, token);

	        int status = Integer.parseInt(response[0]);
	        System.out.println("Status teste: " + status);
	        System.out.println("response string: " + response[1]);

	        LogCatcher.logInfo("\nStatus da chamada: " + status + " | URL: " + urlFinal);

	        cadastrarCredorAlunoCursoTurma(mapaInfAlunos, mapaInfParceiros, response[1], codEmp);

	    } catch (Exception e) {
	        e.printStackTrace();
	        LogCatcher.logError("Erro na iteração do endpoint da empresa " + codEmp + ": " + e.getMessage());
	        LogCatcher.logError(e);
	        throw e;
	    }
	}

	
	

	
	//Retorna a última página processada do banco de dados.
	public int getPagina() throws Exception {
	    EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	    JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
	    PreparedStatement pstmt = null;
	    ResultSet rs = null;

	    int pagina = 0;

	    try {
	        jdbc.openSession();

	        String sqlSlt = "SELECT NVL((SELECT PAGINAATUAL FROM AD_BLOCOPAGINACAO " +
	                        "WHERE IDPAGINA = (SELECT MAX(IDPAGINA) FROM AD_BLOCOPAGINACAO)), 0) AS PAGINA FROM DUAL";

	        pstmt = jdbc.getPreparedStatement(sqlSlt);
	        rs = pstmt.executeQuery();

	        while (rs.next()) {
	            pagina = rs.getInt("PAGINA");
	        }

	        System.out.println("Última página consultada: " + pagina);
	        LogCatcher.logInfo("Última página retornada de AD_BLOCOPAGINACAO: " + pagina);

	    } catch (SQLException e) {
	        e.printStackTrace();
	        LogCatcher.logError("Erro ao recuperar última página: " + e.getMessage());
	        LogCatcher.logError(e);
	    } finally {
	        if (pstmt != null) pstmt.close();
	        if (rs != null) rs.close();
	        jdbc.closeSession();
	    }

	    return pagina;
	}


	
	public void cadastrarCredorAlunoCursoTurma(
	        Map<String, BigDecimal> mapaInfAlunos,
	        Map<String, BigDecimal> mapaInfParceiros,
	        String dadosCombinados,
	        BigDecimal codEmp) {

	    System.out.println("Cadastro principal iniciado");
	    LogCatcher.logInfo("\nIniciando cadastro de credor/aluno para CODEMP: " + codEmp);

	    EnviromentUtils util = new EnviromentUtils();

	    try {
	        JsonParser parser = new JsonParser();
	        JsonArray jsonArray = parser.parse(dadosCombinados).getAsJsonArray();

	        for (JsonElement jsonElement : jsonArray) {
	            JsonObject jsonObject = jsonElement.getAsJsonObject();

	            // Extração dos dados do credor
	            String credorNome = getJsonString(jsonObject, "credor_nome");
	            String credorCpf = getJsonString(jsonObject, "credor_cpf");
	            String credorEndereco = getJsonString(jsonObject, "credor_endereco");
	            String credorCep = getJsonString(jsonObject, "credor_endereco_cep");
	            String credorBairro = getJsonString(jsonObject, "credor_endereco_bairro");
	            String credorCidade = getJsonString(jsonObject, "credor_endereco_cidade");
	            String credorUf = getJsonString(jsonObject, "credor_endereco_uf");
	            String credorResidencial = getJsonString(jsonObject, "credor_telefone_residencial");
	            String credorCelular = getJsonString(jsonObject, "credor_telefone_celular");
	            String credorComercial = getJsonString(jsonObject, "credor_telefone_comercial");

	            // Extração dos dados do aluno
	            String alunoId = getJsonString(jsonObject, "aluno_id");
	            String alunoNome = getJsonString(jsonObject, "aluno_nome");
	            String alunoNomeSocial = getJsonString(jsonObject, "aluno_nome_social");
	            String alunoEndereco = getJsonString(jsonObject, "aluno_endereco");
	            String alunoCep = getJsonString(jsonObject, "aluno_endereco_cep");
	            String alunoBairro = getJsonString(jsonObject, "aluno_endereco_bairro");
	            String alunoCidade = getJsonString(jsonObject, "aluno_endereco_cidade");
	            String alunoUf = getJsonString(jsonObject, "aluno_endereco_uf");
	            String alunoSexo = getJsonString(jsonObject, "aluno_sexo");
	            String alunoDataNascimento = getJsonString(jsonObject, "aluno_data_nascimento");
	            String alunoRg = getJsonString(jsonObject, "aluno_rg");
	            String alunoCpf = getJsonString(jsonObject, "aluno_cpf");
	            String alunoCelular = getJsonString(jsonObject, "aluno_telefone_celular");
	            String alunoResidencial = getJsonString(jsonObject, "aluno_telefone_residencial");
	            String alunoEmail = getJsonString(jsonObject, "aluno_email");

	            // Curso e turma
	            JsonObject cursoObj = jsonObject.getAsJsonArray("cursos").get(0).getAsJsonObject();
	            String cursoDescricao = getJsonString(cursoObj, "curso_descricao");
	            String cursoId = getJsonString(cursoObj, "curso_id");
	            String turmaId = getJsonString(cursoObj, "turma_id");
	            String alunoSituacao = getJsonString(cursoObj, "situacao_descricao");
	            String alunoSituacaoId = getJsonString(cursoObj, "situacao_id");

	            // Validação básica
	            if (credorCpf == null || credorCidade == null) {
	                String msg = "Credor com informações inválidas ou nulas: CPF=" + credorCpf + ", Cidade=" + credorCidade;
	                System.out.println(msg);
	                LogCatcher.logInfo(msg);
	                selectsParaInsert.add("SELECT <#NUMUNICO#>, '" + msg + "', SYSDATE, 'Aviso', " + codEmp + ", '" + alunoId + "' FROM DUAL");
	                continue;
	            }

	            String credorCpfTrim = credorCpf.trim();
	            boolean credorExiste = mapaInfParceiros.containsKey(credorCpfTrim);
	            boolean alunoExiste = alunoId != null && mapaInfAlunos.containsKey(alunoId.trim() + "###" + codEmp);

	            if (!credorExiste) {
	                System.out.println("Cadastrando novo credor: " + credorCpfTrim);
	                LogCatcher.logInfo("Novo credor detectado: " + credorCpfTrim);

	                BigDecimal credorAtual = insertCredor(credorNome, credorCpf, credorEndereco, credorCep,
	                        credorBairro, credorCidade, credorUf, credorResidencial,
	                        credorCelular, credorComercial, alunoNome, codEmp, alunoId);

	                if (credorAtual != null) {
	                    mapaInfParceiros.put(credorCpfTrim, credorAtual);
	                    insertCursoTurma(cursoDescricao, cursoId, turmaId, credorNome, alunoNome, codEmp);

	                    if (!alunoExiste && alunoId != null) {
	                        insertAluno(credorAtual, alunoId, alunoNome, alunoNomeSocial, alunoEndereco,
	                                alunoCep, alunoBairro, alunoCidade, alunoUf, alunoSexo,
	                                alunoDataNascimento, alunoRg, alunoCpf, alunoCelular,
	                                alunoResidencial, alunoEmail, alunoSituacao, alunoSituacaoId,
	                                credorNome, codEmp, cursoDescricao, turmaId);

	                        mapaInfAlunos.put(alunoId.trim() + "###" + codEmp, BigDecimal.ONE);
	                        LogCatcher.logInfo("Aluno cadastrado para novo credor: " + alunoId);
	                    }
	                }
	            } else {
	                System.out.println("Credor já cadastrado: " + credorCpfTrim);
	                LogCatcher.logInfo("Credor já existente: " + credorCpfTrim);

	                BigDecimal credorCadastrado = mapaInfParceiros.get(credorCpfTrim);
	                insertCursoTurma(cursoDescricao, cursoId, turmaId, credorNome, alunoNome, codEmp);

	                if (!alunoExiste && alunoId != null) {
	                    insertAluno(credorCadastrado, alunoId, alunoNome, alunoNomeSocial, alunoEndereco,
	                            alunoCep, alunoBairro, alunoCidade, alunoUf, alunoSexo,
	                            alunoDataNascimento, alunoRg, alunoCpf, alunoCelular,
	                            alunoResidencial, alunoEmail, alunoSituacao, alunoSituacaoId,
	                            credorNome, codEmp, cursoDescricao, turmaId);

	                    mapaInfAlunos.put(alunoId.trim() + "###" + codEmp, BigDecimal.ONE);
	                    LogCatcher.logInfo("Aluno cadastrado para credor existente: " + alunoId);
	                } else if (alunoExiste) {
	                    updateAluno(alunoSituacaoId, alunoSituacao, alunoId, alunoEndereco,
	                            alunoCep, alunoBairro, alunoCidade, alunoUf);
	                    LogCatcher.logInfo("Aluno atualizado: " + alunoId + " | Situação: " + alunoSituacao);
	                }
	            }
	        }

	    } catch (Exception e) {
	        e.printStackTrace();
	        String erro = "Erro no chamado do endpoint: " + e.getMessage();
	        LogCatcher.logError(erro);
	        LogCatcher.logError(e);
	        selectsParaInsert.add("SELECT <#NUMUNICO#>, '" + erro + "', SYSDATE, 'Erro', " + codEmp + ", NULL FROM DUAL");
	    }
	}


	// Método auxiliar para extrair valores String de um JsonObject de forma segura
	private String getJsonString(JsonObject obj, String key) {
	    return (obj.has(key) && !obj.get(key).isJsonNull()) ? obj.get(key).getAsString() : null;
	}

	
	
	
	
//Verifica se um credor já existe no banco de dados.
	public boolean getIfCredorExist(String credorCpf) throws Exception {
	    EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	    JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
	    PreparedStatement pstmt = null;
	    ResultSet rs = null;

	    try {
	        jdbc.openSession();

	        String sql = "SELECT COUNT(0) AS CREDOR FROM TGFPAR WHERE CGC_CPF = ?";
	        pstmt = jdbc.getPreparedStatement(sql);
	        pstmt.setString(1, credorCpf);
	        rs = pstmt.executeQuery();

	        if (rs.next()) {
	            int count = rs.getInt("CREDOR");
	            System.out.println("Verificação de credor existente [" + credorCpf + "]: " + count);
	            return count > 0;
	        }

	    } catch (SQLException e) {
	        LogCatcher.logError("Erro ao verificar credor existente: " + e.getMessage());
	        LogCatcher.logError(e);
	        throw e;
	    } finally {
	        if (rs != null) rs.close();
	        if (pstmt != null) pstmt.close();
	        jdbc.closeSession();
	    }
	    return false;
	}


	//Verifica se um aluno já existe no banco de dados.
	public boolean getIfAlunoExist(String alunoCpf) throws Exception {
	    EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	    JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
	    PreparedStatement pstmt = null;
	    ResultSet rs = null;

	    try {
	        jdbc.openSession();

	        String sql = "SELECT COUNT(0) AS ALUNO FROM AD_ALUNOS WHERE ID_EXTERNO = ?";
	        pstmt = jdbc.getPreparedStatement(sql);
	        pstmt.setString(1, alunoCpf);
	        rs = pstmt.executeQuery();

	        if (rs.next()) {
	            int count = rs.getInt("ALUNO");
	            System.out.println("Verificação de aluno existente [" + alunoCpf + "]: " + count);
	            return count > 0;
	        }

	    } catch (SQLException e) {
	        LogCatcher.logError("Erro ao verificar aluno existente: " + e.getMessage());
	        LogCatcher.logError(e);
	        throw e;
	    } finally {
	        if (rs != null) rs.close();
	        if (pstmt != null) pstmt.close();
	        jdbc.closeSession();
	    }
	    return false;
	}


	
	//Retorna o código do credor cadastrado no banco de dados.
	public BigDecimal getCredorCadastrado(String credorCpf) throws Exception {
	    EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	    JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
	    PreparedStatement pstmt = null;
	    ResultSet rs = null;

	    try {
	        jdbc.openSession();

	        String sql = "SELECT CODPARC FROM TGFPAR WHERE CGC_CPF = ?";
	        pstmt = jdbc.getPreparedStatement(sql);
	        pstmt.setString(1, credorCpf);
	        rs = pstmt.executeQuery();

	        if (rs.next()) {
	            BigDecimal codParc = rs.getBigDecimal("CODPARC");
	            System.out.println("Credor localizado [" + credorCpf + "]: CODPARC = " + codParc);
	            return codParc;
	        }

	    } catch (SQLException e) {
	        LogCatcher.logError("Erro ao buscar código do credor: " + e.getMessage());
	        LogCatcher.logError(e);
	        throw e;
	    } finally {
	        if (rs != null) rs.close();
	        if (pstmt != null) pstmt.close();
	        jdbc.closeSession();
	    }
	    return BigDecimal.ZERO;
	}

	
//Atualiza o número de parceiros no banco de dados.
	public void updateTgfNumParc() throws Exception {
	    EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	    JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
	    PreparedStatement pstmt = null;

	    try {
	        jdbc.openSession();

	        String sqlUpd = "UPDATE TGFNUM SET ULTCOD = ULTCOD + 1 WHERE ARQUIVO = 'TGFPAR'";
	        pstmt = jdbc.getPreparedStatement(sqlUpd);
	        int rows = pstmt.executeUpdate();

	        System.out.println("[INFO] Atualização de TGFNUM realizada com sucesso. Registros afetados: " + rows);
	    } catch (SQLException e) {
	        LogCatcher.logError("Erro ao atualizar TGFNUM: " + e.getMessage());
	        LogCatcher.logError(e);
	        e.printStackTrace();
	        throw e;
	    } finally {
	        if (pstmt != null) pstmt.close();
	        jdbc.closeSession();
	    }
	}


	
	//Atualiza as informações de um aluno no banco de dados.
	public void updateAluno(String idSituacao, String situacao, String idAluno, String endereco, String cep,
			String bairro, String cidade, String uf) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		try {
			jdbc.openSession();

			String sqlUpd = "UPDATE AD_ALUNOS SET SITUACAO_ID = ?, SITUACAO = ?, ENDERECO = ?, CEP = ?, "
					+ "BAIRRO = ?, CIDADE = ?, UF = ? WHERE ID_EXTERNO = ?";

			pstmt = jdbc.getPreparedStatement(sqlUpd);
			pstmt.setString(1, idSituacao);
			pstmt.setString(2, situacao);
			pstmt.setString(3, endereco);
			pstmt.setString(4, cep);
			pstmt.setString(5, bairro);
			pstmt.setString(6, cidade);
			pstmt.setString(7, uf);
			pstmt.setString(8, idAluno);

			pstmt.executeUpdate();

			System.out.println("[INFO] Aluno atualizado com sucesso. ID_EXTERNO: " + idAluno);
		} catch (SQLException e) {
			LogCatcher.logError("Erro ao atualizar aluno " + idAluno + ": " + e.getMessage());
			LogCatcher.logError(e);
			System.err.println("[ERRO] Erro ao atualizar aluno. ID_EXTERNO: " + idAluno);
			e.printStackTrace();
			throw e;
		} finally {
			if (pstmt != null)
				pstmt.close();
			jdbc.closeSession();
		}
	}


	
	//Retorna o número máximo de parceiros cadastrados no banco de dados.
	public BigDecimal getMaxNumParc() throws Exception {
	    EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	    JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
	    PreparedStatement pstmt = null;
	    ResultSet rs = null;
	    BigDecimal ultCod = BigDecimal.ZERO;

	    try {
	        updateTgfNumParc(); // já faz log internamente

	        jdbc.openSession();

	        String sql = "SELECT MAX(ULTCOD) AS ULTCOD FROM TGFNUM WHERE ARQUIVO = 'TGFPAR'";
	        pstmt = jdbc.getPreparedStatement(sql);
	        rs = pstmt.executeQuery();

	        if (rs.next()) {
	            ultCod = rs.getBigDecimal("ULTCOD");
	            System.out.println("[INFO] Último código de parceiro (ULTCOD) obtido: " + ultCod);
	        }
	    } catch (SQLException e) {
	        LogCatcher.logError("Erro ao obter ULTCOD de TGFNUM: " + e.getMessage());
	        LogCatcher.logError(e);
	        e.printStackTrace();
	        throw e;
	    } finally {
	        if (rs != null) rs.close();
	        if (pstmt != null) pstmt.close();
	        jdbc.closeSession();
	    }

	    return ultCod;
	}


	//codusu alterado para 428
	public BigDecimal insertCredor(String credorNome, String credorCpf,
	        String credorEndereco, String credorCep, String credorBairro,
	        String credorCidade, String credorUf, String credorResidencial,
	        String credorCelular, String credorComercial, String alunoNome,
	        BigDecimal codemp, String alunoId) throws Exception {

	    EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	    JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
	    PreparedStatement pstmt = null;
	    PreparedStatement pstmtCheck = null;
	    PreparedStatement pstmtCredor = null;
	    ResultSet rs = null;
	    ResultSet rsCredor = null;

	    EnviromentUtils util = new EnviromentUtils();

	    BigDecimal atualCodparc = null;
	    BigDecimal codCid = null;
	    String tipPessoa = "";

	    BigDecimal countBai = BigDecimal.ZERO;
	    BigDecimal codBai = BigDecimal.ZERO;

	    System.out.println("[TRACE] Iniciando inserção de credor");
	    System.out.println("[TRACE] Dados de entrada:");
	    System.out.println("[TRACE] Nome: " + credorNome);
	    System.out.println("[TRACE] CPF/CNPJ: " + credorCpf);
	    System.out.println("[TRACE] Cidade: " + credorCidade);
	    System.out.println("[TRACE] UF: " + credorUf);
	    LogCatcher.logInfo("\nIniciando inserção de credor: " + credorCpf + " - " + credorNome);

	    if (credorNome == null || credorNome.trim().isEmpty()) {
	        String msg = "[ERROR] Nome do credor não pode ser nulo ou vazio";
	        System.err.println(msg);
	        LogCatcher.logError(msg);
	        return null;
	    }

	    if (credorCpf == null || credorCpf.trim().isEmpty()) {
	        String msg = "[ERROR] CPF/CNPJ do credor não pode ser nulo ou vazio";
	        System.err.println(msg);
	        LogCatcher.logError(msg);
	        return null;
	    }

	    if (credorCpf.length() == 11) {
	        tipPessoa = "F";
	        System.out.println("[TRACE] Tipo de Pessoa: Física (CPF)");
	    } else if (credorCpf.length() == 14) {
	        tipPessoa = "J";
	        System.out.println("[TRACE] Tipo de Pessoa: Jurídica (CNPJ)");
	    } else {
	        String msg = "[ERROR] CPF/CNPJ inválido: tamanho incorreto";
	        System.err.println(msg);
	        LogCatcher.logError(msg);
	        return null;
	    }

	    if (credorBairro != null && validarCadastroBairro(credorBairro, credorNome, alunoNome)) {
	        try {
	            codBai = insertBairro(credorBairro, credorNome, alunoNome);
	            countBai = countBai.add(BigDecimal.ONE);
	            System.out.println("[TRACE] Bairro inserido com sucesso. Código: " + codBai);
	        } catch (Exception e) {
	            String msg = "[ERROR] Falha ao inserir bairro: " + e.getMessage();
	            System.err.println(msg);
	            LogCatcher.logError(e);
	        }
	    }

	    try {
	        jdbc.openSession();

	        System.out.println("[TRACE] Verificando se credor já existe");
	        String checkCredorSQL = "SELECT CODPARC FROM TGFPAR WHERE CGC_CPF = ?";
	        pstmtCredor = jdbc.getPreparedStatement(checkCredorSQL);
	        pstmtCredor.setString(1, credorCpf);
	        rsCredor = pstmtCredor.executeQuery();

	        if (rsCredor.next()) {
	            atualCodparc = rsCredor.getBigDecimal("CODPARC");
	            String msg = "[WARN] Credor já existe. CODPARC: " + atualCodparc;
	            System.out.println(msg);
	            LogCatcher.logInfo(msg);
	            return atualCodparc;
	        }

	        atualCodparc = util.getMaxNumParc();
	        System.out.println("[TRACE] Novo CODPARC gerado: " + atualCodparc);

	        System.out.println("[TRACE] Buscando código da cidade");
	        String checkCidadeSQL = "SELECT max(codcid) as codcid FROM tsicid WHERE " +
	                "TRANSLATE(UPPER(descricaocorreio), 'áéíóúâêîôûàèìòùãõçÁÉÍÓÚÂÊÎÔÛÀÈÌÒÙÃÕÇ', " +
	                "'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC') LIKE TRANSLATE(UPPER(?), " +
	                "'áéíóúâêîôûàèìòùãõçÁÉÍÓÚÂÊÎÔÛÀÈÌÒÙÃÕÇ', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC') " +
	                "OR SUBSTR(UPPER(descricaocorreio), 1, INSTR(UPPER(descricaocorreio), ' ') - 1) " +
	                "LIKE TRANSLATE(UPPER(?), 'áéíóúâêîôûàèìòùãõçÁÉÍÓÚÂÊÎÔÛÀÈÌÒÙÃÕÇ', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC')";

	        pstmtCheck = jdbc.getPreparedStatement(checkCidadeSQL);
	        pstmtCheck.setString(1, credorCidade.trim());
	        pstmtCheck.setString(2, credorCidade.trim());
	        rs = pstmtCheck.executeQuery();

	        if (rs.next()) {
	            codCid = rs.getBigDecimal("codcid");
	            System.out.println("[TRACE] Código da cidade encontrado: " + codCid);
	        }

	        if (codCid == null) {
	            System.out.println("[WARN] Cidade não encontrada. Tentando inserir cidade.");
	            try {
	                codCid = insertCidade(credorCidade, credorUf, jdbc);
	                if (codCid == null) {
	                    String msg = "Erro ao cadastrar credor: Cidade não encontrada ou não pode ser criada: "
	                            + credorCidade + " | Matrícula Aluno: " + alunoId + " | Empresa: " + codemp;
	                    System.err.println("[ERROR] " + msg);
	                    LogCatcher.logError(msg);
	                    insertLogIntegracao(msg, "ERRO", credorNome, alunoNome, codemp);
	                    selectsParaInsert.add("SELECT " + codemp + ", SYSDATE, <#NUMUNICO#>, '" + msg + "' FROM DUAL");
	                    return null;
	                }
	            } catch (Exception e) {
	                String msg = "Erro ao inserir cidade para credor: " + e.getMessage();
	                System.err.println("[ERROR] " + msg);
	                LogCatcher.logError(e);
	                insertLogIntegracao(msg, "ERRO", credorNome, alunoNome);
	                return null;
	            }
	        }

	        String sqlP = "INSERT INTO TGFPAR(CODPARC, NOMEPARC, RAZAOSOCIAL, TIPPESSOA, AD_ENDCREDOR, " +
	                "CODBAI, CODCID, CEP, TELEFONE, CGC_CPF, DTCAD, DTALTER, AD_FLAGALUNO, CODUSU)  " +
	                "VALUES(?, ?, ?, ?, ?, NVL((select max(codbai) from tsibai where TRANSLATE(upper(nomebai), " +
	                "'áéíóúâêîôûàèìòùãõçÁÉÍÓÚÂÊÎÔÛÀÈÌÒÙÃÕÇ', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC') " +
	                "like TRANSLATE(upper(?), 'áéíóúâêîôûàèìòùãõçÁÉÍÓÚÂÊÎÔÛÀÈÌÒÙÃÕÇ', " +
	                "'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC')), 0), ?, ?, ?, ?, SYSDATE, SYSDATE, 'S', ?)";

	        pstmt = jdbc.getPreparedStatement(sqlP);
	        pstmt.setBigDecimal(1, atualCodparc);
	        pstmt.setString(2, credorNome.toUpperCase());
	        pstmt.setString(3, credorNome.toUpperCase());
	        pstmt.setString(4, tipPessoa);
	        pstmt.setString(5, credorEndereco);
	        pstmt.setString(6, credorBairro);
	        pstmt.setBigDecimal(7, codCid);
	        pstmt.setString(8, credorCep);
	        pstmt.setString(9, credorCelular);
	        pstmt.setString(10, credorCpf);
	        pstmt.setInt(11, 428);

	        System.out.println("[TRACE] Preparando para inserir novo credor");
	        int rowsAffected = pstmt.executeUpdate();

	        if (rowsAffected > 0) {
	            String msg = "[INFO] Credor inserido com sucesso! CODPARC: " + atualCodparc;
	            System.out.println(msg);
	            LogCatcher.logInfo(msg);
	        } else {
	            String msg = "[ERROR] Nenhuma linha foi inserida. Possível problema na inserção.";
	            System.err.println(msg);
	            LogCatcher.logError(msg);
	            atualCodparc = null;
	        }

	    } catch (SQLException e) {
	        String msg = "[FATAL] Erro completo durante inserção de credor: " + e.getMessage();
	        System.err.println(msg);
	        LogCatcher.logError(e);
	        selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro ao cadastrar credor: " + e.getMessage()
	                + "', SYSDATE, 'Erro', " + codemp + ", '" + credorNome + "' FROM DUAL");
	        atualCodparc = null;
	    } finally {
	        System.out.println("[TRACE] Fechando recursos de banco de dados");
	        try {
	            if (rsCredor != null) rsCredor.close();
	            if (rs != null) rs.close();
	            if (pstmtCredor != null) pstmtCredor.close();
	            if (pstmtCheck != null) pstmtCheck.close();
	            if (pstmt != null) pstmt.close();
	            jdbc.closeSession();
	            System.out.println("[TRACE] Todos os recursos fechados com sucesso");
	        } catch (SQLException e) {
	            String msg = "[ERROR] Erro ao fechar recursos: " + e.getMessage();
	            System.err.println(msg);
	            LogCatcher.logError(e);
	        }
	    }

	    return atualCodparc;
	}



	
	private BigDecimal insertCidade(String nomeCidade, String uf, JdbcWrapper jdbc) throws Exception {
	    PreparedStatement pstmt = null;
	    ResultSet rs = null;
	    BigDecimal codCid = null;

	    try {
	        System.out.println("DEBUG: Iniciando inserção da cidade: " + nomeCidade + ", UF: " + uf);
	        LogCatcher.logInfo("\nIniciando inserção da cidade: " + nomeCidade + "/" + uf);

	        String nomeCidadeNormalizado = nomeCidade.trim().toUpperCase();
	        String ufNormalizada = uf.trim().toUpperCase();

	        // 1. Verificação exata
	        String sqlVerificaExata = "SELECT CODCID FROM TSICID WHERE UPPER(TRIM(NOMECID)) = ? AND UPPER(TRIM(UF)) = ?";
	        System.out.println("DEBUG: Verificando se cidade já existe (exato): " + sqlVerificaExata);
	        pstmt = jdbc.getPreparedStatement(sqlVerificaExata);
	        pstmt.setString(1, nomeCidadeNormalizado);
	        pstmt.setString(2, ufNormalizada);
	        rs = pstmt.executeQuery();

	        if (rs.next()) {
	            codCid = rs.getBigDecimal("CODCID");
	            System.out.println("DEBUG: Cidade já existe com código (exato): " + codCid);
	            LogCatcher.logInfo("Cidade encontrada (exato): " + codCid);
	            return codCid;
	        }

	        rs.close();
	        pstmt.close();

	        // 2. Verificação por DESCRICAOCORREIO
	        String sqlVerificaDescricao = "SELECT CODCID FROM TSICID WHERE UPPER(TRIM(DESCRICAOCORREIO)) = ? AND UPPER(TRIM(UF)) = ?";
	        System.out.println("DEBUG: Verificando pelo campo DESCRICAOCORREIO: " + sqlVerificaDescricao);
	        pstmt = jdbc.getPreparedStatement(sqlVerificaDescricao);
	        pstmt.setString(1, nomeCidadeNormalizado);
	        pstmt.setString(2, ufNormalizada);
	        rs = pstmt.executeQuery();

	        if (rs.next()) {
	            codCid = rs.getBigDecimal("CODCID");
	            System.out.println("DEBUG: Cidade encontrada via DESCRICAOCORREIO, código: " + codCid);
	            LogCatcher.logInfo("Cidade encontrada (DESCRICAOCORREIO): " + codCid);
	            return codCid;
	        }

	        rs.close();
	        pstmt.close();

	        // 3. Flexível por NOMECID
	        String sqlVerificaFlexivel = "..."; // mesma SQL que você já tem
	        System.out.println("DEBUG: Verificando com busca flexível (NOMECID): " + sqlVerificaFlexivel);
	        pstmt = jdbc.getPreparedStatement(sqlVerificaFlexivel);
	        pstmt.setString(1, nomeCidadeNormalizado);
	        pstmt.setString(2, ufNormalizada);
	        rs = pstmt.executeQuery();

	        if (rs.next()) {
	            codCid = rs.getBigDecimal("CODCID");
	            System.out.println("DEBUG: Cidade encontrada com busca flexível (NOMECID), código: " + codCid);
	            LogCatcher.logInfo("Cidade encontrada (flexível NOMECID): " + codCid);
	            return codCid;
	        }

	        rs.close();
	        pstmt.close();

	        // 4. Flexível por DESCRICAOCORREIO
	        String sqlVerificaFlexivelDescricao = "..."; // mesma SQL que você já tem
	        System.out.println("DEBUG: Verificando com busca flexível (DESCRICAOCORREIO): " + sqlVerificaFlexivelDescricao);
	        pstmt = jdbc.getPreparedStatement(sqlVerificaFlexivelDescricao);
	        pstmt.setString(1, nomeCidadeNormalizado);
	        pstmt.setString(2, ufNormalizada);
	        rs = pstmt.executeQuery();

	        if (rs.next()) {
	            codCid = rs.getBigDecimal("CODCID");
	            System.out.println("DEBUG: Cidade encontrada com busca flexível (DESCRICAOCORREIO), código: " + codCid);
	            LogCatcher.logInfo("Cidade encontrada (flexível DESCRICAOCORREIO): " + codCid);
	            return codCid;
	        }

	        rs.close();
	        pstmt.close();

	        // 5. LIKE
	        String sqlVerificaLike = "..."; // mesma SQL que você já tem
	        System.out.println("DEBUG: Verificando com LIKE (NOMECID): " + sqlVerificaLike);
	        pstmt = jdbc.getPreparedStatement(sqlVerificaLike);
	        pstmt.setString(1, "%" + nomeCidadeNormalizado + "%");
	        pstmt.setString(2, ufNormalizada);
	        rs = pstmt.executeQuery();

	        if (rs.next()) {
	            codCid = rs.getBigDecimal("CODCID");
	            System.out.println("DEBUG: Cidade encontrada com LIKE (NOMECID), código: " + codCid);
	            LogCatcher.logInfo("Cidade encontrada (LIKE): " + codCid);
	            return codCid;
	        }

	        rs.close();
	        pstmt.close();

	        // Inserção
	        System.out.println("DEBUG: Cidade não encontrada após todas as verificações. Criando nova cidade.");
	        LogCatcher.logInfo("Criando nova cidade: " + nomeCidadeNormalizado + "/" + ufNormalizada);

	        String sqlSeq = "SELECT NVL(MAX(CODCID), 0) + 1 AS NEXT_CODCID FROM TSICID";
	        pstmt = jdbc.getPreparedStatement(sqlSeq);
	        rs = pstmt.executeQuery();

	        if (rs.next()) {
	            codCid = rs.getBigDecimal("NEXT_CODCID");
	        } else {
	            codCid = BigDecimal.ONE;
	        }

	        rs.close();
	        pstmt.close();

	        try {
	            String sqlInsert = "INSERT INTO TSICID (CODCID, NOMECID, UF, DESCRICAOCORREIO) VALUES (?, ?, ?, ?)";
	            pstmt = jdbc.getPreparedStatement(sqlInsert);
	            pstmt.setBigDecimal(1, codCid);
	            pstmt.setString(2, nomeCidadeNormalizado);
	            pstmt.setString(3, ufNormalizada);
	            pstmt.setString(4, nomeCidadeNormalizado);

	            int rowsAffected = pstmt.executeUpdate();
	            System.out.println("DEBUG: Inserção realizada. Linhas afetadas: " + rowsAffected);

	            if (rowsAffected > 0) {
	                System.out.println("DEBUG: Cidade inserida com sucesso. CODCID: " + codCid);
	                LogCatcher.logInfo("Cidade inserida: " + codCid);
	                return codCid;
	            } else {
	                LogCatcher.logError("Inserção de cidade não afetou nenhuma linha: " + nomeCidadeNormalizado + "/" + ufNormalizada);
	                return verificarCidadeExistente(nomeCidadeNormalizado, ufNormalizada, jdbc);
	            }
	        } catch (SQLException e) {
	            System.out.println("DEBUG: Erro SQL na inserção: " + e.getMessage());
	            LogCatcher.logError(e);

	            if (e.getErrorCode() == 1 || e.getSQLState().equals("23000") || e.getMessage().toLowerCase().contains("unique")) {
	                return verificarCidadeExistente(nomeCidadeNormalizado, ufNormalizada, jdbc);
	            }

	            return null;
	        }
	    } catch (Exception e) {
	        LogCatcher.logError(e);
	        return null;
	    } finally {
	        System.out.println("DEBUG: Finalizando método insertCidade");
	        try {
	            if (rs != null) rs.close();
	            if (pstmt != null) pstmt.close();
	        } catch (Exception e) {
	            LogCatcher.logError(e);
	        }
	    }
	}



	// Método auxiliar para verificação final após erro de chave duplicada
	private BigDecimal verificarCidadeExistente(String nomeCidade, String uf, JdbcWrapper jdbc) throws Exception {
	    PreparedStatement pstmt = null;
	    ResultSet rs = null;

	    try {
	        String[] sqlQueries = new String[] {
	            // Busca exata
	            "SELECT CODCID FROM TSICID WHERE UPPER(TRIM(NOMECID)) = ? AND UPPER(TRIM(UF)) = ?",
	            // Busca por DESCRICAOCORREIO
	            "SELECT CODCID FROM TSICID WHERE UPPER(TRIM(DESCRICAOCORREIO)) = ? AND UPPER(TRIM(UF)) = ?",
	            // Busca com TRANSLATE em NOMECID
	            "SELECT CODCID FROM TSICID WHERE TRANSLATE(UPPER(TRIM(NOMECID)), 'áéíóúâêîôûàèìòùãõçÁÉÍÓÚÂÊÎÔÛÀÈÌÒÙÃÕÇ', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC') = " +
	            "TRANSLATE(?, 'áéíóúâêîôûàèìòùãõçÁÉÍÓÚÂÊÎÔÛÀÈÌÒÙÃÕÇ', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC') AND UPPER(TRIM(UF)) = ?",
	            // Busca com LIKE em NOMECID
	            "SELECT CODCID FROM TSICID WHERE UPPER(TRIM(NOMECID)) LIKE ? AND UPPER(TRIM(UF)) = ?",
	            // Busca com LIKE em DESCRICAOCORREIO
	            "SELECT CODCID FROM TSICID WHERE UPPER(TRIM(DESCRICAOCORREIO)) LIKE ? AND UPPER(TRIM(UF)) = ?"
	        };

	        for (String sql : sqlQueries) {
	            System.out.println("DEBUG: Verificação final com query: " + sql);
	            LogCatcher.logInfo("Executando verificação final com SQL: " + sql);

	            pstmt = jdbc.getPreparedStatement(sql);

	            if (sql.contains("LIKE")) {
	                pstmt.setString(1, "%" + nomeCidade + "%");
	            } else {
	                pstmt.setString(1, nomeCidade);
	            }
	            pstmt.setString(2, uf);

	            rs = pstmt.executeQuery();

	            if (rs.next()) {
	                BigDecimal codCid = rs.getBigDecimal("CODCID");
	                System.out.println("DEBUG: Cidade encontrada na verificação final, código: " + codCid);
	                LogCatcher.logInfo("Cidade encontrada na verificação final: " + codCid);
	                return codCid;
	            }

	            rs.close();
	            pstmt.close();
	        }

	        // Último recurso: verificar o valor máximo na tabela TSICID
	        String maxSql = "SELECT MAX(CODCID) AS MAX_CODCID FROM TSICID";
	        pstmt = jdbc.getPreparedStatement(maxSql);
	        rs = pstmt.executeQuery();

	        if (rs.next()) {
	            BigDecimal maxCodCid = rs.getBigDecimal("MAX_CODCID");
	            System.out.println("DEBUG: Não foi possível encontrar a cidade. Valor máximo de CODCID: " + maxCodCid);
	            LogCatcher.logInfo("Cidade não encontrada. Próximo CODCID sugerido: " + maxCodCid);
	            return maxCodCid != null ? maxCodCid.add(BigDecimal.ONE) : new BigDecimal(1);
	        }

	        System.out.println("DEBUG: Não foi possível encontrar a cidade nem obter valor máximo");
	        LogCatcher.logError("Falha na verificação final: Cidade não encontrada e CODCID máximo indisponível: " + nomeCidade + "/" + uf);

	        try {
	            insertLogIntegracao(
	                "Falha na verificação final de cidade existente: " + nomeCidade + "/" + uf,
	                "AVISO",
	                "",
	                ""
	            );
	        } catch (Exception logEx) {
	            System.err.println("Erro ao registrar no log: " + logEx.getMessage());
	            LogCatcher.logError(logEx);
	        }

	        return null;
	    } catch (Exception e) {
	        LogCatcher.logError(e);
	        throw e;
	    } finally {
	        if (rs != null) {
	            try { rs.close(); } catch (Exception e) { LogCatcher.logError(e); }
	        }
	        if (pstmt != null) {
	            try { pstmt.close(); } catch (Exception e) { LogCatcher.logError(e); }
	        }
	    }
	}



	
	//Insere um novo curso e turma no banco de dados.
	public void insertCursoTurma(String cursoDescricao, String cursoId,
			String turmaId, String credorNome, String alunoNome,
			BigDecimal codEmp) throws Exception {
		
		if ((cursoDescricao != null)
				&& (validarCadastroCurso(cursoDescricao, credorNome, alunoNome))) {
			// insertCurso(cursoDescricao, cursoId, credorNome, alunoNome);
		} else {
			//updateCurso(cursoDescricao, cursoId, credorNome, alunoNome);
		}
		
		if ((cursoDescricao != null)
				&& (validarCadastroCursoProj(cursoDescricao, credorNome,
						alunoNome, codEmp))) {
			insertCursoProj(cursoDescricao, credorNome, alunoNome, codEmp,
					cursoId);
		}
		
		if ((turmaId != null)
				&& (validarCadastroTurma(turmaId, credorNome, alunoNome,
						cursoDescricao, codEmp))) {
			insertCadastroTurma(turmaId, codEmp, cursoDescricao);
		}
	}

	
	//Valida se um curso já está cadastrado no banco de dados.
	public boolean validarCadastroCurso(String curso, String credorNome,
			String alunoNome) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal count = BigDecimal.ZERO;
		try {
			jdbc.openSession();

			String sqlNota = "SELECT COUNT(0) AS COUNT FROM TSICUS  WHERE TRANSLATE(UPPER (DESCRCENCUS), '������������������������������������', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC') LIKE TRANSLATE (UPPER ('%"
					+ curso.trim()
					+ "%'), "
					+ "'������������������������������������', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC')";

			pstmt = jdbc.getPreparedStatement(sqlNota);
			rs = pstmt.executeQuery();

			if (rs.next()) {
				count = rs.getBigDecimal("COUNT");
			}

		} catch (SQLException e) {
			insertLogIntegracao("Erro validar curso: " + curso + " - " + e.getMessage(), "ERRO", credorNome, alunoNome);
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
		if (count.compareTo(BigDecimal.ZERO) == 0) {
			return true;
		}
		return false;
	}
	
	
	
//Valida se um curso já está cadastrado como projeto no banco de dados.
	public boolean validarCadastroCursoProj(String curso, String credorNome,
			String alunoNome, BigDecimal codEmp) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal count = BigDecimal.ZERO;
		try {
			jdbc.openSession();

			String sqlNota = "SELECT COUNT(0) AS COUNT FROM TCSPRJ  WHERE TRANSLATE(UPPER (IDENTIFICACAO), '������������������������������������', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC') LIKE TRANSLATE (UPPER ('"
					+ curso.trim()
					+ "'), "
					+ "'������������������������������������', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC') AND CODPROJPAI = (SELECT MAX(CODPROJ) FROM TCSPRJ WHERE CODEMP = "
					+ codEmp + ")";

			pstmt = jdbc.getPreparedStatement(sqlNota);
			rs = pstmt.executeQuery();
			if (rs.next()) {
				count = rs.getBigDecimal("COUNT");
			}
		} catch (SQLException e) {
			e.printStackTrace();
			insertLogIntegracao(
					"Erro ao validar se curso ja cadastrado como projeto: "
							+ e.getMessage(), "Erro", credorNome, alunoNome);
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
		if (count.compareTo(BigDecimal.ZERO) == 0) {
			return true;
		}
		return false;
	}

	
	//Valida se uma turma já está cadastrada no banco de dados.
	public boolean validarCadastroTurma(String turma, String credorNome,
			String alunoNome, String cursoNome, BigDecimal codEmp)
			throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal count = BigDecimal.ZERO;
		
		
		try {
			jdbc.openSession();

			String sqlNota = "SELECT COUNT(0) AS COUNT FROM TCSPRJ  WHERE TRANSLATE(UPPER (IDENTIFICACAO), '������������������������������������', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC') LIKE TRANSLATE (UPPER ('%"
					+
					turma.trim()
					+ "%'), "
					+ "'������������������������������������', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC') AND CODPROJPAI = (SELECT CODPROJ FROM TCSPRJ WHERE CODPROJPAI = (SELECT MAX(CODPROJ) FROM TCSPRJ WHERE CODEMP = "
					+ codEmp + ") AND TRANSLATE(UPPER (IDENTIFICACAO), '������������������������������������', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC') LIKE TRANSLATE (UPPER ('"
					+
					cursoNome.trim()
					+ "'), "
					+ "'������������������������������������', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC'))";

			pstmt = jdbc.getPreparedStatement(sqlNota);
			rs = pstmt.executeQuery();
			if (rs.next()) {
				count = rs.getBigDecimal("COUNT");
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
		if (count.compareTo(BigDecimal.ZERO) == 0) {
			return true;
		}
		return false;
	}

	
	//Insere uma nova turma no banco de dados.
	public void insertCadastroTurma(String turmaId, BigDecimal codEmp,
			String cursoDescricao) throws Exception {
		
		EnviromentUtils util = new EnviromentUtils();
		
		String ativo = "S";
		String analitico = "N";
		
		if (turmaId.length() >= 20) {
			turmaId = turmaId.substring(0, 20).toUpperCase();
		} else {
			turmaId = turmaId.toUpperCase();
		}
		
		int grau = 3;
		
		try {
			
			util.insertCurso(cursoDescricao.trim(), codEmp, turmaId.trim(), ativo, analitico, grau);

		} catch (Exception se) {
			selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro cadastrando turma: " + se.getMessage() +" Nome Turma: "+turmaId+"', SYSDATE, 'Erro', "+codEmp+", null FROM DUAL");
			
			/*util.inserirLog("Erro cadastrando turma: " + se.getMessage() + " Nome Turma: " + turmaId,
					"Erro", "", codEmp);*/
			se.printStackTrace();
		} finally {
			
		}
	}
	
	//Insere um novo curso no banco de dados.

	public void insertCurso(String cursoDescricao, String cursoId,
			String credorNome, String alunoNome) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		String ativo = "S";
		String analitico = "S";

		int codCenCusPai = 10101000;
		int grau = 4;

		System.out.println("ID_EXTERNO: " + cursoId);
		try {
			jdbc.openSession();

			String sqlUpdate = "INSERT INTO TSICUS(CODCENCUS, CODCENCUSPAI, DESCRCENCUS, ATIVO, ANALITICO, GRAU, AD_IDEXTERNO)VALUES ((SELECT MAX(CODCENCUS) + 1 FROM TSICUS WHERE CODCENCUSPAI = '10101000'), ? ,? , ?, ?, ?, ?)";

			pstmt = jdbc.getPreparedStatement(sqlUpdate);

			pstmt.setInt(1, codCenCusPai);
			pstmt.setString(2, cursoDescricao);
			pstmt.setString(3, ativo);
			pstmt.setString(4, analitico);
			pstmt.setInt(5, grau);
			pstmt.setString(6, cursoId);
			pstmt.executeUpdate();
		} catch (Exception se) {
			insertLogIntegracao("Erro cadastrando curso: " + se.getMessage(),
					"Erro", credorNome, alunoNome);
			se.printStackTrace();
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}

	
	//Atualiza as informações de um curso no banco de dados.
	public void updateCurso(String cursoDescricao, String cursoId,
			String credorNome, String alunoNome) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		System.out.println("ID_EXTERNO: " + cursoId);
		try {
			jdbc.openSession();

			String sqlUpdate = "UPDATE TSICUS SET AD_IDEXTERNO = '"
					+ cursoId
					+ "' WHERE CODCENCUS = (SELECT CODCENCUS FROM TSICUS  WHERE TRANSLATE(UPPER (DESCRCENCUS), '������������������������������������', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC') LIKE TRANSLATE (UPPER ('%"
					+ cursoDescricao.trim()
					+ "'),  '������������������������������������', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC'))";

			pstmt = jdbc.getPreparedStatement(sqlUpdate);

			pstmt.executeUpdate();
		} catch (Exception se) {
			insertLogIntegracao("Erro cadastrando curso: " + se.getMessage(),
					"Erro", credorNome, alunoNome);
			se.printStackTrace();
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}

	
	// Retorna o código máximo de projetos no banco de dados.
	public BigDecimal getMaxCodProjPaiProj(BigDecimal codEmp) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal codProjPai = BigDecimal.ZERO;
		try {
			jdbc.openSession();

			String sqlSelect = "select codproj from TCSPRJ where codemp = "
					+ codEmp;

			pstmt = jdbc.getPreparedStatement(sqlSelect);

			rs = pstmt.executeQuery();
			if (rs.next()) {
				codProjPai = rs.getBigDecimal("codproj");
			}
		} catch (Exception se) {
			insertLogIntegracao(
					"Erro cadastrando curso como projeto: " + se.getMessage(),
					"Erro", "", "");
			se.printStackTrace();
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
		return codProjPai;
	}

	
	// Insere um novo curso como projeto no banco de dados.
	public BigDecimal insertCursoProj(String cursoDescricao, String credorNome,
			String alunoNome, BigDecimal codEmp, String cursoId)
			throws Exception {
		
		
		EnviromentUtils util = new EnviromentUtils();
		
		BigDecimal codProjPai = getMaxCodProjPaiProj(codEmp);

		String ativo = "S";
		String analitico = "S";
		int grau = 2;

		String cursoAbrev = "";
		if (cursoDescricao.length() >= 20) {
			cursoAbrev = cursoDescricao.substring(0, 20).toUpperCase();
		} else {
			cursoAbrev = cursoDescricao.toUpperCase();
		}
		try {
			
			util.insertProjeto(codProjPai, cursoDescricao.toUpperCase().trim(), 
					cursoAbrev.toUpperCase().trim(), ativo, analitico, grau, cursoId);
			
		} catch (Exception se) {
			selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro cadastrando curso como projeto: " + se.getMessage() +" Nome Curso: "+cursoDescricao+"', SYSDATE, 'Erro', "+codEmp+", null FROM DUAL");
			
			/*util.inserirLog(
					"Erro cadastrando curso como projeto: " + se.getMessage() +" Nome Curso: " + cursoDescricao,
					"Erro", "", codEmp);*/
			se.printStackTrace();
		} finally {
			
		}
		return null;
	}


	
	public void insertAluno(BigDecimal credotAtual, String alunoId,
	        String alunoNome, String alunoNomeSocial, String alunoEndereco,
	        String alunoCep, String alunoBairro, String alunoCidade,
	        String alunoUf, String alunoSexo, String alunoDataNascimento,
	        String alunoRg, String alunoCpf, String alunoCelular,
	        String alunoResidencial, String alunoEmail, String alunoSituacao,
	        String alunoSituacaoId, String credorNome, BigDecimal codEmp,
	        String descrCurso, String turmaId) throws Exception {

	    if (alunoNome == null || alunoNome.trim().isEmpty()) {
	        throw new IllegalArgumentException("O nome do aluno é obrigatório.");
	    }

	    if (alunoNomeSocial == null) {
	        alunoNomeSocial = " ";
	    }

	    EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	    JdbcWrapper jdbc = entityFacade.getJdbcWrapper();

	    try {
	        jdbc.openSession();
	        BigDecimal codCenCus = buscarCodigoCurso(jdbc, descrCurso);

	        String sql = "INSERT INTO AD_ALUNOS (" +
	                "CODPARC, ID_EXTERNO, NOME, NOME_SOCIAL, ENDERECO, CEP, BAIRRO, CIDADE, UF, SEXO, " +
	                "DATA_NASCIMENTO, RG, CPF, TELEFONE_CELULAR, TELEFONE_RESIDENCIAL, EMAIL, SITUACAO, SITUACAO_ID, CODEMP, CODCENCUS, TURMA" +
	                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
	                "(SELECT TO_CHAR(TO_DATE(?, 'yyyy-MM-dd'), 'dd/MM/yyyy') FROM dual), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

	        try (PreparedStatement pstmt = jdbc.getPreparedStatement(sql)) {
	            pstmt.setBigDecimal(1, credotAtual);
	            pstmt.setString(2, alunoId);
	            pstmt.setString(3, StringUtils.normalizarTexto(alunoNome));
	            pstmt.setString(4, alunoNomeSocial);
	            pstmt.setString(5, alunoEndereco);
	            pstmt.setString(6, alunoCep);
	            pstmt.setString(7, alunoBairro);
	            pstmt.setString(8, alunoCidade);
	            pstmt.setString(9, alunoUf);
	            pstmt.setString(10, alunoSexo);
	            pstmt.setString(11, alunoDataNascimento);
	            pstmt.setString(12, alunoRg);
	            pstmt.setString(13, alunoCpf);
	            pstmt.setString(14, alunoCelular);
	            pstmt.setString(15, alunoResidencial);
	            pstmt.setString(16, alunoEmail);
	            pstmt.setString(17, alunoSituacao);
	            pstmt.setString(18, alunoSituacaoId);
	            pstmt.setBigDecimal(19, codEmp);
	            pstmt.setBigDecimal(20, codCenCus);
	            pstmt.setString(21, turmaId);

	            int rows = pstmt.executeUpdate();

	            System.out.println("[DEBUG] Aluno inserido com sucesso. Linhas afetadas: " + rows);
	            LogCatcher.logInfo("[INSERT] Aluno inserido: " + alunoNome + " | ID: " + alunoId + " | CODPARC: " + credotAtual + " | Linhas: " + rows);
	        }

	    } catch (SQLException e) {
	        String error = String.format(
	            "Erro ao inserir aluno. CODPARC: %s, ID_EXTERNO: %s, NOME: %s, Curso: %s, Erro: %s",
	            credotAtual, alunoId, alunoNome, descrCurso, e.getMessage()
	        );

	        System.err.println("[ERRO] " + error);
	        LogCatcher.logError("[SQL ERROR] " + error);

	        selectsParaInsert.add("SELECT <#NUMUNICO#>, '" + error + "', SYSDATE, 'Erro', " + codEmp + ", '" + alunoId + "' FROM DUAL");

	        throw e;
	    } finally {
	        jdbc.closeSession();
	        LogCatcher.logInfo("Sessão JDBC encerrada após tentativa de inserção do aluno: " + alunoId);
	    }
	}

	
	
	public static class StringUtils {

	    public static String normalizarTexto(String texto) {
	        String textoOriginal = texto;
	        if (texto == null) {
	            LogCatcher.logInfo("StringUtils.normalizarTexto foi chamado com valor nulo. Retornando string vazia.");
	            return "";
	        }

	        String textoNormalizado = texto.toUpperCase()
	            .replaceAll("\\s+", " ")
	            .trim()
	            .replace("Á", "A").replace("À", "A").replace("Ã", "A").replace("Â", "A")
	            .replace("É", "E").replace("Ê", "E")
	            .replace("Í", "I")
	            .replace("Ó", "O").replace("Ô", "O").replace("Õ", "O")
	            .replace("Ú", "U")
	            .replace("Ç", "C");

	        String logMessage = String.format(
	            //"normalizarTexto executado. Entrada: \"%s\" | Saída: \"%s\"",
	            textoOriginal,
	            textoNormalizado
	        );
	        LogCatcher.logInfo(logMessage);

	        return textoNormalizado;
	    }
	}

	
	private BigDecimal buscarCodigoCurso(JdbcWrapper jdbc, String descrCurso) throws Exception {
	    String normalizedDescrCurso = StringUtils.normalizarTexto(descrCurso);

	    String queryPrincipal = "SELECT codcencus, descrcencus FROM tsicus";
	    try (PreparedStatement stmt = jdbc.getPreparedStatement(queryPrincipal);
	         ResultSet rs = stmt.executeQuery()) {

	        while (rs.next()) {
	            String descBanco = rs.getString("descrcencus");
	            if (normalizedDescrCurso.equals(StringUtils.normalizarTexto(descBanco))) {
	                return rs.getBigDecimal("codcencus");
	            }
	        }
	    }

	    // Busca menos restritiva
	    String queryAlternativa = "SELECT codcencus, descrcencus FROM tsicus WHERE UPPER(descrcencus) LIKE ?";
	    try (PreparedStatement stmt = jdbc.getPreparedStatement(queryAlternativa)) {
	        String busca = "%" + normalizedDescrCurso.replace("CURSO TECNICO EM", "%") + "%";
	        stmt.setString(1, busca);
	        try (ResultSet rs = stmt.executeQuery()) {
	            if (rs.next()) {
	                return rs.getBigDecimal("codcencus");
	            }
	        }
	    }

	    throw new SQLException("Nenhum curso encontrado para a descrição: " + descrCurso);
	}



	
	//Valida se um endereço já está cadastrado no banco de dados.
	public boolean validarCadastroEndereco(String endereco, String credorNome,
			String alunoNome) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal count = BigDecimal.ZERO;
		try {
			jdbc.openSession();

			String sqlNota = "SELECT COUNT(0) AS COUNT FROM TSIEND WHERE TRANSLATE(  \t    upper(NOMEEND),   \t    '������������������������������������',   \t    'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC'   \t  ) like TRANSLATE(    \t    upper((SELECT LTRIM(SUBSTR(?, INSTR(?, ' ') + 1))  FROM DUAL)),   \t    '������������������������������������',   \t    'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC')";

			pstmt = jdbc.getPreparedStatement(sqlNota);
			pstmt.setString(1, endereco);
			pstmt.setString(2, endereco);

			rs = pstmt.executeQuery();
			if (rs.next()) {
				count = rs.getBigDecimal("COUNT");
			}
		} catch (SQLException e) {
			e.printStackTrace();
			insertLogIntegracao("Erro ao validar se endereco ja cadastrado: "
					+ e.getMessage(), "Erro", credorNome, alunoNome);
			LogCatcher.logError("Erro ao validar se endereco ja cadastrado: "
					+ e.getMessage());
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
		if (count.compareTo(BigDecimal.ZERO) == 0) {
			return true;
		}
		return false;
	}

	
	//Valida se um bairro já está cadastrado no banco de dados.
	public boolean validarCadastroBairro(String bairro, String credorNome,
			String alunoNome) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal count = BigDecimal.ZERO;
		try {
			jdbc.openSession();

			String sqlNota = "SELECT COUNT(0) AS COUNT FROM TSIBAI WHERE TRANSLATE(  \t    upper(NOMEBAI),   \t    '������������������������������������',   \t    'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC'   \t  ) like TRANSLATE(    \t    upper(?),   \t    '������������������������������������',   \t    'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC')";

			pstmt = jdbc.getPreparedStatement(sqlNota);
			pstmt.setString(1, bairro);

			rs = pstmt.executeQuery();
			if (rs.next()) {
				count = rs.getBigDecimal("COUNT");
			}
		} catch (SQLException e) {
			e.printStackTrace();
			insertLogIntegracao(
					"Erro ao validar se bairro ja cadastrado: "
							+ e.getMessage(), "Erro", credorNome, alunoNome);
			LogCatcher.logError(
					"Erro ao validar se bairro ja cadastrado: "
							+ e.getMessage());
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
		if (count.compareTo(BigDecimal.ZERO) == 0) {
			return true;
		}
		return false;
	}

	
	//Insere um novo bairro no banco de dados.
	public BigDecimal insertBairro(String bairro, String credorNome,
			String alunoNome) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		BigDecimal codbai = getMaxNumBai();

		jdbc.openSession();

		String sqlUpdate = "INSERT INTO TSIBAI (CODBAI, NOMEBAI, DTALTER)VALUES (?, ?, SYSDATE) ";

		pstmt = jdbc.getPreparedStatement(sqlUpdate);
		pstmt.setBigDecimal(1, codbai);
		pstmt.setString(2, bairro.toUpperCase());
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
			insertLogIntegracao("Erro ao bairro endereco: " + se.getMessage(),
					"Erro", credorNome, alunoNome);
			LogCatcher.logError("Erro ao bairro endereco: " + se.getMessage());
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
		return codbai;
	}
	
	

	public BigDecimal getMaxNumBai() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal id = BigDecimal.ZERO;
		try {
			updateNumBai();

			jdbc.openSession();

			String sqlNota = "SELECT MAX(ULTCOD) AS ID FROM TGFNUM WHERE ARQUIVO = 'TSIBAI'";

			pstmt = jdbc.getPreparedStatement(sqlNota);

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

	public void updateNumBai() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		try {
			jdbc.openSession();

			String sqlUpdate = "UPDATE TGFNUM SET ULTCOD = ULTCOD + 1  WHERE ARQUIVO = 'TSIBAI'";

			pstmt = jdbc.getPreparedStatement(sqlUpdate);
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

	/**
	 * Overloaded method to include codemp in the log integration record
	 * 
	 * @param descricao   Description of the log entry
	 * @param status      Status of the log entry
	 * @param credorNome  Name of the creditor
	 * @param alunoNome   Name of the student
	 * @param codemp      Company code
	 * @throws Exception  If there's an error during database operations
	 */
	public void insertLogIntegracao(String descricao, String status,
	                               String credorNome, String alunoNome, BigDecimal codemp) throws Exception {
	    EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	    JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
	    PreparedStatement pstmt = null;
	    try {
	        jdbc.openSession();

	        String descricaoCompleta = null;
	        if ((credorNome.equals("")) && (alunoNome.equals(""))) {
	            descricaoCompleta = descricao;
	        } else if ((credorNome.equals("")) && (!alunoNome.isEmpty())) {
	            descricaoCompleta = descricao + " " + " Aluno:" + alunoNome;
	        } else if ((!credorNome.isEmpty()) && (alunoNome.equals(""))) {
	            descricaoCompleta = descricao + " " + " Credor:" + credorNome;
	        } else if ((!credorNome.isEmpty()) && (!alunoNome.isEmpty())) {
	            descricaoCompleta = descricao + " " + " Credor:" + credorNome
	                    + " " + " Aluno:" + alunoNome;
	        }
	        
	        // Updated SQL to include CODEMP column
	        String sqlUpdate = "INSERT INTO AD_LOGINTEGRACAO (NUMUNICO, DESCRICAO, DTHORA, STATUS, CODEMP) " +
	                          "VALUES (((SELECT NVL(MAX(NUMUNICO), 0) + 1 FROM AD_LOGINTEGRACAO)), ?, SYSDATE, ?, ?)";

	        pstmt = jdbc.getPreparedStatement(sqlUpdate);
	        pstmt.setString(1, descricaoCompleta);
	        pstmt.setString(2, status);
	        pstmt.setBigDecimal(3, codemp);
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

	/**
	 * Original method maintained for backward compatibility
	 */
	public void insertLogIntegracao(String descricao, String status,
	                               String credorNome, String alunoNome) throws Exception {
	    // Call the overloaded method with null for codemp
	    insertLogIntegracao(descricao, status, credorNome, alunoNome, null);
	}
	
	public void insertLogList(String listInsert) throws Exception {
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

	
	
	private static final int MAX_REQUESTS_PER_MINUTE = 60;
	private static final long ONE_MINUTE_IN_MS = 60 * 1000;
	private static final Queue<Long> requestTimestamps = new LinkedList<>();

	public synchronized String[] apiGet2(String ur, String token) throws Exception {
	    long currentTime = System.currentTimeMillis();
	    requestTimestamps.removeIf(timestamp -> 
	        currentTime - timestamp > ONE_MINUTE_IN_MS);


	    if (requestTimestamps.size() >= MAX_REQUESTS_PER_MINUTE) {

	        long oldestRequestTime = requestTimestamps.peek();
	        long waitTime = ONE_MINUTE_IN_MS - (currentTime - oldestRequestTime);
	        
	        System.out.println("Limite de 60 requisições por minuto atingido. " +
	                           "Aguardando " + waitTime + "ms");
	        
	        LogCatcher.logInfo("Limite de 60 requisições por minuto atingido. " +
                    "Aguardando " + waitTime + "ms");
	        
	        Thread.sleep(waitTime);
	        
	        requestTimestamps.removeIf(timestamp -> 
	            currentTime - timestamp > ONE_MINUTE_IN_MS);
	    }

	    requestTimestamps.offer(System.currentTimeMillis());

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

	    reader = (status >= 300) 
	        ? new BufferedReader(new InputStreamReader(https.getErrorStream()))
	        : new BufferedReader(new InputStreamReader(https.getInputStream()));
	    
	    String line;
	    while ((line = reader.readLine()) != null)
	        responseContent.append(line);
	    
	    reader.close();
	    
	    System.out.println("Output from Server .... \n" + status);
	    LogCatcher.logInfo("Output from Server .... \n" + status);
	    
	    String response = responseContent.toString();
	    https.disconnect();
	    
	    return new String[] { Integer.toString(status), response };
	}

	
}
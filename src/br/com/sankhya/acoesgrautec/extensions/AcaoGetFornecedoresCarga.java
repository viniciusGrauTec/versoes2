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
import java.util.Random;

import org.activiti.engine.impl.util.json.JSONArray;
import org.cuckoo.core.ScheduledAction;
import org.cuckoo.core.ScheduledActionContext;

public class AcaoGetFornecedoresCarga implements AcaoRotinaJava, ScheduledAction {

	 	private List<String> selectsParaInsert = new ArrayList<String>();
	 	private EnviromentUtils util = new EnviromentUtils();
	 	static {
	 		LogConfiguration.setPath(SWRepositoryUtils.getBaseFolder() + "/logAcao/logs");
		}
	 	
	 	@Override
	 	public void doAction(ContextoAcao contexto) throws Exception {
	 	    String threadName = Thread.currentThread().getName();
	 	    Date startTime = new Date();

	 	    System.out.println("=== INÍCIO DO BOTÃO DE AÇÃO FORNECEDORES === Thread: " + threadName + " - Hora: " + startTime);
	 	    System.out.println("Iniciando processamento de dados de fornecedores");
	 	    
	 	   LogCatcher.logInfo("=== INÍCIO DO BOTÃO DE AÇÃO FORNECEDORES === Thread: " + threadName + " - Hora: " + startTime);
	 	   LogCatcher.logInfo("Iniciando processamento de dados de fornecedores");

	 	    Registro[] linhas = contexto.getLinhas();
	 	    
	 	    if (linhas.length < 1) {
	 	        contexto.mostraErro("É necessário selecionar pelo menos 1 linha.");
	 	        return;
	 	    }
	 	    
	 	    int linhasParaProcessar = Math.min(linhas.length, 65);
	 	    System.out.println("Processando " + linhasParaProcessar + " registro(s)");
	 	    LogCatcher.logInfo("Processando " + linhasParaProcessar + " registro(s)");
	 	    
	 	    String dataInicio = contexto.getParam("DTINICIO").toString().substring(0, 10);
	 	    String dataFim = contexto.getParam("DTFIM").toString().substring(0, 10);
	 	    String idForn = (String) contexto.getParam("IDFORN");

	 	    try {
	 	        // Parceiros
	 	        List<Object[]> listInfParceiro = retornarInformacoesParceiros();
	 	        Map<String, BigDecimal> mapaInfParceiros = new HashMap<String, BigDecimal>();
	 	        for (Object[] obj : listInfParceiro) {
	 	            BigDecimal codParc = (BigDecimal) obj[0];
	 	            String cpf_cnpj = (String) obj[1];
	 	            String idExterno = (String) obj[2];
	 	            BigDecimal codemp = (BigDecimal) obj[3];

	 	            if (mapaInfParceiros.get(cpf_cnpj) == null) {
	 	                mapaInfParceiros.put(cpf_cnpj, codParc);
	 	            }
	 	        }

	 	        // Id Forncedor
	 	        Map<String, BigDecimal> mapaInfIdParceiros = new HashMap<String, BigDecimal>();
	 	        for (Object[] obj : listInfParceiro) {
	 	            BigDecimal codParc = (BigDecimal) obj[0];
	 	            String cpf_cnpj = (String) obj[1];
	 	            String idExterno = (String) obj[2];
	 	            BigDecimal codemp = (BigDecimal) obj[3];

	 	            if (mapaInfIdParceiros.get(idExterno + "###" + cpf_cnpj + "###" + codemp) == null) {
	 	                mapaInfIdParceiros.put(idExterno + "###" + cpf_cnpj + "###" + codemp, codParc);
	 	            }
	 	        }

	 	        // Processa cada registro selecionado
	 	        for (int i = 0; i < linhasParaProcessar; i++) {
	 	            Registro registro = linhas[i];
	 	            
	 	            String url = (String) registro.getCampo("URL");
	 	            String token = (String) registro.getCampo("TOKEN");
	 	            BigDecimal codEmp = (BigDecimal) registro.getCampo("CODEMP");
	 	            
	 	            System.out.println("Processando registro " + (i+1) + " de " + linhasParaProcessar);
	 	            LogCatcher.logInfo("Processando registro " + (i+1) + " de " + linhasParaProcessar);
	 	            
	 	            // Processa o intervalo de datas para este registro
	 	            processDateRangeByMonthsForSuppliers(mapaInfIdParceiros, mapaInfParceiros, url,
	 	                    token, codEmp, dataInicio, dataFim, idForn);
	 	        }

	 	        contexto.setMensagemRetorno("Período processado com sucesso para " + linhasParaProcessar + " registro(s)!");
	 	        System.out.println("Finalizou o processamento do período de fornecedores");
	 	        LogCatcher.logInfo("Finalizou o processamento do período de fornecedores");

	 	    } catch (Exception e) {
	 	        System.out.println("Erro na ação de fornecedores - Thread: " + threadName);
	 	       LogCatcher.logInfo("Erro na ação de fornecedores - Thread: " + threadName);
	 	        e.printStackTrace();
	 	        try {
	 	            // Log para o primeiro registro como referência
	 	            Registro primeiroRegistro = linhas[0];
	 	            String url = (String) primeiroRegistro.getCampo("URL");
	 	            String token = (String) primeiroRegistro.getCampo("TOKEN");
	 	            
	 	           insertLogIntegracao(
	 	                "Erro ao processar período de fornecedores, Mensagem de erro: " + e.getMessage(),
	 	                "Erro", url, token);
	 	          LogCatcher.logError(
		 	                "Erro ao processar período de fornecedores, Mensagem de erro: " + e.getMessage());
	 	        } catch (Exception e1) {
	 	            e1.printStackTrace();
	 	        }
	 	        contexto.mostraErro("Ocorreu um erro durante o processamento: " + e.getMessage());
	 	       LogCatcher.logInfo("Ocorreu um erro durante o processamento: " + e.getMessage());
	 	    } finally {
	 	        if (selectsParaInsert.size() > 0) {
	 	            StringBuilder msgError = new StringBuilder();

	 	            System.out.println("Entrou na lista do finally: " + selectsParaInsert.size());
	 	            LogCatcher.logInfo("Entrou na lista do finally: " + selectsParaInsert.size());

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
	 	           LogCatcher.logInfo("Consulta de log: \n" + msgError);
	 	            insertLogList(msgError.toString());
	 	        }

	 	        System.out.println("=== FIM DO BOTÃO DE AÇÃO FORNECEDORES === Thread: " + threadName + " - Hora: " + new Date());
	 	       LogCatcher.logInfo("=== FIM DO BOTÃO DE AÇÃO FORNECEDORES === Thread: " + threadName + " - Hora: " + new Date());
	 	    }
	 	}
	 	
	 	@Override
	 	public void onTime(ScheduledActionContext arg0) {
	 		LogConfiguration.setPath(SWRepositoryUtils.getBaseFolder() + "/logAcao/logs");
	 	    EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	 	    JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
	 	    PreparedStatement pstmt = null;
	 	    ResultSet rs = null;

	 	    BigDecimal codEmp = BigDecimal.ZERO;
	 	    String url = "";
	 	    String token = "";

	 	    System.out.println("=== INÍCIO DO JOB DE FORNECEDORES === Hora: " + new Date());
	 	    LogCatcher.logInfo("=== INÍCIO DO JOB DE FORNECEDORES === Hora: " + new Date());

	 	    try {
	 	        Map<String, BigDecimal> mapaInfParceiros = carregarMapaParceiros();
	 	        Map<String, BigDecimal> mapaInfIdParceiros = carregarMapaIdParceiros();

	 	        jdbc.openSession();
	 	        
	 	        int[] contadores = processarEmpresasIntegracao(jdbc, mapaInfIdParceiros, mapaInfParceiros);
	 	        int empresasProcessadas = contadores[0];
	 	        int empresasIgnoradas = contadores[1];
	 	        
	 	        System.out.println("Finalizou o cadastro dos fornecedores no Job - Empresas processadas: " + 
	 	            empresasProcessadas + ", Empresas ignoradas: " + empresasIgnoradas);
	 	        
	 	       LogCatcher.logInfo("Finalizou o cadastro dos fornecedores no Job - Empresas processadas: " + 
		 	            empresasProcessadas + ", Empresas ignoradas: " + empresasIgnoradas);
	 	        
	 	    } catch (Exception e) {
	 	        System.out.println("Erro geral no job: " + e.getMessage());
	 	        LogCatcher.logError("Erro geral no job: " + e.getMessage());
	 	        e.printStackTrace();
	 	        registrarLogErroGeral(e, url, token);
	 	    } finally {
	 	        fecharRecursos(pstmt, rs, jdbc);
	 	       
	 	        processarLogsPendentes();
	 	        
	 	        System.out.println("=== FIM DO JOB DE FORNECEDORES === Hora: " + new Date());
	 	       LogCatcher.logInfo("=== FIM DO JOB DE FORNECEDORES === Hora: " + new Date());
	 	    }
	 	}

	 	/**
	 	 * Carrega o mapa de parceiros pelo CPF/CNPJ
	 	 */
	 	private Map<String, BigDecimal> carregarMapaParceiros() throws Exception {
	 	    List<Object[]> listInfParceiro = retornarInformacoesParceiros();
	 	    Map<String, BigDecimal> mapaInfParceiros = new HashMap<>();
	 	    
	 	    for (Object[] obj : listInfParceiro) {
	 	        BigDecimal codParc = (BigDecimal) obj[0];
	 	        String cpf_cnpj = (String) obj[1];
	 	        
	 	        if (mapaInfParceiros.get(cpf_cnpj) == null) {
	 	            mapaInfParceiros.put(cpf_cnpj, codParc);
	 	        }
	 	    }
	 	    
	 	    return mapaInfParceiros;
	 	}

	 	/**
	 	 * Carrega o mapa de IDs de parceiros
	 	 */
	 	private Map<String, BigDecimal> carregarMapaIdParceiros() throws Exception {
	 	    List<Object[]> listInfParceiro = retornarInformacoesParceiros();
	 	    Map<String, BigDecimal> mapaInfIdParceiros = new HashMap<>();
	 	    
	 	    for (Object[] obj : listInfParceiro) {
	 	        BigDecimal codParc = (BigDecimal) obj[0];
	 	        String cpf_cnpj = (String) obj[1];
	 	        String idExterno = (String) obj[2];
	 	        BigDecimal codemp = (BigDecimal) obj[3];

	 	        String chave = idExterno + "###" + cpf_cnpj + "###" + codemp;
	 	        if (mapaInfIdParceiros.get(chave) == null) {
	 	            mapaInfIdParceiros.put(chave, codParc);
	 	        }
	 	    }
	 	    
	 	    return mapaInfIdParceiros;
	 	}

	 	/**
	 	 * Processa as empresas para integração
	 	 * @return int[] com [empresasProcessadas, empresasIgnoradas]
	 	 */
	 	private int[] processarEmpresasIntegracao(JdbcWrapper jdbc, 
	 	        Map<String, BigDecimal> mapaInfIdParceiros, 
	 	        Map<String, BigDecimal> mapaInfParceiros) throws Exception {
	 	    
	 	    PreparedStatement pstmt = null;
	 	    ResultSet rs = null;
	 	    int empresasProcessadas = 0;
	 	    int empresasIgnoradas = 0;
	 	    
	 	    try {
	 	        String query = "SELECT CODEMP, URL, TOKEN, INTEGRACAO FROM AD_LINKSINTEGRACAO";
	 	        pstmt = jdbc.getPreparedStatement(query);
	 	        rs = pstmt.executeQuery();
	 	        
	 	        while (rs.next()) {
	 	            BigDecimal codEmp = rs.getBigDecimal("CODEMP");
	 	            String url = rs.getString("URL");
	 	            String token = rs.getString("TOKEN");
	 	            String statusIntegracao = rs.getString("INTEGRACAO");

	 	            if (!"S".equals(statusIntegracao)) {
	 	                System.out.println("Integração desativada para a empresa " + codEmp + " - pulando processamento");
	 	                LogCatcher.logInfo("Integração desativada para a empresa " + codEmp + " - pulando processamento");
	 	                empresasIgnoradas++;
	 	                continue;
	 	            }
	 	            
	 	            try {
	 	                System.out.println("Iniciando processamento para empresa: " + codEmp);
	 	                LogCatcher.logInfo("Iniciando processamento para empresa: " + codEmp);
	 	                iterarEndpoint(mapaInfIdParceiros, mapaInfParceiros, url, token, codEmp);
	 	                empresasProcessadas++;

	 	                insertLogIntegracao("Integração de fornecedores executada com sucesso para empresa: " + codEmp, 
	 	                    "Sucesso", url, token);
	 	               LogCatcher.logInfo("Integração de fornecedores executada com sucesso para empresa: " + codEmp);
	 	            } catch (Exception e) {
	 	                System.out.println("Erro ao processar empresa " + codEmp + ": " + e.getMessage());
	 	               LogCatcher.logError("Erro ao processar empresa " + codEmp + ": " + e.getMessage());
	 	                e.printStackTrace();
	 	                
	 	                insertLogIntegracao("Erro ao processar fornecedores para empresa: " + codEmp + 
	 	                    ", Mensagem: " + e.getMessage(), "Erro", url, token);
	 	               LogCatcher.logError("Erro ao processar fornecedores para empresa: " + codEmp + 
		 	                    ", Mensagem: " + e.getMessage());
		 	               
	 	            }
	 	        }
	 	        
	 	        return new int[] {empresasProcessadas, empresasIgnoradas};
	 	        
	 	    } finally {
	 	        fecharRecursos(pstmt, rs, null);
	 	    }
	 	}

	 	/**
	 	 * Fecha recursos de banco de dados de forma segura
	 	 */
	 	private void fecharRecursos(PreparedStatement pstmt, ResultSet rs, JdbcWrapper jdbc) {
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
	 	    
	 	    if (jdbc != null) {
	 	        jdbc.closeSession();
	 	    }
	 	}

	 	/**
	 	 * Registra log de erro geral
	 	 */
	 	private void registrarLogErroGeral(Exception e, String url, String token) {
	 	    try {
	 	        insertLogIntegracao(
	 	            "Erro geral ao integrar Fornecedores, Mensagem de erro: " + e.getMessage(), 
	 	            "Erro", url, token);
	 	       LogCatcher.logError(
		 	            "Erro geral ao integrar Fornecedores, Mensagem de erro: " + e.getMessage());
	 	    } catch (Exception e1) {
	 	        e1.printStackTrace();
	 	    }
	 	}

	 	/**
	 	 * Processa os logs pendentes para inserção no banco
	 	 */
	 	private void processarLogsPendentes() {
	 	    if (selectsParaInsert.size() > 0) {
	 	        StringBuilder msgError = new StringBuilder();
	 	        EnviromentUtils util = new EnviromentUtils();
	 	        
	 	        System.out.println("Processando logs pendentes: " + selectsParaInsert.size());
	 	        LogCatcher.logInfo("Processando logs pendentes: " + selectsParaInsert.size());
	 	        
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
	 	        
	 	        System.out.println("Executando inserção de logs pendentes...");
	 	       LogCatcher.logInfo("Executando inserção de logs pendentes...");
	 	        try {
	 	            insertLogList(msgError.toString());
	 	            System.out.println("Inserção de logs concluída com sucesso");
	 	           LogCatcher.logInfo("Inserção de logs concluída com sucesso");
	 	        } catch (Exception e) {
	 	            System.out.println("Erro ao inserir logs: " + e.getMessage());
	 	            LogCatcher.logError("Erro ao inserir logs: " + e.getMessage());
	 	            e.printStackTrace();
	 	        }
	 	        
	 	        selectsParaInsert = new ArrayList<String>();
	 	    }
	 	}


	 	public void processDateRangeByMonthsForSuppliers(
	 	        Map<String, BigDecimal> mapaInfIdParceiros,
	 	        Map<String, BigDecimal> mapaInfParceiros,
	 	        String url,
	 	        String token,
	 	        BigDecimal codEmp,
	 	        String dataInicio,
	 	        String dataFim,
	 	        String idForn) throws Exception {

	 	    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
	 	    LocalDate inicio = LocalDate.parse(dataInicio, formatter);
	 	    LocalDate fim = LocalDate.parse(dataFim, formatter);

	 	    LocalDate periodoInicio = inicio;
	 	    while (periodoInicio.isBefore(fim) || periodoInicio.isEqual(fim)) {
	 	        LocalDate periodoFim = periodoInicio.plusMonths(1).withDayOfMonth(1).minusDays(1);
	 	        if (periodoFim.isAfter(fim)) {
	 	            periodoFim = fim;
	 	        }

	 	        System.out.println("Processando período: " + periodoInicio + " até " + periodoFim);
	 	       LogCatcher.logInfo("Processando período: " + periodoInicio + " até " + periodoFim);
	 	        processDateRangeForSuppliers(
	 	            mapaInfIdParceiros,
	 	            mapaInfParceiros,
	 	            url,
	 	            token,
	 	            codEmp,
	 	            periodoInicio.format(formatter),
	 	            periodoFim.format(formatter),
	 	            idForn
	 	            
	 	        );

	 	        periodoInicio = periodoInicio.plusMonths(1).withDayOfMonth(1);
	 	    }
	 	}

	 	public void processDateRangeForSuppliers(
	 	        Map<String, BigDecimal> mapaInfIdParceiros,
	 	        Map<String, BigDecimal> mapaInfParceiros,
	 	        String url,
	 	        String token,
	 	        BigDecimal codEmp,
	 	        String dataInicio,
	 	        String dataFim,
	 	        String idForn) throws Exception {

	 	    try {
	 	        String dataInicialCompleta = dataInicio + " 00:00:00";
	 	        String dataFinalCompleta = dataFim + " 23:59:59";

	 	        String dataInicialEncoded = URLEncoder.encode(dataInicialCompleta, "UTF-8");
	 	        String dataFinalEncoded = URLEncoder.encode(dataFinalCompleta, "UTF-8");

	 	        JSONArray todosRegistros = new JSONArray();
	 	        int pagina = 1;
	 	        boolean temMaisRegistros = true;
	 	        
	 	        int tentativas = 0;
	 	        final int MAX_TENTATIVAS = 5; // Aumentado para 5 tentativas
	 	        final long TEMPO_ESPERA_BASE = 10000; // 10 segundos de base
	 	        final long TEMPO_ESPERA_MAX = 300000; // Máximo de 5 minutos de espera
	 	        final Random random = new Random(); // Para adicionar jitter ao tempo de espera

	 	        while (temMaisRegistros) {
	 	            StringBuilder urlBuilder = new StringBuilder();
	 	            urlBuilder.append(url.trim())
	 	                    .append("/financeiro/clientes/fornecedores")
	 	                    .append("?pagina=").append(pagina)
	 	                    .append("&quantidade=100")
	 	                    .append("&dataInicial=").append(dataInicialEncoded)
	 	                    .append("&dataFinal=").append(dataFinalEncoded);

	 	            if (idForn != null && !idForn.isEmpty()) {
	 	                String fornecedorEncoded = URLEncoder.encode(idForn, "UTF-8");      
	 	                urlBuilder.append("&fornecedor=").append(fornecedorEncoded);
	 	            }
	 	            
	 	            
	 	            String urlCompleta = urlBuilder.toString();
	 	            System.out.println("URL para fornecedores (período: " + dataInicio + " a " + dataFim + ", página " + pagina + "): " + urlCompleta);
	 	           LogCatcher.logInfo("URL para fornecedores (período: " + dataInicio + " a " + dataFim + ", página " + pagina + "): " + urlCompleta);

	 	            try {
	 	                // Fazer a requisição
	 	                String[] response = apiGet2(urlCompleta, token);
	 	                int status = Integer.parseInt(response[0]);

	 	                if (status == 200) {
	 	                    JSONArray paginaAtual = new JSONArray(response[1]);
	 	                    
	 	                    // Verificar se há registros nesta página
	 	                    if (paginaAtual.length() == 0) {
	 	                        // Página vazia, parar o processamento
	 	                        temMaisRegistros = false;
	 	                        System.out.println("Página " + pagina + " vazia. Finalizando coleta de dados.");
	 	                       LogCatcher.logInfo("Página " + pagina + " vazia. Finalizando coleta de dados.");
	 	                    } else {
	 	                        // Adicionar registros ao array acumulado
	 	                        for (int i = 0; i < paginaAtual.length(); i++) {
	 	                            todosRegistros.put(paginaAtual.getJSONObject(i));
	 	                        }
	 	                        if (paginaAtual.length() < 100) {
	 	                            temMaisRegistros = false;
	 	                            System.out.println("Última página encontrada com " + paginaAtual.length() + " registros.");
	 	                           LogCatcher.logInfo("Última página encontrada com " + paginaAtual.length() + " registros.");
	 	                        } else {
	 	                            pagina++;
	 	                            System.out.println("Página " + (pagina-1) + " completa com 100 registros. Avançando para página " + pagina);
	 	                           LogCatcher.logInfo("Página " + (pagina-1) + " completa com 100 registros. Avançando para página " + pagina);
	 	                            
	 	                            Thread.sleep(2000 + random.nextInt(1000));
	 	                        }
	 	                        
	 	                       LogCatcher.logInfo("Período " + dataInicio + " a " + dataFim + " - Página " + (pagina-1) + ": " + 
                                        paginaAtual.length() + " registros. Total acumulado: " + 
                                        todosRegistros.length());
	 	                        System.out.println("Período " + dataInicio + " a " + dataFim + " - Página " + (pagina-1) + ": " + 
	 	                                        paginaAtual.length() + " registros. Total acumulado: " + 
	 	                                        todosRegistros.length());
	 	                    }
	 	                   
	 	                    tentativas = 0;
	 	                } else if (status == 404) {
	 	               
	 	                    temMaisRegistros = false;
	 	                    System.out.println("Página " + pagina + " não encontrada (404). Finalizando coleta de dados.");
	 	                   LogCatcher.logError("Página " + pagina + " não encontrada (404). Finalizando coleta de dados.");
	 	                } else if (status == 429) {
	 	                  
	 	                    tentativas++;
	 	                    if (tentativas <= MAX_TENTATIVAS) {
	 	                       
	 	                        long tempoEspera = Math.min(
	 	                            TEMPO_ESPERA_MAX,
	 	                            TEMPO_ESPERA_BASE * (long)Math.pow(2, tentativas-1) + random.nextInt(3000)
	 	                        );
	 	                        
	 	                       LogCatcher.logError("Erro 429 (Too Many Requests). Tentativa " + tentativas + 
                                        " de " + MAX_TENTATIVAS + ". Código: CORE_E01128. Aguardando " + 
                                        (tempoEspera / 1000) + " segundos.");
	 	                        System.out.println("Erro 429 (Too Many Requests). Tentativa " + tentativas + 
	 	                                         " de " + MAX_TENTATIVAS + ". Código: CORE_E01128. Aguardando " + 
	 	                                         (tempoEspera / 1000) + " segundos.");
	 	                        
	 	                        System.out.println("Detalhes da requisição: URL=" + urlCompleta);
	 	                        System.out.println("Resposta do servidor: " + response[1]);
	 	                       LogCatcher.logInfo("Detalhes da requisição: URL=" + urlCompleta);
	 	                       LogCatcher.logInfo("Resposta do servidor: " + response[1]);
	 	                        
	 	                        Thread.sleep(tempoEspera);
	 	                        continue; 
	 	                    } else {
	 	                        System.err.println("Erro 429 (Too Many Requests) persistente após " + 
	 	                                        MAX_TENTATIVAS + " tentativas. URL: " + urlCompleta);
	 	                        
	 	                       LogCatcher.logError("Erro 429 (Too Many Requests) persistente após " + 
                                        MAX_TENTATIVAS + " tentativas. URL: " + urlCompleta);
	 	                       
	 	                        if (todosRegistros.length() > 0) {
	 	                            System.out.println("Processando os " + todosRegistros.length() + 
	 	                                             " registros obtidos antes do erro persistente 429...");
	 	                           LogCatcher.logInfo("Processando os " + todosRegistros.length() + 
                                            " registros obtidos antes do erro persistente 429...");
	 	                            break; 
	 	                        }
	 	                        
	 	                        throw new Exception(String.format(
	 	                            "Erro 429 (Too Many Requests) persistente após %d tentativas. Código: CORE_E01128. URL: %s",
	 	                            MAX_TENTATIVAS, urlCompleta
	 	                        ));
	 	                    }
	 	                } else {
	 	                    throw new Exception(String.format(
	 	                        "Erro na requisição de fornecedores. Status: %d. Resposta: %s. URL: %s",
	 	                        status, response[1], urlCompleta
	 	                    ));
	 	                }
	 	            } catch (Exception e) {

	 	                if (e.getMessage().contains("timeout") || 
	 	                    e.getMessage().contains("connection") ||
	 	                    e.getMessage().contains("reset")) {
	 	                    
	 	                    tentativas++;
	 	                    if (tentativas <= MAX_TENTATIVAS) {
	 	                        long tempoEspera = Math.min(
	 	                            TEMPO_ESPERA_MAX,
	 	                            TEMPO_ESPERA_BASE * (long)Math.pow(2, tentativas-1) + random.nextInt(3000)
	 	                        );
	 	                        
	 	                        System.out.println("Erro de conexão. Tentativa " + tentativas + 
	 	                                         " de " + MAX_TENTATIVAS + ". Aguardando " + 
	 	                                         (tempoEspera / 1000) + " segundos.");
	 	                       LogCatcher.logError("Erro de conexão. Tentativa " + tentativas + 
                                        " de " + MAX_TENTATIVAS + ". Aguardando " + 
                                        (tempoEspera / 1000) + " segundos.");
	 	                        Thread.sleep(tempoEspera);
	 	                        continue; 
	 	                    }
	 	                }
	 	                throw e;
	 	            }
	 	        }

	 	        if (todosRegistros.length() == 0) {
	 	            System.out.println("Nenhum registro de fornecedor encontrado para o período " + dataInicio + " a " + dataFim);
	 	           LogCatcher.logInfo("Nenhum registro de fornecedor encontrado para o período " + dataInicio + " a " + dataFim);
	 	        } else {

	 	            String[] responseArray = new String[]{String.valueOf(200), todosRegistros.toString()};
	 	            cadastrarFornecedor(mapaInfIdParceiros, mapaInfParceiros, responseArray, codEmp);
	 	            System.out.println("Total de registros processados para o período " + dataInicio + " a " + dataFim + ": " + todosRegistros.length());
	 	           LogCatcher.logInfo("Total de registros processados para o período " + dataInicio + " a " + dataFim + ": " + todosRegistros.length());
	 	        }

	 	    } catch (Exception e) {
	 	        System.err.println("Erro ao processar período " + dataInicio + " até " + dataFim + ": " + e.getMessage());
	 	        LogCatcher.logError("Erro ao processar período " + dataInicio + " até " + dataFim + ": " + e.getMessage());
	 	        e.printStackTrace();
	 	        throw e;
	 	    }
	 	}
	 	
		public void iterarEndpoint(Map<String, BigDecimal> mapaInfIdParceiros,
				Map<String, BigDecimal> mapaInfParceiros, String url, String token,
				BigDecimal codEmp) throws Exception {
			
			Date dataAtual = new Date();

			SimpleDateFormat formato = new SimpleDateFormat("yyyy-MM-dd");

			String dataFormatada = formato.format(dataAtual);
			
			try {

				LogCatcher.logInfo("IterarEndpoint do titulos de Fornecedores");

				String[] response = apiGet2(url
						+ "/financeiro/clientes/fornecedores?" + "quantidade=0"
						+ "&dataInicial=" + dataFormatada + " 00:00:00&dataFinal="
						+ dataFormatada + " 23:59:59", token);

				int status = Integer.parseInt(response[0]);
				String responseString = response[1];

				LogCatcher.logInfo("Status teste: " + status);

				LogCatcher.logInfo("response do job: " + responseString);

				if (status == 200) {
					LogCatcher.logInfo("Sucesso (200): Requisição bem-sucedida. Processando dados de fornecedores...");
					cadastrarFornecedor(mapaInfIdParceiros, mapaInfParceiros,
							response, codEmp);
				} else if (status >= 400 && status < 500) {
					String erroMsg;
					if (status == 400) {
						erroMsg = "Erro do Cliente (400 - Requisição Inválida): A requisição para buscar fornecedores falhou. Resposta: " + responseString;
					} else if (status == 401) {
						erroMsg = "Erro do Cliente (401 - Não Autorizado): O token de autenticação é inválido ou ausente para fornecedores. Resposta: " + responseString;
					} else if (status == 403) {
						erroMsg = "Erro do Cliente (403 - Proibido): Você não tem permissão para acessar fornecedores. Resposta: " + responseString;
					} else if (status == 404) {
						erroMsg = "Erro do Cliente (404 - Não Encontrado): O recurso de fornecedores solicitado não foi encontrado. Resposta: " + responseString;
					} else if (status == 429) {
						erroMsg = "Erro do Cliente (429 - Muitas Requisições): Limite de taxa da API excedido para fornecedores. Resposta: " + responseString;
					} else {
						erroMsg = "Erro do Cliente (" + status + "): A requisição para buscar fornecedores falhou. Resposta: " + responseString;
					}
					LogCatcher.logError(erroMsg);
					 selectsParaInsert.add("SELECT <#NUMUNICO#>, \'" + erroMsg.replace("\'", "\'\'") + "\', SYSDATE, \'Erro\', " + codEmp + ", NULL FROM DUAL");
				} else if (status >= 500 && status < 600) {
					String erroMsg = "Erro do Servidor (" + status + "): Ocorreu um problema no servidor da API ao buscar fornecedores. Resposta: " + responseString;
					LogCatcher.logError(erroMsg);
					 selectsParaInsert.add("SELECT <#NUMUNICO#>, \'" + erroMsg.replace("\'", "\'\'") + "\', SYSDATE, \'Erro\', " + codEmp + ", NULL FROM DUAL");
				} else {
					String erroMsg = "Status inesperado (" + status + "): A API retornou um código não previsto ao buscar fornecedores. Resposta: " + responseString;
					LogCatcher.logError(erroMsg);
					 selectsParaInsert.add("SELECT <#NUMUNICO#>, \'" + erroMsg.replace("\'", "\'\'") + "\', SYSDATE, \'Erro\', " + codEmp + ", NULL FROM DUAL");
				}

			} catch (Exception e) {
				e.printStackTrace();
				LogCatcher.logError("Erro na iteração do endpoint de fornecedores da empresa " + codEmp + ": " + e.getMessage());
				LogCatcher.logError(e);
				throw e;
			}
		}
	 	

	 	public void cadastrarFornecedor(Map<String, BigDecimal> mapaInfIdParceiros,
	 	        Map<String, BigDecimal> mapaInfParceiros, String[] response,
	 	        BigDecimal codEmp) {
	 	    
	 	    String fornecedorId = "";
	 	    
	 	    try {
	 	        if (response == null || response.length < 2) {
	 	            registrarErro("Resposta da API inválida ou incompleta", null, codEmp);
	 	           LogCatcher.logError("Resposta da API inválida ou incompleta");
	 	            return;
	 	        }
	 	        
	 	        String status = response[0];
	 	        String responseString = response[1];
	 	        
	 	        if (!status.equalsIgnoreCase("200")) {
	 	            selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Status de Retorno da API Diferente de Sucesso nos Fornecedores, Status Retornado: " + 
	 	                    status + "', SYSDATE, 'Aviso', " + codEmp + ", '" + fornecedorId + "' FROM DUAL");
	 	           LogCatcher.logError("SELECT <#NUMUNICO#>, 'Status de Retorno da API Diferente de Sucesso nos Fornecedores, Status Retornado: " + 
	 	                    status + "', SYSDATE, 'Aviso', " + codEmp + ", '" + fornecedorId + "' FROM DUAL");
	 	            return;
	 	        }
	 	        
	 	        JsonParser parser = new JsonParser();
	 	        JsonArray jsonArray = parser.parse(responseString).getAsJsonArray();
	 	        int fornecedoresCadastrados = 0;
	 	        
	 	        for (JsonElement jsonElement : jsonArray) {
	 	            JsonObject jsonObject = jsonElement.getAsJsonObject();

	 	            fornecedorId = extrairValorJson(jsonObject, "fornecedor_id");
	 	            String fornecedorTipo = extrairValorJson(jsonObject, "fornecedor_tipo");
	 	            String fornecedorNome = extrairValorJson(jsonObject, "fornecedor_nome");
	 	            String fornecedorCpfcnpj = extrairValorJson(jsonObject, "fornecedor_cpfcnpj");
	 	            String fornecedorCidade = extrairValorJson(jsonObject, "fornecedor_cidade");
	 	            
	 	            if (fornecedorCidade == null || fornecedorCidade.trim().isEmpty()) {
	 	                registrarErro("Cadastro de fornecedor rejeitado: Cidade ausente", fornecedorId, codEmp);
	 	               LogCatcher.logError("Cadastro de fornecedor rejeitado: Cidade ausente");
	 	                continue; 
	 	            }
	 	            
	 	            String fornecedorNomeFantasia = extrairValorJson(jsonObject, "fornecedor_nomefantasia");
	 	            if (fornecedorNomeFantasia == null) {
	 	                fornecedorNomeFantasia = fornecedorNome;
	 	            }
	 	            
	 	            String fornecedorEndereco = extrairValorJson(jsonObject, "fornecedor_endereco");
	 	            String fornecedorBairro = extrairValorJson(jsonObject, "fornecedor_bairro");
	 	            String fornecedorUf = extrairValorJson(jsonObject, "fornecedor_uf");
	 	            String fornecedorCep = extrairValorJson(jsonObject, "fornecedor_cep");
	 	            String fornecedorInscMunicipal = extrairValorJson(jsonObject, "fornecedor_isncmunicipal");
	 	            String fornecedorInscestadual = extrairValorJson(jsonObject, "fornecedor_inscestadual");
	 	            String fornecedorFone1 = extrairValorJson(jsonObject, "fornecedor_fone1");
	 	            String fornecedorFone2 = extrairValorJson(jsonObject, "fornecedor_fone2");
	 	            String fornecedorFax = extrairValorJson(jsonObject, "fornecedor_fax");
	 	            String fornecedorCelular = extrairValorJson(jsonObject, "fornecedor_celular");
	 	            String fornecedorContato = extrairValorJson(jsonObject, "fornecedor_contato");
	 	            String fornecedorEmail = extrairValorJson(jsonObject, "fornecedor_email");
	 	            String fornecedorHomepage = extrairValorJson(jsonObject, "fornecedor_homepage");
	 	            
	 	            String fornecedorAtivo = extrairValorJson(jsonObject, "fornecedor_ativo");
	 	            if (fornecedorAtivo == null || fornecedorAtivo.isEmpty()) {
	 	                fornecedorAtivo = "S";
	 	            }
	 	            
	 	            String dataAtualizacao = extrairValorJson(jsonObject, "data_atualizacao");
	 	            
	 	            boolean dadosValidos = validarCamposObrigatorios(
	 	                    fornecedorId, fornecedorNome, fornecedorCpfcnpj, fornecedorCidade);
	 	            
	 	            if (dadosValidos) {
	 	            	if (verificarIdAcadWebExiste(fornecedorId, codEmp)) {
	 	                    // ID já existe para esta empresa, registrar erro e continuar para o próximo
	 	                  //  registrarErro("ID AcadWeb já cadastrado para empresa " + codEmp + ": " + fornecedorId, fornecedorId, codEmp);
	 	                    continue;
	 	                }
	 	                boolean fornecedorExiste = mapaInfParceiros.get(fornecedorCpfcnpj) != null;
	 	                
	 	               BigDecimal codParc;
	 	                
	 	                if (!fornecedorExiste) {
	 	                	codParc =  insertFornecedor(
	 	                            fornecedorTipo, fornecedorId, fornecedorNome,
	 	                            fornecedorNomeFantasia, fornecedorEndereco,
	 	                            fornecedorBairro, fornecedorCidade,
	 	                            fornecedorUf, fornecedorCep,
	 	                            fornecedorInscMunicipal, fornecedorCpfcnpj,
	 	                            fornecedorHomepage, fornecedorAtivo,
	 	                            dataAtualizacao, fornecedorInscestadual,
	 	                            fornecedorFone1, fornecedorFone2,
	 	                            fornecedorFax, fornecedorCelular,
	 	                            fornecedorContato, fornecedorNome,
	 	                            fornecedorEmail, codEmp, mapaInfParceiros);
	 	                	
	 	                	if (codParc == null) {
	 	                        continue;
	 	                    }
	 	                	fornecedoresCadastrados++;
	 	                } else {
	 	                    codParc = mapaInfParceiros.get(fornecedorCpfcnpj);
	 	                }
	 	                
	 	                boolean idFornInserido = insertIdForn(fornecedorId, codParc, codEmp, mapaInfIdParceiros);
	 	                if (idFornInserido) {
	 	                    fornecedoresCadastrados++;
	 	                }
	 	            } else {
	 	                registrarErroCamposObrigatorios(fornecedorId, fornecedorNome, 
	 	                        fornecedorCpfcnpj, fornecedorCidade, codEmp);
	 	            }
	 	        }
	 	        
	 	        System.out.println("Processamento concluído. " + fornecedoresCadastrados + " fornecedores processados.");
	 	       LogCatcher.logInfo("Processamento concluído. " + fornecedoresCadastrados + " fornecedores processados.");
	 	        
	 	    } catch (Exception e) {
	 	        e.printStackTrace();
	 	        registrarErro("Erro no chamado do endpoint: " + e.getMessage(), fornecedorId, codEmp);
	 	       LogCatcher.logError("Erro no chamado do endpoint: " + e.getMessage());
	 	    }
	 	}
	 	
	 	private boolean verificarIdAcadWebExiste(String idAcadWeb, BigDecimal codEmp) throws Exception {
	 	    EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	 	    JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
	 	    PreparedStatement pstmt = null;
	 	    ResultSet rs = null;
	 	    
	 	    try {
	 	        jdbc.openSession();
	 	        
	 	        // Verificar se o IDACADWEB já existe para esta empresa específica
	 	        String sql = "SELECT COUNT(*) AS TOTAL FROM AD_IDFORNACAD WHERE IDACADWEB = ? AND CODEMP = ?";
	 	        pstmt = jdbc.getPreparedStatement(sql);
	 	        pstmt.setString(1, idAcadWeb);
	 	        pstmt.setBigDecimal(2, codEmp);
	 	        rs = pstmt.executeQuery();
	 	        
	 	        if (rs.next() && rs.getInt("TOTAL") > 0) {
	 	            System.out.println("IDACADWEB já existe para empresa " + codEmp + ": " + idAcadWeb);
	 	           LogCatcher.logInfo("IDACADWEB já existe para empresa " + codEmp + ": " + idAcadWeb);
	 	            return true; // ID já existe para esta empresa
	 	        }
	 	        
	 	        System.out.println("IDACADWEB não existe para empresa " + codEmp + ": " + idAcadWeb);
	 	       LogCatcher.logInfo("IDACADWEB não existe para empresa " + codEmp + ": " + idAcadWeb);
	 	        return false; // ID não existe para esta empresa
	 	    } finally {
	 	        if (rs != null) {
	 	            try { rs.close(); } catch (SQLException e) { e.printStackTrace(); }
	 	        }
	 	        if (pstmt != null) {
	 	            try { pstmt.close(); } catch (SQLException e) { e.printStackTrace(); }
	 	        }
	 	        jdbc.closeSession();
	 	    }
	 	}

	 	
		public boolean insertIdForn(String fornecedorId, BigDecimal codParc, BigDecimal codEmp,
				Map<String, BigDecimal> mapaInfIdParceiros) throws Exception {
			EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
			JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
			PreparedStatement pstmt = null;
			ResultSet rs = null;

			try {
				jdbc.openSession();

// Verificar se o IDACADWEB já existe para esta empresa específica
				String sqlVerificacao = "SELECT COUNT(*) AS TOTAL FROM AD_IDFORNACAD WHERE IDACADWEB = ? AND CODEMP = ?";
				pstmt = jdbc.getPreparedStatement(sqlVerificacao);
				pstmt.setString(1, fornecedorId);
				pstmt.setBigDecimal(2, codEmp);
				rs = pstmt.executeQuery();

				if (rs.next() && rs.getInt("TOTAL") > 0) {
					// ID já existe para esta empresa
					System.out.println("ID AcadWeb já existe para empresa " + codEmp + ": " + fornecedorId);
					selectsParaInsert.add("SELECT <#NUMUNICO#>, 'ID AcadWeb já cadastrado para empresa " + codEmp + ": "
							+ fornecedorId + "', SYSDATE, 'Erro', " + codEmp + ", '" + fornecedorId + "' FROM DUAL");
					LogCatcher.logInfo("ID AcadWeb já existe para empresa " + codEmp + ": " + fornecedorId);
					selectsParaInsert.add("SELECT <#NUMUNICO#>, 'ID AcadWeb já cadastrado para empresa " + codEmp + ": "
							+ fornecedorId + "', SYSDATE, 'Erro', " + codEmp + ", '" + fornecedorId + "' FROM DUAL");
					return false;
				}

// Fechar recursos da consulta
				if (rs != null)
					rs.close();
				if (pstmt != null)
					pstmt.close();

// Obter próximo ID para a tabela
				String sqlNextId = "SELECT NVL(MAX(ID), 0) + 1 AS NEXT_ID FROM AD_IDFORNACAD";
				pstmt = jdbc.getPreparedStatement(sqlNextId);
				rs = pstmt.executeQuery();

				BigDecimal nextId = BigDecimal.ONE; // Valor padrão caso a consulta falhe
				if (rs.next()) {
					nextId = rs.getBigDecimal("NEXT_ID");
				}

// Fechar recursos da consulta de ID
				if (rs != null)
					rs.close();
				if (pstmt != null)
					pstmt.close();

// Inserir na AD_IDFORNACAD incluindo a coluna ID
				String sqlInsert = "INSERT INTO AD_IDFORNACAD (ID, CODPARC, IDACADWEB, CODEMP) VALUES (?, ?, ?, ?)";
				pstmt = jdbc.getPreparedStatement(sqlInsert);
				pstmt.setBigDecimal(1, nextId);
				pstmt.setBigDecimal(2, codParc);
				pstmt.setString(3, fornecedorId);
				pstmt.setBigDecimal(4, codEmp);

// Imprimir valores para debug
				System.out.println("Inserindo na AD_IDFORNACAD: ID=" + nextId + ", CODPARC=" + codParc + ", IDACADWEB="
						+ fornecedorId + ", CODEMP=" + codEmp);
				LogCatcher.logInfo("Inserindo na AD_IDFORNACAD: ID=" + nextId + ", CODPARC=" + codParc + ", IDACADWEB="
						+ fornecedorId + ", CODEMP=" + codEmp);

				pstmt.executeUpdate();

				System.out.println("Inserção bem-sucedida na AD_IDFORNACAD");
				LogCatcher.logInfo("Inserção bem-sucedida na AD_IDFORNACAD");

// Atualizar o mapa de IDs de parceiros
				String cpfCnpj = obterCpfCnpjDoParceiro(codParc, jdbc);
				if (cpfCnpj != null) {
					String chaveIdFornecedor = fornecedorId + "###" + cpfCnpj + "###" + codEmp;
					mapaInfIdParceiros.put(chaveIdFornecedor, codParc);
				}

				return true;

			} catch (SQLException e) {
// Tratamento de erros (mantido como antes)
				String codigoErro = e.getMessage();
				String mensagemAmigavel = "Erro ao cadastrar ID do fornecedor: ";
				LogCatcher.logInfo("Erro ao cadastrar ID do fornecedor: ");

				if (codigoErro.contains("unique constraint") || codigoErro.contains("ORA-00001")) {
					mensagemAmigavel += "Este ID de fornecedor já existe no sistema para esta empresa.";
				} else if (codigoErro.contains("integrity constraint") || codigoErro.contains("ORA-02291")) {
					mensagemAmigavel += "O fornecedor referenciado não foi encontrado no sistema.";
				} else if (codigoErro.contains("cannot insert NULL") || codigoErro.contains("ORA-01400")) {
					mensagemAmigavel += "Dados obrigatórios estão ausentes.";
				} else if (codigoErro.contains("value too large") || codigoErro.contains("ORA-12899")) {
					mensagemAmigavel += "Um ou mais valores excede o tamanho permitido.";
				} else if (codigoErro.contains("subconsulta de uma única linha") || codigoErro.contains("ORA-01427")) {
					mensagemAmigavel += "Foram encontrados múltiplos fornecedores com o mesmo CPF/CNPJ no sistema.";
				} else {
					mensagemAmigavel += "Ocorreu um problema técnico. Detalhes: " + codigoErro;
					LogCatcher.logError("Ocorreu um problema técnico. Detalhes: " + codigoErro);
					
				}

// Registrar erro detalhado para debug
				System.err.println("ERRO SQL: " + codigoErro);
				LogCatcher.logError("ERRO SQL: " + codigoErro);

				selectsParaInsert.add("SELECT <#NUMUNICO#>, '" + mensagemAmigavel.replace("'", "\"")
						+ "', SYSDATE, 'Erro', " + codEmp + ", '" + fornecedorId + "' FROM DUAL");
				LogCatcher.logInfo("SELECT <#NUMUNICO#>, '" + mensagemAmigavel.replace("'", "\"")
				+ "', SYSDATE, 'Erro', " + codEmp + ", '" + fornecedorId + "' FROM DUAL");
				e.printStackTrace();
				return false;
			} finally {
				if (rs != null) {
					try {
						rs.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
				if (pstmt != null) {
					try {
						pstmt.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
				jdbc.closeSession();
			}
		}


		
		private String obterCpfCnpjDoParceiro(BigDecimal codParc, JdbcWrapper jdbc) throws SQLException {
			PreparedStatement pstmt = null;
			ResultSet rs = null;
			try {
				String sql = "SELECT CGC_CPF FROM TGFPAR WHERE CODPARC = ?";
				try {
					pstmt = jdbc.getPreparedStatement(sql);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				pstmt.setBigDecimal(1, codParc);
				rs = pstmt.executeQuery();

				if (rs.next()) {
					return rs.getString("CGC_CPF");
				}
				return null;
			} finally {
				if (rs != null)
					rs.close();
				if (pstmt != null)
					pstmt.close();
			}
		}

	 	/**
	 	 * Extrai um valor de um campo do objeto JSON, retornando null se for nulo
	 	 * 
	 	 * @param jsonObject Objeto JSON a ser consultado
	 	 * @param campoNome Nome do campo a ser extraído
	 	 * @return O valor do campo ou null se for nulo
	 	 */
	 	private String extrairValorJson(JsonObject jsonObject, String campoNome) {
	 	    return jsonObject.get(campoNome).isJsonNull() ? null : jsonObject.get(campoNome).getAsString();
	 	}

	 	/**
	 	 * Registra erro relacionado a campos obrigatórios não preenchidos
	 	 * 
	 	 * @param fornecedorId ID do fornecedor
	 	 * @param fornecedorNome Nome do fornecedor
	 	 * @param fornecedorCpfcnpj CPF/CNPJ do fornecedor
	 	 * @param fornecedorCidade Cidade do fornecedor
	 	 * @param codEmp Código da empresa
	 	 */
	 	private void registrarErroCamposObrigatorios(String fornecedorId, String fornecedorNome, 
	 	        String fornecedorCpfcnpj, String fornecedorCidade, BigDecimal codEmp) {
	 	    
	 	    StringBuilder mensagemErro = new StringBuilder("Campos obrigatórios não preenchidos: ");
	 	    
	 	    if (fornecedorId == null || fornecedorId.isEmpty()) {
	 	        mensagemErro.append("ID, ");
	 	    }
	 	    
	 	    if (fornecedorNome == null || fornecedorNome.isEmpty()) {
	 	        mensagemErro.append("Nome, ");
	 	    }
	 	    
	 	    if (fornecedorCpfcnpj == null || fornecedorCpfcnpj.isEmpty()) {
	 	        mensagemErro.append("CPF/CNPJ, ");
	 	    }
	 	    
	 	    if (fornecedorCidade == null || fornecedorCidade.isEmpty()) {
	 	        mensagemErro.append("Cidade, ");
	 	    }

	 	    String mensagem = mensagemErro.substring(0, mensagemErro.length() - 2);
	 	    
	 	    registrarErro(mensagem, fornecedorId, codEmp);
	 	}

	 	/**
	 	 * Registra erro genérico no log
	 	 * 
	 	 * @param mensagem Mensagem de erro
	 	 * @param fornecedorId ID do fornecedor (pode ser null)
	 	 * @param codEmp Código da empresa
	 	 */
	 	private void registrarErro(String mensagem, String fornecedorId, BigDecimal codEmp) {
	 	    try {
	 	        selectsParaInsert.add("SELECT <#NUMUNICO#>, '" + mensagem + "', SYSDATE, 'Erro', " + 
	 	                codEmp + ", '" + (fornecedorId != null ? fornecedorId : "") + "' FROM DUAL");
	 	       LogCatcher.logInfo("SELECT <#NUMUNICO#>, '" + mensagem + "', SYSDATE, 'Erro', " + 
	 	                codEmp + ", '" + (fornecedorId != null ? fornecedorId : "") + "' FROM DUAL");
	 	    } catch (Exception e) {
	 	        e.printStackTrace();
	 	    }
	 	}

	 	/**
	 	 * Valida se os campos obrigatórios estão preenchidos
	 	 * 
	 	 * @param id ID do fornecedor
	 	 * @param nome Nome do fornecedor
	 	 * @param cpfCnpj CPF/CNPJ do fornecedor
	 	 * @param cidade Cidade do fornecedor
	 	 * @return true se todos os campos obrigatórios estiverem preenchidos, false caso contrário
	 	 */
	 	private boolean validarCamposObrigatorios(String id, String nome, String cpfCnpj, String cidade) {
	 	    return id != null && !id.isEmpty() && 
	 	           nome != null && !nome.isEmpty() && 
	 	           cpfCnpj != null && !cpfCnpj.isEmpty() && 
	 	           cidade != null && !cidade.isEmpty();
	 	}


	 	public boolean getIfIdFornecedorExist(String fornecedorCpfcnpj,
	 			String idAcad, BigDecimal codemp) throws Exception {
	 		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	 		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
	 		PreparedStatement pstmt = null;
	 		ResultSet rs = null;

	 		int fornecedor = 0;
	 		try {

	 			jdbc.openSession();

	 			String sqlSlt = "SELECT COUNT(0) AS C FROM AD_IDFORNACAD WHERE CODPARC = (SELECT CODPARC FROM TGFPAR WHERE CGC_CPF = '"
	 					+ fornecedorCpfcnpj
	 					+ "') AND IDACADWEB = '"
	 					+ idAcad
	 					+ "' AND CODEMP = " + codemp;

	 			pstmt = jdbc.getPreparedStatement(sqlSlt);
	 			rs = pstmt.executeQuery();
	 			while (rs.next()) {
	 				fornecedor = rs.getInt("C");
	 			}
	 		} catch (SQLException e) {
	 			e.printStackTrace();
	 		} finally {
	 			if (pstmt != null) {
	 				pstmt.close();
	 			}
	 			if (rs != null) {
	 				rs.close();
	 			}
	 			jdbc.closeSession();
	 		}
	 		if (fornecedor > 0) {
	 			return false;
	 		}
	 		return true;
	 	}

	 	public void updateTgfNumParc() throws Exception {
	 		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	 		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
	 		PreparedStatement pstmt = null;
	 		try {
	 			jdbc.openSession();

	 			String sqlUpd = "UPDATE TGFNUM SET ULTCOD = ULTCOD + 1  WHERE ARQUIVO = 'TGFPAR'";

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

	 	public BigDecimal getMaxNumParc() throws Exception {
	 		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	 		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
	 		PreparedStatement pstmt = null;
	 		ResultSet rs = null;
	 		BigDecimal bd = BigDecimal.ZERO;
	 		try {
	 			updateTgfNumParc();

	 			jdbc.openSession();

	 			String sqlUpd = "SELECT MAX (ULTCOD) AS ULTCOD FROM TGFNUM WHERE ARQUIVO = 'TGFPAR'";

	 			pstmt = jdbc.getPreparedStatement(sqlUpd);
	 			rs = pstmt.executeQuery();
	 			while (rs.next()) {
	 				bd = rs.getBigDecimal("ULTCOD");
	 			}
	 		} catch (SQLException e) {
	 			e.printStackTrace();
	 		} finally {
	 			if (pstmt != null) {
	 				pstmt.close();
	 			}
	 			if (rs != null) {
	 				rs.close();
	 			}
	 			jdbc.closeSession();
	 		}
	 		return bd;
	 	}

	 	
	 	public BigDecimal insertFornecedor(
	 		    String fornecedorTipo, String fornecedorId, String fornecedorNome,
	 		    String fornecedorNomeFantasia, String fornecedorEndereco,
	 		    String fornecedorBairro, String fornecedorCidade, String fornecedorUf,
	 		    String fornecedorCep, String fornecedorInscMunicipal, String fornecedorCpfcnpj,
	 		    String fornecedorHomepage, String fornecedorAtivo, String dataAtualizacao,
	 		    String fornecedorInscestadual, String fornecedorFone1, String fornecedorFone2,
	 		    String fornecedorFax, String fornecedorCelular, String fornecedorContato,
	 		    String fornecedorNome2, String fornecedorEmail, BigDecimal codEmp,
	 		    Map<String, BigDecimal> mapaInfParceiros) throws Exception {
	 		    
	 		    EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	 		    JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
	 		    PreparedStatement pstmt = null;
	 		    ResultSet rs = null;
	 		    EnviromentUtils util = new EnviromentUtils();
	 		    
	 		    try {
	 		        jdbc.openSession();
	 		        
	 		        // VERIFICAÇÃO DIRETA NO BANCO ANTES DE INSERIR
	 		        String sqlVerificacao = "SELECT CODPARC FROM TGFPAR WHERE CGC_CPF = ?";
	 		        pstmt = jdbc.getPreparedStatement(sqlVerificacao);
	 		        pstmt.setString(1, fornecedorCpfcnpj);
	 		        rs = pstmt.executeQuery();
	 		        
	 		        if (rs.next()) {
	 		            // Parceiro já existe, obter o CODPARC existente
	 		            BigDecimal codParcExistente = rs.getBigDecimal("CODPARC");
	 		            
	 		            System.out.println("Parceiro com CPF/CNPJ " + fornecedorCpfcnpj + " já existe (CODPARC=" + codParcExistente + ")");
	 		           LogCatcher.logInfo("Parceiro com CPF/CNPJ " + fornecedorCpfcnpj + " já existe (CODPARC=" + codParcExistente + ")");
	 		            
	 		            // Atualizar o mapa para refletir o parceiro existente
	 		            mapaInfParceiros.put(fornecedorCpfcnpj, codParcExistente);
	 		            
	 		            return codParcExistente; // Retorna o CODPARC existente
	 		        }
	 		        
	 		        // Fechar recursos da consulta de verificação
	 		        if (rs != null) rs.close();
	 		        if (pstmt != null) pstmt.close();
	 		        
	 		        // Continuar com o código existente para inserção
	 		        BigDecimal atualCodparc = util.getMaxNumParc();

	 		        // Validação da Cidade (CODCID)
	 		        BigDecimal codCid = getCodCid(fornecedorCidade.trim());
	 		        if (codCid == null || codCid.equals(BigDecimal.ZERO)) {
	 		            String erroMsg = "Cidade não encontrada: " + fornecedorCidade;
	 		            selectsParaInsert.add(
	 		                "SELECT <#NUMUNICO#>, '" + erroMsg + "', SYSDATE, 'Erro', " + codEmp + ", '" + fornecedorId + "' FROM DUAL"
	 		            );
	 		           LogCatcher.logInfo(
		 		                "SELECT <#NUMUNICO#>, '" + erroMsg + "', SYSDATE, 'Erro', " + codEmp + ", '" + fornecedorId + "' FROM DUAL"
	 		 		            );
	 		            return null; // Aborta a inserção
	 		        }

	 		        String tipPessoa = (fornecedorCpfcnpj.length() == 11) ? "F" : "J";
	 		        
	 		        String sqlP = "INSERT INTO TGFPAR ( " +
	 		              "CODPARC, AD_ID_EXTERNO_FORN, AD_IDENTINSCMUNIC, AD_TIPOFORNECEDOR, " +
	 		              "FORNECEDOR, IDENTINSCESTAD, HOMEPAGE, ATIVO, NOMEPARC, RAZAOSOCIAL, " +
	 		              "TIPPESSOA, AD_ENDCREDOR, CODCID, CEP, CGC_CPF, DTCAD, DTALTER, CODEMP, CODUSU " +
	 		              ") VALUES ( " +
	 		              "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
	 		              "?, ?, ?, ?, ?, SYSDATE, SYSDATE, ?, ?" +
	 		              ")";

	 		        pstmt = jdbc.getPreparedStatement(sqlP);
	 		        pstmt.setBigDecimal(1, atualCodparc); // CODPARC
	 		        pstmt.setString(2, fornecedorId); // AD_ID_EXTERNO_FORN
	 		        pstmt.setString(3, fornecedorInscMunicipal); // AD_IDENTINSCMUNIC
	 		        pstmt.setString(4, fornecedorTipo); // AD_TIPOFORNECEDOR
	 		        pstmt.setString(5, "S"); // FORNECEDOR
	 		        pstmt.setString(6, fornecedorInscestadual); // IDENTINSCESTAD
	 		        pstmt.setString(7, fornecedorHomepage); // HOMEPAGE
	 		        pstmt.setString(8, "S"); // ATIVO
	 		        pstmt.setString(9, fornecedorNome.toUpperCase()); // NOMEPARC
	 		        pstmt.setString(10, fornecedorNomeFantasia.toUpperCase()); // RAZAOSOCIAL
	 		        pstmt.setString(11, tipPessoa); // TIPPESSOA
	 		        pstmt.setString(12, fornecedorEndereco); // AD_ENDCREDOR
	 		        pstmt.setBigDecimal(13, codCid); // CODCID (validado)
	 		        pstmt.setString(14, fornecedorCep); // CEP
	 		        pstmt.setString(15, fornecedorCpfcnpj); // CGC_CPF
	 		        pstmt.setBigDecimal(16, codEmp); // CODEMP
	 		        pstmt.setInt(17, 428); // CODUSU = 428

	 		        pstmt.executeUpdate();
	 		        
	 		        System.out.println("Novo parceiro inserido: CODPARC=" + atualCodparc + ", CPF/CNPJ=" + fornecedorCpfcnpj);
	 		       LogCatcher.logInfo("Novo parceiro inserido: CODPARC=" + atualCodparc + ", CPF/CNPJ=" + fornecedorCpfcnpj);

	 		        // ATUALIZAR O MAPA APÓS INSERÇÃO BEM-SUCEDIDA
	 		        mapaInfParceiros.put(fornecedorCpfcnpj, atualCodparc);
	 		        
	 		        return atualCodparc;
	 		    } catch (SQLException e) {
	 		        String codigoErro = e.getMessage();
	 		        String mensagemAmigavel = "Erro ao cadastrar fornecedor: ";
	 		        
	 		        if (codigoErro.contains("unique constraint") || codigoErro.contains("ORA-00001")) {
	 		            mensagemAmigavel += "Este ID de fornecedor já existe no sistema.";
	 		        } else if (codigoErro.contains("integrity constraint") || codigoErro.contains("ORA-02291")) {
	 		            mensagemAmigavel += "O fornecedor referenciado não foi encontrado no sistema.";
	 		        } else if (codigoErro.contains("cannot insert NULL") || codigoErro.contains("ORA-01400")) {
	 		            mensagemAmigavel += "Dados obrigatórios estão ausentes.";
	 		        } else if (codigoErro.contains("value too large") || codigoErro.contains("ORA-12899")) {
	 		            mensagemAmigavel += "Um ou mais valores excede o tamanho permitido.";
	 		        } else if (codigoErro.contains("subconsulta de uma única linha") || codigoErro.contains("ORA-01427")) {
	 		            mensagemAmigavel += "Foram encontrados múltiplos fornecedores com o mesmo CPF/CNPJ no sistema.";
	 		        } else {
	 		            mensagemAmigavel += "Ocorreu um problema técnico. Detalhes: " + codigoErro;
	 		           LogCatcher.logError("Ocorreu um problema técnico. Detalhes: " + codigoErro);
	 		        }
	 		        
	 		        selectsParaInsert.add(
	 		            "SELECT <#NUMUNICO#>, '" + mensagemAmigavel.replace("'", "\"") + "', SYSDATE, 'Erro', " + 
	 		            codEmp + ", '" + fornecedorId + "' FROM DUAL"
	 		        );
	 		       LogCatcher.logInfo(
		 		            "SELECT <#NUMUNICO#>, '" + mensagemAmigavel.replace("'", "\"") + "', SYSDATE, 'Erro', " + 
		 		 		            codEmp + ", '" + fornecedorId + "' FROM DUAL"
		 		 		        );
	 		        e.printStackTrace();
	 		        return null;
	 		    } finally {
	 		        if (rs != null) {
	 		            try { rs.close(); } catch (SQLException e) { e.printStackTrace(); }
	 		        }
	 		        if (pstmt != null) {
	 		            try { pstmt.close(); } catch (SQLException e) { e.printStackTrace(); }
	 		        }
	 		        jdbc.closeSession();
	 		    }
	 		}

	 	
	 

	 		// Método auxiliar para buscar CODCID (mantido)
	 	private BigDecimal getCodCid(String cidade) throws Exception {
	 	    EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	 	    JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
	 	    PreparedStatement pstmt = null;
	 	    ResultSet rs = null;
	 	    BigDecimal codCid = BigDecimal.ZERO;
	 	    try {
	 	        jdbc.openSession();
	 	        String sql = 
	 	            "SELECT CODCID FROM TSICID " +
	 	            "WHERE UPPER(TRANSLATE(DESCRICAOCORREIO, " +
	 	            "'áéíóúàèìòùâêîôûãõçÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕÇ', " +
	 	            "'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC')) " +
	 	            "= UPPER(TRANSLATE(?, " +
	 	            "'áéíóúàèìòùâêîôûãõçÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕÇ', " +
	 	            "'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC'))";
	 	        pstmt = jdbc.getPreparedStatement(sql);
	 	        pstmt.setString(1, cidade);
	 	        rs = pstmt.executeQuery();
	 	        if (rs.next()) {
	 	            codCid = rs.getBigDecimal("CODCID");
	 	        }
	 	    } finally {
	 	        if (rs != null) rs.close();
	 	        if (pstmt != null) pstmt.close();
	 	        jdbc.closeSession();
	 	    }
	 	    return codCid;
	 	}
	 	
	

	 	public void updateIdForn(String idForn, String cgc) throws Exception {
	 		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	 		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
	 		PreparedStatement pstmt = null;

	 		try {
	 			jdbc.openSession();

	 			String sqlP = "UPDATE TGFPAR SET AD_ID_EXTERNO_FORN = '"
	 					+ idForn
	 					+ "' WHERE CODPARC = (SELECT CODPARC FROM TGFPAR WHERE CGC_CPF = '"
	 					+ cgc + "')";

	 			pstmt = jdbc.getPreparedStatement(sqlP);

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

	 	public void insertIdForn(String idForn, String cgc, BigDecimal codemp) throws Exception {
	 	    EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	 	    JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
	 	    PreparedStatement pstmt = null;
	 	    ResultSet rs = null;

	 	    try {
	 	        jdbc.openSession();
	 	        
	 	        // Primeiro, vamos verificar se existem múltiplos fornecedores com o mesmo CNPJ/CPF
	 	        String sqlVerificaMultiplosFornecedores = "SELECT COUNT(*) FROM TGFPAR WHERE CGC_CPF = ?";
	 	        pstmt = jdbc.getPreparedStatement(sqlVerificaMultiplosFornecedores);
	 	        pstmt.setString(1, cgc);
	 	        rs = pstmt.executeQuery();
	 	        
	 	        int quantidadeFornecedores = 0;
	 	        if (rs.next()) {
	 	            quantidadeFornecedores = rs.getInt(1);
	 	        }
	 	        
	 	        // Se existem múltiplos fornecedores, precisamos tratar isso
	 	        if (quantidadeFornecedores > 1) {
	 	            // Vamos tentar encontrar um fornecedor que já esteja associado a esta empresa
	 	            rs.close();
	 	            pstmt.close();
	 	            
	 	           String sqlFornecedorEmpresa = 
	 	        		    "SELECT PAR.CODPARC FROM TGFPAR PAR " +
	 	        		    "WHERE PAR.CGC_CPF = ? " +
	 	        		    "AND EXISTS (SELECT 1 FROM TGFCPL CPL WHERE CPL.CODPARC = PAR.CODPARC) " +
	 	        		    "AND PAR.CODEMP = ?";
	 	            
	 	            pstmt = jdbc.getPreparedStatement(sqlFornecedorEmpresa);
	 	            pstmt.setString(1, cgc);
	 	            pstmt.setBigDecimal(2, codemp);
	 	            rs = pstmt.executeQuery();
	 	            
	 	            BigDecimal codparc = null;
	 	            if (rs.next()) {
	 	                codparc = rs.getBigDecimal("CODPARC");
	 	            } else {
	 	                // Se não encontrar, pega o primeiro disponível
	 	                rs.close();
	 	                pstmt.close();
	 	                
	 	                String sqlPrimeiroFornecedor = "SELECT CODPARC FROM TGFPAR WHERE CGC_CPF = ? AND ROWNUM = 1";
	 	                pstmt = jdbc.getPreparedStatement(sqlPrimeiroFornecedor);
	 	                pstmt.setString(1, cgc);
	 	                rs = pstmt.executeQuery();
	 	                
	 	                if (rs.next()) {
	 	                    codparc = rs.getBigDecimal("CODPARC");
	 	                } else {
	 	                    throw new Exception("Não foi possível encontrar um fornecedor com o CNPJ/CPF: " + cgc);
	 	                }
	 	            }
	 	            
	 	            // Agora verificamos se já existe um registro para esse CODPARC e CODEMP
	 	            rs.close();
	 	            pstmt.close();
	 	            
	 	            String sqlVerificaExistente = 
	 	                "SELECT COUNT(*) FROM AD_IDFORNACAD WHERE CODPARC = ? AND CODEMP = ? AND IDACADWEB = ?";
	 	            
	 	            pstmt = jdbc.getPreparedStatement(sqlVerificaExistente);
	 	            pstmt.setBigDecimal(1, codparc);
	 	            pstmt.setBigDecimal(2, codemp);
	 	            pstmt.setString(3, idForn);
	 	            rs = pstmt.executeQuery();
	 	            
	 	            boolean registroExiste = false;
	 	            if (rs.next()) {
	 	                registroExiste = rs.getInt(1) > 0;
	 	            }
	 	            
	 	            if (registroExiste) {
	 	                // Registro já existe, não é necessário inserir novamente
	 	                return;
	 	            }
	 	            
	 	            // Insere usando o CODPARC específico
	 	            rs.close();
	 	            pstmt.close();
	 	            
	 	            String sqlInsertEspecifico = 
	 	                "INSERT INTO AD_IDFORNACAD (CODPARC, ID, IDACADWEB, CODEMP) " +
	 	                "VALUES (?, (SELECT NVL(MAX(ID), 0) + 1 FROM AD_IDFORNACAD), ?, ?)";
	 	            
	 	            pstmt = jdbc.getPreparedStatement(sqlInsertEspecifico);
	 	            pstmt.setBigDecimal(1, codparc);
	 	            pstmt.setString(2, idForn);
	 	            pstmt.setBigDecimal(3, codemp);
	 	            
	 	            pstmt.executeUpdate();
	 	        } else {
	 	            // Caso onde há apenas um fornecedor com esse CNPJ
	 	            rs.close();
	 	            pstmt.close();
	 	            
	 	            // Verifica se já existe o registro
	 	            String sqlVerificacao = 
	 	                "SELECT COUNT(*) FROM AD_IDFORNACAD AIF " +
	 	                "JOIN TGFPAR PAR ON AIF.CODPARC = PAR.CODPARC " +
	 	                "WHERE PAR.CGC_CPF = ? AND AIF.CODEMP = ? AND AIF.IDACADWEB = ?";
	 	            
	 	            pstmt = jdbc.getPreparedStatement(sqlVerificacao);
	 	            pstmt.setString(1, cgc);
	 	            pstmt.setBigDecimal(2, codemp);
	 	            pstmt.setString(3, idForn);
	 	            rs = pstmt.executeQuery();
	 	            
	 	            boolean registroExiste = false;
	 	            if (rs.next()) {
	 	                registroExiste = rs.getInt(1) > 0;
	 	            }
	 	            
	 	            if (registroExiste) {
	 	                // Registro já existe, não é necessário inserir novamente
	 	                return;
	 	            }
	 	            
	 	            // Registro não existe, vamos inserir
	 	            rs.close();
	 	            pstmt.close();
	 	            
	 	            String sqlInsert = 
	 	                "INSERT INTO AD_IDFORNACAD (CODPARC, ID, IDACADWEB, CODEMP) " +
	 	                "VALUES ((SELECT CODPARC FROM TGFPAR WHERE CGC_CPF = ?), " +
	 	                "(SELECT NVL(MAX(ID), 0) + 1 FROM AD_IDFORNACAD), ?, ?)";
	 	            
	 	            pstmt = jdbc.getPreparedStatement(sqlInsert);
	 	            pstmt.setString(1, cgc);
	 	            pstmt.setString(2, idForn);
	 	            pstmt.setBigDecimal(3, codemp);
	 	            
	 	            pstmt.executeUpdate();
	 	        }

	 	    } catch (SQLException e) {
	 	        e.printStackTrace();

	 	        String mensagemAmigavel = "Erro ao cadastrar ID do fornecedor. ";
	 	        String codigoErro = e.getMessage();

	 	        if (codigoErro.contains("unique constraint") || codigoErro.contains("ORA-00001")) {
	 	            mensagemAmigavel += "Este ID de fornecedor já existe no sistema.";
	 	        } else if (codigoErro.contains("integrity constraint") || codigoErro.contains("ORA-02291")) {
	 	            mensagemAmigavel += "O fornecedor referenciado não foi encontrado no sistema.";
	 	        } else if (codigoErro.contains("cannot insert NULL") || codigoErro.contains("ORA-01400")) {
	 	            mensagemAmigavel += "Dados obrigatórios estão ausentes.";
	 	        } else if (codigoErro.contains("value too large") || codigoErro.contains("ORA-12899")) {
	 	            mensagemAmigavel += "Um ou mais valores excede o tamanho permitido.";
	 	        } else if (codigoErro.contains("subconsulta de uma única linha") || codigoErro.contains("ORA-01427")) {
	 	            mensagemAmigavel += "Foram encontrados múltiplos fornecedores com o mesmo CPF/CNPJ no sistema. Por favor, verifique os dados cadastrais deste fornecedor.";
	 	        } else {
	 	            mensagemAmigavel += "Ocorreu um problema técnico. Detalhes: " + codigoErro;
	 	        }

	 	        selectsParaInsert.add("SELECT <#NUMUNICO#>, '" + mensagemAmigavel.replace("'", "\"") + "', SYSDATE, 'Erro', "+codemp+", '"+idForn+"' FROM DUAL");
	 	       LogCatcher.logInfo("SELECT <#NUMUNICO#>, '" + mensagemAmigavel.replace("'", "\"") + "', SYSDATE, 'Erro', "+codemp+", '"+idForn+"' FROM DUAL");
	 	    } finally {
	 	        if (rs != null) {
	 	            rs.close();
	 	        }
	 	        if (pstmt != null) {
	 	            pstmt.close();
	 	        }
	 	        jdbc.closeSession();
	 	    }
	 	}

	 	public void insertLogIntegracao(String descricao, String status, String url, String token) throws Exception {
	 	    EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	 	    JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
	 	    PreparedStatement pstmt = null;
	 	    
	 	    try {
	 	        jdbc.openSession();

	 	        String descricaoCompleta = null;
	 	        
	 	        // Adiciona informações de URL e token à descrição para melhor rastreabilidade
	 	        if (url != null && !url.isEmpty()) {
	 	            descricaoCompleta = descricao + " URL: " + url;
	 	            
	 	            if (token != null && !token.isEmpty()) {
	 	                // Pode ser útil incluir apenas parte do token por segurança
	 	                String tokenMasked = token.length() > 10 ? 
	 	                    token.substring(0, 5) + "..." + token.substring(token.length() - 5) : 
	 	                    "***";
	 	                descricaoCompleta += " Token: " + tokenMasked;
	 	            }
	 	        } else {
	 	            descricaoCompleta = descricao;
	 	        }

	 	        String sqlUpdate = "INSERT INTO AD_LOGINTEGRACAO (NUMUNICO, DESCRICAO, DTHORA, STATUS, TIPO_INTEGRACAO) " +
	 	                           "VALUES (((SELECT NVL(MAX(NUMUNICO), 0) + 1 FROM AD_LOGINTEGRACAO)), ?, SYSDATE, ?, 'FORNECEDOR')";

	 	        pstmt = jdbc.getPreparedStatement(sqlUpdate);
	 	        pstmt.setString(1, descricaoCompleta);
	 	        pstmt.setString(2, status);
	 	        pstmt.executeUpdate();
	 	        
	 	        System.out.println("Log de integração fornecedor inserido: " + descricaoCompleta);
	 	       LogCatcher.logInfo("Log de integração fornecedor inserido: " + descricaoCompleta);
	 	        
	 	    } catch (Exception se) {
	 	        se.printStackTrace();
	 	        System.out.println("Erro ao inserir log de integração fornecedor: " + se.getMessage());
	 	       LogCatcher.logInfo("Erro ao inserir log de integração fornecedor: " + se.getMessage());
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
		    
		    LogCatcher.logInfo("Entrou na API");
		    LogCatcher.logInfo("URL: " + encodedUrl);
		    LogCatcher.logInfo("Token Enviado: [" + token + "]");
		    
		    
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
	 	 

	 	private List<Object[]> retornarInformacoesParceiros() throws Exception {
	 		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	 		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
	 		PreparedStatement pstmt = null;
	 		ResultSet rs = null;
	 		List<Object[]> listRet = new ArrayList<>();

	 		try {
	 			jdbc.openSession();
	 			String sql = "SELECT p.CODPARC, p.CGC_CPF, "
	 					+ "a.IDACADWEB, a.codemp " + "FROM TGFPAR p "
	 					+ "LEFT join AD_IDFORNACAD a "
	 					+ "on a.codparc = p.codparc " + "where fornecedor = 'S'";

	 			pstmt = jdbc.getPreparedStatement(sql);
	 			rs = pstmt.executeQuery();

	 			while (rs.next()) {
	 				Object[] ret = new Object[4];
	 				ret[0] = rs.getBigDecimal("CODPARC");
	 				ret[1] = rs.getString("CGC_CPF");
	 				ret[2] = rs.getString("IDACADWEB");
	 				ret[3] = rs.getBigDecimal("codemp");

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
	 }
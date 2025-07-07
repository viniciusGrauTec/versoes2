package br.com.sankhya.acoesgrautec.extensions;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.io.StringWriter;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
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
import br.com.sankhya.modelcore.util.SWRepositoryUtils;

public class AcaoGetTituloFornecedorCarga implements AcaoRotinaJava, ScheduledAction{
	
	private List<String> selectsParaInsertLog = new ArrayList<String>();
	private EnviromentUtils util = new EnviromentUtils();
	static {
		LogConfiguration.setPath(SWRepositoryUtils.getBaseFolder() + "/logAcao/logs");
	}
	
	@Override
	public void doAction(ContextoAcao contexto) throws Exception {
	    String threadName = Thread.currentThread().getName();
	    Date startTime = new Date();

	    System.out.println("=== INÍCIO DO BOTÃO DE AÇÃO === Thread: " + threadName + " - Hora: " + startTime);
	    System.out.println("Iniciando processamento de dados");
	    
	    LogCatcher.logInfo("=== INÍCIO DO BOTÃO DE AÇÃO === Thread: " + threadName + " - Hora: " + startTime);
	    LogCatcher.logInfo("Iniciando processamento de dados");

	    Registro[] linhas = contexto.getLinhas();
	    
	    // Verifica se existe pelo menos 1 linha
	    if (linhas.length < 1) {
	        contexto.mostraErro("É necessário selecionar pelo menos 1 linha.");
	        return;
	    }
	    
	    // Determina quantas linhas processar (máximo 65, como no doActionForne)
	    int linhasParaProcessar = Math.min(linhas.length, 65);
	    System.out.println("Processando " + linhasParaProcessar + " registro(s)");
	    LogCatcher.logInfo("Processando " + linhasParaProcessar + " registro(s)");
	    
	    String dataInicio = contexto.getParam("DTINICIO").toString().substring(0, 10);
	    String dataFim = contexto.getParam("DTFIM").toString().substring(0, 10);
	    String tituloAberto = (String) contexto.getParam("TITABERTO");
	    String idForn = (String) contexto.getParam("IDFORN");
	    
	    try {
	        // Carrega todas as informações necessárias uma única vez
	        
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
	                mapaInfNaturezaEmp.put(idExternoObj + "###" + codemp, natureza);
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

	        // Processa cada registro selecionado
	        for (int i = 0; i < linhasParaProcessar; i++) {
	            Registro registro = linhas[i];
	            
	            String url = (String) registro.getCampo("URL");
	            String token = (String) registro.getCampo("TOKEN");
	            BigDecimal codEmp = (BigDecimal) registro.getCampo("CODEMP");
	            
	            String profissionalizante = Optional.ofNullable(registro.getCampo("PROFISSIONAL")).orElse("N").toString(); 
	            String tecnico = (String) Optional.ofNullable(registro.getCampo("TECNICO")).orElse("N");
	            
	            String tipoEmpresa = "";
	            
	            if (profissionalizante.equalsIgnoreCase("S")) {
	                tipoEmpresa = "P";
	            } else if (tecnico.equalsIgnoreCase("S")) {
	                tipoEmpresa = "T";
	            } else {
	                tipoEmpresa = "N";
	            }
	            
	            System.out.println("Processando registro " + (i+1) + " de " + linhasParaProcessar);
	            LogCatcher.logInfo("Processando registro " + (i+1) + " de " + linhasParaProcessar);
	            
	            try {
	                // Processa o registro atual
	                processDateRange(tituloAberto.trim(), tipoEmpresa, mapaInfNaturezaEmp, mapaInfCenCusEmp,
	                        mapaInfFinanceiroBanco, mapaInfFinanceiroBaixado, 
	                        mapaInfNatureza, mapaInfBanco, mapaInfConta, 
	                        mapaInfFinanceiro, mapaInfCenCus, 
	                        mapaInfParceiros, url, token, codEmp, dataInicio, dataFim, idForn);
	                
	                // Log de sucesso específico para este registro
	                //insertLogIntegracao("Processamento de período realizado com sucesso. Empresa: " + codEmp, "Sucesso");
	                LogCatcher.logInfo("Processamento de período realizado com sucesso. Empresa: " + codEmp);
	                
	            } catch (Exception e) {
	                System.out.println("Erro ao processar registro " + (i+1) + ": " + e.getMessage());
	                LogCatcher.logError("Erro ao processar registro " + (i+1) + ": " + e.getMessage());
	                e.printStackTrace();
	                
	                // Registra o erro mas continua processando os próximos registros
	                insertLogIntegracao("Erro ao processar período para empresa: " + codEmp + 
	                    ", Mensagem: " + e.getMessage(), "Erro", url, token);
	                LogCatcher.logError("Erro ao processar período para empresa: " + codEmp + 
		                    ", Mensagem: " + e.getMessage());
	            }
	        }

	        // Mensagem de sucesso geral
	        contexto.setMensagemRetorno("Período processado com sucesso para " + linhasParaProcessar + " registro(s)!");
	        LogCatcher.logInfo("Período processado com sucesso para " + linhasParaProcessar + " registro(s)!");
	        
	        System.out.println("Finalizou o processamento do período");
	        LogCatcher.logInfo("Finalizou o processamento do período");

	    } catch (Exception e) {
	        System.out.println("Erro geral na ação - Thread: " + threadName);
	        LogCatcher.logError("Erro geral na ação - Thread: " + threadName);
	        e.printStackTrace();
	        try {
	            // Log para o primeiro registro como referência
	            Registro primeiroRegistro = linhas[0];
	            String url = (String) primeiroRegistro.getCampo("URL");
	            String token = (String) primeiroRegistro.getCampo("TOKEN");
	            BigDecimal codEmp = (BigDecimal) primeiroRegistro.getCampo("CODEMP");
	            
	            insertLogIntegracao(
	                "Erro ao processar período, Mensagem de erro: " + e.getMessage(),
	                "Erro", url, token);
	            LogCatcher.logError(
		                "Erro ao processar período, Mensagem de erro: " + e.getMessage());
	        } catch (Exception e1) {
	            e1.printStackTrace();
	        }
	        contexto.mostraErro("Ocorreu um erro durante o processamento: " + e.getMessage());
	        LogCatcher.logError("Ocorreu um erro durante o processamento: " + e.getMessage());
	    } finally {
	        // Processa logs pendentes
	        if (selectsParaInsertLog.size() > 0) {
	            StringBuilder msgError = new StringBuilder();
	            
	            System.out.println("Entrou na lista do finally: " + selectsParaInsertLog.size());
	            LogCatcher.logInfo("Entrou na lista do finally: " + selectsParaInsertLog.size());
	            
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
	            
	            // Recupera CODEMP do primeiro registro para o log - ajuste conforme necessário
	            BigDecimal codEmpLog = (BigDecimal) linhas[0].getCampo("CODEMP");
	            insertLogList(msgError.toString(), codEmpLog);
	        }

	        LogCatcher.logInfo("=== FIM DO BOTÃO DE AÇÃO === Thread: " + threadName + " - Hora: " + new Date());
	        System.out.println("=== FIM DO BOTÃO DE AÇÃO === Thread: " + threadName + " - Hora: " + new Date());
	    }
	}
	
	@Override
	public void onTime(ScheduledActionContext arg0) {
	    EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	    JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
	    BigDecimal codEmp = BigDecimal.ZERO;
	    
	    System.out.println("Iniciou a JobGetTituloFornecedor");
	    LogCatcher.logInfo("Iniciou a JobGetTituloFornecedor");

	    try {
	        // Inicialização dos mapas de dados
	        Map<String, BigDecimal> mapaInfFinanceiro = carregarMapaInfFinanceiro();
	        Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco = carregarMapaInfFinanceiroBanco();
	        Map<BigDecimal, String> mapaInfFinanceiroBaixado = carregarMapaInfFinanceiroBaixado();
	        Map<String, BigDecimal> mapaInfParceiros = carregarMapaInfParceiros();
	        Map<String, BigDecimal> mapaInfCenCus = carregarMapaInfCenCus();
	        Map<String, BigDecimal> mapaInfCenCusEmp = carregarMapaInfCenCusEmp();
	        Map<String, BigDecimal> mapaInfNatureza = carregarMapaInfNatureza();
	        Map<String, BigDecimal> mapaInfNaturezaEmp = carregarMapaInfNaturezaEmp();
	        Map<String, String> mapaInfRecDesp = carregarMapaInfRecDesp();
	        Map<String, BigDecimal> mapaInfBanco = carregarMapaInfBanco();
	        Map<String, BigDecimal> mapaInfConta = carregarMapaInfConta();
	        
	        jdbc.openSession();
	        
	        // Chama o método que processa as empresas com integração ativa
	        processarEmpresas(jdbc, mapaInfNaturezaEmp, mapaInfCenCusEmp, 
	                mapaInfFinanceiroBanco, mapaInfFinanceiroBaixado, 
	                mapaInfNatureza, mapaInfBanco, mapaInfConta, 
	                mapaInfFinanceiro, mapaInfCenCus, mapaInfParceiros);

	        System.out.println("Finalizou a JobGetTituloFornecedor");
	        LogCatcher.logInfo("Finalizou a JobGetTituloFornecedor");

	    } catch (Exception e) {
	        try {
	            e.printStackTrace();
	            // Logar erro geral da execução do job
	            insertLogIntegracao("Erro ao processar JobGetTituloFornecedor: " + e.getMessage(), "ERRO", null, null);
	            LogCatcher.logError("Erro ao processar JobGetTituloFornecedor: " + e.getMessage());
	        } catch (Exception e1) {
	            e.printStackTrace();
	        }
	    } finally {
	        jdbc.closeSession();
	        
	        if(selectsParaInsertLog.size() > 0) {
	            StringBuilder msgError = new StringBuilder();
	            
	            System.out.println("Entrou na lista do finally: " + selectsParaInsertLog.size());
	            LogCatcher.logInfo("Entrou na lista do finally: " + selectsParaInsertLog.size());
	            
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
	            LogCatcher.logInfo("Consulta de log: \n" + msgError);
	            try {
	                insertLogList(msgError.toString(), codEmp);
	            } catch (Exception e) {
	                e.printStackTrace();
	            }
	            
	            msgError = null;
	            this.selectsParaInsertLog = new ArrayList<String>();
	        }
	    }
	}

	/**
	 * Carrega o mapa de informações financeiras
	 */
	private Map<String, BigDecimal> carregarMapaInfFinanceiro() throws Exception {
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
	    return mapaInfFinanceiro;
	}

	/**
	 * Carrega o mapa de informações de banco financeiro
	 */
	private Map<BigDecimal, BigDecimal> carregarMapaInfFinanceiroBanco() throws Exception {
	    List<Object[]> listInfFinanceiro = retornarInformacoesFinanceiro();
	    Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco = new HashMap<BigDecimal, BigDecimal>();
	    for (Object[] obj : listInfFinanceiro) {
	        BigDecimal nuFin = (BigDecimal) obj[0];
	        BigDecimal nuBco = (BigDecimal) obj[5];
	        if (mapaInfFinanceiroBanco.get(nuFin) == null) {
	            mapaInfFinanceiroBanco.put(nuFin, nuBco);
	        }
	    }
	    return mapaInfFinanceiroBanco;
	}

	/**
	 * Carrega o mapa de informações de financeiro baixado
	 */
	private Map<BigDecimal, String> carregarMapaInfFinanceiroBaixado() throws Exception {
	    List<Object[]> listInfFinanceiro = retornarInformacoesFinanceiro();
	    Map<BigDecimal, String> mapaInfFinanceiroBaixado = new HashMap<BigDecimal, String>();
	    for (Object[] obj : listInfFinanceiro) {
	        BigDecimal nuFin = (BigDecimal) obj[0];
	        String baixado = (String) obj[3];
	        if (mapaInfFinanceiroBaixado.get(nuFin) == null) {
	            mapaInfFinanceiroBaixado.put(nuFin, baixado);
	        }
	    }
	    return mapaInfFinanceiroBaixado;
	}

	/**
	 * Carrega o mapa de informações de parceiros
	 */
	private Map<String, BigDecimal> carregarMapaInfParceiros() throws Exception {
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
	    return mapaInfParceiros;
	}

	/**
	 * Carrega o mapa de informações de centro de custo
	 */
	private Map<String, BigDecimal> carregarMapaInfCenCus() throws Exception {
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
	    return mapaInfCenCus;
	}

	/**
	 * Carrega o mapa de informações de centro de custo por empresa
	 */
	private Map<String, BigDecimal> carregarMapaInfCenCusEmp() throws Exception {
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
	    return mapaInfCenCusEmp;
	}

	/**
	 * Carrega o mapa de informações de natureza
	 */
	private Map<String, BigDecimal> carregarMapaInfNatureza() throws Exception {
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
	    return mapaInfNatureza;
	}

	/**
	 * Carrega o mapa de informações de natureza por empresa
	 */
	private Map<String, BigDecimal> carregarMapaInfNaturezaEmp() throws Exception {
	    List<Object[]> listInfNaturezaEmpresa = retornarInformacoesNaturezaEmpresa();
	    Map<String, BigDecimal> mapaInfNaturezaEmp = new HashMap<String, BigDecimal>();
	    for (Object[] obj : listInfNaturezaEmpresa) {
	        BigDecimal natureza = (BigDecimal) obj[0];
	        String idExternoObj = (String) obj[1];
	        BigDecimal codemp = (BigDecimal) obj[2];

	        if (mapaInfNaturezaEmp.get(idExternoObj + "###" + codemp) == null) {
	            mapaInfNaturezaEmp.put(idExternoObj + "###" + codemp, natureza);
	        }
	    }
	    return mapaInfNaturezaEmp;
	}

	/**
	 * Carrega o mapa de informações de receita/despesa
	 */
	private Map<String, String> carregarMapaInfRecDesp() throws Exception {
	    List<Object[]> listInfRecDesp = retornarInformacoesRecDesp();
	    Map<String, String> mapaInfRecDesp = new HashMap<String, String>();
	    for (Object[] obj : listInfRecDesp) {
	        String recDesp = (String) obj[0];
	        String idExternoObj = (String) obj[1];

	        if (mapaInfRecDesp.get(idExternoObj) == null) {
	            mapaInfRecDesp.put(idExternoObj, recDesp);
	        }
	    }
	    return mapaInfRecDesp;
	}

	/**
	 * Carrega o mapa de informações de banco
	 */
	private Map<String, BigDecimal> carregarMapaInfBanco() throws Exception {
	    List<Object[]> listInfBancoConta = retornarInformacoesBancoConta();
	    Map<String, BigDecimal> mapaInfBanco = new HashMap<String, BigDecimal>();
	    for (Object[] obj : listInfBancoConta) {
	        Long codEmpObj = (Long) obj[1];
	        BigDecimal codBcoObj = (BigDecimal) obj[3];

	        if (mapaInfBanco.get(codEmpObj.toString()) == null) {
	            mapaInfBanco.put(codEmpObj.toString(), codBcoObj);
	        }
	    }
	    return mapaInfBanco;
	}

	/**
	 * Carrega o mapa de informações de conta
	 */
	private Map<String, BigDecimal> carregarMapaInfConta() throws Exception {
	    List<Object[]> listInfBancoConta = retornarInformacoesBancoConta();
	    Map<String, BigDecimal> mapaInfConta = new HashMap<String, BigDecimal>();
	    for (Object[] obj : listInfBancoConta) {
	        BigDecimal codCtabCointObj = (BigDecimal) obj[0];
	        Long codEmpObj = (Long) obj[1];

	        if (mapaInfConta.get(codEmpObj.toString()) == null) {
	            mapaInfConta.put(codEmpObj.toString(), codCtabCointObj);
	        }
	    }
	    return mapaInfConta;
	}

	/**
	 * Processa todas as empresas com integração ativa
	 */
	private void processarEmpresas(JdbcWrapper jdbc, 
	        Map<String, BigDecimal> mapaInfNaturezaEmp,
	        Map<String, BigDecimal> mapaInfCenCusEmp,
	        Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco,
	        Map<BigDecimal, String> mapaInfFinanceiroBaixado,
	        Map<String, BigDecimal> mapaInfNatureza,
	        Map<String, BigDecimal> mapaInfBanco,
	        Map<String, BigDecimal> mapaInfConta,
	        Map<String, BigDecimal> mapaInfFinanceiro,
	        Map<String, BigDecimal> mapaInfCenCus,
	        Map<String, BigDecimal> mapaInfParceiros) throws Exception {
	    
	    String query = "SELECT CODEMP, URL, TOKEN, " +  
	                  "CASE WHEN PROFISSIONAL = 'S' THEN 'P' WHEN TECNICO = 'S' THEN 'T' ELSE 'N' END AS TIPEMP " +
	                  "FROM AD_LINKSINTEGRACAO WHERE INTEGRACAO = 'S'";
	    
	    PreparedStatement pstmt = null;
	    ResultSet rs = null;
	    
	    try {
	        System.out.println("Iniciando processamento de empresas com integração ativa");
	        LogCatcher.logInfo("Iniciando processamento de empresas com integração ativa");
	        
	        pstmt = jdbc.getPreparedStatement(query);
	        rs = pstmt.executeQuery();
	        
	        int empresasProcessadas = 0;
	        int empresasComErro = 0;
	        
	        while (rs.next()) {
	            BigDecimal codEmp = rs.getBigDecimal("CODEMP");
	            String url = rs.getString("URL");
	            String token = rs.getString("TOKEN");
	            String tipoEmpresa = rs.getString("TIPEMP");
	            
	            try {
	                System.out.println("Processando empresa: " + codEmp + " (Tipo: " + tipoEmpresa + ")");
	                LogCatcher.logInfo("Processando empresa: " + codEmp + " (Tipo: " + tipoEmpresa + ")");
	                
	                // Validações dos dados da empresa
	                if (url == null || url.trim().isEmpty()) {
	                    throw new IllegalArgumentException("URL de integração não configurada");
	                }
	                
	                if (token == null || token.trim().isEmpty()) {
	                    throw new IllegalArgumentException("Token de integração não configurado");
	                }
	                
	                if (tipoEmpresa == null || !("P".equals(tipoEmpresa.trim()) || "T".equals(tipoEmpresa.trim()) || "N".equals(tipoEmpresa.trim()))) {
	                    throw new IllegalArgumentException("Tipo de empresa inválido: " + tipoEmpresa);
	                }
	                
	                // Chama o método que processa cada empresa
	                iterarEndpoint(
	                    tipoEmpresa.trim(),
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
	                    url,
	                    token,
	                    codEmp
	                );
	                
	                System.out.println("Empresa " + codEmp + " processada com sucesso");
	                LogCatcher.logInfo("Empresa " + codEmp + " processada com sucesso");
	                empresasProcessadas++;
	                
	            } catch (IllegalArgumentException configEx) {
	                // Erro de configuração da empresa
	                String msgErro = "Erro de configuração para empresa " + codEmp + ": " + configEx.getMessage();
	                System.err.println(msgErro);
	                LogCatcher.logInfo(msgErro);
	                insertLogIntegracao(msgErro, "ERRO_CONFIG", null, null);
	                empresasComErro++;
	                
	            } catch (SQLException sqlEx) {
	                // Erro de SQL durante o processamento da empresa
	                String msgErro = "Erro SQL ao processar empresa " + codEmp + ": " 
	                               + sqlEx.getMessage() + " (Código: " + sqlEx.getErrorCode() + ")";
	                System.err.println(msgErro);
	                LogCatcher.logInfo(msgErro);
	                insertLogIntegracao(msgErro, "ERRO_SQL", null, null);
	                empresasComErro++;
	                
	            } catch (Exception ex) {
	                // Erro genérico durante o processamento da empresa
	                String msgErro = "Erro ao processar empresa " + codEmp + ": " + ex.getMessage()
	                               + " - Tipo: " + ex.getClass().getName();
	                System.err.println(msgErro);
	                LogCatcher.logInfo(msgErro);
	                StackTraceElement[] stackTrace = ex.getStackTrace();
	                if (stackTrace.length > 0) {
	                    msgErro += " [" + stackTrace[0].getClassName() + "." + stackTrace[0].getMethodName() 
	                             + " - linha: " + stackTrace[0].getLineNumber() + "]";
	                }
	                insertLogIntegracao(msgErro, "ERRO", null, null);
	                
	                // Log da pilha de exceção para debug
	                System.err.println("Stack trace do erro:");
	                ex.printStackTrace();
	                empresasComErro++;
	            }
	        }
	        
	        System.out.println("Processamento finalizado. Empresas processadas: " + empresasProcessadas 
	                        + " / Empresas com erro: " + empresasComErro);
	        LogCatcher.logInfo("Processamento finalizado. Empresas processadas: " + empresasProcessadas 
                    + " / Empresas com erro: " + empresasComErro);
	        
	    } catch (SQLException dbEx) {
	        // Erro ao executar a consulta principal
	        String msgErro = "Erro ao consultar empresas para integração: " + dbEx.getMessage() 
	                      + " (Código: " + dbEx.getErrorCode() + ")";
	        System.err.println(msgErro);
	        LogCatcher.logInfo(msgErro);
	        insertLogIntegracao(msgErro, "ERRO_SQL", null, null);
	        throw dbEx;
	        
	    } catch (Exception ex) {
	        // Erro genérico no método
	        String msgErro = "Erro geral no processamento de empresas: " + ex.getMessage();
	        System.err.println(msgErro);
	        LogCatcher.logInfo(msgErro);
	        insertLogIntegracao(msgErro, "ERRO_FATAL", null, null);
	        throw ex;
	        
	    } finally {
	        // Fechamento seguro dos recursos
	        try {
	            if (rs != null && !rs.isClosed()) {
	                rs.close();
	            }
	        } catch (SQLException closeEx) {
	            System.err.println("Erro ao fechar ResultSet: " + closeEx.getMessage());
	            LogCatcher.logInfo("Erro ao fechar ResultSet: " + closeEx.getMessage());
	        }
	        
	        try {
	            if (pstmt != null && !pstmt.isClosed()) {
	                pstmt.close();
	            }
	        } catch (SQLException closeEx) {
	            System.err.println("Erro ao fechar PreparedStatement: " + closeEx.getMessage());
	            LogCatcher.logInfo("Erro ao fechar PreparedStatement: " + closeEx.getMessage());
	        }
	    }
	}

	/**
	 * Método que realiza a iteração nos endpoints para cada empresa
	 */
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
	        System.out.println("Iniciando iteração de endpoint para empresa: " + codEmp);
	        LogCatcher.logInfo("Iniciando iteração de endpoint para empresa: " + codEmp);

	        String[] response = apiGet(url
	                + "/financeiro/clientes/titulos-pagar?quantidade=0"
	                + "&dataInicial=" + dataFormatada + " 00:00:00" + "&dataFinal="
	                + dataFormatada + " 23:59:59", token);

	        int status = Integer.parseInt(response[0]);
	        System.out.println("Status da resposta: " + status);
	        LogCatcher.logInfo("Status da resposta: " + status);

	        String responseString = response[1];
	        System.out.println("Resposta recebida para processamento");
	        LogCatcher.logInfo("Resposta recebida para processamento");

	        cadastrarTituloFornecedor(tipoEmpresa, mapaInfNaturezaEmp, mapaInfCenCusEmp,
	                mapaInfFinanceiroBanco, mapaInfFinanceiroBaixado,
	                mapaInfNatureza, mapaInfBanco, mapaInfConta,
	                mapaInfFinanceiro, mapaInfCenCus, mapaInfParceiros,
	                response, url, token, codEmp);
	        
	        System.out.println("Processamento de JSON concluído para empresa: " + codEmp);
	        LogCatcher.logInfo("Processamento de JSON concluído para empresa: " + codEmp);
	    }
	    catch (Exception e) {
	        System.err.println("Erro na iteração de endpoint para empresa " + codEmp + ": " + e.getMessage());
	        insertLogIntegracao("Erro na iteração de endpoint: " + e.getMessage(), "ERRO", url, token);
	        LogCatcher.logInfo("Erro na iteração de endpoint para empresa " + codEmp + ": " + e.getMessage());
	        throw e;
	    }
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
	        LogCatcher.logInfo("Iniciando consulta de títulos para o período: " + dataInicio + " até " + dataFim);

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
	                LogCatcher.logInfo("Adicionando fornecedor à requisição: " + idForn);
	            }

	            String urlCompleta = urlBuilder.toString();
	            System.out.println("URL para títulos (página " + pagina + "): " + urlCompleta);
	            LogCatcher.logInfo("URL para títulos (página " + pagina + "): " + urlCompleta);

	            // Fazer a requisição
	            String[] response = apiGet(urlCompleta, token);
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
	                LogCatcher.logInfo("Página " + pagina + ": " + paginaAtual.length() + 
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
	        LogCatcher.logInfo("Total de registros de títulos acumulados: " + todosRegistros.length());

	        // Processar todos os registros acumulados
	        cadastrarTituloFornecedor(
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
	        LogCatcher.logInfo("Erro ao processar títulos para o período " + 
                    dataInicio + " até " + dataFim + ": " + e.getMessage());
	        e.printStackTrace();
	        throw e;
	    }
	}
	
	
	public void cadastrarTituloFornecedor(String tipoEmpresa,
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
		LogCatcher.logInfo("Inicio leitura do JSON - JobGetTituloFornecedor");
		
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
        
        LogCatcher.logInfo("data um dia atras forn titulo: " + dataUmDiaFormatada);
        LogCatcher.logInfo("data normal forn titulo: " + dataAtualFormatada);
		
		StringBuilder consulta = new StringBuilder();
		
		EnviromentUtils util = new EnviromentUtils();
		
		
		
		try {

			JsonParser parser = new JsonParser();
			
			System.out.println("Response length: " + response.length);
		    System.out.println("Response content: " + response[1]);
		    
		    LogCatcher.logInfo("Response length: " + response.length);
		    LogCatcher.logInfo("Response content: " + response[1]);
		    
		    // Adicione estas instruções de debug logo após criar o parser
		    System.out.println("Tipo de resposta: " + parser.parse(response[1]).getClass().getName());
		    System.out.println("Conteúdo bruto da resposta: ");
		    System.out.println(response[1]);
		    
		    
		    LogCatcher.logInfo("Tipo de resposta: " + parser.parse(response[1]).getClass().getName());
		    LogCatcher.logInfo("Conteúdo bruto da resposta: ");
		    LogCatcher.logInfo(response[1]);
		    
		    try {
		        // Tente analisar a resposta de maneira diferente
		        JsonElement element = parser.parse(response[1]);
		        
		        if (element.isJsonArray()) {
		            JsonArray jsonArray = element.getAsJsonArray();
		            System.out.println("Analisado com sucesso como array JSON com " + jsonArray.size() + " elementos");
		            LogCatcher.logInfo("Analisado com sucesso como array JSON com " + jsonArray.size() + " elementos");
		            
		            // Continue com o processamento do array
		            if (response[0].equalsIgnoreCase("200")) {
		                // Seu código existente para processar o array...
		            }
		        } else if (element.isJsonObject()) {
		            JsonObject jsonObject = element.getAsJsonObject();
		            System.out.println("A resposta é um objeto JSON, não um array");
		            LogCatcher.logInfo("A resposta é um objeto JSON, não um array");
		            // Se os dados reais estiverem dentro de um objeto, você pode precisar extraí-los
		            if (jsonObject.has("data") && jsonObject.get("data").isJsonArray()) {
		                System.out.println("Array de dados encontrado dentro do objeto JSON");
		                LogCatcher.logInfo("Array de dados encontrado dentro do objeto JSON");
		                JsonArray jsonArray = jsonObject.get("data").getAsJsonArray();
		                
		                // Continue com o processamento do array extraído
		                if (response[0].equalsIgnoreCase("200")) {
		                    // Seu código existente para processar o array...
		                }
		            } else {
		                System.out.println("Nenhum array 'data' encontrado no objeto JSON");
		                LogCatcher.logInfo("Nenhum array 'data' encontrado no objeto JSON");
		            }
		        } else {
		            System.out.println("A resposta não é nem um array JSON nem um objeto JSON");
		            LogCatcher.logInfo("A resposta não é nem um array JSON nem um objeto JSON");
		        }
		    } catch (Exception e) {
		        System.out.println("Exceção ao tentar analisar o JSON: " + e.getMessage());
		        LogCatcher.logInfo("Exceção ao tentar analisar o JSON: " + e.getMessage());
		        e.printStackTrace();
		    }
		    
		    
		    //fim do debug
		    
			System.out.println("Response length: " + response.length);
			
			System.out.println("Response content: " + response[1]);
			
			LogCatcher.logInfo("Response length: " + response.length);
			LogCatcher.logInfo("Response content: " + response[1]);

			JsonArray jsonArray = parser.parse(response[1]).getAsJsonArray();         

			if (response[0].equalsIgnoreCase("200")) {
				System.out.println("API response code: " + response[0]);
				LogCatcher.logInfo("API response code: " + response[0]);

				int count = 0;
				int total = jsonArray.size();
				int qtdInsert = 0;

				List<String> selectsParaInsert = new ArrayList<String>();   //922

				for (JsonElement jsonElement : jsonArray) {
					System.out.println("comecou a leitura do JSON");
					LogCatcher.logInfo("comecou a leitura do JSON");
					JsonObject JSON = jsonElement.getAsJsonObject();

					String fornecedorId = JSON.get("fornecedor_id")
							.getAsString();
					
					System.out.println("FornecedorId: " + fornecedorId);
					LogCatcher.logInfo("FornecedorId: " + fornecedorId);
					
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
					
					
                    //alteracao aqui
					String tituloObservacao = "";
					if (!JSON.get("titulo_observacao").isJsonNull()) {
					    tituloObservacao = JSON.get("titulo_observacao").getAsString().replace("'", "''");
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
					
					LogCatcher.logInfo("CodCenCus: " + codCenCus);
					LogCatcher.logInfo("Taxa id: " + taxaId);
					LogCatcher.logInfo("CodParc: " + codparc);

					if (!tituloSituacao.equalsIgnoreCase("X")) {

						if (codparc.compareTo(BigDecimal.ZERO) != 0) {
							System.out
									.println("Entrou no parceiro: " + codparc);
							
							LogCatcher.logInfo("Entrou no parceiro: " + codparc);

							BigDecimal validarNufin = Optional.ofNullable(
									mapaInfFinanceiro.get(codemp + "###"
											+ idFin)).orElse(BigDecimal.ZERO);

							if (validarNufin.compareTo(BigDecimal.ZERO) == 0) {
								System.out.println("Entrou no financeiro");
								LogCatcher.logInfo("Entrou no financeiro");

								BigDecimal codConta = mapaInfConta.get(codemp
										.toString());

								BigDecimal codBanco = mapaInfBanco.get(codemp
										.toString());

								System.out.println("Size Natureza: " + mapaInfNatureza.size());
								LogCatcher.logInfo("Size Natureza: " + mapaInfNatureza.size());
								
								BigDecimal natureza = Optional.ofNullable(mapaInfNatureza.get(taxaId + "###" + tipoEmpresa))
													 .orElse(BigDecimal.ZERO);
								
								System.out.println("Natureza: " + natureza);
								LogCatcher.logInfo("Natureza: " + natureza);

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
									LogCatcher.logInfo("Financeiro cadastrado");

								}else{
									
									selectsParaInsertLog.add("SELECT <#NUMUNICO#>, 'Sem \"de para\" para a Taxa ID: "+ taxaId+"', SYSDATE, 'Aviso', "+codemp+", '' FROM DUAL");
									LogCatcher.logInfo("SELECT <#NUMUNICO#>, 'Sem \"de para\" para a Taxa ID: "+ taxaId+"', SYSDATE, 'Aviso', "+codemp+", '' FROM DUAL");
									
				
								}

							} else {
								System.out.println("Financeiro " + idFin
										+ " ja cadastrado para o parceiro: "
										+ codparc);
								LogCatcher.logInfo("Financeiro " + idFin
										+ " ja cadastrado para o parceiro: "
										+ codparc);
								
							}

						} else {
							System.out.println("Sem Parceiro");
						}
						
					} else if(tituloSituacao.equalsIgnoreCase("X")){
						System.out.println("Cancelado");
						LogCatcher.logInfo("Cancelado");

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

					count++;
				}

				if (qtdInsert > 0) {
					// Capturar o tgfnum
					BigDecimal nuFinInicial = util.getMaxNumFin(false);

					// Atualizar o nufin adicionando a quantidade de lista
					util.updateNumFinByQtd(qtdInsert);
					System.out.println("nuFinInicial: " + nuFinInicial);
					LogCatcher.logInfo("nuFinInicial: " + nuFinInicial);

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

					System.out.println("Consulta Apis Tratamento: "
							+ sqlInsertFin);
					LogCatcher.logInfo("Consulta Apis Tratamento: "
							+ sqlInsertFin);

					insertFinByList(sqlInsertFin, codemp);

				}
			} else {
				
				selectsParaInsertLog.add("SELECT <#NUMUNICO#>, 'Api Retornou Status Diferente de Sucesso, Status Retornado: "+response[0]+"', SYSDATE, 'Erro', "+codemp+", '' FROM DUAL");
				LogCatcher.logInfo("SELECT <#NUMUNICO#>, 'Api Retornou Status Diferente de Sucesso, Status Retornado: "+response[0]+"', SYSDATE, 'Erro', "+codemp+", '' FROM DUAL");

			}


		}catch(Exception e){
			
			StringWriter sw = new StringWriter();
			String stackTraceAsString = sw.toString();

			selectsParaInsertLog.add("SELECT <#NUMUNICO#>, 'Erro ao integrar financeiro, Mensagem de erro: "
					+ e.getMessage()+"', SYSDATE, 'Erro', "+codemp+", '' FROM DUAL");
			
			LogCatcher.logInfo("SELECT <#NUMUNICO#>, 'Erro ao integrar financeiro, Mensagem de erro: "
					+ e.getMessage()+"', SYSDATE, 'Erro', "+codemp+", '' FROM DUAL");

			
			e.printStackTrace();
			
		}
		finally{
			
			System.out.println("Fim leitura do JSON - JobGetTituloFornecedor");
			LogCatcher.logInfo("Fim leitura do JSON - JobGetTituloFornecedor");
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
			LogCatcher.logInfo("Deu inicio ao insertTGFFIN()");
			
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
					+ "         CODUSU, "
					+ "         AD_IDEXTERNO, "
					+ "         AD_IDALUNO) "
					+ "        VALUES (?, "
					+ "               NULL, "
					+ "               0, "
					+ "               'F', "
					+ "               -1, "
					+ "               "+codemp+" , "
					+ "               "+codCenCus+" , "
					+ "               "+codNat+" , "
					+ "               "+codTipOper+" , "
					+ "               (SELECT MAX(DHALTER) FROM TGFTOP WHERE CODTIPOPER = "+codTipOper+"), "
					+ "               0, "
					+ "               (SELECT MAX(DHALTER) FROM TGFTOP WHERE CODTIPOPER = 0), "
					+ "               "+codparc+" , "
					+ "               "+codTipTit+" , "
					+ "               "+vlrDesdbo+" , "
					+ "               0, "
					+ "               0, "
					+ "               "+codBanco+", "
					+ "               "+codConta+", "
					+ "               '"+dtPedido+"' , "
					+ "               SYSDATE, "
					+ "               SYSDATE, "
					+ "               '"+dtVenc+"' , "
					+ "               SYSDATE, "
					+ "               '"+dtVenc+"' , "
					+ "               1 , "
					+ "               1 , "
					+ "               '"+obs+"' , "
					+ "               'I' , "
					+ "               'N' , "
					+ "               'N' , "
					+ "               'N' , "
					+ "               'N' , "
					+ "               'N' , "
					+ "               'N' , "
					+ "               'N' , "
					+ "               'S' , "
					+ "               'S' , "
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
					+ "               428, " // Alterado CODUSU para 428
					+ "               '"+idExterno+"'," 
					+ "               '"+idAluno+"')";


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
			LogCatcher.logInfo("Deu Fim ao insertTGFFIN()");
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

			System.out.println("Passou do updateFinExtorno");
			LogCatcher.logInfo("Passou do updateFinExtorno");
		} catch (SQLException e) {
			selectsParaInsertLog.add("SELECT <#NUMUNICO#>, 'Erro ao Extornar Titulo "+nufin+": "+e.getMessage()+"', SYSDATE, 'Erro', "+codemp+", '' FROM DUAL");
			LogCatcher.logInfo("SELECT <#NUMUNICO#>, 'Erro ao Extornar Titulo "+nufin+": "+e.getMessage()+"', SYSDATE, 'Erro', "+codemp+", '' FROM DUAL");

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

			System.out.println("Passou do Update deleteTgfMbc");
			LogCatcher.logInfo("Passou do  Update deleteTgfMbc");
		} catch (SQLException e) {
			e.printStackTrace();
			selectsParaInsertLog.add("SELECT <#NUMUNICO#>, 'Erro ao Excluir Movimentacao Bancaria "+nubco+": "+e.getMessage()+"', SYSDATE, 'Erro', "+codemp+", '' FROM DUAL");
			LogCatcher.logInfo("SELECT <#NUMUNICO#>, 'Erro ao Excluir Movimentacao Bancaria "+nubco+": "+e.getMessage()+"', SYSDATE, 'Erro', "+codemp+", '' FROM DUAL");
			
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
			
			System.out.println("Passou do update deleteTgfFin");
			LogCatcher.logInfo("Passou do update deleteTgfFin");
		} catch (SQLException e) {
			e.printStackTrace();
			selectsParaInsertLog.add("SELECT <#NUMUNICO#>, 'Erro ao Excluir Titulo "+nufin+": "+e.getMessage()+"', SYSDATE, 'Erro', "+codemp+", '' FROM DUAL");
			LogCatcher.logInfo("SELECT <#NUMUNICO#>, 'Erro ao Excluir Titulo "+nufin+": "+e.getMessage()+"', SYSDATE, 'Erro', "+codemp+", '' FROM DUAL");
			
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

	        String sqlUpdate = "INSERT INTO TGFFIN " +
	                "(NUFIN, NUNOTA, NUMNOTA, ORIGEM, RECDESP, CODEMP, CODCENCUS, CODNAT, CODTIPOPER, DHTIPOPER, " +
	                "CODTIPOPERBAIXA, DHTIPOPERBAIXA, CODPARC, CODTIPTIT, VLRDESDOB, VLRDESC, VLRBAIXA, CODBCO, " +
	                "CODCTABCOINT, DTNEG, DHMOV, DTALTER, DTVENC, DTPRAZO, DTVENCINIC, TIPJURO, TIPMULTA, " +
	                "HISTORICO, TIPMARCCHEQ, AUTORIZADO, BLOQVAR, INSSRETIDO, ISSRETIDO, PROVISAO, RATEADO, " +
	                "TIMBLOQUEADA, IRFRETIDO, TIMTXADMGERALU, VLRDESCEMBUT, VLRINSS, VLRIRF, VLRISS, VLRJURO, " +
	                "VLRJUROEMBUT, VLRJUROLIB, VLRJURONEGOC, VLRMOEDA, VLRMOEDABAIXA, VLRMULTA, VLRMULTAEMBUT, " +
	                "VLRMULTALIB, VLRMULTANEGOC, VLRPROV, VLRVARCAMBIAL, VLRVENDOR, ALIQICMS, BASEICMS, " +
	                "CARTAODESC, CODMOEDA, CODPROJ, CODVEICULO, CODVEND, DESPCART, NUMCONTRATO, ORDEMCARGA, " +
	                "CODUSU, AD_IDEXTERNO, AD_IDALUNO) " + listInsert.toString();

	        pstmt = jdbc.getPreparedStatement(sqlUpdate);
	        pstmt.executeUpdate();

	    } catch (Exception se) {
	        se.printStackTrace();
	        selectsParaInsertLog.add("SELECT <#NUMUNICO#>, 'Erro ao integrar financeiro, Mensagem de erro: "
	                + se.getMessage().replace("'", "\"") + "', SYSDATE, 'Erro', " + codemp + ", '' FROM DUAL");
	        LogCatcher.logInfo("SELECT <#NUMUNICO#>, 'Erro ao integrar financeiro, Mensagem de erro: "
	                + se.getMessage().replace("'", "\"") + "', SYSDATE, 'Erro', " + codemp + ", '' FROM DUAL");

	    } finally {
	        if (pstmt != null) pstmt.close();
	        if (jdbc != null) jdbc.closeSession();
	    }
	}


	
	/**
	 * Método para inserir logs de integração
	 * 
	 * @param descricao Descrição do log
	 * @param status Status da operação (Sucesso, Erro, etc.)
	 * @param url URL do serviço (opcional)
	 * @param token Token de autenticação (opcional)
	 * @throws Exception
	 */
	public void insertLogIntegracao(String descricao, String status, String url, String token) throws Exception {
	    EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	    JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
	    PreparedStatement pstmt = null;
	    
	    try {
	        jdbc.openSession();

	        String descricaoCompleta = descricao;
	        
	        // Adiciona informações de URL e token à descrição para melhor rastreabilidade
	        if (url != null && !url.isEmpty()) {
	            descricaoCompleta += " URL: " + url;
	            
	            if (token != null && !token.isEmpty()) {
	                // Pode ser útil incluir apenas parte do token por segurança
	                String tokenMasked = token.length() > 10 ? 
	                    token.substring(0, 5) + "..." + token.substring(token.length() - 5) : 
	                    "***";
	                descricaoCompleta += " Token: " + tokenMasked;
	            }
	        }

	        String sqlUpdate = "INSERT INTO AD_LOGINTEGRACAO (NUMUNICO, DESCRICAO, DTHORA, STATUS) " +
	                         "VALUES (((SELECT NVL(MAX(NUMUNICO), 0) + 1 FROM AD_LOGINTEGRACAO)), ?, SYSDATE, ?)";

	        pstmt = jdbc.getPreparedStatement(sqlUpdate);
	        pstmt.setString(1, descricaoCompleta);
	        pstmt.setString(2, status);
	        pstmt.executeUpdate();
	        
	        System.out.println("Log de integração inserido: " + descricaoCompleta);
	        LogCatcher.logInfo("Log de integração inserido: " + descricaoCompleta);
	        
	    } catch (Exception se) {
	        se.printStackTrace();
	        System.out.println("Erro ao inserir log de integração: " + se.getMessage());
	        LogCatcher.logInfo("Erro ao inserir log de integração: " + se.getMessage());
	    } finally {
	        if (pstmt != null) {
	            pstmt.close();
	        }
	        jdbc.closeSession();
	    }
	}

	/**
	 * Sobrecarga do método insertLogIntegracao para manter compatibilidade com código existente
	 */
	public void insertLogIntegracao(String descricao, String status, String fornecedorNome) throws Exception {
	    EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	    JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
	    PreparedStatement pstmt = null;
	    
	    try {
	        jdbc.openSession();

	        String descricaoCompleta = null;
	        if (fornecedorNome.equals("")) {
	            descricaoCompleta = descricao;
	        } else if (!fornecedorNome.isEmpty()) {
	            descricaoCompleta = descricao + " " + " Fornecedor:" + fornecedorNome;
	        } else {
	            descricaoCompleta = descricao;
	        }
	        
	        String sqlUpdate = "INSERT INTO AD_LOGINTEGRACAO (NUMUNICO, DESCRICAO, DTHORA, STATUS) " +
	                         "VALUES (((SELECT NVL(MAX(NUMUNICO), 0) + 1 FROM AD_LOGINTEGRACAO)), ?, SYSDATE, ?)";

	        pstmt = jdbc.getPreparedStatement(sqlUpdate);
	        pstmt.setString(1, descricaoCompleta);
	        pstmt.setString(2, status);
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
	 * Versão otimizada do método apiGet com melhor tratamento de erros e recursos
	 */
	
	public String[] apiGet(String ur, String token) throws Exception {
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
	    https.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)");
	    https.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
	    https.setRequestProperty("Accept", "application/json");
	    https.setRequestProperty("Authorization", "Bearer " + token);
	    https.setDoInput(true);

	    int status = https.getResponseCode();

	    // Se o status for 429, implemente a lógica de backoff
	    if (status == 429) {
	        // Tenta ler o cabeçalho Retry-After, se disponível
	        String retryAfter = https.getHeaderField("Retry-After");
	        int waitTime = 5; // valor padrão em segundos
	        if (retryAfter != null) {
	            try {
	                waitTime = Integer.parseInt(retryAfter);
	            } catch (NumberFormatException e) {
	                // Use o tempo padrão se a conversão falhar
	            }
	        }
	        System.out.println("Limite atingido. Aguardando " + waitTime + " segundos antes de tentar novamente.");
	        LogCatcher.logInfo("Limite atingido. Aguardando " + waitTime + " segundos antes de tentar novamente.");
	        Thread.sleep(waitTime * 1000L);
	        https.disconnect();
	        // Aqui você pode optar por recursão ou um loop para tentar novamente
	        return apiGet(ur, token);
	    }

	    if (status >= 300) {
	        reader = new BufferedReader(new InputStreamReader(https.getErrorStream()));
	    } else {
	        reader = new BufferedReader(new InputStreamReader(https.getInputStream()));
	    }
	    String line;
	    while ((line = reader.readLine()) != null) {
	        responseContent.append(line);
	    }
	    reader.close();
	    System.out.println("Output from Server .... \n" + status);
	    LogCatcher.logInfo("Output from Server .... \n" + status);
	    String response = responseContent.toString();
	    https.disconnect();
	    return new String[] { Integer.toString(status), response };
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
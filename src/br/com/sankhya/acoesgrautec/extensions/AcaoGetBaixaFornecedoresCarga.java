
package br.com.sankhya.acoesgrautec.extensions;

import br.com.sankhya.acoesgrautec.util.EnviromentUtils;
import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.extensions.actionbutton.Registro;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.core.JapeSession;
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
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;

import org.activiti.engine.impl.util.json.JSONArray;
import org.cuckoo.core.ScheduledAction;
import org.cuckoo.core.ScheduledActionContext;

public class AcaoGetBaixaFornecedoresCarga implements AcaoRotinaJava, ScheduledAction {
    private List<String> selectsParaInsert = new ArrayList();
    private EnviromentUtils util = new EnviromentUtils();
    static {
    	LogConfiguration.setPath(SWRepositoryUtils.getBaseFolder() + "/logAcao/logs");
    }

    
    @Override
    public void doAction(ContextoAcao contexto) throws Exception {
        String threadName = Thread.currentThread().getName();
        Date startTime = new Date();

        LogCatcher.logInfo("=== INÍCIO DO doAction Baixa de Fornecedores === Thread: " + threadName + " - Hora: " + startTime);

        Registro[] linhas = contexto.getLinhas();
        
        if (linhas.length < 1) {
            contexto.mostraErro("É necessário selecionar pelo menos 1 linha.");
            return;
        }
        
        // Determina quantas linhas processar (máximo 65)
        int linhasParaProcessar = Math.min(linhas.length, 65);
        LogCatcher.logInfo("Processando " + linhasParaProcessar + " registro(s)");
        
        String dataInicio = contexto.getParam("DTINICIO").toString().substring(0, 10);
        String dataFim = contexto.getParam("DTFIM").toString().substring(0, 10);
        String idForn = (String) contexto.getParam("IDFORN");

        try {
            List<Object[]> listInfBancoConta = this.retornarInformacoesBancoConta();
            Map<String, BigDecimal> mapaInfBanco = new HashMap();
            Map<String, BigDecimal> mapaInfConta = new HashMap();

            for(Object[] obj : listInfBancoConta) {
                Long codEmpObj = (Long)obj[1];
                String idExternoObj = (String)obj[2];
                BigDecimal codBcoObj = (BigDecimal)obj[3];
                BigDecimal codCtabCointObj = (BigDecimal)obj[0];
                
                if (mapaInfBanco.get(codEmpObj + "###" + idExternoObj) == null) {
                    mapaInfBanco.put(codEmpObj + "###" + idExternoObj, codBcoObj);
                }
                
                if (mapaInfConta.get(codEmpObj + "###" + idExternoObj) == null) {
                    mapaInfConta.put(codEmpObj + "###" + idExternoObj, codCtabCointObj);
                }
            }

            List<Object[]> listInfFinanceiro = this.retornarInformacoesFinanceiro();
            Map<String, BigDecimal> mapaInfFinanceiro = new HashMap();
            Map<BigDecimal, String> mapaInfFinanceiroBaixado = new HashMap();
            Map<BigDecimal, BigDecimal> mapaInfFinanceiroValor = new HashMap();
            Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco = new HashMap();

            for(Object[] obj : listInfFinanceiro) {
                BigDecimal nuFin = (BigDecimal)obj[0];
                BigDecimal codEmpObj = (BigDecimal)obj[1];
                String idExternoObj = (String)obj[2];
                String baixado = (String)obj[3];
                BigDecimal vlrDesdob = (BigDecimal)obj[4];
                BigDecimal nuBco = (BigDecimal)obj[5];
                
                if (mapaInfFinanceiro.get(codEmpObj + "###" + idExternoObj) == null) {
                    mapaInfFinanceiro.put(codEmpObj + "###" + idExternoObj, nuFin);
                }
                
                if (mapaInfFinanceiroBaixado.get(nuFin) == null) {
                    mapaInfFinanceiroBaixado.put(nuFin, baixado);
                }
                
                if (mapaInfFinanceiroValor.get(nuFin) == null) {
                    mapaInfFinanceiroValor.put(nuFin, vlrDesdob);
                }
                
                if (mapaInfFinanceiroBanco.get(nuFin) == null) {
                    mapaInfFinanceiroBanco.put(nuFin, nuBco);
                }
            }

            List<Object[]> listInfTipoTitulo = this.retornarInformacoesTipoTitulo();
            Map<String, BigDecimal> mapaInfTipoTitulo = new HashMap();
            Map<String, BigDecimal> mapaInfTipoTituloTaxa = new HashMap();

            for(Object[] obj : listInfTipoTitulo) {
                BigDecimal codTipTit = (BigDecimal)obj[0];
                Long codEmpObj = (Long)obj[1];
                String idExternoObj = (String)obj[2];
                BigDecimal taxa = (BigDecimal)obj[3];
                
                if (mapaInfTipoTitulo.get(codEmpObj + "###" + idExternoObj) == null) {
                    mapaInfTipoTitulo.put(codEmpObj + "###" + idExternoObj, codTipTit);
                }
                
                if (mapaInfTipoTituloTaxa.get(codEmpObj + "###" + idExternoObj) == null) {
                    mapaInfTipoTituloTaxa.put(codEmpObj + "###" + idExternoObj, taxa);
                }
            }

            List<Object[]> listInfMenorDataMovBancariaPorConta = this.retornarInformacoesMenorDataMovBancariaPorConta();
            Map<Long, Date> mapaInfMenorDataMovBancariaPorConta = new HashMap();

            for(Object[] obj : listInfMenorDataMovBancariaPorConta) {
                Long codCtabCointObj = (Long)obj[0];
                Date dtMinRef = (Date)obj[1];
                if (mapaInfMenorDataMovBancariaPorConta.get(codCtabCointObj) == null) {
                    mapaInfMenorDataMovBancariaPorConta.put(codCtabCointObj, dtMinRef);
                }
            }

            for (int i = 0; i < linhasParaProcessar; i++) {
                Registro registro = linhas[i];
                String url = (String)registro.getCampo("URL");
                String token = (String)registro.getCampo("TOKEN");
                BigDecimal codEmp = (BigDecimal)registro.getCampo("CODEMP");
                
                LogCatcher.logInfo("Processando registro " + (i+1) + " de " + linhasParaProcessar);
                
                this.processDateRange(
                    url, 
                    token, 
                    codEmp, 
                    mapaInfTipoTituloTaxa, 
                    mapaInfBanco, 
                    mapaInfConta, 
                    mapaInfFinanceiro, 
                    mapaInfTipoTitulo, 
                    mapaInfMenorDataMovBancariaPorConta, 
                    mapaInfFinanceiroBaixado, 
                    mapaInfFinanceiroValor, 
                    mapaInfFinanceiroBanco, 
                    dataInicio, 
                    dataFim, 
                    idForn
                );
            }
            
            contexto.setMensagemRetorno("Período processado com sucesso para " + linhasParaProcessar + " registro(s)!");
            LogCatcher.logInfo("Período processado com sucesso para " + linhasParaProcessar + " registro(s)!");
        } catch (Exception e) {
            LogCatcher.logInfo("Erro no processamento - Thread: " + threadName);
            e.printStackTrace();
            try {
                if (linhas.length > 0) {
                    Registro primeiroRegistro = linhas[0];
                    String url = (String) primeiroRegistro.getCampo("URL");
                    String token = (String) primeiroRegistro.getCampo("TOKEN");
                    BigDecimal codEmp = (BigDecimal) primeiroRegistro.getCampo("CODEMP");
                    
                }
            } catch (Exception e1) {
                e1.printStackTrace();
            }
            contexto.mostraErro(e.getMessage());
        } finally {
            if (this.selectsParaInsert.size() > 0) {
                StringBuilder msgError = new StringBuilder();
                LogCatcher.logInfo("Entrou na lista do finally: " + this.selectsParaInsert.size());
                int qtdInsert = this.selectsParaInsert.size();
                int i = 1;

                for(String sqlInsert : this.selectsParaInsert) {
                    int nuFin = this.util.getMaxNumLog();
                    String sql = sqlInsert.replace("<#NUMUNICO#>", String.valueOf(nuFin));
                    msgError.append(sql);
                    if (i < qtdInsert) {
                        msgError.append(" \nUNION ALL ");
                    }

                    ++i;
                }

                LogCatcher.logInfo("Consulta de log: \n" + msgError);
                
                if (linhas.length > 0) {
                    BigDecimal codEmp = (BigDecimal) linhas[0].getCampo("CODEMP");
                    this.insertLogList(msgError.toString(), codEmp);
                } else {
                    this.insertLogList(msgError.toString());
                }
            }
            LogCatcher.logInfo("=== FIM DO doAction Baixa de Fornecedores === Thread: " + threadName + " - Hora: " + new Date());
        }
    }

    public void onTime(ScheduledActionContext arg0) {
    	LogConfiguration.setPath(SWRepositoryUtils.getBaseFolder() + "/logAcao/logs");
        EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
        JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        BigDecimal codEmp = BigDecimal.ZERO;
        String url = "";
        String token = "";

        try {
            List<Object[]> listInfBancoConta = this.retornarInformacoesBancoConta();
            Map<String, BigDecimal> mapaInfBanco = new HashMap();

            for(Object[] obj : listInfBancoConta) {
                Long codEmpObj = (Long)obj[1];
                String idExternoObj = (String)obj[2];
                BigDecimal codBcoObj = (BigDecimal)obj[3];
                if (mapaInfBanco.get(codEmpObj + "###" + idExternoObj) == null) {
                    mapaInfBanco.put(codEmpObj + "###" + idExternoObj, codBcoObj);
                }
            }

            Map<String, BigDecimal> mapaInfConta = new HashMap();

            for(Object[] obj : listInfBancoConta) {
                BigDecimal codCtabCointObj = (BigDecimal)obj[0];
                Long codEmpObj = (Long)obj[1];
                String idExternoObj = (String)obj[2];
                if (mapaInfConta.get(codEmpObj + "###" + idExternoObj) == null) {
                    mapaInfConta.put(codEmpObj + "###" + idExternoObj, codCtabCointObj);
                }
            }

            List<Object[]> listInfFinanceiro = this.retornarInformacoesFinanceiro();
            Map<String, BigDecimal> mapaInfFinanceiro = new HashMap();

            for(Object[] obj : listInfFinanceiro) {
                BigDecimal nuFin = (BigDecimal)obj[0];
                BigDecimal codEmpObj = (BigDecimal)obj[1];
                String idExternoObj = (String)obj[2];
                if (mapaInfFinanceiro.get(codEmpObj + "###" + idExternoObj) == null) {
                    mapaInfFinanceiro.put(codEmpObj + "###" + idExternoObj, nuFin);
                }
            }

            Map<BigDecimal, String> mapaInfFinanceiroBaixado = new HashMap();

            for(Object[] obj : listInfFinanceiro) {
                BigDecimal nuFin = (BigDecimal)obj[0];
                String baixado = (String)obj[3];
                if (mapaInfFinanceiroBaixado.get(nuFin) == null) {
                    mapaInfFinanceiroBaixado.put(nuFin, baixado);
                }
            }

            Map<BigDecimal, BigDecimal> mapaInfFinanceiroValor = new HashMap();

            for(Object[] obj : listInfFinanceiro) {
                BigDecimal nuFin = (BigDecimal)obj[0];
                BigDecimal vlrDesdob = (BigDecimal)obj[4];
                if (mapaInfFinanceiroValor.get(nuFin) == null) {
                    mapaInfFinanceiroValor.put(nuFin, vlrDesdob);
                }
            }

            Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco = new HashMap();

            for(Object[] obj : listInfFinanceiro) {
                BigDecimal nuFin = (BigDecimal)obj[0];
                BigDecimal nuBco = (BigDecimal)obj[5];
                if (mapaInfFinanceiroBanco.get(nuFin) == null) {
                    mapaInfFinanceiroBanco.put(nuFin, nuBco);
                }
            }

            List<Object[]> listInfTipoTitulo = this.retornarInformacoesTipoTitulo();
            Map<String, BigDecimal> mapaInfTipoTitulo = new HashMap();

            for(Object[] obj : listInfTipoTitulo) {
                BigDecimal codTipTit = (BigDecimal)obj[0];
                Long codEmpObj = (Long)obj[1];
                String idExternoObj = (String)obj[2];
                if (mapaInfTipoTitulo.get(codEmpObj + "###" + idExternoObj) == null) {
                    mapaInfTipoTitulo.put(codEmpObj + "###" + idExternoObj, codTipTit);
                }
            }

            Map<String, BigDecimal> mapaInfTipoTituloTaxa = new HashMap();

            for(Object[] obj : listInfTipoTitulo) {
                BigDecimal taxa = (BigDecimal)obj[3];
                Long codEmpObj = (Long)obj[1];
                String idExternoObj = (String)obj[2];
                if (mapaInfTipoTituloTaxa.get(codEmpObj + "###" + idExternoObj) == null) {
                    mapaInfTipoTituloTaxa.put(codEmpObj + "###" + idExternoObj, taxa);
                }
            }

            List<Object[]> listInfMenorDataMovBancariaPorConta = this.retornarInformacoesMenorDataMovBancariaPorConta();
            Map<Long, Date> mapaInfMenorDataMovBancariaPorConta = new HashMap();

            for(Object[] obj : listInfMenorDataMovBancariaPorConta) {
                Long codCtabCointObj = (Long)obj[0];
                Date dtMinRef = (Date)obj[1];
                if (mapaInfMenorDataMovBancariaPorConta.get(codCtabCointObj) == null) {
                    mapaInfMenorDataMovBancariaPorConta.put(codCtabCointObj, dtMinRef);
                }
            }

            jdbc.openSession();
            String query = "SELECT CODEMP, URL, TOKEN, INTEGRACAO FROM AD_LINKSINTEGRACAO";
            pstmt = jdbc.getPreparedStatement(query);
            rs = pstmt.executeQuery();

            while(rs.next()) {
                codEmp = rs.getBigDecimal("CODEMP");
                url = rs.getString("URL");
                token = rs.getString("TOKEN");
                String statusIntegracao = rs.getString("INTEGRACAO");
                if (!"S".equals(statusIntegracao)) {
                    LogCatcher.logInfo("Integração desativada para a empresa " + codEmp + " - pulando processamento");
                } else {
                    this.iterarEndpoint(url, token, codEmp, mapaInfTipoTituloTaxa, mapaInfBanco, mapaInfConta, mapaInfFinanceiro, mapaInfTipoTitulo, mapaInfMenorDataMovBancariaPorConta, mapaInfFinanceiroBaixado, mapaInfFinanceiroValor, mapaInfFinanceiroBanco);
                }
            }
        } catch (Exception var55) {
            Exception e = var55;
            var55.printStackTrace();

            try {
                this.insertLogIntegracao("Erro ao integrar Baixas, Mensagem de erro: " + e.getMessage(), "Erro");
                LogCatcher.logInfo("Erro ao integrar Baixas, Mensagem de erro: " + e.getMessage());
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
            if (this.selectsParaInsert.size() > 0) {
                StringBuilder msgError = new StringBuilder();
                LogCatcher.logInfo("Entrou na lista do finally: " + this.selectsParaInsert.size());
                int qtdInsert = this.selectsParaInsert.size();
                int i = 1;

                for(String sqlInsert : this.selectsParaInsert) {
                    int nuFin = 0;

                    try {
                        nuFin = this.util.getMaxNumLog();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    String sql = sqlInsert.replace("<#NUMUNICO#>", String.valueOf(nuFin));
                    msgError.append(sql);
                    if (i < qtdInsert) {
                        msgError.append(" \nUNION ALL ");
                    }

                    ++i;
                }

                LogCatcher.logInfo("Consulta de log: \n" + msgError);

                try {
                    this.insertLogList(msgError.toString(), codEmp);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                StringBuilder var99 = null;
                this.selectsParaInsert = new ArrayList();
            }

        }

    }

    public void processDateRange(String url, String token, BigDecimal codemp, Map<String, BigDecimal> mapaInfTipoTituloTaxa, Map<String, BigDecimal> mapaInfBanco, Map<String, BigDecimal> mapaInfConta, Map<String, BigDecimal> mapaInfFinanceiro, Map<String, BigDecimal> mapaInfTipoTitulo, Map<Long, Date> mapaInfMenorDataMovBancariaPorConta, Map<BigDecimal, String> mapaInfFinanceiroBaixado, Map<BigDecimal, BigDecimal> mapaInfFinanceiroValor, Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco, String dataInicio, String dataFim, String idForn) throws Exception {
        try {
            String dataInicialCompleta = dataInicio + " 00:00:00";
            String dataFinalCompleta = dataFim + " 23:59:59";
            String dataInicialEncoded = URLEncoder.encode(dataInicialCompleta, "UTF-8");
            String dataFinalEncoded = URLEncoder.encode(dataFinalCompleta, "UTF-8");
            LogCatcher.logInfo("Iniciando processamento de baixas para o período: " + dataInicio + " até " + dataFim);
            JSONArray todosRegistros = new JSONArray();
            int pagina = 1;

            JSONArray paginaAtual;
            for(boolean temMaisRegistros = true; temMaisRegistros; System.out.println("Página " + pagina + ": " + paginaAtual.length() + " registros. Total acumulado: " + todosRegistros.length())) {
                StringBuilder urlBuilder = new StringBuilder();
                urlBuilder.append(url.trim()).append("/financeiro/clientes/titulos-pagar-baixa").append("?pagina=").append(pagina).append("&quantidade=100").append("&dataInicial=").append(dataInicialEncoded).append("&dataFinal=").append(dataFinalEncoded);
                if (idForn != null && !idForn.isEmpty()) {
                    String fornecedorEncoded = URLEncoder.encode(idForn, "UTF-8");
                    urlBuilder.append("&fornecedor=").append(fornecedorEncoded);
                    LogCatcher.logInfo("Processando baixas para o fornecedor: " + idForn);
                }

                String urlCompleta = urlBuilder.toString();
                LogCatcher.logInfo("URL para baixas (página " + pagina + "): " + urlCompleta);
                String[] response = this.apiGet(urlCompleta, token);
                int status = Integer.parseInt(response[0]);
                if (status != 200) {
                    throw new Exception(String.format("Erro na requisição de baixas. Status: %d. Resposta: %s. URL: %s", status, response[1], urlCompleta));
                }

                paginaAtual = new JSONArray(response[1]);

                for(int i = 0; i < paginaAtual.length(); ++i) {
                    todosRegistros.put(paginaAtual.getJSONObject(i));
                }

                if (paginaAtual.length() < 100) {
                    temMaisRegistros = false;
                } else {
                    ++pagina;
                }
            }

            String[] responseArray = new String[]{String.valueOf(200), todosRegistros.toString()};
            LogCatcher.logInfo("Total de registros de baixas acumulados: " + todosRegistros.length());
            
           
            this.efetuarBaixa(responseArray, url, token, codemp, mapaInfTipoTituloTaxa, mapaInfBanco, mapaInfConta, mapaInfFinanceiro, mapaInfTipoTitulo, mapaInfMenorDataMovBancariaPorConta, mapaInfFinanceiroBaixado, mapaInfFinanceiroValor, mapaInfFinanceiroBanco);
            
        } catch (Exception e) {
            LogCatcher.logInfo("Erro ao processar baixas para o período " + dataInicio + " até " + dataFim + (idForn != null ? " (Fornecedor: " + idForn + ")" : "") + ": " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }

    public void iterarEndpoint(String url, String token, BigDecimal codemp, Map<String, BigDecimal> mapaInfTipoTituloTaxa, Map<String, BigDecimal> mapaInfBanco, Map<String, BigDecimal> mapaInfConta, Map<String, BigDecimal> mapaInfFinanceiro, Map<String, BigDecimal> mapaInfTipoTitulo, Map<Long, Date> mapaInfMenorDataMovBancariaPorConta, Map<BigDecimal, String> mapaInfFinanceiroBaixado, Map<BigDecimal, BigDecimal> mapaInfFinanceiroValor, Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco) throws Exception {
        Date dataAtual = new Date();
        SimpleDateFormat formato = new SimpleDateFormat("yyyy-MM-dd");
        String dataFormatada = formato.format(dataAtual);

        try {
			String[] response = this.apiGet(url + "/financeiro" + "/clientes" + "/titulos-pagar-baixa" + "?quantidade=0" + "&dataInicial="
					+ dataFormatada + " 00:00:00&dataFinal=" + dataFormatada + " 23:59:59", token);
			int status = Integer.parseInt(response[0]);
			String responseString = response[1];

			LogCatcher.logInfo("Status da requisição: " + status);
			LogCatcher.logInfo("Resposta da API (baixas): " + responseString);

			if (status == 200) {
				LogCatcher.logInfo("Sucesso (200): Requisição bem-sucedida.");
				this.efetuarBaixa(response, url, token, codemp, 
						mapaInfTipoTituloTaxa, mapaInfBanco, mapaInfConta,  mapaInfFinanceiro,
						mapaInfTipoTitulo, mapaInfMenorDataMovBancariaPorConta, mapaInfFinanceiroBaixado,
						mapaInfFinanceiroValor, mapaInfFinanceiroBanco
						 );
			} else if (status >= 400 && status < 500) {
				String erroMsg = "Erro do Cliente (" + status + "): A requisição para buscar baixas falhou. Resposta: "
						+ responseString;
				LogCatcher.logError(erroMsg);
				selectsParaInsert.add("SELECT <#NUMUNICO#>, \'" + erroMsg.replace("'", "''")
						+ "\', SYSDATE, \'Erro\', " + codemp + ", NULL FROM DUAL");
			} else if (status >= 500) {
				String erroMsg = "Erro do Servidor (" + status
						+ "): Ocorreu um problema no servidor da API ao buscar baixas. Resposta: " + responseString;
				LogCatcher.logError(erroMsg);
				selectsParaInsert.add("SELECT <#NUMUNICO#>, \'" + erroMsg.replace("'", "''")
						+ "\', SYSDATE, \'Erro\', " + codemp + ", NULL FROM DUAL");
			} else {
				String erroMsg = "Status inesperado (" + status
						+ "): A API retornou um código não previsto ao buscar baixas. Resposta: " + responseString;
				LogCatcher.logError(erroMsg);
				selectsParaInsert.add("SELECT <#NUMUNICO#>, \'" + erroMsg.replace("'", "''")
						+ "\', SYSDATE, \'Erro\', " + codemp + ", NULL FROM DUAL");
			}
		} catch (NumberFormatException e) {
			String erroMsg = "Erro de Formato de Número: Não foi possível converter o status da resposta da API para um número inteiro.";
			LogCatcher.logError(erroMsg);
			LogCatcher.logError(e);
			e.printStackTrace();
			selectsParaInsert.add("SELECT <#NUMUNICO#>, \'" + erroMsg.replace("'", "''")
					+ "\', SYSDATE, \'Erro\', " + codemp + ", NULL FROM DUAL");
		} catch (Exception e) {
			String erroMsg = "Exceção não tratada ao chamar a API de baixas: " + e.getMessage();
			LogCatcher.logError(erroMsg);
			LogCatcher.logError(e); 
			e.printStackTrace();
			selectsParaInsert.add("SELECT <#NUMUNICO#>, \'" + erroMsg.replace("'", "''")
					+ "\', SYSDATE, \'Erro\', " + codemp + ", NULL FROM DUAL");
		}
	}

    public void efetuarBaixa(String[] apiResponse, String url, String token, BigDecimal codemp, Map<String, BigDecimal> mapaInfTipoTituloTaxa, Map<String, BigDecimal> mapaInfBanco, Map<String, BigDecimal> mapaInfConta, Map<String, BigDecimal> mapaInfFinanceiro, Map<String, BigDecimal> mapaInfTipoTitulo, Map<Long, Date> mapaInfMenorDataMovBancariaPorConta, Map<BigDecimal, String> mapaInfFinanceiroBaixado, Map<BigDecimal, BigDecimal> mapaInfFinanceiroValor, Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco) throws Exception {
        boolean movimentacaoBancariaEfetuada = false;
        SimpleDateFormat formatoOriginal = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat formatoDesejado = new SimpleDateFormat("dd/MM/yyyy");
        
        BigDecimal codTipoTitulo = BigDecimal.ZERO;
        BigDecimal codigoBanco = BigDecimal.ZERO;
        BigDecimal codigoConta = BigDecimal.ZERO;
        BigDecimal numeroUnicoMovimentacaoBancaria = BigDecimal.ZERO;
        BigDecimal numeroUnicoFinanceiro = BigDecimal.ZERO;
        String dataEstorno = null;
        
        JapeSession.SessionHandle hnd = null;

        try {
            hnd = JapeSession.open();
            LogCatcher.logInfo("Iniciando processamento de baixas no método efetuarBaixa.");

            if (apiResponse[0].equalsIgnoreCase("200")) {
                JsonParser parser = new JsonParser();
                JsonArray baixasArray = parser.parse(apiResponse[1]).getAsJsonArray();

                for(JsonElement jsonElement : baixasArray) {
                    LogCatcher.logInfo("**INÍCIO DO LAÇO DE PROCESSAMENTO DE BAIXA DE FORNECEDOR**");
                    
                    JsonObject baixaJson = jsonElement.getAsJsonObject();
                    String tituloId = baixaJson.get("titulo_id").getAsString();
                    BigDecimal valorBaixa = new BigDecimal(baixaJson.get("baixa_valor").getAsString());
                    BigDecimal valorJuros = Optional.ofNullable(baixaJson.get("baixa_juros")).filter((element) -> !element.isJsonNull()).map(JsonElement::getAsString).map(BigDecimal::new).orElse(BigDecimal.ZERO);
                    BigDecimal valorMulta = Optional.ofNullable(baixaJson.get("baixa_multa")).filter((element) -> !element.isJsonNull()).map(JsonElement::getAsString).map(BigDecimal::new).orElse(BigDecimal.ZERO);
                    BigDecimal valorDesconto = Optional.ofNullable(baixaJson.get("baixa_desconto")).filter((element) -> !element.isJsonNull()).map(JsonElement::getAsString).map(BigDecimal::new).orElse(BigDecimal.ZERO);
                    BigDecimal valorOutrosAcrescimos = Optional.ofNullable(baixaJson.get("baixa_outros_acrescimos")).filter((element) -> !element.isJsonNull()).map(JsonElement::getAsString).map(BigDecimal::new).orElse(BigDecimal.ZERO);
                    String dataBaixaStr = baixaJson.get("baixa_data").getAsString();
                    String baixaId = baixaJson.get("baixa_id").getAsString();
                    Date dataBaixa = formatoOriginal.parse(dataBaixaStr);
                    String dataBaixaFormatada = formatoDesejado.format(dataBaixa);
                    numeroUnicoFinanceiro = mapaInfFinanceiro.get(codemp + "###" + tituloId);
                    
                    if (baixaJson.has("baixa_estorno_data") && !baixaJson.get("baixa_estorno_data").isJsonNull()) {
                        dataEstorno = baixaJson.get("baixa_estorno_data").getAsString();
                    } else {
                        dataEstorno = null;
                    }

                    String idLocalPagamento = baixaJson.get("local_pagamento_id").getAsString();
                    codigoBanco = mapaInfBanco.get(codemp + "###" + idLocalPagamento);
                    LogCatcher.logInfo("Banco: " + codigoBanco);
                    codigoConta = mapaInfConta.get(codemp + "###" + idLocalPagamento);
                    LogCatcher.logInfo("Conta: " + codigoConta);
                    
                    String[] formaPagamentoResponse = this.apiGet(url + "/financeiro/clientes/titulos-pagar-baixa-forma-pagamento?baixa=" + baixaId, token);
                    JsonParser formaPagamentoParser = new JsonParser();
                    JsonArray formaPagamentoArray = formaPagamentoParser.parse(formaPagamentoResponse[1]).getAsJsonArray();
                    JsonObject formaPagamentoJson = formaPagamentoArray.get(0).getAsJsonObject();
                    
                    String formaPagamentoId = formaPagamentoJson.get("forma_pagamento_id").getAsString().trim();
                    codTipoTitulo = Optional.ofNullable(mapaInfTipoTitulo.get(codemp + "###" + formaPagamentoId)).orElse(BigDecimal.ZERO);
                    BigDecimal taxaCartao = Optional.ofNullable(mapaInfTipoTituloTaxa.get(codemp + "###" + formaPagamentoId)).orElse(BigDecimal.ZERO);
                    
                    if (taxaCartao.compareTo(BigDecimal.ZERO) != 0) {
                        valorBaixa = valorBaixa.subtract(valorBaixa.multiply(taxaCartao).divide(BigDecimal.valueOf(100L)));
                    }

                    LogCatcher.logInfo("Data de estorno: " + (dataEstorno != null ? dataEstorno : "N/A"));
                    
                    if (numeroUnicoFinanceiro != null && numeroUnicoFinanceiro.compareTo(BigDecimal.ZERO) != 0) {
                        LogCatcher.logInfo("Financeiro encontrado para baixa: " + numeroUnicoFinanceiro);
                        Date dataMinimaMovimentacaoConta = mapaInfMenorDataMovBancariaPorConta.get(Long.parseLong(codigoConta.toString()));
                        LogCatcher.logInfo("Data mínima de movimentação da conta: " + dataMinimaMovimentacaoConta);
                        
                        if (dataEstorno == null) { // Processamento de baixa normal
                            if (dataMinimaMovimentacaoConta != null && (dataBaixa.before(dataMinimaMovimentacaoConta) || dataBaixa.equals(dataMinimaMovimentacaoConta))) {
                                this.selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Baixa Para o Titulo " + numeroUnicoFinanceiro + " Não Efetuada Pois a Data Minima de Movimentação Bancaria Para a Conta " + codigoConta + " é Superior ou Igual a Data de Baixa: " + dataBaixaFormatada + "' , SYSDATE, 'Aviso', " + codemp + ", '' FROM DUAL");
                            } else if (codTipoTitulo != null && codTipoTitulo.compareTo(BigDecimal.ZERO) != 0) {
                                if ("N".equalsIgnoreCase(mapaInfFinanceiroBaixado.get(numeroUnicoFinanceiro))) {
                                    LogCatcher.logInfo("Iniciando atualização do financeiro.");
                                    if (valorBaixa.compareTo(mapaInfFinanceiroValor.get(numeroUnicoFinanceiro)) == 0) {
                                        this.updateFin(codTipoTitulo, numeroUnicoFinanceiro, codigoBanco, codigoConta, valorDesconto, valorJuros, valorMulta, valorOutrosAcrescimos, codemp);
                                    } else {
                                        this.updateFinComVlrBaixa(codTipoTitulo, numeroUnicoFinanceiro, codigoBanco, codigoConta, valorBaixa, valorDesconto, valorJuros, valorMulta, valorOutrosAcrescimos, codemp);
                                    }

                                    LogCatcher.logInfo("Valores de ajuste: Desconto=" + valorDesconto + ", Juros=" + valorJuros + ", Multa=" + valorMulta);
                                    
                                    numeroUnicoMovimentacaoBancaria = this.insertMovBancaria(codigoConta, valorBaixa, numeroUnicoFinanceiro, dataBaixaFormatada, codemp);
                                    movimentacaoBancariaEfetuada = true;
                                    LogCatcher.logInfo("Movimentação bancária inserida com NUBCO: " + numeroUnicoMovimentacaoBancaria);
                                    
                                    this.updateBaixa(numeroUnicoFinanceiro, numeroUnicoMovimentacaoBancaria, valorBaixa, dataBaixaFormatada, baixaId, codemp);
                                    
                                } else {
                                    LogCatcher.logInfo("Financeiro " + numeroUnicoFinanceiro + " já baixado. Pulando.");
                                }
                            } else {
                                this.selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Sem \"de para\" de Tipo de Titulo Configurado Para o Metodo de Pagamento: " + formaPagamentoId + "' , SYSDATE, 'Aviso', " + codemp + ", '' FROM DUAL");
                            }
                        } else { // Processamento de estorno
                            if ("S".equalsIgnoreCase(mapaInfFinanceiroBaixado.get(numeroUnicoFinanceiro))) {
                                numeroUnicoMovimentacaoBancaria = mapaInfFinanceiroBanco.get(numeroUnicoFinanceiro);
                                this.updateFinExtorno(numeroUnicoFinanceiro, codemp);
                                this.deleteTgfMbc(numeroUnicoMovimentacaoBancaria, codemp);
                                this.deleteTgfFin(numeroUnicoFinanceiro, codemp);
                                LogCatcher.logInfo("Estorno do financeiro " + numeroUnicoFinanceiro + " e movimentação bancária " + numeroUnicoMovimentacaoBancaria + " realizado com sucesso.");
                            } else {
                                LogCatcher.logInfo("Financeiro " + numeroUnicoFinanceiro + " não está baixado para estorno. Pulando.");
                            }
                        }
                    } else {
                        this.selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Não foi possível encontrar financeiro com ID externo: " + tituloId + "' , SYSDATE, 'Erro', " + codemp + ", '' FROM DUAL");
                        LogCatcher.logInfo("Não foi possível encontrar financeiro com ID externo " + tituloId);
                    }
                }
            } else {
                this.selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Resposta da API inválida ou vazia: código: " + apiResponse[0] + "\nResposta: " + apiResponse[1] + "' , SYSDATE, 'Erro', " + codemp + ", '' FROM DUAL");
            }
        } catch (Exception e) {
            e.printStackTrace();
            if (movimentacaoBancariaEfetuada) {
                this.updateFinExtorno(numeroUnicoFinanceiro, codemp);
                this.deleteTgfMbc(numeroUnicoMovimentacaoBancaria, codemp);
                LogCatcher.logInfo("Rollback da movimentação bancária e financeiro devido a erro.");
            }
            this.selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Mensagem de erro nas Baixas: " + e.getMessage().replace("\'", "\'\'") + "' , SYSDATE, 'Erro', " + codemp + ", '' FROM DUAL");
        } finally {
            if (hnd != null) {
                JapeSession.close(hnd);
            }
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
            LogCatcher.logInfo("Passou do update do deleteTgfMbc");
        } catch (SQLException e) {
            e.printStackTrace();
            this.selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro Ao Deletar Mov. Bancaria: " + e.getMessage() + "' , SYSDATE, 'Erro', " + codemp + ", '' FROM DUAL");
            LogCatcher.logInfo("SELECT <#NUMUNICO#>, 'Erro Ao Deletar Mov. Bancaria: " + e.getMessage() + "' , SYSDATE, 'Erro', " + codemp + ", '' FROM DUAL");

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
            String sqlNota = "UPDATE TGFFIN SET VLRBAIXA = 0, DHBAIXA = NULL, NUBCO = NULL, CODTIPOPERBAIXA = 0, DHTIPOPERBAIXA = (SELECT MAX(DHALTER) FROM TGFTOP WHERE CODTIPOPER = 0), CODUSUBAIXA = NULL  WHERE NUFIN = " + nufin;
            pstmt = jdbc.getPreparedStatement(sqlNota);
            pstmt.executeUpdate();
            System.out.println("Passou do update");
            LogCatcher.logInfo("Passou do update do updateFinExtorno");
        } catch (SQLException e) {
            e.printStackTrace();
            this.selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro Ao Extornar Titulo: " + e.getMessage() + "' , SYSDATE, 'Erro', " + codemp + ", '' FROM DUAL");
            LogCatcher.logInfo("SELECT <#NUMUNICO#>, 'Erro Ao Extornar Titulo: " + e.getMessage() + "' , SYSDATE, 'Erro', " + codemp + ", '' FROM DUAL");
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

	public synchronized String[] apiGet(String ur, String token) throws Exception {
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

        return count <= 0;
    }

    public boolean validarDataMinMovBancaria(BigDecimal codConta, String dataBaixa) throws Exception {
        EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
        JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        int count = 0;

        try {
            jdbc.openSession();
            String sqlNota = "SELECT COUNT(0) AS COUNT FROM (SELECT MIN(REFERENCIA) DTREF \t    FROM TGFSBC \t   WHERE CODCTABCOINT = " + codConta + ") " + "\tWHERE DTREF > TO_DATE('" + dataBaixa + "', 'DD/MM/YYYY')";
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

        return count <= 0;
    }

    public BigDecimal getTipTit(String idExterno, BigDecimal codEmp) throws Exception {
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

    public BigDecimal getCodBanco(String idExterno, BigDecimal codEmp) throws Exception {
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

    public BigDecimal getCodConta(String idExterno, BigDecimal codEmp) throws Exception {
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
            String sqlNota = "select VLRDESDOB FROM TGFFIN WHERE NUFIN = " + nufin;
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

    public void updateFin(BigDecimal codtiptit, BigDecimal nufin, BigDecimal codBanco, BigDecimal codConta, BigDecimal vlrDesconto, BigDecimal vlrJuros, BigDecimal vlrMulta, BigDecimal vlrOutrosAcrescimos, BigDecimal codemp) throws Exception {
        EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
        JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
        PreparedStatement pstmt = null;

        try {
            jdbc.openSession();
            String sqlNota = "UPDATE TGFFIN SET CODTIPTIT = ?, CODBCO = ?, CODCTABCOINT = ?, ";
            sqlNota = sqlNota + "AD_VLRDESCINT = " + vlrDesconto + ", ";
            sqlNota = sqlNota + "VLRINSS = 0, VLRIRF = 0, VLRISS = 0, ";
            sqlNota = sqlNota + "AD_VLRMULTAINT = " + vlrMulta + ", ";
            sqlNota = sqlNota + "AD_VLRJUROSINT = " + vlrJuros + ", AD_OUTACRESCIMOS = " + vlrOutrosAcrescimos;
            sqlNota = sqlNota + ", TIPJURO = null, ";
            sqlNota = sqlNota + "TIPMULTA = null";
            sqlNota = sqlNota + " WHERE nufin = ?";
            pstmt = jdbc.getPreparedStatement(sqlNota);
            pstmt.setBigDecimal(1, codtiptit);
            pstmt.setBigDecimal(2, codBanco);
            pstmt.setBigDecimal(3, codConta);
            pstmt.setBigDecimal(4, nufin);
            pstmt.executeUpdate();
            System.out.println("Passou do update");
            LogCatcher.logInfo("Passou do update do updateFin");
        } catch (SQLException e) {
            e.printStackTrace();
            this.selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro Ao Atualizar Financeiro Para baixa: " + e.getMessage() + "' , SYSDATE, 'Erro', " + codemp + ", '' FROM DUAL");
            LogCatcher.logInfo("SELECT <#NUMUNICO#>, 'Erro Ao Atualizar Financeiro Para baixa: " + e.getMessage() + "' , SYSDATE, 'Erro', " + codemp + ", '' FROM DUAL");
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
            LogCatcher.logInfo("Passou do update no deleteTgfFin");
            
        } catch (SQLException e) {
            e.printStackTrace();
            this.selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro Ao Excluir Titulo: " + e.getMessage() + "' , SYSDATE, 'Erro', " + codemp + ", '' FROM DUAL");
            LogCatcher.logInfo("SELECT <#NUMUNICO#>, 'Erro Ao Excluir Titulo: " + e.getMessage() + "' , SYSDATE, 'Erro', " + codemp + ", '' FROM DUAL");
        } finally {
            if (pstmt != null) {
                pstmt.close();
            }

            jdbc.closeSession();
        }

    }


    public void updateFinComVlrBaixa(BigDecimal codtiptit, BigDecimal nufin, BigDecimal codBanco, BigDecimal codConta, BigDecimal vlrBaixa, BigDecimal vlrDesconto, BigDecimal vlrJuros, BigDecimal vlrMulta, BigDecimal outrosAcrescimos, BigDecimal codemp) throws Exception {
        EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
        JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
        PreparedStatement pstmt = null;

        try {
            jdbc.openSession();
            String sqlNota = "UPDATE TGFFIN SET CODTIPTIT = ?, CODBCO = ?, CODCTABCOINT = ?, AD_VLRDESCINT = " + vlrDesconto + ", VLRINSS = 0, VLRIRF = 0, VLRISS = 0, AD_VLRJUROSINT = " + vlrJuros + ", AD_VLRMULTAINT = " + vlrMulta + ", TIPJURO = null, AD_VLRORIG = VLRDESDOB, VLRDESDOB = " + vlrBaixa + "," + "TIPMULTA = null, AD_OUTACRESCIMOS = " + outrosAcrescimos + " WHERE nufin = ?";
            pstmt = jdbc.getPreparedStatement(sqlNota);
            pstmt.setBigDecimal(1, codtiptit);
            pstmt.setBigDecimal(2, codBanco);
            pstmt.setBigDecimal(3, codConta);
            pstmt.setBigDecimal(4, nufin);
            pstmt.executeUpdate();
            System.out.println("Passou do update");
            LogCatcher.logInfo("Passou do update");
        } catch (SQLException e) {
            e.printStackTrace();
            this.selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro Ao Atualizar Titulo Para Baixa: " + e.getMessage() + "' , SYSDATE, 'Erro', " + codemp + ", '' FROM DUAL");
            LogCatcher.logInfo("SELECT <#NUMUNICO#>, 'Erro Ao Atualizar Titulo Para Baixa: " + e.getMessage() + "' , SYSDATE, 'Erro', " + codemp + ", '' FROM DUAL");
        } finally {
            if (pstmt != null) {
                pstmt.close();
            }

            jdbc.closeSession();
        }

    }

    public void updateBaixa(BigDecimal nufin, BigDecimal nubco, BigDecimal vlrDesdob, String dataBaixaFormatada, String baixaId, BigDecimal codemp) throws Exception {
        EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
        JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
        PreparedStatement pstmt = null;
        BigDecimal USUARIO_PADRAO = BigDecimal.valueOf(0L);

        try {
            jdbc.openSession();
            String sqlNota = "UPDATE TGFFIN SET VLRBAIXA = ?, DHBAIXA = TO_DATE(?, 'DD/MM/YYYY'), NUBCO = ?, CODTIPOPERBAIXA = 1500, DHTIPOPERBAIXA = (SELECT MAX(DHALTER) FROM TGFTOP WHERE CODTIPOPER = 1500), CODUSUBAIXA = ?, AD_BAIXAID = ? WHERE NUFIN = ?";
            pstmt = jdbc.getPreparedStatement(sqlNota);
            pstmt.setBigDecimal(1, vlrDesdob);
            pstmt.setString(2, dataBaixaFormatada);
            pstmt.setBigDecimal(3, nubco);
            pstmt.setBigDecimal(4, USUARIO_PADRAO);
            pstmt.setString(5, baixaId);
            pstmt.setBigDecimal(6, nufin);
            pstmt.executeUpdate();
            LogCatcher.logInfo("Passou do update da baixa");
        } catch (SQLException e) {
            e.printStackTrace();
            this.selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro Ao Baixar Titulo: " + e.getMessage() + "' , SYSDATE, 'Erro', " + codemp + ", '' FROM DUAL");
            LogCatcher.logInfo("SELECT <#NUMUNICO#>, 'Erro Ao Baixar Titulo: " + e.getMessage() + "' , SYSDATE, 'Erro', " + codemp + "' FROM DUAL");
        } finally {
            if (pstmt != null) {
                pstmt.close();
            }

            jdbc.closeSession();
        }

    }

    public void insertLogIntegracao(String descricao, String status) throws Exception {
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
    
    public BigDecimal insertMovBancaria(BigDecimal contaBancaria, BigDecimal vlrDesdob, BigDecimal nufin, String dataBaixaFormatada, BigDecimal codemp) throws Exception {
        EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
        JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
        PreparedStatement pstmt = null;
        EnviromentUtils util = new EnviromentUtils();
        BigDecimal nubco = util.getMaxNumMbc();
        jdbc.openSession();

        try {
            String sqlUpdate = "INSERT INTO TGFMBC (NUBCO, CODLANC, DTLANC, CODTIPOPER, DHTIPOPER, DTCONTAB, HISTORICO, CODCTABCOINT, NUMDOC, VLRLANC, TALAO, PREDATA, CONCILIADO, DHCONCILIACAO, ORIGMOV, NUMTRANSF, RECDESP, DTALTER, DTINCLUSAO, CODUSU, VLRMOEDA, SALDO, CODCTABCOCONTRA, NUBCOCP, CODPDV ) VALUES (" + 
                nubco + ", " +
                "1, '" + dataBaixaFormatada + "', " +
                "1500, (SELECT MAX(DHALTER) FROM TGFTOP WHERE CODTIPOPER = 1500), " +
                "NULL, (SELECT HISTORICO FROM TGFFIN WHERE NUFIN = " + nufin + "), " +
                contaBancaria + ", 0, " + vlrDesdob + ", " +
                "NULL, '" + dataBaixaFormatada + "', " +
                "'N', NULL, 'F', NULL, -1, " +
                "SYSDATE, SYSDATE, 428, 0, " + vlrDesdob + ", " +
                "NULL, NULL, NULL)";
            pstmt = jdbc.getPreparedStatement(sqlUpdate);
            pstmt.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
            this.selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro Ao Inserir Mov. Bancaria: " + e.getMessage().replace("'", "\"") + "' , SYSDATE, 'Erro', " + codemp + ", '' FROM DUAL");
            LogCatcher.logInfo("SELECT <#NUMUNICO#>, 'Erro Ao Inserir Mov. Bancaria: " + e.getMessage().replace("'", "\"") + "' , SYSDATE, 'Erro', " + codemp + ", '' FROM DUAL");
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

        return nubco;
    }

  

    public BigDecimal getMaxNumMbc() throws Exception {
        EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
        JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        BigDecimal id = BigDecimal.ZERO;

        try {
            this.updateNumMbc();
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
        } finally {
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
        } finally {
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
        List<Object[]> listRet = new ArrayList();

        try {
            jdbc.openSession();
            String sql = "\tSELECT \tCODCTABCOINT, CODEMP, IDEXTERNO, CODBCO ";
            sql = sql + "\t\tFROM  \tad_infobankbaixa ";
            pstmt = jdbc.getPreparedStatement(sql);
            rs = pstmt.executeQuery();

            while(rs.next()) {
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
        List<Object[]> listRet = new ArrayList();

        try {
            jdbc.openSession();
            String sql = "\tSELECT \tCODEMP, NUFIN, AD_IDEXTERNO, (CASE WHEN DHBAIXA IS NOT NULL THEN 'S' ELSE 'N' END) BAIXADO, VLRDESDOB, NUBCO ";
            sql = sql + "\t\tFROM  \tTGFFIN ";
            sql = sql + "\t\tWHERE  \tRECDESP = -1 ";
            sql = sql + "\t\t    AND PROVISAO = 'N' ";
            pstmt = jdbc.getPreparedStatement(sql);
            rs = pstmt.executeQuery();

            while(rs.next()) {
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
        List<Object[]> listRet = new ArrayList();

        try {
            jdbc.openSession();
            String sql = "\tSELECT \tCODEMP, CODTIPTIT, IDEXTERNO, TAXACART ";
            sql = sql + "\t\tFROM  \tAD_TIPTITINTEGRACAO ";
            pstmt = jdbc.getPreparedStatement(sql);
            rs = pstmt.executeQuery();

            while(rs.next()) {
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

    public List<Object[]> retornarInformacoesMenorDataMovBancariaPorConta() throws Exception {
        EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
        JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        List<Object[]> listRet = new ArrayList();

        try {
            jdbc.openSession();
            String sql = "\tSELECT \tCODCTABCOINT, MIN(REFERENCIA) DTREF ";
            sql = sql + "\t\tFROM  \tTGFSBC ";
            sql = sql + "\t    GROUP BY CODCTABCOINT ";
            pstmt = jdbc.getPreparedStatement(sql);
            rs = pstmt.executeQuery();

            while(rs.next()) {
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
    
    


    /**
     * Método para inserir log na tabela AD_LOGINTEGRACAO.
     * Sobrecarga para compatibilidade com código existente.
     * 
     * @param listInsert String contendo a consulta de inserção
     * @throws Exception em caso de erro de banco de dados
     */
    public void insertLogList(String listInsert) throws Exception {
        // Chama o método principal passando codemp como null
        insertLogList(listInsert, null);
    }

    /**
     * Método para inserir log na tabela AD_LOGINTEGRACAO com codemp.
     * 
     * @param listInsert String contendo a consulta de inserção
     * @param codemp Código da empresa
     * @throws Exception em caso de erro de banco de dados
     */
    public void insertLogList(String listInsert, BigDecimal codemp) throws Exception {
        EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
        JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
        PreparedStatement pstmt = null;

        try {
            jdbc.openSession();
            String sqlUpdate = "INSERT INTO AD_LOGINTEGRACAO (NUMUNICO, DESCRICAO, DTHORA, STATUS, CODEMP, MATRICULA_IDFORN) " + listInsert;
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
}

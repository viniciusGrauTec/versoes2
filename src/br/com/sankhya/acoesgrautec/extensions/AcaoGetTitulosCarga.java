package br.com.sankhya.acoesgrautec.extensions;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.activiti.engine.impl.util.json.JSONArray;
import org.activiti.engine.impl.util.json.JSONException;
import org.cuckoo.core.ScheduledAction;
import org.cuckoo.core.ScheduledActionContext;

import br.com.sankhya.acoesgrautec.mapas.MapasDeDadosFinAluno;  //import dos mapas usados na classe

import br.com.sankhya.acoesgrautec.util.EnviromentUtils;
import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.extensions.actionbutton.Registro;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import br.com.sankhya.modelcore.util.SWRepositoryUtils;
import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.core.JsonParseException;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

public class AcaoGetTitulosCarga implements AcaoRotinaJava, ScheduledAction {
	
	private List<String> selectsParaInsertLog = new ArrayList<String>();
	private EnviromentUtils util = new EnviromentUtils();
	static {
		LogConfiguration.setPath(SWRepositoryUtils.getBaseFolder() + "/logAcao/logs");
	}
	
	// Estrutura para armazenar informações detalhadas sobre erros de títulos
		private static class ErroTitulo {
			private String idTitulo;
			private BigDecimal valor;
			private String dataVencimento;
			private String tipoErro;
			private String razaoErro;
			private BigDecimal codEmp;
			
			public ErroTitulo(String idTitulo, BigDecimal valor, String dataVencimento, 
					String tipoErro, String razaoErro, BigDecimal codEmp) {
				this.idTitulo = idTitulo;
				this.valor = valor;
				this.dataVencimento = dataVencimento;
				this.tipoErro = tipoErro;
				this.razaoErro = razaoErro;
				this.codEmp = codEmp;
			}
			
			public String formatarParaLog() {
				return String.format("Título: %s, Valor: %s, Vencimento: %s, Erro: %s - %s", 
						idTitulo, 
						(valor != null ? valor.toString() : "N/A"), 
						dataVencimento, 
						tipoErro, 
						razaoErro);
			}
			
			public String formatarParaSql() {
				return String.format("SELECT <#NUMUNICO#>, 'Título: %s, Valor: %s, Vencimento: %s, Erro: %s - %s Empresa:%s', SYSDATE, '%s', %s, '%s' FROM DUAL", 
						idTitulo.replace("'", "''"), 
						(valor != null ? valor.toString() : "N/A"), 
						dataVencimento.replace("'", "''"), 
						tipoErro.replace("'", "''"), 
						razaoErro.replace("'", "''"),
						codEmp,
						tipoErro.replace("'", "''"),
						codEmp,
						idTitulo.replace("'", "''"));
			}
		}
		
		// Estrutura para armazenar contadores por empresa
		private static class ContadorEmpresa {
			private int titulosProcessados = 0;
			private int titulosComErro = 0;
			private List<ErroTitulo> erros = new ArrayList<>();
			
			public void incrementarProcessados() {
				titulosProcessados++;
			}
			
			public void incrementarErros() {
				titulosComErro++;
			}
			
			public void adicionarErro(ErroTitulo erro) {
				erros.add(erro);
				incrementarErros();
			}
			
			public String gerarResumo(BigDecimal codEmp) {
				StringBuilder resumo = new StringBuilder();
				resumo.append(String.format("Empresa: %s - Títulos processados: %d, Títulos não processados: %d", 
						codEmp, titulosProcessados, titulosComErro));
				
				if (titulosComErro > 0) {
					resumo.append(" | Detalhes dos erros: ");
					for (int i = 0; i < erros.size(); i++) {
						if (i > 0) {
							resumo.append(" | ");
						}
						resumo.append(erros.get(i).formatarParaLog());
					}
				}
				
				return resumo.toString();
			}
		}
		
		// Mapa para armazenar contadores por empresa
		private Map<BigDecimal, ContadorEmpresa> contadoresPorEmpresa = new HashMap<>();
		
		// Lista para armazenar erros de títulos
		private List<ErroTitulo> errosTitulos = new ArrayList<>();
	
		@Override
		public void doAction(ContextoAcao contexto) throws Exception {
			
			 LogCatcher.logInfo("\nIniciou o JOB financeiro dos alunos");
		    
		    Registro[] linhas = contexto.getLinhas();
		    
		    if (linhas.length < 1) {
		        contexto.mostraErro("É necessário selecionar pelo menos 1 linha.");
		        return;
		    }

		    int linhasParaProcessar = Math.min(linhas.length, 65);
		    LogCatcher.logInfo("Processando " + linhasParaProcessar + " registro(s)");
		    
		    String dataInicio = contexto.getParam("DTINICIO").toString().substring(0, 10);
		    String dataFim = contexto.getParam("DTFIM").toString().substring(0, 10);
		    String tituloAberto = (String) contexto.getParam("TITABERTO");
		    String matricula = (String) contexto.getParam("Matricula");
		    
		    String[] urls = new String[linhasParaProcessar];
		    String[] tokens = new String[linhasParaProcessar];
		    BigDecimal[] codEmps = new BigDecimal[linhasParaProcessar];
		    String[] tipoEmpresas = new String[linhasParaProcessar];
		    
		    // Inicializar contadores para cada empresa
		    for (int i = 0; i < linhasParaProcessar; i++) {
		        Registro registro = linhas[i];
		        
		        urls[i] = (String) registro.getCampo("URL");
		        tokens[i] = (String) registro.getCampo("TOKEN");
		        codEmps[i] = (BigDecimal) registro.getCampo("CODEMP");
		        String profissionalizante = Optional.ofNullable(registro.getCampo("PROFISSIONAL")).orElse("N").toString();
		        String tecnico = (String) Optional.ofNullable(registro.getCampo("TECNICO")).orElse("N");

		        tipoEmpresas[i] = determinaTipoEmpresa(profissionalizante, tecnico);
		        
		        // Inicializar contador para esta empresa
		        if (!contadoresPorEmpresa.containsKey(codEmps[i])) {
		            contadoresPorEmpresa.put(codEmps[i], new ContadorEmpresa());
		        }
		        
		        LogCatcher.logInfo("Preparando registro " + (i+1) + " de " + linhasParaProcessar);
		    }
		    
		    try {
		        MapasDeDadosFinAluno mapas = new MapasDeDadosFinAluno();
		        
		        for (int j = 0; j < linhasParaProcessar; j++) {
		            LogCatcher.logInfo("Processando registro " + (j+1) + " de " + linhasParaProcessar);
		            
		            processDateRangeByMonths(
		                tituloAberto.trim(), 
		                tipoEmpresas[j],
		                mapas,
		                urls[j],
		                tokens[j],
		                codEmps[j],
		                dataInicio,
		                dataFim,
		                matricula
		            );
		        }
		        
		        // Gerar resumo por empresa
		        StringBuilder resumoEmpresas = new StringBuilder("Resumo do processamento:\n");
		        for (int k = 0; k < linhasParaProcessar; k++) {
		            ContadorEmpresa contador = contadoresPorEmpresa.get(codEmps[k]);
		            if (contador != null) {
		                resumoEmpresas.append(contador.gerarResumo(codEmps[k])).append("\n");
		            }
		        }
		        
		        contexto.setMensagemRetorno("Período Processado para " + linhasParaProcessar + " registro(s)!\n" + resumoEmpresas.toString());
		        LogCatcher.logInfo("Período Processado para " + linhasParaProcessar + " registro(s)!\n" + resumoEmpresas.toString());
		        
		    } catch(Exception e) {
		        e.printStackTrace();
		        contexto.mostraErro(e.getMessage());
		        insertLogIntegracao("Erro ao processar período: " + e.getMessage(), "ERRO", matricula, null);
		        LogCatcher.logError("Erro ao processar período: " + e.getMessage());
		    } finally {
		        if(selectsParaInsertLog.size() > 0) {
		            StringBuilder msgError = new StringBuilder();
		            
		            System.out.println("Entrou na lista do finally: doAction " + selectsParaInsertLog.size());
		            LogCatcher.logInfo("Entrou na lista do finally: doAction " + selectsParaInsertLog.size());

		            int qtdInsert = selectsParaInsertLog.size();
		            
		            int k = 1;
		            for (String sqlInsert : selectsParaInsertLog) {
		                String sql = sqlInsert;
		                int nuFin = util.getMaxNumLog();
		                sql = sql.replace("<#NUMUNICO#>", String.valueOf(nuFin));
		                msgError.append(sql);

		                if (k < qtdInsert) {
		                    msgError.append(" \nUNION ALL ");
		                }
		                k++;
		            }

		            LogCatcher.logError("Consulta de log: \n" + msgError);

		            for (int m = 0; m < linhasParaProcessar; m++) {
		                insertLogList(msgError.toString(), codEmps[m]);
		            }
		        }
		    }
		}


		private String determinaTipoEmpresa(String profissionalizante, String tecnico) {
		    if(profissionalizante.equalsIgnoreCase("S")) {
		        return "P";
		    } else if(tecnico.equalsIgnoreCase("S")) {
		        return "T";
		    } else {
		        return "N";
		    }
		}
		
		/**
		 * Processa um intervalo de datas agrupando por meses
		 */
		private void processDateRangeByMonths(String tituloAberto, String tipoEmpresa, MapasDeDadosFinAluno mapas,
		        String url, String token, BigDecimal codEmp, String dataInicio, String dataFim, String matricula)
		        throws Exception {
		    LocalDate inicio = LocalDate.parse(dataInicio);
		    LocalDate fim = LocalDate.parse(dataFim);

		    LocalDate currentStart = inicio;
		    while (!currentStart.isAfter(fim)) {
		        LocalDate currentEnd = currentStart.withDayOfMonth(currentStart.lengthOfMonth());
		        if (currentEnd.isAfter(fim))
		            currentEnd = fim;

		        String startStr = currentStart.toString();
		        String endStr = currentEnd.toString();

		        try {
		            processDateRange(tituloAberto, tipoEmpresa, mapas.getMapaInfNaturezaEmp(), mapas.getMapaInfCenCusEmp(),
		                    mapas.getMapaInfFinanceiroBaixado(), mapas.getMapaInfFinanceiroBanco(),
		                    mapas.getMapaInfFinanceiro(), mapas.getMapaInfRecDesp(), mapas.getMapaInfConta(),
		                    mapas.getMapaInfBanco(), mapas.getMapaInfNatureza(), mapas.getMapaInfCenCus(),
		                    mapas.getMapaInfCenCusAluno(), mapas.getMapaInfAlunos(), url, token, codEmp, startStr, endStr,
		                    matricula);
		            
		            
		        } catch (Exception e) {
		            String mensagemErro = "Erro ao processar período " + startStr + " até " + endStr + 
		                               ". Tipo Empresa: " + tipoEmpresa + ". Erro: " + e.getMessage();
		            LogCatcher.logError("Erro ao processar período " + startStr + " até " + endStr + 
                            ". Tipo Empresa: " + tipoEmpresa + ". Erro: " + e.getMessage());
		            
		            // Registrar erro detalhado do título
		            ErroTitulo erro = new ErroTitulo(
		                "Período: " + startStr + " a " + endStr,  // ID do título (neste caso, usamos o período)
		                null,                                     // Valor (não disponível neste contexto)
		                "N/A",                                    // Data de vencimento (não disponível)
		                "ERRO_PROCESSAMENTO",                     // Tipo de erro
		                e.getMessage(),                           // Razão do erro
		                codEmp                                    // Código da empresa
		            );
		            
		            // Adicionar erro ao contador da empresa
		            ContadorEmpresa contador = contadoresPorEmpresa.get(codEmp);
		            if (contador != null) {
		                contador.adicionarErro(erro);
		            }
		            
		            // Adicionar à lista geral de erros
		            errosTitulos.add(erro);
		            
		            // Adicionar ao log de erros para SQL
		            selectsParaInsertLog.add(erro.formatarParaSql());
		            
		            insertLogIntegracao(mensagemErro, "ERRO", matricula, codEmp);

		            System.err.println(mensagemErro);
		            LogCatcher.logError(mensagemErro);
		            e.printStackTrace();
		        }

		        currentStart = currentEnd.plusDays(1);
		    }
		}
	
		@Override
		public void onTime(ScheduledActionContext arg0) {
		    EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		    resetarTotalTitulos();
		    JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		    BigDecimal codEmp = BigDecimal.ZERO;
		    
		    // Limpar contadores e erros antes de iniciar
		    contadoresPorEmpresa.clear();
		    errosTitulos.clear();

		    LogCatcher.logInfo("\nIniciou o JOB financeiro dos alunos");

		    try {
		        MapasDeDadosFinAluno mapas = new MapasDeDadosFinAluno();
		        
		        jdbc.openSession();
		        
		        processarEmpresas(jdbc, mapas);
		        
		        imprimirTotalTitulos();
		        
		        // Gerar resumo por empresa
		        StringBuilder resumoEmpresas = new StringBuilder("Resumo do processamento JOB:\n");
		        for (Map.Entry<BigDecimal, ContadorEmpresa> entry : contadoresPorEmpresa.entrySet()) {
		            resumoEmpresas.append(entry.getValue().gerarResumo(entry.getKey())).append("\n");
		        }
		        
		        // Registrar resumo no log
		        insertLogIntegracao(resumoEmpresas.toString(), "INFO", "", null);

		        System.out.println("Finalizou o financeiro dos alunos Empresas");
		        System.out.println(resumoEmpresas.toString());
		        LogCatcher.logInfo("\nFinalizou o financeiro dos alunos Empresas");
		        LogCatcher.logInfo(resumoEmpresas.toString());

		    } catch (Exception e) {
		        try {
		            insertLogIntegracao("Erro ao processar JOB financeiro dos alunos: " + e.getMessage(), "ERRO", "", codEmp);
		            LogCatcher.logError("Erro ao processar JOB financeiro dos alunos: " + e.getMessage());
		        } catch (Exception e1) {
		            e.printStackTrace();
		        }
		    } finally {
		        jdbc.closeSession();
		        
		        if(selectsParaInsertLog.size() > 0) {
		            StringBuilder msgError = new StringBuilder();
		            
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
		            
		            LogCatcher.logInfo("Consulta de log: onTime \n" + msgError);

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
		 * Processa todas as empresas com integração ativa
		 */
		private void processarEmpresas(JdbcWrapper jdbc, MapasDeDadosFinAluno mapas) throws Exception {
		    String query = "SELECT CODEMP, URL, TOKEN, "
		                + "CASE WHEN PROFISSIONAL='S' THEN 'P' WHEN TECNICO='S' THEN 'T' ELSE 'N' END AS TIPEMP "
		                + "FROM AD_LINKSINTEGRACAO WHERE INTEGRACAO = 'S'";
		    
		    PreparedStatement pstmt = null;
		    ResultSet rs = null;
		    
		    try {
		    	LogCatcher.logInfo("\nIniciando processamento de empresas com integração ativa pelo Job");
		        System.out.println("Iniciando processamento de empresas com integração ativa pelo Job");
		        
		        pstmt = jdbc.getPreparedStatement(query);
		        rs = pstmt.executeQuery();
		        
		        int empresasProcessadas = 0;
		        int empresasComErro = 0;
		        
		        while (rs.next()) {
		            BigDecimal codEmp = rs.getBigDecimal("CODEMP");
		            String url = rs.getString("URL");
		            String token = rs.getString("TOKEN");
		            String tipoEmpresa = rs.getString("TIPEMP");
		            
		            // Inicializar contador para esta empresa
		            if (!contadoresPorEmpresa.containsKey(codEmp)) {
		                contadoresPorEmpresa.put(codEmp, new ContadorEmpresa());
		            }
		            
		            try {
		            	LogCatcher.logInfo("Processando empresa: " + codEmp + " (Tipo: " + tipoEmpresa + ") pelo Job");
		                System.out.println("Processando empresa: " + codEmp + " (Tipo: " + tipoEmpresa + ") pelo Job");
		                
		                if (url == null || url.trim().isEmpty()) {
		                    throw new IllegalArgumentException("URL de integração não configurada");
		                }
		                
		                if (token == null || token.trim().isEmpty()) {
		                    throw new IllegalArgumentException("Token de integração não configurado");
		                }
		                
		                if (tipoEmpresa == null || !("P".equals(tipoEmpresa.trim()) || "T".equals(tipoEmpresa.trim()) || "N".equals(tipoEmpresa.trim()))) {
		                    throw new IllegalArgumentException("Tipo de empresa inválido: " + tipoEmpresa);
		                }
		                
		                iterarEndpoint(
		                    tipoEmpresa.trim(),
		                    mapas.getMapaInfNaturezaEmp(),
		                    mapas.getMapaInfCenCusEmp(),
		                    mapas.getMapaInfFinanceiroBaixado(),
		                    mapas.getMapaInfFinanceiroBanco(),
		                    mapas.getMapaInfFinanceiro(),
		                    mapas.getMapaInfRecDesp(),
		                    mapas.getMapaInfConta(),
		                    mapas.getMapaInfBanco(),
		                    mapas.getMapaInfNatureza(),
		                    mapas.getMapaInfCenCus(),
		                    mapas.getMapaInfCenCusAluno(),
		                    mapas.getMapaInfAlunos(),
		                    url,
		                    token,
		                    codEmp
		                );
		                
		                LogCatcher.logInfo("Empresa " + codEmp + " processada com sucesso pelo Job");
		                System.out.println("Empresa " + codEmp + " processada com sucesso pelo Job");
		                empresasProcessadas++;
		                
		            } catch (IllegalArgumentException configEx) {
		            	
		                String msgErro = "Erro de configuração para empresa " + codEmp + ": " + configEx.getMessage();
		                System.err.println(msgErro);
		                LogCatcher.logError(msgErro);
		                
		                // Registrar erro detalhado
		                ErroTitulo erro = new ErroTitulo(
		                    "Configuração",                // ID do título
		                    null,                          // Valor
		                    "N/A",                         // Data de vencimento
		                    "ERRO_CONFIG",                 // Tipo de erro
		                    configEx.getMessage(),         // Razão do erro
		                    codEmp                         // Código da empresa
		                );
		                
		                // Adicionar erro ao contador da empresa
		                ContadorEmpresa contador = contadoresPorEmpresa.get(codEmp);
		                if (contador != null) {
		                    contador.adicionarErro(erro);
		                }
		                
		                // Adicionar à lista geral de erros
		                errosTitulos.add(erro);
		                
		                // Adicionar ao log de erros para SQL
		                selectsParaInsertLog.add(erro.formatarParaSql());
		                
		                insertLogIntegracao(msgErro, "ERRO_CONFIG", "", codEmp);
		                LogCatcher.logError(msgErro);
		                empresasComErro++;
		                
		            } catch (SQLException sqlEx) {
		                String msgErro = "Erro SQL ao processar empresa " + codEmp + ": " 
		                               + sqlEx.getMessage() + " (Código: " + sqlEx.getErrorCode() + ")";
		                System.err.println(msgErro);
		                LogCatcher.logError(msgErro);
		                
		                // Registrar erro detalhado
		                ErroTitulo erro = new ErroTitulo(
		                    "SQL",                                    // ID do título
		                    null,                                     // Valor
		                    "N/A",                                    // Data de vencimento
		                    "ERRO_SQL",                               // Tipo de erro
		                    sqlEx.getMessage() + " (Código: " + sqlEx.getErrorCode() + ")", // Razão do erro
		                    codEmp                                    // Código da empresa
		                );
		                
		                // Adicionar erro ao contador da empresa
		                ContadorEmpresa contador = contadoresPorEmpresa.get(codEmp);
		                if (contador != null) {
		                    contador.adicionarErro(erro);
		                }
		                
		                // Adicionar à lista geral de erros
		                errosTitulos.add(erro);
		                
		                // Adicionar ao log de erros para SQL
		                selectsParaInsertLog.add(erro.formatarParaSql());
		                
		                insertLogIntegracao(msgErro, "ERRO_SQL", "", codEmp);
		                empresasComErro++;
		                LogCatcher.logError(msgErro);
		                
		            } catch (Exception ex) {
		                String msgErro = "Erro ao processar empresa " + codEmp + ": " + ex.getMessage()
		                               + " - Tipo: " + ex.getClass().getName();
		                System.err.println(msgErro);
		                LogCatcher.logError(msgErro);
		                StackTraceElement[] stackTrace = ex.getStackTrace();
		                if (stackTrace.length > 0) {
		                    msgErro += " [" + stackTrace[0].getClassName() + "." + stackTrace[0].getMethodName() 
		                             + " - linha: " + stackTrace[0].getLineNumber() + "]";
		                }
		                
		                // Registrar erro detalhado
		                ErroTitulo erro = new ErroTitulo(
		                    "Processamento",                          // ID do título
		                    null,                                     // Valor
		                    "N/A",                                    // Data de vencimento
		                    "ERRO",                                   // Tipo de erro
		                    ex.getMessage() + " - Tipo: " + ex.getClass().getName(), // Razão do erro
		                    codEmp                                    // Código da empresa
		                );
		                
		                // Adicionar erro ao contador da empresa
		                ContadorEmpresa contador = contadoresPorEmpresa.get(codEmp);
		                if (contador != null) {
		                    contador.adicionarErro(erro);
		                }
		                
		                // Adicionar à lista geral de erros
		                errosTitulos.add(erro);
		                
		                // Adicionar ao log de erros para SQL
		                selectsParaInsertLog.add(erro.formatarParaSql());
		                
		                insertLogIntegracao(msgErro, "ERRO", "", codEmp);

		                System.err.println("Stack trace do erro:");
		                ex.printStackTrace();
		                empresasComErro++;
		                LogCatcher.logInfo("Stack trace do erro:");
		                LogCatcher.logError(msgErro);
		            }
		        }
		        
		        System.out.println("Processamento finalizado. Empresas processadas pelo Job: " + empresasProcessadas 
		                        + " / Empresas com erro pelo Job: " + empresasComErro);
		        LogCatcher.logInfo("\nProcessamento finalizado. Empresas processadas pelo Job: " + empresasProcessadas 
                        + " / Empresas com erro pelo Job: " + empresasComErro);
		        
		    } catch (SQLException dbEx) {
		        String msgErro = "Erro ao consultar empresas para integração: " + dbEx.getMessage() 
		                      + " (Código: " + dbEx.getErrorCode() + ")";
		        System.err.println(msgErro);
		        LogCatcher.logError(msgErro);
		        insertLogIntegracao(msgErro, "ERRO_SQL", "", null);
		        throw dbEx;
		        
		    } catch (Exception ex) {
		        String msgErro = "Erro geral no processamento de empresas pelo Job: " + ex.getMessage();
		        System.err.println(msgErro);
		        LogCatcher.logError(msgErro);
		        insertLogIntegracao(msgErro, "ERRO_FATAL", "", null);
		        throw ex;
		        
		    } finally {
		        try {
		            if (rs != null && !rs.isClosed()) {
		                rs.close();
		            }
		        } catch (SQLException closeEx) {
		        	LogCatcher.logError("Erro ao fechar ResultSet pelo Job: " + closeEx.getMessage());
		            System.err.println("Erro ao fechar ResultSet pelo Job: " + closeEx.getMessage());
		        }
		        
		        try {
		            if (pstmt != null && !pstmt.isClosed()) {
		                pstmt.close();
		            }
		        } catch (SQLException closeEx) {
		        	LogCatcher.logError("Erro ao fechar PreparedStatement pelo Job: " + closeEx.getMessage());
		            System.err.println("Erro ao fechar PreparedStatement pelo Job: " + closeEx.getMessage());
		        }
		    }
		}
		
		// Método modificado para registrar erros de títulos
		public void registrarErroTitulo(String idTitulo, BigDecimal valor, String dataVencimento, 
		        String tipoErro, String razaoErro, BigDecimal codEmp) {
		    
		    // Criar objeto de erro
		    ErroTitulo erro = new ErroTitulo(idTitulo, valor, dataVencimento, tipoErro, razaoErro, codEmp);
		    
		    // Adicionar à lista geral de erros
		    errosTitulos.add(erro);
		    
		    // Adicionar ao contador da empresa
		    ContadorEmpresa contador = contadoresPorEmpresa.get(codEmp);
		    if (contador != null) {
		        contador.adicionarErro(erro);
		    }
		    
		    // Adicionar ao log de erros para SQL
		    selectsParaInsertLog.add(erro.formatarParaSql());
		    
		    // Registrar no log de integração
		    try {
		        insertLogIntegracao(erro.formatarParaLog(), tipoErro, "", codEmp);
		    } catch (Exception e) {
		        System.err.println("Erro ao registrar erro de título no log: " + e.getMessage());
		        LogCatcher.logError("Erro ao registrar erro de título no log: " + e.getMessage());
		        e.printStackTrace();
		    }
		}
		
		

	
	
	public void processDateRange(
	        String tituloAberto,
	        String tipoEmpresa,
	        Map<String, BigDecimal> mapaInfNaturezaEmp,
	        Map<String, BigDecimal> mapaInfCenCusEmp,
	        Map<BigDecimal, String> mapaInfFinanceiroBaixado,
	        Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco,
	        Map<String, BigDecimal> mapaInfFinanceiro,
	        Map<String, String> mapaInfRecDesp,
	        Map<String, BigDecimal> mapaInfConta,
	        Map<String, BigDecimal> mapaInfBanco,
	        Map<String, BigDecimal> mapaInfNatureza,
	        Map<String, BigDecimal> mapaInfCenCus,
	        Map<String, BigDecimal> mapaInfCenCusAluno,
	        Map<String, BigDecimal> mapaInfAlunos,
	        String url,
	        String token,
	        BigDecimal codEmp,
	        String dataInicio,
	        String dataFim,
	        String matricula) throws Exception {

	    try {
	        String dataInicialCompleta = dataInicio + " 00:00:00";
	        String dataFinalCompleta = dataFim + " 23:59:59";
	        
	        String dataInicialParam = URLEncoder.encode(dataInicialCompleta, "UTF-8");
	        String dataFinalParam = URLEncoder.encode(dataFinalCompleta, "UTF-8");

	        JSONArray todosRegistros = new JSONArray();
	        
	        List<String> situacoes = new ArrayList<>();
	        
	        if (tituloAberto != null && tituloAberto.equalsIgnoreCase("S")) {
	            // Se tituloAberto for "S", processar situações A e B
	            situacoes.add("A");
	            situacoes.add("B");
	        } else {

	        }

	        for (int idxSituacao = 0; idxSituacao < Math.max(1, situacoes.size()); idxSituacao++) {
	            int pagina = 1;
	            boolean temMaisRegistros = true;
	            
	            while (temMaisRegistros) {
	                StringBuilder urlBuilder = new StringBuilder(url.trim())
	                        .append("/financeiro/titulos?")
	                        .append("pagina=").append(pagina)
	                        .append("&quantidade=100")   
	                        .append("&dataInicial=").append(dataInicialParam)
	                        .append("&dataFinal=").append(dataFinalParam);

	                if (!situacoes.isEmpty()) {
	                    urlBuilder.append("&situacao=").append(situacoes.get(idxSituacao));
	                }

	                if (matricula != null && !matricula.trim().isEmpty()) {
	                    String matriculaEncoded = URLEncoder.encode(matricula.trim(), "UTF-8");
	                    urlBuilder.append("&matricula=").append(matriculaEncoded);
	                }
	                
	                String urlCompleta = urlBuilder.toString();
	                System.out.println("URL para financeiro (página " + pagina + 
	                                   (situacoes.isEmpty() ? "" : ", situação " + situacoes.get(idxSituacao)) + 
	                                   "): " + urlCompleta);
	                
	                LogCatcher.logInfo("URL para financeiro (página " + pagina + 
                            (situacoes.isEmpty() ? "" : ", situação " + situacoes.get(idxSituacao)) + 
                            "): " + urlCompleta);

	                try {

	                    String[] response = apiGet(urlCompleta, token);
	                    int status = Integer.parseInt(response[0]);

	                    if (status == 200) {
	                        JSONArray paginaAtual = new JSONArray(response[1]);
	                        
	                        for (int i = 0; i < paginaAtual.length(); i++) {
	                            todosRegistros.put(paginaAtual.getJSONObject(i));
	                        }
	                        
	                        if (paginaAtual.length() < 100) {
	                            temMaisRegistros = false;
	                        } else {
	                            pagina++;
	                        }
	                        
	                        LogCatcher.logInfo("Página " + pagina + 
                                    (situacoes.isEmpty() ? "" : ", situação " + situacoes.get(idxSituacao)) + 
                                    ": " + paginaAtual.length() + 
                                    " registros. Total acumulado: " + todosRegistros.length());
	                        System.out.println("Página " + pagina + 
	                                          (situacoes.isEmpty() ? "" : ", situação " + situacoes.get(idxSituacao)) + 
	                                          ": " + paginaAtual.length() + 
	                                          " registros. Total acumulado: " + todosRegistros.length());
	                    } else {
	                        String mensagemErro = String.format(
	                            "Erro na requisição de financeiro. Status: %d. Resposta: %s. URL: %s",
	                            status, response[1], urlCompleta
	                        );
	                        
	                        LogCatcher.logError(
		                            "Erro na requisição de financeiro. Status: %d. Resposta: %s. URL: %s"
		                        );
	                        
	                        insertLogIntegracao(mensagemErro, "ERRO", matricula, codEmp);
	                        
	                        throw new Exception(mensagemErro);
	                    }
	                } catch (Exception e) {
	                    String mensagemErro = "Erro ao processar requisição para URL: " + urlCompleta + 
	                                        ". Erro: " + e.getMessage();
	                    
	                    LogCatcher.logError(mensagemErro);
	                    insertLogIntegracao(mensagemErro, "ERRO", matricula, codEmp);
	                    
	                    throw e; // Relançar exceção para tratamento externo
	                }
	            }
	        }
	        
	        String[] respostaCombinada = new String[] {
	            "200",
	            todosRegistros.toString()
	        };
	        
	        
	        LogCatcher.logInfo("Total de registros acumulados: " + todosRegistros.length());
	        System.out.println("Total de registros acumulados: " + todosRegistros.length());
	        
	        try {
	            cadastrarFinanceiro(
	                    tipoEmpresa,
	                    mapaInfNaturezaEmp,
	                    mapaInfCenCusEmp,
	                    mapaInfFinanceiroBaixado,
	                    mapaInfFinanceiroBanco,
	                    mapaInfFinanceiro,
	                    mapaInfRecDesp,
	                    mapaInfConta,
	                    mapaInfBanco,
	                    mapaInfNatureza,
	                    mapaInfCenCus,
	                    mapaInfCenCusAluno,
	                    mapaInfAlunos,
	                    respostaCombinada,
	                    url,
	                    token,
	                    codEmp
	            );
	        } catch (Exception e) {
	        	
	            String mensagemErro = "Erro ao cadastrar financeiro para período " + dataInicio + " até " + dataFim + 
	                               ". Tipo Empresa: " + tipoEmpresa + 
	                               ". Total registros: " + todosRegistros.length() + 
	                               ". Erro: " + e.getMessage();

	            insertLogIntegracao(mensagemErro, "ERRO", matricula, codEmp);
	            
	            LogCatcher.logError(mensagemErro);
	            
	            throw e;
	        }   

	    } catch (Exception e) {
	        String mensagemErro = "Erro ao processar requisição financeira para período " +
	                dataInicio + " até " + dataFim + ". Tipo Empresa: " + tipoEmpresa + 
	                ". Erro: " + e.getMessage();
	        
	        System.err.println(mensagemErro);
	        LogCatcher.logError(mensagemErro);
	        
	        insertLogIntegracao(mensagemErro, "ERRO", matricula, codEmp);
	        
	        throw e;
	    }
	}

    private static AtomicInteger titulosTotais = new AtomicInteger(0);
    

 // Adicione estes contadores como variáveis de classe
    private static AtomicInteger titulosProcessados = new AtomicInteger(0);
    private static AtomicInteger titulosCancelados = new AtomicInteger(0);

    public static void imprimirTotalTitulos() {
        int totalProcessados = titulosProcessados.get();
        int totalCancelados = titulosCancelados.get();
        int totalGeral = totalProcessados + totalCancelados;

        LogCatcher.logInfo("=== RESUMO DE TÍTULOS DO DIA ===");
        LogCatcher.logInfo("Total de títulos recebidos: " + totalGeral);
        LogCatcher.logInfo("Títulos processados (ativos): " + totalProcessados);
        LogCatcher.logInfo("Títulos cancelados/estornados: " + totalCancelados);
        LogCatcher.logInfo("\n===============================");

        try {
            new AcaoGetTitulosCarga().insertLogIntegracao(
                "Total de títulos - Recebidos: " + totalGeral 
                + " | Processados: " + totalProcessados 
                + " | Cancelados: " + totalCancelados,
                "INFO",
                "", 
                null
            );
        } catch (Exception e) {
            LogCatcher.logError("Erro ao registrar log: " + e.getMessage());
        }
    }


    public static void resetarTotalTitulos() {
        titulosTotais.set(0);
        LogCatcher.logInfo("Contador de títulos totais foi resetado.");
    }

	public void iterarEndpoint(String tipoEmpresa, Map<String, BigDecimal> mapaInfNaturezaEmp,
			Map<String, BigDecimal> mapaInfCenCusEmp, Map<BigDecimal, String> mapaInfFinanceiroBaixado,
			Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco, Map<String, BigDecimal> mapaInfFinanceiro,
			Map<String, String> mapaInfRecDesp, Map<String, BigDecimal> mapaInfConta,
			Map<String, BigDecimal> mapaInfBanco, Map<String, BigDecimal> mapaInfNatureza,
			Map<String, BigDecimal> mapaInfCenCus, Map<String, BigDecimal> mapaInfCenCusAluno,
			Map<String, BigDecimal> mapaInfAlunos, String url, String token, BigDecimal codEmp) throws Exception {

		LogCatcher.logInfo("=== iterarEndpoint do JOB iniciado ===");
		LogCatcher.logInfo("tipoEmpresa: " + tipoEmpresa);
		LogCatcher.logInfo("codEmp: " + codEmp);
		LogCatcher.logInfo("\nURL base: " + url);

		LocalDate dataAtual = LocalDate.now();
		LocalDate dataFim = LocalDate.now();
		String dataInicialParam = null;
		String dataFinalParam = null;
		String endpointCompleto = null;

		try {
			dataInicialParam = dataAtual.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
			dataFinalParam = dataFim.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));

			LogCatcher.logInfo("Período processado: " + dataInicialParam + " a " + dataFinalParam);

			endpointCompleto = url + "/financeiro/titulos" + "?quantidade=0" + "&dataInicial="
					+ URLEncoder.encode(dataInicialParam + " 00:00:00", "UTF-8") + "&dataFinal="
					+ URLEncoder.encode(dataFinalParam + " 23:59:59", "UTF-8");

			LogCatcher.logInfo("Endpoint chamado: " + endpointCompleto);

			String[] response = apiGet(endpointCompleto, token);

			if (response == null) {
				String msgErro = "Resposta nula da API. Verifique a conexão ou disponibilidade do servidor.";
				LogCatcher.logError(msgErro);
				insertLogIntegracao(msgErro, "ERRO_API", "", codEmp);
				return;
			}

			if (response.length < 2) {
				String msgErro = "Resposta da API em formato inválido. Esperado array com status e corpo.";
				LogCatcher.logError(msgErro);
				insertLogIntegracao(msgErro, "ERRO_API", "", codEmp);
				return;
			}

			int status;
			try {
				status = Integer.parseInt(response[0]);
			} catch (NumberFormatException nfe) {
				String msgErro = "Código de status inválido na resposta da API: " + response[0];
				LogCatcher.logError(msgErro);
				insertLogIntegracao(msgErro, "ERRO_API", "", codEmp);
				return;
			}

			if (status == 200) {
				LogCatcher.logInfo("Dados recebidos com sucesso!");

				try {
					JSONArray titulos = new JSONArray(response[1]);
					int qtdTitulosEmpresa = titulos.length();
					LogCatcher.logInfo(
							"Empresa " + tipoEmpresa + " (código " + codEmp + "): " + qtdTitulosEmpresa + " títulos");
					titulosTotais.addAndGet(qtdTitulosEmpresa);
				} catch (JSONException jsonEx) {
					LogCatcher.logError("Erro ao parsear JSON para contagem de títulos: " + jsonEx.getMessage());
				}

				try {
					cadastrarFinanceiro(tipoEmpresa, mapaInfNaturezaEmp, mapaInfCenCusEmp, mapaInfFinanceiroBaixado,
							mapaInfFinanceiroBanco, mapaInfFinanceiro, mapaInfRecDesp, mapaInfConta, mapaInfBanco,
							mapaInfNatureza, mapaInfCenCus, mapaInfCenCusAluno, mapaInfAlunos, response, url, token,
							codEmp);
				} catch (Exception cadEx) {
					String msgErro = "Erro ao cadastrar financeiro para empresa " + codEmp + ": " + cadEx.getMessage();
					LogCatcher.logError(msgErro);
					insertLogIntegracao(msgErro, "ERRO_CADASTRO", "", codEmp);
					throw cadEx;
				}

			} else if (status >= 400 && status < 500) {
				String msgErro = "Erro de cliente na API (Status " + status + "): " + response[1];
				if (status == 401 || status == 403) {
					msgErro = "Erro de autenticação/autorização na API (Status " + status + "): Verifique o token";
				} else if (status == 404) {
					msgErro = "Recurso não encontrado na API (Status 404): Verifique o endpoint";
				}
				LogCatcher.logError(msgErro);
				insertLogIntegracao(msgErro, "ERRO_API_CLIENTE", "", codEmp);
				LogCatcher.logInfo("Empresa " + tipoEmpresa + " (código " + codEmp + "): 0 títulos (erro de cliente)");

			} else if (status >= 500) {
				String msgErro = "Erro de servidor na API (Status " + status + "): " + response[1];
				LogCatcher.logError(msgErro);
				insertLogIntegracao(msgErro, "ERRO_API_SERVIDOR", "", codEmp);
				LogCatcher.logInfo("Empresa " + tipoEmpresa + " (código " + codEmp + "): 0 títulos (erro de servidor)");

			} else {
				String msgErro = "Status não reconhecido na API (Status " + status + "): " + response[1];
				LogCatcher.logError(msgErro);
				insertLogIntegracao(msgErro, "ERRO_API", "", codEmp);
				LogCatcher
						.logInfo("Empresa " + tipoEmpresa + " (código " + codEmp + "): 0 títulos (erro desconhecido)");
			}

		} catch (UnsupportedEncodingException encEx) {
			String msgErro = "Erro de codificação ao criar URL para empresa " + codEmp + ": " + encEx.getMessage();
			LogCatcher.logError(msgErro);
			insertLogIntegracao(msgErro, "ERRO_ENCOD", "", codEmp);
			LogCatcher.logInfo("Empresa " + tipoEmpresa + " (código " + codEmp + "): 0 títulos (erro de codificação)");
			throw encEx;

		} catch (DateTimeException dtEx) {
			String msgErro = "Erro ao manipular datas para empresa " + codEmp + ": " + dtEx.getMessage();
			LogCatcher.logError(msgErro);
			insertLogIntegracao(msgErro, "ERRO_DATA", "", codEmp);
			LogCatcher.logInfo("Empresa " + tipoEmpresa + " (código " + codEmp + "): 0 títulos (erro de data)");
			throw dtEx;

		} catch (IOException ioEx) {
			String msgErro = "Erro de I/O ao comunicar com API para empresa " + codEmp + ": " + ioEx.getMessage();
			if (endpointCompleto != null) {
				msgErro += " (Endpoint: " + endpointCompleto + ")";
			}
			LogCatcher.logError(msgErro);
			insertLogIntegracao(msgErro, "ERRO_IO", "", codEmp);
			LogCatcher.logInfo("Empresa " + tipoEmpresa + " (código " + codEmp + "): 0 títulos (erro de I/O)");
			throw ioEx;

		} catch (Exception e) {
			String msgErro = "Falha ao processar endpoint para empresa " + codEmp + ": " + e.getMessage() + " - Tipo: "
					+ e.getClass().getName();
			StackTraceElement[] stackTrace = e.getStackTrace();
			if (stackTrace.length > 0) {
				msgErro += " [" + stackTrace[0].getClassName() + "." + stackTrace[0].getMethodName() + " - linha: "
						+ stackTrace[0].getLineNumber() + "]";
			}
			LogCatcher.logError(msgErro);
			insertLogIntegracao(msgErro, "ERRO", "", codEmp);
			LogCatcher.logInfo("Empresa " + tipoEmpresa + " (código " + codEmp + "): 0 títulos (erro genérico)");
			throw e;

		} finally {
			LogCatcher.logInfo("\n=== iterarEndpoint do JOB finalizado ===");
		}
	}

	
	/**
	 *Inicio do bloco cadastrarFinanceiro
	 */
	public void cadastrarFinanceiro(String tipoEmpresa,
	        Map<String, BigDecimal> mapaInfNaturezaEmp,
	        Map<String, BigDecimal> mapaInfCenCusEmp,
	        Map<BigDecimal, String> mapaInfFinanceiroBaixado,
	        Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco,
	        Map<String, BigDecimal> mapaInfFinanceiro,
	        Map<String, String> mapaInfRecDesp,
	        Map<String, BigDecimal> mapaInfConta,
	        Map<String, BigDecimal> mapaInfBanco,
	        Map<String, BigDecimal> mapaInfNatureza,
	        Map<String, BigDecimal> mapaInfCenCus,
	        Map<String, BigDecimal> mapaInfCenCusAluno,
	        Map<String, BigDecimal> mapaInfAlunos,
	        String[] respostaCombinada,
	        String url, String token, BigDecimal codemp) throws Exception {

	    LogCatcher.logInfo("===== INÍCIO cadastrarFinanceiro =====");
	    LogCatcher.logInfo("tipoEmpresa: " + tipoEmpresa);
	    LogCatcher.logInfo("\ncodemp: " + codemp);

	    EnviromentUtils util = new EnviromentUtils();
	    List<String> selectsParaInsert = new ArrayList<>();
	    List<String> selectsParaInsertLog = new ArrayList<>();
	    int qtdInsert = 0;
	    String idAluno = "";

	    try {
	        String responseStatus = respostaCombinada[0];
	        String responseString = respostaCombinada[1];

	        LogCatcher.logInfo("Status da resposta: " + responseStatus);
	        LogCatcher.logInfo("Tamanho da resposta: " + (responseString != null ? responseString.length() : "null"));

	        if (!responseStatus.equalsIgnoreCase("200")) {
	            String erro = "Status da API diferente de 200: " + responseStatus;
	            LogCatcher.logError(erro);
	            adicionarLogErro(selectsParaInsertLog, "Api Retornou Status Diferente de 200: " + responseStatus, "Erro", codemp, "");
	            return;
	        }

	        JsonParser parser = new JsonParser();
	        JsonArray jsonArray = parser.parse(responseString).getAsJsonArray();
	        int total = jsonArray.size();
	        LogCatcher.logInfo("Total de registros no JSON: " + total);

	        processarRegistrosJson(jsonArray, tipoEmpresa, codemp, mapaInfFinanceiro, mapaInfFinanceiroBaixado,
	                mapaInfFinanceiroBanco, mapaInfAlunos, mapaInfCenCus, mapaInfCenCusAluno, mapaInfRecDesp,
	                mapaInfConta, mapaInfBanco, mapaInfNatureza, selectsParaInsert, selectsParaInsertLog);

	        qtdInsert = selectsParaInsert.size();
	        LogCatcher.logInfo("Total de registros processados: " + total);
	        LogCatcher.logInfo("Total de inserts a realizar: " + qtdInsert);

	        if (qtdInsert > 0) {
	            executarInsercoesFinanceiro(util, selectsParaInsert, qtdInsert, codemp);
	        } else {
	            LogCatcher.logInfo("Nenhum insert a realizar");
	        }

	    } catch (JsonParseException | JsonSyntaxException e) {
	        LogCatcher.logError(e);
	        processarErroFinanceiro(e, codemp, idAluno, selectsParaInsertLog);
	    } catch (SQLException e) {
	        LogCatcher.logError(e);
	        processarErroFinanceiro(e, codemp, idAluno, selectsParaInsertLog);
	    } catch (NullPointerException e) {
	        LogCatcher.logError(e);
	        processarErroFinanceiro(e, codemp, idAluno, selectsParaInsertLog);
	    } catch (Exception e) {
	        LogCatcher.logError(e);
	        processarErroFinanceiro(e, codemp, idAluno, selectsParaInsertLog);
	    } finally {
	        LogCatcher.logInfo("\n===== FIM cadastrarFinanceiro =====");
	    }
	}

	
	private void processarErroFinanceiro(Exception e, BigDecimal codemp, String idAluno,
			List<String> selectsParaInsertLog) {
		String tipoErro;
		String mensagemDetalhada;
		String nivelGravidade;

		if (e instanceof JsonParseException || e instanceof JsonSyntaxException) {
			tipoErro = "ERRO_PARSE_JSON";
			mensagemDetalhada = "Falha ao processar resposta JSON: " + e.getMessage();
			nivelGravidade = "ALTO";
		} else if (e instanceof NullPointerException) {
			tipoErro = "ERRO_DADOS_AUSENTES";
			mensagemDetalhada = "Dados obrigatórios ausentes: " + e.getMessage();
			nivelGravidade = "MEDIO";
		} else if (e instanceof SQLException) {
			SQLException sqlEx = (SQLException) e;
			tipoErro = "ERRO_BANCO_DADOS";
			mensagemDetalhada = String.format("Erro SQL: Código %d, Estado %s - %s", sqlEx.getErrorCode(),
					sqlEx.getSQLState(), sqlEx.getMessage());
			nivelGravidade = "ALTO";
		} else if (e instanceof IOException) {
			tipoErro = "ERRO_IO";
			mensagemDetalhada = "Erro de I/O: " + e.getMessage();
			nivelGravidade = "MEDIO";
		} else {
			tipoErro = "ERRO_DESCONHECIDO";
			mensagemDetalhada = e.getMessage();
			nivelGravidade = "ALTO";
		}

		String errorDetails = String.format(
				"ERRO em cadastrarFinanceiro - Tipo: %s, Nível: %s, CodEmp: %s, ID_EXTERNO: %s, Detalhes: %s", tipoErro,
				nivelGravidade, codemp, (idAluno != null && !idAluno.isEmpty()) ? idAluno : "N/A", mensagemDetalhada);

		LogCatcher.logError(errorDetails);
		LogCatcher.logError(e);

		try {
			insertLogIntegracao(String.format("%s: %s", tipoErro, mensagemDetalhada), "ERRO", idAluno, codemp);
		} catch (Exception logEx) {
			LogCatcher.logError("ERRO ADICIONAL ao tentar registrar log de integração: " + logEx.getMessage());
		}

		StringBuilder sql = new StringBuilder();
		sql.append("SELECT <#NUMUNICO#>, 'Erro ao integrar financeiro: ");
		sql.append(errorDetails.replace("'", "''"));
		sql.append("', SYSDATE, '");
		sql.append(nivelGravidade);
		sql.append("', ");
		sql.append(codemp);
		sql.append(", '");
		sql.append(idAluno != null ? idAluno.replace("'", "''") : "");
		sql.append("' FROM DUAL");

		selectsParaInsertLog.add(sql.toString());
	}


	/**
	 * Processa cada registro do JSON retornado pela API
	 */
	private void processarRegistrosJson(JsonArray jsonArray, String tipoEmpresa, BigDecimal codemp,
	        Map<String, BigDecimal> mapaInfFinanceiro, Map<BigDecimal, String> mapaInfFinanceiroBaixado,
	        Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco, Map<String, BigDecimal> mapaInfAlunos,
	        Map<String, BigDecimal> mapaInfCenCus, Map<String, BigDecimal> mapaInfCenCusAluno,
	        Map<String, String> mapaInfRecDesp, Map<String, BigDecimal> mapaInfConta,
	        Map<String, BigDecimal> mapaInfBanco, Map<String, BigDecimal> mapaInfNatureza,
	        List<String> selectsParaInsert, List<String> selectsParaInsertLog) throws Exception {

	    SimpleDateFormat formatoEntrada = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	    SimpleDateFormat formatoOriginal = new SimpleDateFormat("yyyy-MM-dd");
	    SimpleDateFormat formatoDesejado = new SimpleDateFormat("dd/MM/yyyy");

	    int count = 0;
	    int total = jsonArray.size();

	    for (JsonElement jsonElement : jsonArray) {
	        System.out.println("\n----- Processando registro " + (count + 1) + " de " + total + " -----");

	        JsonObject jsonObject = jsonElement.getAsJsonObject();

	        String idFin = jsonObject.get("titulo_id").getAsString();
	        BigDecimal vlrDesdob = new BigDecimal(jsonObject.get("titulo_valor").getAsDouble());
	        String dtVenc = jsonObject.get("titulo_vencimento").getAsString();
	        String idCurso = jsonObject.get("curso_id").isJsonNull() ? "" : jsonObject.get("curso_id").getAsString();
	        String taxaId = jsonObject.get("taxa_id").getAsString();
	        String dtPedidoOrig = jsonObject.get("data_atualizacao").getAsString();
	        String idAluno = jsonObject.get("aluno_id").getAsString();
	        String situacao_titulo = jsonObject.get("titulo_situacao").getAsString();

	        System.out.println("ID Financeiro: " + idFin);
	        System.out.println("Valor: " + vlrDesdob);
	        System.out.println("Data vencimento original: " + dtVenc);
	        System.out.println("ID Curso: " + (idCurso.isEmpty() ? "vazio" : idCurso));
	        System.out.println("Taxa ID: " + taxaId);
	        System.out.println("Data pedido original: " + dtPedidoOrig);
	        System.out.println("ID Aluno: " + idAluno);
	        System.out.println("Situação título: " + situacao_titulo);

	        LogCatcher.logInfo(String.format("[Registro %d/%d] ID_FIN: %s | Valor: %s | Vencimento: %s | Aluno: %s | Situação: %s",
	                count + 1, total, idFin, vlrDesdob.toPlainString(), dtVenc, idAluno, situacao_titulo));

	        Date dataPedido = formatoEntrada.parse(dtPedidoOrig);
	        String dtPedido = formatoDesejado.format(dataPedido);
	        Date data = formatoOriginal.parse(dtVenc);
	        String dataVencFormatada = formatoDesejado.format(data);

	        System.out.println("Data Pedido formatada: " + dtPedido);
	        System.out.println("Data vencimento formatada: " + dataVencFormatada);

	        BigDecimal codparc = Optional.ofNullable(mapaInfAlunos.get(idAluno + "###" + codemp))
	                .orElse(BigDecimal.ZERO);

	        String codParcLog = (codparc.compareTo(BigDecimal.ZERO) == 0) ? " (NÃO ENCONTRADO)" : "";
	        System.out.println("Código parceiro: " + codparc + codParcLog);
	        LogCatcher.logInfo("Código parceiro para aluno " + idAluno + ": " + codparc + codParcLog);

	        if (situacao_titulo.equalsIgnoreCase("X")) {
	            processarTituloCancelado(idFin, codemp, mapaInfFinanceiro, mapaInfFinanceiroBaixado, mapaInfFinanceiroBanco);
	        } else {
	            processarTituloAtivo(idFin, vlrDesdob, dtPedido, dataVencFormatada, idAluno, taxaId,
	                    tipoEmpresa, codemp, codparc, mapaInfFinanceiro, mapaInfCenCus, mapaInfCenCusAluno,
	                    mapaInfRecDesp, mapaInfConta, mapaInfBanco, mapaInfNatureza, selectsParaInsert, selectsParaInsertLog);
	        }

	        count++;
	        System.out.println("\n----- Registro " + count + " processado -----");
	    }
	}


	/**
	 * Processa um título que está cancelado (situacao_titulo = 'X')
	 */
	private void processarTituloCancelado(String idFin, BigDecimal codemp,
	        Map<String, BigDecimal> mapaInfFinanceiro, Map<BigDecimal, String> mapaInfFinanceiroBaixado,
	        Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco) throws Exception {

	    System.out.println("Título cancelado - processando exclusão");
	    LogCatcher.logInfo("Título cancelado detectado para ID_FIN: " + idFin + ", empresa: " + codemp);

	    String chaveFinanceiro = codemp + "###" + idFin;
	    System.out.println("Chave para busca financeiro: " + chaveFinanceiro);
	    LogCatcher.logInfo("Chave composta para busca de financeiro: " + chaveFinanceiro);

	    BigDecimal validarNufin = Optional.ofNullable(mapaInfFinanceiro.get(chaveFinanceiro))
	            .orElse(BigDecimal.ZERO);

	    String nufinMsg = "NUFIN para exclusão: " + validarNufin +
	            (validarNufin.compareTo(BigDecimal.ZERO) == 0 ? " (NÃO ENCONTRADO)" : "");
	    System.out.println(nufinMsg);
	    LogCatcher.logInfo(nufinMsg);

	    if (validarNufin.compareTo(BigDecimal.ZERO) != 0) {
	        System.out.println("Financeiro encontrado para exclusão");
	        LogCatcher.logInfo("Financeiro localizado para exclusão - NUFIN: " + validarNufin);

	        if ("S".equalsIgnoreCase(mapaInfFinanceiroBaixado.get(validarNufin))) {
	            System.out.println("Financeiro baixado - processando estorno");
	            LogCatcher.logInfo("Financeiro já baixado - iniciando estorno para NUFIN: " + validarNufin);

	            BigDecimal nubco = mapaInfFinanceiroBanco.get(validarNufin);
	            System.out.println("NUBCO: " + nubco);
	            LogCatcher.logInfo("Número bancário (NUBCO) associado: " + nubco);

	            System.out.println("Executando updateFinExtorno para NUFIN: " + validarNufin);
	            LogCatcher.logInfo("Executando updateFinExtorno(NUFIN: " + validarNufin + ", empresa: " + codemp + ")");
	            updateFinExtorno(validarNufin, codemp);

	            System.out.println("Executando deleteTgfMbc para NUBCO: " + nubco);
	            LogCatcher.logInfo("Executando deleteTgfMbc(NUBCO: " + nubco + ", empresa: " + codemp + ")");
	            deleteTgfMbc(nubco, codemp);

	            System.out.println("Executando deleteTgfFin para NUFIN: " + validarNufin);
	            LogCatcher.logInfo("Executando deleteTgfFin(NUFIN: " + validarNufin + ", empresa: " + codemp + ")");
	            deleteTgfFin(validarNufin, codemp);
	        } else {
	            System.out.println("Financeiro não baixado - excluindo diretamente");
	            LogCatcher.logInfo("Financeiro ainda não baixado - excluindo NUFIN diretamente: " + validarNufin);
	            deleteTgfFin(validarNufin, codemp);
	        }

	        titulosCancelados.incrementAndGet(); // Título cancelado processado
	        LogCatcher.logInfo("Título cancelado contabilizado para NUFIN: " + validarNufin);
	    } else {
	        System.out.println("Financeiro não encontrado para exclusão - ignorando");
	        LogCatcher.logInfo("Chave não encontrada no mapa financeiro - título será ignorado.");
	    }
	}

	
	
	// Método modificado para registrar erros de títulos durante o processamento
	public void processarTitulo(String idTitulo, BigDecimal valor, String dataVencimento, BigDecimal codEmp) {
	    try {
	        // Lógica de processamento do título
	        
	        // Incrementar contador de títulos processados com sucesso
	        ContadorEmpresa contador = contadoresPorEmpresa.get(codEmp);
	        if (contador != null) {
	            contador.incrementarProcessados();
	        }
	        
	    } catch (Exception e) {
	        // Registrar erro detalhado do título
	        registrarErroTitulo(idTitulo, valor, dataVencimento, "ERRO_PROCESSAMENTO", e.getMessage(), codEmp);
	        
	        System.err.println("Erro ao processar título " + idTitulo + ": " + e.getMessage());
	        LogCatcher.logError("Erro ao processar título " + idTitulo + ": " + e.getMessage());
	        e.printStackTrace();
	    }
	}
	


	/**
	 * Processa um título ativo (não cancelado)
	 */
	private void processarTituloAtivo(String idFin, BigDecimal vlrDesdob, String dtPedido,
	        String dataVencFormatada, String idAluno, String taxaId, String tipoEmpresa,
	        BigDecimal codemp, BigDecimal codparc, Map<String, BigDecimal> mapaInfFinanceiro,
	        Map<String, BigDecimal> mapaInfCenCus, Map<String, BigDecimal> mapaInfCenCusAluno,
	        Map<String, String> mapaInfRecDesp, Map<String, BigDecimal> mapaInfConta,
	        Map<String, BigDecimal> mapaInfBanco, Map<String, BigDecimal> mapaInfNatureza,
	        List<String> selectsParaInsert, List<String> selectsParaInsertLog) {

	    System.out.println("Processando título não cancelado");
	    LogCatcher.logInfo("\nIniciando processamento de título ativo: " + idFin + " para empresa " + codemp);

	    if (vlrDesdob.compareTo(new BigDecimal("5")) <= 0) {
	        System.out.println("AVISO: Valor <= 5 - ignorando registro");
	        LogCatcher.logInfo("Título ignorado por valor <= 5: " + vlrDesdob);
	        return;
	    }

	    if (codparc.compareTo(BigDecimal.ZERO) == 0) {
	        System.out.println("AVISO: Aluno com Id: " + idAluno + " não encontrado");
	        LogCatcher.logInfo("Aluno com ID " + idAluno + " não localizado - título ignorado");
	        return;
	    }

	    try {
	        if (!validarDataLimite(dtPedido)) {
	            String aviso = "Data pedido inferior à data limite: " + dtPedido;
	            System.out.println("ATENÇÃO: " + aviso);
	            LogCatcher.logInfo(aviso);
	            adicionarLogErro(selectsParaInsertLog, aviso, "Aviso", codemp, "");
	            return;
	        }
	    } catch (Exception e) {
	        System.out.println("Erro ao validar data limite: " + e.getMessage());
	        LogCatcher.logError("Erro ao validar data limite para título " + idFin + ": " + e.getMessage());
	        e.printStackTrace();
	    }

	    String chaveCenCus = taxaId + "###" + tipoEmpresa;
	    System.out.println("Chave para busca Centro Custo: " + chaveCenCus);
	    LogCatcher.logInfo("Buscando Centro de Custo com chave: " + chaveCenCus);

	    BigDecimal codCenCus = Optional.ofNullable(mapaInfCenCus.get(chaveCenCus))
	        .orElseGet(() -> {
	            System.out.println("Centro Custo não encontrado pela taxa, buscando pelo aluno");
	            LogCatcher.logInfo("Centro de Custo não encontrado pela taxa. Tentando pelo aluno: " + idAluno);
	            return Optional.ofNullable(mapaInfCenCusAluno.get(idAluno))
	                .orElse(BigDecimal.ZERO);
	        });

	    System.out.println("CodCenCus: " + codCenCus + 
	        (codCenCus.compareTo(BigDecimal.ZERO) == 0 ? " (NÃO ENCONTRADO)" : ""));
	    LogCatcher.logInfo("Centro de Custo resolvido: " + codCenCus);

	    if (codCenCus.compareTo(BigDecimal.ZERO) == 0) {
	        System.out.println("ATENÇÃO: Centro de Custo inválido - adicionando log");
	        LogCatcher.logInfo("Centro de Custo inválido para taxa ID " + taxaId + " - título ignorado");
	        adicionarLogErro(selectsParaInsertLog, "Sem \"de para\" para a Taxa ID: " + taxaId, "Aviso", codemp, "");
	        selectsParaInsert.add("SELECT " + codemp + ", SYSDATE, <#NUMUNICO#>, '" + "' FROM DUAL");
	        return;
	    }

	    String chaveFinanceiro = codemp + "###" + idFin;
	    System.out.println("Chave para busca Financeiro: " + chaveFinanceiro);
	    LogCatcher.logInfo("Buscando financeiro com chave: " + chaveFinanceiro);

	    BigDecimal validarNufin = Optional.ofNullable(mapaInfFinanceiro.get(chaveFinanceiro))
	        .orElse(BigDecimal.ZERO);

	    System.out.println("NUFIN: " + validarNufin + 
	        (validarNufin.compareTo(BigDecimal.ZERO) == 0 
	            ? " (FINANCEIRO NÃO ENCONTRADO - PRONTO PARA CADASTRO)" 
	            : " (FINANCEIRO JÁ EXISTENTE)"));
	    LogCatcher.logInfo("Resultado da busca de NUFIN: " + validarNufin);

	    if (validarNufin.compareTo(BigDecimal.ZERO) != 0) {
	        System.out.println("AVISO: Financeiro " + idFin + " já cadastrado para o parceiro: " + codparc);
	        LogCatcher.logInfo("Financeiro " + idFin + " já existe - cadastro ignorado");
	        return;
	    }

	    BigDecimal codConta = mapaInfConta.get(codemp.toString());
	    System.out.println("Código Conta: " + codConta);
	    LogCatcher.logInfo("Conta bancária resolvida: " + codConta);

	    BigDecimal codBanco = mapaInfBanco.get(codemp.toString());
	    System.out.println("Código Banco: " + codBanco);
	    LogCatcher.logInfo("Banco resolvido: " + codBanco);

	    String recDesp = mapaInfRecDesp.get(taxaId);
	    System.out.println("Rec/Desp: " + recDesp);
	    LogCatcher.logInfo("RecDesp resolvido: " + recDesp);

	    String chaveNatureza = taxaId + "###" + tipoEmpresa;
	    System.out.println("Chave para busca Natureza: " + chaveNatureza);
	    LogCatcher.logInfo("Buscando natureza com chave: " + chaveNatureza);

	    BigDecimal natureza = Optional.ofNullable(mapaInfNatureza.get(chaveNatureza))
	        .orElse(BigDecimal.ZERO);
	    System.out.println("Natureza: " + natureza + 
	        (natureza.compareTo(BigDecimal.ZERO) == 0 ? " (NÃO ENCONTRADA)" : ""));
	    LogCatcher.logInfo("Natureza resolvida: " + natureza);

	    if (recDesp == null || recDesp.isEmpty() || natureza.compareTo(BigDecimal.ZERO) == 0) {
	        System.out.println("ATENÇÃO: RecDesp ou Natureza inválidos - adicionando log");

	        String mensagemErro = "Sem \"de para\" para a Taxa ID: " + taxaId;
	        String status = "Aviso";
	        LogCatcher.logInfo("Erro de mapeamento: " + mensagemErro);

	        adicionarLogErro(selectsParaInsertLog, mensagemErro, status, codemp, "");
	        try {
	            insertLogIntegracao(mensagemErro, status, "", codemp);
	        } catch (Exception e) {
	            LogCatcher.logError("Erro ao registrar log de integração: " + e.getMessage());
	            e.printStackTrace();
	        }

	        return;
	    }

	    String sqlInsert = criarSqlInsertFinanceiro(codCenCus, natureza, codemp, codparc, vlrDesdob,
	        codBanco, codConta, dtPedido, dataVencFormatada, idFin, idAluno);

	    LogCatcher.logInfo("SQL Insert gerado com sucesso para título " + idFin);
	    selectsParaInsert.add(sqlInsert);

	    titulosProcessados.incrementAndGet();

	    ContadorEmpresa contador = contadoresPorEmpresa.get(codemp);
	    if (contador != null) {
	        contador.incrementarProcessados();
	    }

	    System.out.println("Financeiro cadastrado com sucesso no array de inserts");
	    LogCatcher.logInfo("\nTítulo " + idFin + " cadastrado com sucesso no array de inserts.");
	}

	

	/**
	 * Cria o SQL para inserção do financeiro 
	 */
	private String criarSqlInsertFinanceiro(BigDecimal codCenCus, BigDecimal natureza, BigDecimal codemp,
	        BigDecimal codparc, BigDecimal vlrDesdob, BigDecimal codBanco, BigDecimal codConta,
	        String dtPedido, String dataVencFormatada, String idFin, String idAluno) {
	    
	    return " SELECT <#NUFIN#>, NULL, 0, 'F', "
	            + "1" // recDesp sempre 1 conforme o SQL de consulta
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
	}

	/**
	 * Executa as inserções de financeiro no banco
	 */
	private void executarInsercoesFinanceiro(EnviromentUtils util, List<String> selectsParaInsert, int qtdInsert, 
	        BigDecimal codemp) throws Exception {

	    System.out.println("Iniciando processamento de inserts");
	    LogCatcher.logInfo("\nIniciando processamento de inserts para empresa: " + codemp);

	    BigDecimal nuFinInicial = util.getMaxNumFin(false);
	    System.out.println("NUFIN inicial obtido: " + nuFinInicial);
	    LogCatcher.logInfo("NUFIN inicial obtido: " + nuFinInicial);

	    System.out.println("Atualizando NUFIN com incremento de: " + qtdInsert);
	    LogCatcher.logInfo("Atualizando sequência NUFIN com incremento de: " + qtdInsert);
	    util.updateNumFinByQtd(qtdInsert);

	    StringBuilder sqlInsertFin = new StringBuilder();
	    int i = 1;
	    for (String sqlInsert : selectsParaInsert) {
	        String sql = sqlInsert;
	        int nuFin = nuFinInicial.intValue() + i;
	        sql = sql.replace("<#NUFIN#>", String.valueOf(nuFin));
	        sqlInsertFin.append(sql);
	        System.out.println("SQL #" + i + " - NUFIN: " + nuFin);
	        LogCatcher.logInfo("Preparado SQL #" + i + " com NUFIN: " + nuFin);

	        if (i < qtdInsert) {
	            sqlInsertFin.append(" \nUNION ALL ");
	        }
	        i++;
	    }

	    System.out.println("SQL de insert pronto para execução");
	    LogCatcher.logInfo("SQL de insert TGFFIN pronto para execução com " + qtdInsert + " linhas.");

	    System.out.println("Executando insertFinByList");
	    LogCatcher.logInfo("Executando insertFinByList para empresa " + codemp);
	    insertFinByList(sqlInsertFin, codemp);
	    System.out.println("InsertFinByList concluído com sucesso");
	    LogCatcher.logInfo("\nInsertFinByList concluído com sucesso para empresa " + codemp);
	}


	/**
	 * Adiciona um log de erro ou aviso para ser inserido posteriormente
	 */
	private void adicionarLogErro(List<String> selectsParaInsertLog, String mensagem, 
	        String tipo, BigDecimal codemp, String idExterno) {
	    
	    StringBuilder sql = new StringBuilder();
	    sql.append("SELECT <#NUMUNICO#>, '");
	    sql.append(mensagem.replace("'", "''"));
	    sql.append("', SYSDATE, '");
	    sql.append(tipo);
	    sql.append("', ");
	    sql.append(codemp);
	    sql.append(", '");
	    sql.append(idExterno);
	    sql.append("' FROM DUAL");
	    
	    selectsParaInsertLog.add(sql.toString());
	    
	    // Se for um erro de título, registrar detalhadamente
	    if (idExterno != null && !idExterno.isEmpty()) {
	        registrarErroTitulo(idExterno, null, "N/A", tipo, mensagem, codemp);
	    }
	}
	
	/**
	 *FIM do bloco cadastrarFinanceiro
	 */
	
	
	public String[] apiGet(String ur, String token) throws Exception {
	    BufferedReader reader;
	    StringBuilder responseContent = new StringBuilder();
	    String encodedUrl = ur.replace(" ", "%20");
	    URL obj = new URL(encodedUrl);
	    HttpURLConnection https = (HttpURLConnection) obj.openConnection();

	    System.out.println("Entrou na API");
	    System.out.println("URL: " + encodedUrl);
	    System.out.println("Token Enviado: [" + token + "]");

	    LogCatcher.logInfo("Chamada de API iniciada. URL: " + encodedUrl);
	    LogCatcher.logInfo("Token Enviado: [" + token + "]");

	    https.setRequestMethod("GET");
	    https.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)");
	    https.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
	    https.setRequestProperty("Accept", "application/json");
	    https.setRequestProperty("Authorization", "Bearer " + token);
	    https.setDoInput(true);

	    int status = https.getResponseCode();

	    if (status == 429) {
	        String retryAfter = https.getHeaderField("Retry-After");
	        int waitTime = 5;
	        if (retryAfter != null) {
	            try {
	                waitTime = Integer.parseInt(retryAfter);
	            } catch (NumberFormatException e) {
	                LogCatcher.logError("Falha ao interpretar Retry-After: " + retryAfter);
	            }
	        }
	        System.out.println("Limite atingido. Aguardando " + waitTime + " segundos antes de tentar novamente.");
	        LogCatcher.logInfo("Status 429 recebido. Reenviando a requisição após " + waitTime + " segundos.");

	        Thread.sleep(waitTime * 1000L);
	        https.disconnect();
	        return apiGet(ur, token); // Recursão segura aqui, pois há espera
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
	    LogCatcher.logInfo("Resposta recebida da API. Status: " + status);

	    String response = responseContent.toString();
	    https.disconnect();
	    return new String[]{Integer.toString(status), response};
	}


		 

		 
		 
     //mudar para dynamicVO
	public void insertFinByList(StringBuilder listInsert, BigDecimal codemp) throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		
		EnviromentUtils util = new EnviromentUtils();
		
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
			LogCatcher.logError("SELECT <#NUMUNICO#>, 'Erro ao integrar financeiro, Mensagem de erro: "
					+ se.getMessage().replace("'", "\"")+"', SYSDATE, 'Erro', "+codemp+", '' FROM DUAL");
			selectsParaInsertLog.add("SELECT <#NUMUNICO#>, 'Erro ao integrar financeiro, Mensagem de erro: "
								+ se.getMessage().replace("'", "\"")+"', SYSDATE, 'Erro', "+codemp+", '' FROM DUAL");

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

	
	
	 //mudar para dynamicVO
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

		System.out.println("Insert financeiro: " + nufin);

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
					+ "               0,"
					+ "               428," // CODUSU alterado para 428
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

			//updateNumFin();

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
	    BigDecimal id = BigDecimal.ZERO;
	    
	    try {
	        JapeWrapper alunoDAO = JapeFactory.dao("AD_ALUNOS");
	        DynamicVO alunoVO = alunoDAO.findOne("ID_EXTERNO = ?", new Object[]{idAluno});
	        
	        if (alunoVO != null) {
	            id = alunoVO.asBigDecimal("CODPARC");
	            // Se o valor for nulo, mantém o ZERO definido inicialmente
	            if (id == null) {
	                id = BigDecimal.ZERO;
	            }
	        }
	    } catch (Exception e) {
	    	LogCatcher.logError("Erro ao buscar código do aluno " + idAluno + ": " + e.getMessage());
	    	 insertLogIntegracao("Erro ao buscar código do aluno " + idAluno + ": " + e.getMessage(), "ERRO", idAluno, null);
	         throw e;
	    }
	    
	    return id;
	}

	public BigDecimal getCodBanco(BigDecimal codEmp) throws Exception {
	    BigDecimal id = BigDecimal.ZERO;

	    try {
	        JapeWrapper bancoDAO = JapeFactory.dao("AD_INFOBANKBAIXA");
	        DynamicVO bancoVO = bancoDAO.findOne("CODEMP = ? AND IDEXTERNO IS NULL", new Object[]{codEmp});

	        if (bancoVO != null) {
	            id = bancoVO.asBigDecimal("CODBCO");
	            // Se o valor for nulo, mantém o ZERO definido inicialmente
	            if (id == null) {
	                id = BigDecimal.ZERO;
	            }
	        }
	    } catch (Exception e) {
	        e.printStackTrace();
	        throw e;
	    }

	    return id;
	}


	public BigDecimal getNatureza(String idExterno) throws Exception {
	    BigDecimal id = BigDecimal.ZERO;

	    try {
	        JapeWrapper naturezaDAO = JapeFactory.dao("AD_NATACAD");
	        DynamicVO naturezaVO = naturezaDAO.findOne("IDEXTERNO = ?", new Object[]{idExterno});

	        if (naturezaVO != null) {
	            id = naturezaVO.asBigDecimal("CODNAT");
	            if (id == null) {
	                id = BigDecimal.ZERO;
	            }
	        }
	    } catch (Exception e) {
	        e.printStackTrace();
	        throw e;
	    }

	    // Se não encontrou ou era zero, ajusta para 10101002
	    if (id.compareTo(BigDecimal.ZERO) == 0) {
	        id = BigDecimal.valueOf(10101002);
	    }

	    return id;
	}


	public BigDecimal getCodConta(BigDecimal codEmp) throws Exception {
	    BigDecimal id = BigDecimal.ZERO;

	    try {
	        JapeWrapper contaDAO = JapeFactory.dao("AD_INFOBANKBAIXA");
	        DynamicVO contaVO = contaDAO.findOne("CODEMP = ? AND IDEXTERNO IS NULL", new Object[]{codEmp});

	        if (contaVO != null) {
	            id = contaVO.asBigDecimal("CODCTABCOINT");
	            if (id == null) {
	                id = BigDecimal.ZERO;
	            }
	        }
	    } catch (Exception e) {
	        e.printStackTrace();
	        throw e;
	    }

	    return id;
	}


	public BigDecimal getCodCenCus(String idCurso) throws Exception {
	    BigDecimal id = BigDecimal.ZERO;

	    try {
	        JapeWrapper cencusDAO = JapeFactory.dao("TSICUS");
	        DynamicVO cencusVO = cencusDAO.findOne("AD_IDEXTERNO = ?", new Object[]{idCurso});

	        if (cencusVO != null) {
	            id = cencusVO.asBigDecimal("CODCENCUS");
	            if (id == null) {
	                id = BigDecimal.ZERO;
	            }
	        }
	    } catch (Exception e) {
	        e.printStackTrace();
	        throw e;
	    }

	    return id;
	}

	public BigDecimal getCodCenCusPeloCusto(String idTaxa) throws Exception {
	    BigDecimal id = BigDecimal.ZERO;

	    try {
	        JapeWrapper natacadDAO = JapeFactory.dao("AD_NATACAD");
	        DynamicVO natacadVO = natacadDAO.findOne("IDEXTERNO = ?", new Object[]{idTaxa});

	        if (natacadVO != null) {
	            id = natacadVO.asBigDecimal("CODCENCUS");
	            if (id == null) {
	                id = BigDecimal.ZERO;
	            }
	        }
	    } catch (Exception e) {
	        e.printStackTrace();
	        throw e;
	    }

	    return id;
	}


	public boolean validarFin(String idFin, BigDecimal codemp) throws Exception {
	    try {
	        JapeWrapper finDAO = JapeFactory.dao("TGFFIN");
	        DynamicVO finVO = finDAO.findOne("AD_IDEXTERNO = ? AND CODEMP = ?", new Object[]{idFin, codemp});

	        // Se encontrou, já existe — retorna false.
	        return finVO == null;
	    } catch (Exception e) {
	        // Se der erro de "registro não encontrado", consideramos como válido.
	        if (e.getMessage() != null && e.getMessage().contains("Nenhum registro encontrado")) {
	            return true;
	        }
	        e.printStackTrace();
	        throw e;
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
			insertLogIntegracao("Erro na validação de data: " + e.getMessage(), "ERRO_SQL", "", null);
			LogCatcher.logError("Erro na validação de data: " + e.getMessage());
			
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

	    try {
	        jdbc.openSession();

	        String sql = "SELECT ID_EXTERNO FROM AD_ALUNOS WHERE CODPARC = ?";

	        pstmt = jdbc.getPreparedStatement(sql);
	        pstmt.setBigDecimal(1, codparc);

	        rs = pstmt.executeQuery();

	        if (rs.next()) {
	            return rs.getBigDecimal("ID_EXTERNO"); // já retorna direto
	        } else {
	            return BigDecimal.ZERO;
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

	    try (PreparedStatement pstmt = jdbc.getPreparedStatement(
	            "UPDATE TGFNUM SET ULTCOD = NVL(ULTCOD, 0) + 1 WHERE ARQUIVO = 'TGFFIN'")) {

	        jdbc.openSession();
	        pstmt.executeUpdate();

	    } catch (Exception e) {
	        e.printStackTrace();
	        throw e;  // Não engole a exceção, propaga para quem chamou.
	    } finally {
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
			LogCatcher.logInfo("Entrou no UPDATE da flag dos alunos");
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

	public void updateFinExtorno(BigDecimal nufin, BigDecimal codemp) throws Exception {
	    JapeWrapper financeiroDAO = JapeFactory.dao("Financeiro");
	    
	    try {
	        // Obtém a data de alteração mais recente do tipo de operação 0
	        JapeWrapper tipoOperacaoDAO = JapeFactory.dao("TipoOperacao");
	        DynamicVO tipoOperacaoVO = tipoOperacaoDAO.findOne("CODTIPOPER = ?", new Object[]{BigDecimal.ZERO});
	        Date dhtipoPerBaixa = tipoOperacaoVO != null ? tipoOperacaoVO.asTimestamp("DHALTER") : null;
	        
	        // Realiza a atualização utilizando JapeWrapper, definindo campo por campo
	        financeiroDAO.prepareToUpdateByPK(nufin)
	                     .set("VLRBAIXA", BigDecimal.ZERO)
	                     .set("DHBAIXA", null)
	                     .set("NUBCO", null)
	                     .set("CODTIPOPERBAIXA", BigDecimal.ZERO)
	                     .set("DHTIPOPERBAIXA", dhtipoPerBaixa)
	                     .set("CODUSUBAIXA", null)
	                     .update();
	        
	        System.out.println("Passou do update");
	    } catch (Exception e) {
	        e.printStackTrace();
	        insertLogIntegracao("Erro ao Extornar Titulo " + nufin + ": " + e.getMessage(), "ERRO", "", codemp);
	        selectsParaInsertLog.add("SELECT <#NUMUNICO#>, 'Erro ao Extornar Titulo " + nufin + ": " + e.getMessage() + "', SYSDATE, 'Erro', " + codemp + ", '' FROM DUAL");
	        LogCatcher.logError("SELECT <#NUMUNICO#>, 'Erro ao Extornar Titulo " + nufin + ": " + e.getMessage() + "', SYSDATE, 'Erro', " + codemp + ", '' FROM DUAL");
	    }
	}


	public void deleteTgfMbc(BigDecimal nubco, BigDecimal codemp) throws Exception {
	    JapeWrapper movBancariaDAO = JapeFactory.dao("MovimentacaoBancaria");
	    
	    try {
	        // Exclui a movimentação bancária utilizando JapeWrapper
	        movBancariaDAO.delete("NUBCO = ?", new Object[]{nubco});
	        
	        System.out.println("Passou do update");
	    } catch (Exception e) {
	        e.printStackTrace();
	        insertLogIntegracao("Erro ao Excluir Movimentação Bancaria " + nubco + ": " + e.getMessage(), "ERRO", "", codemp);
	        LogCatcher.logError("SELECT <#NUMUNICO#>, 'Erro ao Excluir Movimentação Bancaria " + nubco + ": " + e.getMessage() + "', SYSDATE, 'Erro', " + codemp + ", '' FROM DUAL");
	        selectsParaInsertLog.add("SELECT <#NUMUNICO#>, 'Erro ao Excluir Movimentação Bancaria " + nubco + ": " + e.getMessage() + "', SYSDATE, 'Erro', " + codemp + ", '' FROM DUAL");
	        throw e;
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

	        int resultado = pstmt.executeUpdate();
	        System.out.println("Passou do update");
	        LogCatcher.logInfo("deleteTgfFin: Título " + nufin + " excluído com sucesso. Empresa: " + codemp);

	        if (resultado == 0) {
	            LogCatcher.logInfo("deleteTgfFin: Nenhum título foi excluído (NUFIN inexistente: " + nufin + ")");
	        }

	    } catch (SQLException e) {
	        String msgErro = String.format("Erro ao excluir título %s: [SQL State %s] %s", nufin, e.getSQLState(), e.getMessage());
	        LogCatcher.logError(msgErro);
	        insertLogIntegracao(msgErro, "ERRO_SQL", "", codemp);
	        e.printStackTrace();

	    } finally {
	        if (pstmt != null) {
	            pstmt.close();
	        }
	        jdbc.closeSession();
	    }
	}


	
	public void insertLogIntegracao(String descricao, String status, String idMatricula, BigDecimal codemp,
	        String recDesp, BigDecimal natureza, String taxaId) throws Exception {

	    if (recDesp == null || recDesp.isEmpty() || natureza == null || natureza.compareTo(BigDecimal.ZERO) == 0) {
	        System.out.println("ATENÇÃO: RecDesp ou Natureza inválidos - adicionando log");
	        LogCatcher.logInfo("RecDesp ou Natureza inválidos para Taxa ID: " + taxaId);

	        String erroTaxa = "Sem \"de para\" para a Taxa ID: " + taxaId;

	        if (descricao != null && !descricao.contains(erroTaxa)) {
	            descricao = descricao + " - " + erroTaxa;
	        } else if (descricao == null) {
	            descricao = erroTaxa;
	        }

	        if (status == null || status.isEmpty()) {
	            status = "Aviso";
	        }
	    }

	    insertLogIntegracao(descricao, status, idMatricula, codemp);
	}

	public void insertLogIntegracao(String descricao, String status, String idMatricula, BigDecimal codemp)
	        throws Exception {
	    EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	    JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
	    PreparedStatement pstmt = null;

	    try {
	        jdbc.openSession();
	        String descricaoCompleta;

	        if ((idMatricula == null || idMatricula.isEmpty()) && codemp == null) {
	            descricaoCompleta = descricao;
	        } else if ((idMatricula == null || idMatricula.isEmpty())) {
	            descricaoCompleta = descricao + " Empresa:" + codemp;
	        } else if (codemp == null) {
	            descricaoCompleta = descricao + " Matrícula:" + idMatricula;
	        } else {
	            descricaoCompleta = descricao + " Matrícula:" + idMatricula + " Empresa:" + codemp;
	        }

	        String sqlUpdate = "INSERT INTO AD_LOGINTEGRACAO (NUMUNICO, DESCRICAO, DTHORA, STATUS) "
	                         + "VALUES ((SELECT NVL(MAX(NUMUNICO), 0) + 1 FROM AD_LOGINTEGRACAO), ?, SYSDATE, ?)";

	        pstmt = jdbc.getPreparedStatement(sqlUpdate);
	        pstmt.setString(1, descricaoCompleta);
	        pstmt.setString(2, status);
	        pstmt.executeUpdate();

	        LogCatcher.logInfo("Log de integração inserido com sucesso: " + descricaoCompleta);

	    } catch (Exception se) {
	        String erro = "Erro ao inserir log de integração: " + se.getMessage();
	        System.err.println(erro);
	        LogCatcher.logError(erro);
	        se.printStackTrace();
	    } finally {
	        if (pstmt != null) pstmt.close();
	        if (jdbc != null) jdbc.closeSession();
	    }
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
			selectsParaInsertLog.add("SELECT <#NUMUNICO#>, 'Erro ao Inserir Lista de Financeiros Titulo: "+se.getMessage()+"', SYSDATE, 'Erro', "+codemp+", '' FROM DUAL");
			
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}
	


}

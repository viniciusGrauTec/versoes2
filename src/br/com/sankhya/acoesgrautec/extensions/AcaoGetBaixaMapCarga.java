package br.com.sankhya.acoesgrautec.extensions;

import br.com.sankhya.acoesgrautec.util.EnviromentUtils;
import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.extensions.actionbutton.Registro;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.modelcore.auth.AuthenticationInfo;
import br.com.sankhya.modelcore.financeiro.helper.EstornoHelper;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import br.com.sankhya.modelcore.util.SWRepositoryUtils;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sankhya.util.JdbcUtils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Queue;

import org.activiti.engine.impl.util.json.JSONArray;
import org.cuckoo.core.ScheduledAction;
import org.cuckoo.core.ScheduledActionContext;

public class AcaoGetBaixaMapCarga implements AcaoRotinaJava, ScheduledAction {
	private List<String> selectsParaInsert = new ArrayList();
	private EnviromentUtils util = new EnviromentUtils();
	static {
		LogConfiguration.setPath(SWRepositoryUtils.getBaseFolder() + "/logAcao/logs");
	}

	public void doAction(ContextoAcao contexto) throws Exception {


		Registro[] linhas = contexto.getLinhas();
		Registro registro = linhas[0];
		String url = (String) registro.getCampo("URL");
		String token = (String) registro.getCampo("TOKEN");
		BigDecimal codEmp = (BigDecimal) registro.getCampo("CODEMP");
		
		String dataInicio = contexto.getParam("DTINICIO").toString().substring(0, 10);
		String dataFim = contexto.getParam("DTFIM").toString().substring(0, 10);
		String matricula = (String) contexto.getParam("Matricula");

		LogCatcher.logInfo("\nIniciando doAction AcaoGetBaixaMapCarga - Empresa: " + codEmp + ", Período: " + dataInicio + " a " + dataFim + ", Matrícula: " + matricula);


		try {
			LogCatcher.logInfo("Carregando informações de Banco e Conta doAction...");
			List<Object[]> listInfBancoConta = this.retornarInformacoesBancoConta();
			Map<String, BigDecimal> mapaInfBanco = new HashMap();

			for (Object[] obj : listInfBancoConta) {
				Long codEmpObj = (Long) obj[1];
				String idExternoObj = (String) obj[2];
				BigDecimal codBcoObj = (BigDecimal) obj[3];
				if (mapaInfBanco.get(codEmpObj + "###" + idExternoObj) == null) {
					mapaInfBanco.put(codEmpObj + "###" + idExternoObj, codBcoObj);
				}
			}

			Map<String, BigDecimal> mapaInfConta = new HashMap();

			for (Object[] obj : listInfBancoConta) {
				BigDecimal codCtabCointObj = (BigDecimal) obj[0];
				Long codEmpObj = (Long) obj[1];
				String idExternoObj = (String) obj[2];
				if (mapaInfConta.get(codEmpObj + "###" + idExternoObj) == null) {
					mapaInfConta.put(codEmpObj + "###" + idExternoObj, codCtabCointObj);
				}
			}

			LogCatcher.logInfo("Carregando informações de Alunos doAction...");
			List<Object[]> listInfAlunos = this.retornarInformacoesAlunos();

			Map<String, BigDecimal> mapaInfAlunos = new HashMap();

			for (Object[] obj : listInfAlunos) {
				BigDecimal codParc = (BigDecimal) obj[0];
				String idExternoObj = (String) obj[1];
				if (mapaInfAlunos.get(idExternoObj) == null) {
					mapaInfAlunos.put(idExternoObj, codParc);
				}
			}

			LogCatcher.logInfo("Carregando informações de Financeiro doAction...");
			List<Object[]> listInfFinanceiro = this.retornarInformacoesFinanceiro();

			Map<String, BigDecimal> mapaInfFinanceiro = new HashMap();

			for (Object[] obj : listInfFinanceiro) {
				BigDecimal nuFin = (BigDecimal) obj[0];
				BigDecimal codEmpObj = (BigDecimal) obj[1];
				String idExternoObj = (String) obj[2];
				if (mapaInfFinanceiro.get(codEmpObj + "###" + idExternoObj) == null) {
					mapaInfFinanceiro.put(codEmpObj + "###" + idExternoObj, nuFin);
				}
			}

			LogCatcher.logInfo("Carregando informações de Baixa doAction...");
			List<Object[]> listInfIdBaixa = this.retornarInformacoesIdBaixa();

			Map<BigDecimal, String> mapaInfIdBaixa = new HashMap();

			for (Object[] obj : listInfIdBaixa) {
				BigDecimal nuFin = (BigDecimal) obj[0];
				String idBaixa = (String) obj[1];
				if (mapaInfIdBaixa.get(nuFin) == null) {
					mapaInfIdBaixa.put(nuFin, idBaixa);
				}
			}

			/*  NUFIN → baixaId  já existe (mapaInfIdBaixa)                */
			/*  Agora criamos  baixaId → NUFIN  para localizar pelo id único*/
			Map<String, BigDecimal> mapaBaixaIdParaNufin = new HashMap<>();

			for (Object[] obj : listInfIdBaixa) {
				BigDecimal nuFin   = (BigDecimal) obj[0];
				String     baixaId = (String)     obj[1];
				mapaBaixaIdParaNufin.put(codEmp + "###" + baixaId, nuFin);
			}


			LogCatcher.logInfo("Carregando informações de Contagem da Baixa doAction...");
			List<Object[]> listInfContagemIdBaixa = this.retornarInformacoesContagemIdBaixa();
			Map<String, BigDecimal> mapaInfIdBaixaParcelas = new HashMap();

			for (Object[] obj : listInfContagemIdBaixa) {
				BigDecimal codEmpObj = (BigDecimal) obj[0];
				String idBaixa = (String) obj[1];
				BigDecimal countParcelas = (BigDecimal) obj[2];
				if (mapaInfIdBaixaParcelas.get(idBaixa + "###" + codEmpObj) == null) {
					mapaInfIdBaixaParcelas.put(idBaixa + "###" + codEmpObj, countParcelas);
				}
			}

			LogCatcher.logInfo("Carregando informações de Contagem da BaixaOrig doAction...");
			List<Object[]> listInfIdBaixaOrig = this.retornarInformacoesIdBaixaOrig();
			Map<String, BigDecimal> mapaInfIdBaixaOrig = new HashMap();

			for (Object[] obj : listInfIdBaixaOrig) {
				BigDecimal nuFin = (BigDecimal) obj[0];
				String idBaixa = (String) obj[1];
				String idFinOrig = (String) obj[2];
				if (mapaInfIdBaixaOrig.get(idBaixa + "###" + idFinOrig) == null) {
					mapaInfIdBaixaOrig.put(idBaixa + "###" + idFinOrig, nuFin);
				}
			}

			Map<BigDecimal, String> mapaInfFinanceiroBaixado = new HashMap();

			for (Object[] obj : listInfFinanceiro) {
				BigDecimal nuFin = (BigDecimal) obj[0];
				String baixado = (String) obj[3];
				if (mapaInfFinanceiroBaixado.get(nuFin) == null) {
					mapaInfFinanceiroBaixado.put(nuFin, baixado);
				}
			}

			Map<BigDecimal, BigDecimal> mapaInfFinanceiroValor = new HashMap();

			for (Object[] obj : listInfFinanceiro) {
				BigDecimal nuFin = (BigDecimal) obj[0];
				BigDecimal vlrDesdob = (BigDecimal) obj[4];
				if (mapaInfFinanceiroValor.get(nuFin) == null) {
					mapaInfFinanceiroValor.put(nuFin, vlrDesdob);
				}
			}

			Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco = new HashMap();

			for (Object[] obj : listInfFinanceiro) {
				BigDecimal nuFin = (BigDecimal) obj[0];
				BigDecimal nuBco = (BigDecimal) obj[5];
				if (mapaInfFinanceiroBanco.get(nuFin) == null) {
					mapaInfFinanceiroBanco.put(nuFin, nuBco);
				}
			}

			LogCatcher.logInfo("Carregando informações de Tipo de Titulos doAction...");
			List<Object[]> listInfTipoTitulo = this.retornarInformacoesTipoTitulo();
			Map<String, BigDecimal> mapaInfTipoTitulo = new HashMap();


			//QTDPARCELAS
			// First map - mapaInfTipoTitulo
			for (Object[] obj : listInfTipoTitulo) {
				BigDecimal codTipTit = (BigDecimal) obj[0];
				Long codEmpObj = (Long) obj[1];
				String idExternoObj = (String) obj[2];
				// Removed qtdParcelas from the key
				if (mapaInfTipoTitulo.get(codEmpObj + "###" + idExternoObj) == null) {
					mapaInfTipoTitulo.put(codEmpObj + "###" + idExternoObj, codTipTit);
				}
			}

			Map<String, BigDecimal> mapaInfTipoTituloTaxa = new HashMap();

			// Second map - mapaInfTipoTituloTaxa
			for (Object[] obj : listInfTipoTitulo) {
				BigDecimal taxa = (BigDecimal) obj[3];
				Long codEmpObj = (Long) obj[1];
				String idExternoObj = (String) obj[2];
				// Removed qtdParcelas from the key
				if (mapaInfTipoTituloTaxa.get(codEmpObj + "###" + idExternoObj) == null) {
					mapaInfTipoTituloTaxa.put(codEmpObj + "###" + idExternoObj, taxa);
				}
			}

			Map<String, BigDecimal> mapaInfTipoTituloCodparcCartao = new HashMap();

			// Third map - mapaInfTipoTituloCodparcCartao
			for (Object[] obj : listInfTipoTitulo) {
				BigDecimal codParcCartao = (BigDecimal) obj[5];
				Long codEmpObj = (Long) obj[1];
				String idExternoObj = (String) obj[2];
				// Removed qtdParcelas from the key
				if (mapaInfTipoTituloCodparcCartao.get(codEmpObj + "###" + idExternoObj) == null) {
					mapaInfTipoTituloCodparcCartao.put(codEmpObj + "###" + idExternoObj, codParcCartao);
				}
			}
			//QTDPARCELAS

			List<Object[]> listInfMenorDataMovBancariaPorConta = this.retornarInformacoesMenorDataMovBancariaPorConta();
			Map<Long, Date> mapaInfMenorDataMovBancariaPorConta = new HashMap();

			for (Object[] obj : listInfMenorDataMovBancariaPorConta) {
				Long codCtabCointObj = (Long) obj[0];
				Date dtMinRef = (Date) obj[1];
				if (mapaInfMenorDataMovBancariaPorConta.get(codCtabCointObj) == null) {
					mapaInfMenorDataMovBancariaPorConta.put(codCtabCointObj, dtMinRef);
				}
			}

			LogCatcher.logInfo("Chamando processDateRange com dados carregados.");

			this.processDateRange(url, token, codEmp, mapaInfIdBaixaOrig, mapaInfIdBaixa, mapaInfTipoTituloTaxa,
					mapaInfBanco, mapaInfConta, mapaInfAlunos, mapaInfFinanceiro, mapaInfTipoTitulo,
					mapaInfMenorDataMovBancariaPorConta, mapaInfFinanceiroBaixado, mapaInfFinanceiroValor,
					mapaInfFinanceiroBanco, mapaInfTipoTituloCodparcCartao, dataInicio, dataFim, matricula,
					mapaInfIdBaixaParcelas,mapaBaixaIdParaNufin);

			contexto.setMensagemRetorno("Periodo Processado!");
		} catch (Exception e) {
			e.printStackTrace();
			LogCatcher.logInfo("Erro durante o processamento doAction: " + e.getMessage());

			contexto.mostraErro(e.getMessage());
		} finally {
			if (this.selectsParaInsert.size() > 0) {
				LogCatcher.logInfo("Foram encontrados " + this.selectsParaInsert.size() + " inserts pendentes para log.");

				StringBuilder msgError = new StringBuilder();
				System.out.println("Entrou na lista do finally: " + this.selectsParaInsert.size());
				int qtdInsert = this.selectsParaInsert.size();
				int i = 1;

				for (String sqlInsert : this.selectsParaInsert) {
					int nuFin = this.util.getMaxNumLog();
					String sql = sqlInsert.replace("<#NUMUNICO#>", String.valueOf(nuFin));
					msgError.append(sql);
					if (i < qtdInsert) {
						msgError.append(" \nUNION ALL ");
					}

					++i;
				}

				System.out.println("Consulta de log: \n" + msgError);
				LogCatcher.logInfo("Consulta de log: \n" + msgError);
				this.insertLogList(msgError.toString(), codEmp);
			}

		}

	}

	public void onTime(ScheduledActionContext arg0) {
		System.out.println("/*************** Inicio - JobGetBaixaMap *****************/ ");
		LogCatcher.logInfo("\n/*************** Inicio - onTime GetBaixaMap *****************/ ");
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
			LogCatcher.logInfo("Carregando informações de Banco e Conta do Job...");
			List<Object[]> listInfBancoConta = this.retornarInformacoesBancoConta();
			Map<String, BigDecimal> mapaInfBanco = new HashMap();

			for (Object[] obj : listInfBancoConta) {
				Long codEmpObj = (Long) obj[1];
				String idExternoObj = (String) obj[2];
				BigDecimal codBcoObj = (BigDecimal) obj[3];
				if (mapaInfBanco.get(codEmpObj + "###" + idExternoObj) == null) {
					mapaInfBanco.put(codEmpObj + "###" + idExternoObj, codBcoObj);
				}
			}

			Map<String, BigDecimal> mapaInfConta = new HashMap();

			for (Object[] obj : listInfBancoConta) {
				BigDecimal codCtabCointObj = (BigDecimal) obj[0];
				Long codEmpObj = (Long) obj[1];
				String idExternoObj = (String) obj[2];
				if (mapaInfConta.get(codEmpObj + "###" + idExternoObj) == null) {
					mapaInfConta.put(codEmpObj + "###" + idExternoObj, codCtabCointObj);
				}
			}

			LogCatcher.logInfo("Carregando informações de Alunos do Job...");
			List<Object[]> listInfAlunos = this.retornarInformacoesAlunos();
			Map<String, BigDecimal> mapaInfAlunos = new HashMap();

			for (Object[] obj : listInfAlunos) {
				BigDecimal codParc = (BigDecimal) obj[0];
				String idExternoObj = (String) obj[1];
				if (mapaInfAlunos.get(idExternoObj) == null) {
					mapaInfAlunos.put(idExternoObj, codParc);
				}
			}

			LogCatcher.logInfo("Carregando informações de Financeiro do Job...");
			List<Object[]> listInfFinanceiro = this.retornarInformacoesFinanceiro();
			Map<String, BigDecimal> mapaInfFinanceiro = new HashMap();

			for (Object[] obj : listInfFinanceiro) {
				BigDecimal nuFin = (BigDecimal) obj[0];
				BigDecimal codEmpObj = (BigDecimal) obj[1];
				String idExternoObj = (String) obj[2];
				if (mapaInfFinanceiro.get(codEmpObj + "###" + idExternoObj) == null) {
					mapaInfFinanceiro.put(codEmpObj + "###" + idExternoObj, nuFin);
				}
			}

			LogCatcher.logInfo("Carregando informações do idBaixa do Job...");
			List<Object[]> listInfIdBaixa = this.retornarInformacoesIdBaixa();
			Map<BigDecimal, String> mapaInfIdBaixa = new HashMap();

			for (Object[] obj : listInfIdBaixa) {
				BigDecimal nuFin = (BigDecimal) obj[0];
				String idBaixa = (String) obj[1];
				if (mapaInfIdBaixa.get(nuFin) == null) {
					mapaInfIdBaixa.put(nuFin, idBaixa);
				}
			}

			/* ---------- NUFIN  → baixaId já existe (mapaInfIdBaixa)
			   ---------- agora cria  baixaId → NUFIN ------------ */
			Map<String, BigDecimal> mapaBaixaIdParaNufin = new HashMap<>();

			for (Object[] obj : listInfIdBaixa) {
				BigDecimal nuFin   = (BigDecimal) obj[0];
				String     baixaId = (String)     obj[1];
				mapaBaixaIdParaNufin.put(codEmp + "###" + baixaId, nuFin);
			}

			LogCatcher.logInfo("Carregando informações do ContagemIdBaixa do Job...");
			List<Object[]> listInfContagemIdBaixa = this.retornarInformacoesContagemIdBaixa();
			Map<String, BigDecimal> mapaInfIdBaixaParcelas = new HashMap();

			for (Object[] obj : listInfContagemIdBaixa) {
				BigDecimal codEmpObj = (BigDecimal) obj[0];
				String idBaixa = (String) obj[1];
				BigDecimal countParcelas = (BigDecimal) obj[2];
				if (mapaInfIdBaixaParcelas.get(idBaixa + "###" + codEmpObj) == null) {
					mapaInfIdBaixaParcelas.put(idBaixa + "###" + codEmpObj, countParcelas);
				}
			}


			LogCatcher.logInfo("Carregando informações do listInfIdBaixaOrig do Job...");
			List<Object[]> listInfIdBaixaOrig = this.retornarInformacoesIdBaixaOrig();
			Map<String, BigDecimal> mapaInfIdBaixaOrig = new HashMap();

			for (Object[] obj : listInfIdBaixaOrig) {
				BigDecimal nuFin = (BigDecimal) obj[0];
				String idBaixa = (String) obj[1];
				String idFinOrig = (String) obj[2];
				if (mapaInfIdBaixaOrig.get(idBaixa + "###" + idFinOrig) == null) {
					mapaInfIdBaixaOrig.put(idBaixa + "###" + idFinOrig, nuFin);
				}
			}

			Map<BigDecimal, String> mapaInfFinanceiroBaixado = new HashMap();

			for (Object[] obj : listInfFinanceiro) {
				BigDecimal nuFin = (BigDecimal) obj[0];
				String baixado = (String) obj[3];
				if (mapaInfFinanceiroBaixado.get(nuFin) == null) {
					mapaInfFinanceiroBaixado.put(nuFin, baixado);
				}
			}

			Map<BigDecimal, BigDecimal> mapaInfFinanceiroValor = new HashMap();

			for (Object[] obj : listInfFinanceiro) {
				BigDecimal nuFin = (BigDecimal) obj[0];
				BigDecimal vlrDesdob = (BigDecimal) obj[4];
				if (mapaInfFinanceiroValor.get(nuFin) == null) {
					mapaInfFinanceiroValor.put(nuFin, vlrDesdob);
				}
			}

			Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco = new HashMap();

			for (Object[] obj : listInfFinanceiro) {
				BigDecimal nuFin = (BigDecimal) obj[0];
				BigDecimal nuBco = (BigDecimal) obj[5];
				if (mapaInfFinanceiroBanco.get(nuFin) == null) {
					mapaInfFinanceiroBanco.put(nuFin, nuBco);
				}
			}


			LogCatcher.logInfo("Carregando informações do listInfTipoTitulo do Job...");
			List<Object[]> listInfTipoTitulo = this.retornarInformacoesTipoTitulo();
			Map<String, BigDecimal> mapaInfTipoTitulo = new HashMap();

			//QT PARCELAS
			// Para o primeiro mapa
			for (Object[] obj : listInfTipoTitulo) {
				BigDecimal codTipTit = (BigDecimal) obj[0];
				Long codEmpObj = (Long) obj[1];
				String idExternoObj = (String) obj[2];
				// BigDecimal qtdParcelas = (BigDecimal) obj[4]; // Comentado
				if (mapaInfTipoTitulo.get(codEmpObj + "###" + idExternoObj) == null) {
					mapaInfTipoTitulo.put(codEmpObj + "###" + idExternoObj, codTipTit);
				}
			}

			Map<String, BigDecimal> mapaInfTipoTituloTaxa = new HashMap();

			for (Object[] obj : listInfTipoTitulo) {
				BigDecimal taxa = (BigDecimal) obj[3];
				Long codEmpObj = (Long) obj[1];
				String idExternoObj = (String) obj[2];
				// BigDecimal qtdParcelas = (BigDecimal) obj[4]; // Comentado
				if (mapaInfTipoTituloTaxa.get(codEmpObj + "###" + idExternoObj) == null) {
					mapaInfTipoTituloTaxa.put(codEmpObj + "###" + idExternoObj, taxa);
				}
			}

			Map<String, BigDecimal> mapaInfTipoTituloCodparcCartao = new HashMap();

			for (Object[] obj : listInfTipoTitulo) {
				BigDecimal codParcCartao = (BigDecimal) obj[5];
				Long codEmpObj = (Long) obj[1];
				String idExternoObj = (String) obj[2];
				// BigDecimal qtdParcelas = (BigDecimal) obj[4]; // Comentado
				if (mapaInfTipoTituloCodparcCartao.get(codEmpObj + "###" + idExternoObj) == null) {
					mapaInfTipoTituloCodparcCartao.put(codEmpObj + "###" + idExternoObj, codParcCartao);
				}
			}
			//QT PARCELAS


			LogCatcher.logInfo("Carregando informações do listInfMenorDataMovBancariaPorConta do Job...");
			List<Object[]> listInfMenorDataMovBancariaPorConta = this.retornarInformacoesMenorDataMovBancariaPorConta();
			Map<Long, Date> mapaInfMenorDataMovBancariaPorConta = new HashMap();

			for (Object[] obj : listInfMenorDataMovBancariaPorConta) {
				Long codCtabCointObj = (Long) obj[0];
				Date dtMinRef = (Date) obj[1];
				if (mapaInfMenorDataMovBancariaPorConta.get(codCtabCointObj) == null) {
					mapaInfMenorDataMovBancariaPorConta.put(codCtabCointObj, dtMinRef);
				}
			}

			jdbc.openSession();
			// Modificado para incluir a verificação da flag INTEGRACAO
			String query = "SELECT CODEMP, URL, TOKEN, INTEGRACAO FROM AD_LINKSINTEGRACAO";
			pstmt = jdbc.getPreparedStatement(query);
			rs = pstmt.executeQuery();

			for (tempoAnterior = this.printLogDebug(tempoAnterior,
					"Consulta para capturar o link de integração: AD_LINKSINTEGRACAO"); rs
						 .next(); tempoAnterior = this.printLogDebug(tempoAnterior,
					"onTime - updateCarga da empresa(" + codEmp + ")")) {
				++count;
				System.out.println("Contagem: " + count);
				LogCatcher.logInfo("Contagem: " + count);
				codEmp = rs.getBigDecimal("CODEMP");
				url = rs.getString("URL");
				token = rs.getString("TOKEN");
				String statusIntegracao = rs.getString("INTEGRACAO");

				// Verifica se a integração está ativa para esta empresa
				if (!"S".equals(statusIntegracao)) {
					System.out.println("Integração desativada para a empresa " + codEmp + " - pulando processamento");
					continue; // Pula para a próxima iteração do loop
				}

				this.iterarEndpoint(url, token, codEmp, mapaInfIdBaixaOrig, mapaInfIdBaixa, mapaInfTipoTituloTaxa,
						mapaInfBanco, mapaInfConta, mapaInfAlunos, mapaInfFinanceiro, mapaInfTipoTitulo,
						mapaInfMenorDataMovBancariaPorConta, mapaInfFinanceiroBaixado, mapaInfFinanceiroValor,
						mapaInfFinanceiroBanco, mapaInfTipoTituloCodparcCartao, mapaInfIdBaixaParcelas,mapaBaixaIdParaNufin);
				tempoAnterior = this.printLogDebug(tempoAnterior, "onTime - efetuarBaixa da empresa(" + codEmp + ")");
			}

			System.out.println("Chegou ao final da baixa");
			System.out.println("\n/*************** Fim - JobGetBaixaMap *****************/");
			this.printLogDebug(tempoInicio, "Tempo Total: ");
		} catch (Exception var71) {
			Exception e = var71;
			LogCatcher.logError(e);
			var71.printStackTrace();

			try {
				this.insertLogIntegracao("Erro ao integrar Baixas, Mensagem de erro: " + e.getMessage(), "Erro");
			} catch (Exception e1) {
				LogCatcher.logError(e);
				e1.printStackTrace();
			}
		} finally {
			LogCatcher.logInfo("Finalizando execução doOnTime...");
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
				System.out.println("Entrou na lista do finally: " + this.selectsParaInsert.size());
				int qtdInsert = this.selectsParaInsert.size();
				System.out.println("Lista de selects: " + this.selectsParaInsert.toString());
				int i = 1;

				for (String sqlInsert : this.selectsParaInsert) {
					int nuFin = 0;

					try {
						nuFin = this.util.getMaxNumLog();
					} catch (Exception e) {
						e.printStackTrace();
						LogCatcher.logError(e);
					}

					String sql = sqlInsert.replace("<#NUMUNICO#>", String.valueOf(nuFin));
					msgError.append(sql);
					System.out.println("Iteração: " + i + " de " + qtdInsert);
					if (i < qtdInsert) {
						msgError.append(" \nUNION ALL ");
					}

					++i;
				}

				System.out.println("Consulta de log: \n" + msgError);

				try {
					this.insertLogList(msgError.toString(), codEmp);
				} catch (Exception e) {
					LogCatcher.logError(e);
					e.printStackTrace();
				}

				StringBuilder var142 = null;
				this.selectsParaInsert = new ArrayList();
			}

		}

	}

	public void processDateRange(String url, String token, BigDecimal codemp,
								 Map<String, BigDecimal> mapaInfIdBaixaOrig, Map<BigDecimal, String> mapaInfIdBaixa,
								 Map<String, BigDecimal> mapaInfTipoTituloTaxa, Map<String, BigDecimal> mapaInfBanco,
								 Map<String, BigDecimal> mapaInfConta, Map<String, BigDecimal> mapaInfAlunos,
								 Map<String, BigDecimal> mapaInfFinanceiro, Map<String, BigDecimal> mapaInfTipoTitulo,
								 Map<Long, Date> mapaInfMenorDataMovBancariaPorConta, Map<BigDecimal, String> mapaInfFinanceiroBaixado,
								 Map<BigDecimal, BigDecimal> mapaInfFinanceiroValor, Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco,
								 Map<String, BigDecimal> mapaInfTipoTituloCodparcCartao, String dataInicio, String dataFim, String matricula,
								 Map<String, BigDecimal> mapaInfIdBaixaParcelas, Map<String, BigDecimal> mapaBaixaIdParaNufin)
			throws Exception {

		try {
			LogCatcher.logInfo("\nIniciando processDateRange - Empresa: " + codemp + ", Período: " + dataInicio + " a "
					+ dataFim + ", Matrícula: " + matricula);

			LocalDate startDate = LocalDate.parse(dataInicio);
			LocalDate endDate = LocalDate.parse(dataFim);

			LocalDate currentDate = startDate;
			while (!currentDate.isAfter(endDate)) {

				LogCatcher.logInfo("Processando data: " + currentDate);

				String dataInicialStr = currentDate.toString() + "T00:00:00";
				String dataFinalStr = currentDate.toString() + "T23:59:59";
				String dataInicialParam = URLEncoder.encode(dataInicialStr, "UTF-8");
				String dataFinalParam = URLEncoder.encode(dataFinalStr, "UTF-8");

				JSONArray todosRegistrosDoDia = new JSONArray();
				int pagina = 1;
				boolean temMaisRegistros = true;

				while (temMaisRegistros) {
					StringBuilder urlBuilder = new StringBuilder(url).append("/financeiro/baixas?").append("pagina=")
							.append(pagina).append("&quantidade=100").append("&dataInicial=").append(dataInicialParam)
							.append("&dataFinal=").append(dataFinalParam);

					if (matricula != null && !matricula.trim().isEmpty()) {
						String matriculaEncoded = URLEncoder.encode(matricula.trim(), "UTF-8");
						urlBuilder.append("&matricula=").append(matriculaEncoded);
					}

					String urlCompleta = urlBuilder.toString();
					System.out.println(
							"URL para baixas (dia: " + currentDate + ", página " + pagina + "): " + urlCompleta);
					LogCatcher.logInfo(
							"Chamando API - Data: " + currentDate + ", Página: " + pagina + ", URL: " + urlCompleta);

					String[] response1 = apiGet2(urlCompleta, token);
					int status = Integer.parseInt(response1[0]);

					if (status == 200) {
						JSONArray paginaAtual = new JSONArray(response1[1]);
						for (int i = 0; i < paginaAtual.length(); i++) {
							todosRegistrosDoDia.put(paginaAtual.getJSONObject(i));
						}

						if (paginaAtual.length() < 100) {
							temMaisRegistros = false;
						} else {
							pagina++;
						}

						LogCatcher.logInfo("Recebidos " + paginaAtual.length() + " registros na página " + pagina
								+ " para o dia " + currentDate);
						System.out.println("Dia " + currentDate + ", página " + pagina + ": " + paginaAtual.length()
								+ " registros. Total acumulado: " + todosRegistrosDoDia.length());
					} else {
						System.err.println("Erro na requisição: Status " + status);
						break;
					}
				}

				if (todosRegistrosDoDia.length() > 0) {
					System.out.println("Processando " + todosRegistrosDoDia.length()
							+ " registros de baixas para o dia " + currentDate);

					String[] response = new String[] { "200", todosRegistrosDoDia.toString() };

					System.out.println("Dados sendo enviados para efetuarBaixa:");
					System.out.println("Tamanho do array: " + todosRegistrosDoDia.length());
					System.out.println("Conteúdo do todosRegistrosDoDia: " + todosRegistrosDoDia.toString());
					LogCatcher.logInfo("Chamando efetuarBaixa para o dia " + currentDate + " com "
							+ todosRegistrosDoDia.length() + " registros.");

					efetuarBaixa(response, url, token, codemp, mapaInfIdBaixaOrig, mapaInfIdBaixa,
							mapaInfTipoTituloTaxa, mapaInfBanco, mapaInfConta, mapaInfAlunos, mapaInfFinanceiro,
							mapaInfTipoTitulo, mapaInfMenorDataMovBancariaPorConta, mapaInfFinanceiroBaixado,
							mapaInfFinanceiroValor, mapaInfFinanceiroBanco, mapaInfTipoTituloCodparcCartao,
							mapaInfIdBaixaParcelas, mapaBaixaIdParaNufin);
				}

				currentDate = currentDate.plusDays(1);
			}
		} catch (Exception e) {
			LogCatcher.logError(e);
			throw e;
		}
	}


	public void iterarEndpoint(String url, String token, BigDecimal codemp, Map<String, BigDecimal> mapaInfIdBaixaOrig,
							   Map<BigDecimal, String> mapaInfIdBaixa, Map<String, BigDecimal> mapaInfTipoTituloTaxa,
							   Map<String, BigDecimal> mapaInfBanco, Map<String, BigDecimal> mapaInfConta,
							   Map<String, BigDecimal> mapaInfAlunos, Map<String, BigDecimal> mapaInfFinanceiro,
							   Map<String, BigDecimal> mapaInfTipoTitulo, Map<Long, Date> mapaInfMenorDataMovBancariaPorConta,
							   Map<BigDecimal, String> mapaInfFinanceiroBaixado, Map<BigDecimal, BigDecimal> mapaInfFinanceiroValor,
							   Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco, Map<String, BigDecimal> mapaInfTipoTituloCodparcCartao,
							   Map<String, BigDecimal> mapaInfIdBaixaParcelas,Map<String, BigDecimal> mapaBaixaIdParaNufin) throws Exception {
		Date dataAtual = new Date();
		SimpleDateFormat formato = new SimpleDateFormat("yyyy-MM-dd");
		String dataFormatada = formato.format(dataAtual);

		try {
			String[] response = this.apiGet2(url + "/financeiro" + "/baixas" + "?quantidade=0" + "&dataInicial="
					+ dataFormatada + " 00:00:00&dataFinal=" + dataFormatada + " 23:59:59", token);
			int status = Integer.parseInt(response[0]);
			System.out.println("Status teste: " + status);
			LogCatcher.logInfo("Status teste: " + status);
			String responseString = response[1];
			System.out.println("response string baixas: " + responseString);
			LogCatcher.logInfo("response string baixas: " + responseString);
			this.efetuarBaixa(response, url, token, codemp, mapaInfIdBaixaOrig, mapaInfIdBaixa, mapaInfTipoTituloTaxa,
					mapaInfBanco, mapaInfConta, mapaInfAlunos, mapaInfFinanceiro, mapaInfTipoTitulo,
					mapaInfMenorDataMovBancariaPorConta, mapaInfFinanceiroBaixado, mapaInfFinanceiroValor,
					mapaInfFinanceiroBanco, mapaInfTipoTituloCodparcCartao, mapaInfIdBaixaParcelas,mapaBaixaIdParaNufin);
		} catch (Exception e) {
			LogCatcher.logError(e);
			e.printStackTrace();
		}

	}

	
	private boolean tituloJaBaixadoNoBanco(String tituloId, BigDecimal codemp) {
	    boolean baixado = false;
	    JdbcWrapper jdbc = null;
	    Connection conn = null;
	    PreparedStatement stmt = null;
	    ResultSet rset = null;

	    try {
	        EntityFacade entity = EntityFacadeFactory.getDWFFacade();
	        jdbc = entity.getJdbcWrapper();
	        jdbc.openSession();

	        conn = jdbc.getConnection();
	        String sql = "SELECT 1 FROM AD_ESTORNOINT WHERE TITULO_ID = ? AND STATUS_ESTORNO = 'E'";
	        stmt = conn.prepareStatement(sql);
	        stmt.setBigDecimal(1, new BigDecimal(tituloId));
	        rset = stmt.executeQuery();

	        if (rset.next()) {
	            baixado = true;
	        }
	    } catch (Exception e) {
	        LogCatcher.logError("Erro ao verificar se título " + tituloId + " já está baixado e estornado no banco: " + e.getMessage());
	    } finally {
	        JdbcUtils.closeResultSet(rset);
	        JdbcUtils.closeStatement(stmt);
	        if (jdbc != null) {
	            jdbc.closeSession();
	        }
	    }

	    return baixado;
	}






	//BAIXA DOS CARTOES  ATUALIZADO COM ESTORNO
	public void efetuarBaixa(String[] response, String url, String token, BigDecimal codemp,
							 Map<String, BigDecimal> mapaInfIdBaixaOrig, Map<BigDecimal, String> mapaInfIdBaixa,
							 Map<String, BigDecimal> mapaInfTipoTituloTaxa, Map<String, BigDecimal> mapaInfBanco,
							 Map<String, BigDecimal> mapaInfConta, Map<String, BigDecimal> mapaInfAlunos,
							 Map<String, BigDecimal> mapaInfFinanceiro, Map<String, BigDecimal> mapaInfTipoTitulo,
							 Map<Long, Date> mapaInfMenorDataMovBancariaPorConta, Map<BigDecimal, String> mapaInfFinanceiroBaixado,
							 Map<BigDecimal, BigDecimal> mapaInfFinanceiroValor, Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco,
							 Map<String, BigDecimal> mapaInfTipoTituloCodparcCartao, Map<String, BigDecimal> mapaInfIdBaixaParcelas,Map<String, BigDecimal> mapaBaixaIdParaNufin)
			throws Exception {

		LogCatcher.logInfo("\nIniciando efetuarBaixa - Empresa: " + codemp + ", com conteúdo da resposta.");


		boolean movBanc = false;
		SimpleDateFormat formatoOriginal = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat formatoDesejado = new SimpleDateFormat("dd/MM/yyyy");
		Date dataAtual = new Date();
		SimpleDateFormat formato = new SimpleDateFormat("yyyy-MM-dd");
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(dataAtual);
		calendar.add(5, -1);
		Date dataUmDiaAtras = calendar.getTime();
		String dataUmDiaFormatada = formato.format(dataUmDiaAtras);
		String dataAtualFormatada = formato.format(dataAtual);
		LogCatcher.logInfo("data um dia atras: " + dataUmDiaFormatada);
		LogCatcher.logInfo("data normal: " + dataAtualFormatada);

		System.out.println("data um dia atras: " + dataUmDiaFormatada);
		System.out.println("data normal: " + dataAtualFormatada);

		BigDecimal codTipTit = BigDecimal.ZERO;
		BigDecimal codBanco = BigDecimal.ZERO;
		BigDecimal codConta = BigDecimal.ZERO;
		BigDecimal nubco = BigDecimal.ZERO;
		String dataEstorno = "";
		BigDecimal nufin = BigDecimal.ZERO;
		String idAluno = "";
		String formaDePagamento = "";
		Map<BigDecimal, String> mapIdBaixaAtual = new HashMap();
		Map<String, BigDecimal> mapaBaixaEstornada = new HashMap<>();
		

		try {
			LogCatcher.logInfo("Verificando a resposta: " + response[1]);
			System.out.println("Verificando a resposta: " + response[1]);
			JsonParser parser = new JsonParser();

			//apenas debug
			if (response[1] == null) {
				LogCatcher.logInfo("Resposta nula recebida na efetuarBaixa - Empresa: " + codemp);

				return;
			}

			//apenas debug
			String jsonContent = response[1];
			System.out.println("Primeiro caractere: " + (jsonContent.isEmpty() ? "vazio" : jsonContent.charAt(0)));
			System.out.println("\nÚltimo caractere: " + (jsonContent.isEmpty() ? "vazio" : jsonContent.charAt(jsonContent.length() - 1)));

			LogCatcher.logInfo("Primeiro caractere: " + (jsonContent.isEmpty() ? "vazio" : jsonContent.charAt(0)));
			LogCatcher.logInfo("\nÚltimo caractere: " + (jsonContent.isEmpty() ? "vazio" : jsonContent.charAt(jsonContent.length() - 1)));


			for (JsonElement jsonElement : parser.parse(response[1]).getAsJsonArray()) {
				JsonObject jsonObject = jsonElement.getAsJsonObject();

				System.out.println("Titulo ID: " + jsonObject.get("titulo_id").getAsInt());
				System.out.println("Valor da Baixa: " + jsonObject.get("baixa_valor").getAsString());

				LogCatcher.logInfo("Titulo ID: " + jsonObject.get("titulo_id").getAsInt());
				LogCatcher.logInfo("Valor da Baixa: " + jsonObject.get("baixa_valor").getAsString());

				idAluno = jsonObject.get("aluno_id").getAsString().trim();
				System.out.println("IdAluno: " + idAluno);

				LogCatcher.logInfo("IdAluno: " + idAluno);

				BigDecimal codParc = (BigDecimal) mapaInfAlunos.get(idAluno);
				if (codParc != null) {
					String tituloId = jsonObject.get("titulo_id").getAsString();
					String baixaId = jsonObject.get("baixa_id").getAsString();
					BigDecimal vlrBaixa = new BigDecimal(jsonObject.get("baixa_valor").getAsString());

					BigDecimal vlrJuros = (BigDecimal) Optional.ofNullable(jsonObject.get("baixa_juros"))
							.filter((element) -> !element.isJsonNull()).map(JsonElement::getAsString)
							.map(BigDecimal::new).orElse(BigDecimal.ZERO);
					BigDecimal vlrMulta = (BigDecimal) Optional.ofNullable(jsonObject.get("baixa_multa"))
							.filter((element) -> !element.isJsonNull()).map(JsonElement::getAsString)
							.map(BigDecimal::new).orElse(BigDecimal.ZERO);
					BigDecimal vlrDesconto = (BigDecimal) Optional.ofNullable(jsonObject.get("baixa_desconto"))
							.filter((element) -> !element.isJsonNull()).map(JsonElement::getAsString)
							.map(BigDecimal::new).orElse(BigDecimal.ZERO);
					BigDecimal vlrOutrosAcrescimos = (BigDecimal) Optional
							.ofNullable(jsonObject.get("baixa_outros_acrescimos"))
							.filter((element) -> !element.isJsonNull()).map(JsonElement::getAsString)
							.map(BigDecimal::new).orElse(BigDecimal.ZERO);
					String dataBaixa = jsonObject.get("baixa_data").getAsString();

					LogCatcher.logInfo("Data Baixa: " + dataBaixa);
					System.out.println("Data Baixa: " + dataBaixa);
					Date data = formatoOriginal.parse(dataBaixa);
					System.out.println("Date Baixa: " + data);
					LogCatcher.logInfo("Date Baixa: " + data);
					String dataBaixaFormatada = formatoDesejado.format(data);
					nufin = (BigDecimal) mapaInfFinanceiro.get(codemp + "###" + tituloId);
					
					
					// Verificar se o título já foi baixado no banco (TGFFIN)
					if (tituloJaBaixadoNoBanco(tituloId, codemp)) {
					    LogCatcher.logInfo("[VALIDAÇÃO] Título " + tituloId + " já está baixado e estornado no banco para a empresa " + codemp + ". Ignorando estorno e nova baixa.");
					    selectsParaInsert.add(
					        "SELECT <#NUMUNICO#>, 'Estorno ignorado: título " + tituloId + " já baixado e estornado', SYSDATE, 'Aviso', " + codemp + ", '" + idAluno + "' FROM DUAL");
					    continue; // Pula o título atual
					}




					if (jsonObject.has("baixa_estorno_data") && !jsonObject.get("baixa_estorno_data").isJsonNull()) {
						LogCatcher.logInfo("Detectado estorno para título ID: " + tituloId + " - Data estorno: " + dataEstorno);

						dataEstorno = jsonObject.get("baixa_estorno_data").getAsString();
					} else {
						System.out.println("Entrou no else de estorno");
						dataEstorno = null;
					}

					String idExterno = jsonObject.get("local_pagamento_id").getAsString();
					codBanco = (BigDecimal) mapaInfBanco.get(codemp + "###" + idExterno);
					System.out.println("Banco: " + codBanco);
					LogCatcher.logInfo("Banco: " + codBanco);
					codConta = (BigDecimal) mapaInfConta.get(codemp + "###" + idExterno);
					System.out.println("Conta: " + codConta);
					LogCatcher.logInfo("Conta: " + codConta);
					String nsu_Cartao = "";
					String autorizacao = "";


					if (codConta != null && codBanco != null) {
						JsonArray formas_de_pagamento = jsonObject.getAsJsonArray("formas_de_pagamento");
						System.out.println("quantidade de formar de pagamento: " + formas_de_pagamento.size());
						LogCatcher.logInfo("quantidade de formar de pagamento: " + formas_de_pagamento.size());
						BigDecimal taxaCartao = BigDecimal.ZERO;

						if (nufin != null && nufin.compareTo(BigDecimal.ZERO) != 0 && formas_de_pagamento.size() == 1) {
							BigDecimal codParcCartao = BigDecimal.ZERO;
							String dtCredito = "";

							for (JsonElement formas_de_pagamentoElement : formas_de_pagamento) {

								JsonObject formas_de_pagamentoObject = formas_de_pagamentoElement.getAsJsonObject();
								System.out.println("Forma de pagamento: "
										+ formas_de_pagamentoObject.get("forma_pagamento_id").getAsString());
								LogCatcher.logInfo("Forma de pagamento: "
										+ formas_de_pagamentoObject.get("forma_pagamento_id").getAsString());



								LogCatcher.logInfo("codemp: " + codemp);
								System.out.println("codemp: " + codemp);
								formaDePagamento = formas_de_pagamentoObject.get("forma_pagamento_id").getAsString()
										.trim();

								String chaveCodTipTit = codemp + "###" + formaDePagamento;
								LogCatcher.logInfo("[DEBUG] Buscando codTipTit com chave: " + chaveCodTipTit);
								codTipTit = Optional.ofNullable(mapaInfTipoTitulo.get(chaveCodTipTit)).orElse(BigDecimal.ZERO);
								System.out.println("[RESULTADO] codTipTit encontrado: " + codTipTit);
								LogCatcher.logInfo("[RESULTADO] codTipTit encontrado: " + codTipTit);

								if (codTipTit.compareTo(BigDecimal.ZERO) != 0) {
									System.out.println("[VALIDAÇÃO] codTipTit é válido: " + codTipTit);
									LogCatcher.logInfo("[VALIDAÇÃO] codTipTit é válido: " + codTipTit);
								} else {
									System.out.println("[VALIDAÇÃO] codTipTit é inválido (ZERO) para forma: " + formaDePagamento);
									LogCatcher.logInfo("[VALIDAÇÃO] codTipTit é inválido (ZERO) para forma: " + formaDePagamento);
								}

								//modificado
								taxaCartao = (BigDecimal) Optional
										.ofNullable((BigDecimal) mapaInfTipoTituloTaxa
												.get(codemp + "###" + formaDePagamento)) // removed "###" + qtdParcelas
										.orElse(BigDecimal.ZERO);
								System.out.println("Taxa Cartão: " + taxaCartao);
								LogCatcher.logInfo("Taxa Cartão: " + taxaCartao);

								nsu_Cartao = (String) Optional.ofNullable(jsonObject.get("forma_pagamento_nsu"))
										.filter((element) -> !element.isJsonNull()).map(JsonElement::getAsString)
										.orElse("");
								autorizacao = (String) Optional
										.ofNullable(jsonObject.get("forma_pagamento_autorizacao"))
										.filter((element) -> !element.isJsonNull()).map(JsonElement::getAsString)
										.orElse("");

								//modificado
								codParcCartao = (BigDecimal) Optional
										.ofNullable((BigDecimal) mapaInfTipoTituloCodparcCartao
												.get(codemp + "###" + formaDePagamento)) // removed "###" + qtdParcelas
										.orElse(BigDecimal.ZERO);
								dtCredito = (String) Optional.ofNullable(jsonObject.get("forma_pagamento_data_credito"))
										.filter((element) -> !element.isJsonNull()).map(JsonElement::getAsString)
										.orElse("");
							}

							System.out.println("[USO] Enviando codTipTit para update/insert: " + codTipTit);
							LogCatcher.logInfo("[USO] Enviando codTipTit para update/insert: " + codTipTit);

							if (taxaCartao.compareTo(BigDecimal.ZERO) != 0) {
								vlrBaixa = vlrBaixa
										.subtract(vlrBaixa.multiply(taxaCartao).divide(BigDecimal.valueOf(100L)));
							}

							System.out.println("estorno: " + dataEstorno);
							LogCatcher.logInfo("estorno: " + dataEstorno);
							System.out.println("Data estorno: " + jsonObject.get("baixa_estorno_data"));
							LogCatcher.logInfo("Data estorno: " + jsonObject.get("baixa_estorno_data"));
							Date dtMinMovConta = (Date) mapaInfMenorDataMovBancariaPorConta
									.get(Long.parseLong(codConta.toString()));
							System.out.println("dtMinMovConta: " + dtMinMovConta);
							LogCatcher.logInfo("dtMinMovConta: " + dtMinMovConta);
							System.out.println("Verificando dados de estorno - dataEstorno: " + dataEstorno);
							LogCatcher.logInfo("Verificando dados de estorno - dataEstorno: " + dataEstorno);
							System.out.println("Status de baixa do titulo: " + mapaInfFinanceiroBaixado.get(nufin));
							LogCatcher.logInfo("Status de baixa do titulo: " + mapaInfFinanceiroBaixado.get(nufin));


							if (dataEstorno != null) {
								LogCatcher.logInfo("[ESTORNO] Iniciando processo de estorno para NUFIN: " + nufin);
								

								BigDecimal nufinDaBaixa =
										mapaBaixaIdParaNufin.get(codemp + "###" + baixaId);

								boolean jaBaixado =
										nufinDaBaixa != null &&
												"S".equalsIgnoreCase(
														mapaInfFinanceiroBaixado.getOrDefault(nufinDaBaixa, "N"));

								if (jaBaixado) {
									System.out.println("[ESTORNO] BaixaId " + baixaId +
											" já estava baixada. NUFIN = " + nufinDaBaixa);
									LogCatcher.logInfo("[ESTORNO] BaixaId " + baixaId +
											" já estava baixada. NUFIN = " + nufinDaBaixa);
									
									estornarTgfFin(nufinDaBaixa, codemp);
									LogCatcher.logInfo("[ESTORNO] Estornando NUFIN: " + nufin + ", Empresa: " + codemp);
									mapaBaixaEstornada.put(codemp + "###" + baixaId, nufinDaBaixa);

								} else {
									System.out.println("[INFO] Estorno antes da baixa. " +
											"BaixaId " + baixaId + " ficará em aberto.");
									LogCatcher.logInfo("[INFO] Estorno antes da baixa. " +
											"BaixaId " + baixaId + " ficará em aberto.");
									selectsParaInsert.add(
											"SELECT <#NUMUNICO#>, " +
													"'Estorno ignorado: baixa " + baixaId + " ainda não registrada', " +
													"SYSDATE, 'Aviso', " + codemp + ", '" + idAluno + "' FROM DUAL");
									LogCatcher.logInfo(
											"SELECT <#NUMUNICO#>, " +
													"'Estorno ignorado: baixa " + baixaId + " ainda não registrada', " +
													"SYSDATE, 'Aviso', " + codemp + ", '" + idAluno + "' FROM DUAL");
								}
							}



							else if (dtMinMovConta != null) {
								System.out.println("[BAIXA-NORMAL] Processando baixa normal (sem estorno)");
								System.out.println("[BAIXA-NORMAL] TituloId:" + tituloId + ", NUFIN:" + nufin);

								if (!data.equals(dtMinMovConta) && !data.after(dtMinMovConta)) {
									this.selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Baixa Para o Titulo: " + nufin
											+ " Não Efetuada Pois a Data Minima de Movimentação Bancaria "
											+ "Para a Conta " + codConta + " é Superior a Data de Baixa: "
											+ dataBaixaFormatada + "', SYSDATE, 'Aviso', " + codemp + ", '" + idAluno
											+ "' FROM DUAL");
								} else if (codTipTit != null && codTipTit.compareTo(BigDecimal.ZERO) != 0) {
									if ("N".equalsIgnoreCase((String) mapaInfFinanceiroBaixado.get(nufin))) {
										System.out.println("Chegou no update");
										if (vlrBaixa.compareTo((BigDecimal) mapaInfFinanceiroValor.get(nufin)) != 0
												|| nsu_Cartao != null && !nsu_Cartao.isEmpty()) {
											if (nsu_Cartao != null && !nsu_Cartao.isEmpty()) {

												this.updateFinCartao(codTipTit, nufin, codBanco, codConta, vlrBaixa, vlrDesconto, vlrJuros, vlrMulta, vlrOutrosAcrescimos, baixaId, codemp, codParc, dtCredito, nsu_Cartao, autorizacao);
											} else {
												LogCatcher.logInfo("Atualizando NUFIN: " + nufin + " com valor: " + vlrBaixa);

												this.updateFinComVlrBaixa(codTipTit, nufin, codBanco, codConta,
														vlrBaixa, vlrDesconto, vlrJuros, vlrMulta, vlrOutrosAcrescimos,
														baixaId, codemp);
											}
										} else {
											System.out.println("Entrou no if do valor");
											this.updateFinComVlrBaixa(codTipTit, nufin, codBanco, codConta, vlrBaixa,
													vlrDesconto, vlrJuros, vlrMulta, vlrOutrosAcrescimos, baixaId,
													codemp);
										}

										System.out.println("vlrDesconto: " + vlrDesconto);
										System.out.println("vlrJuros: " + vlrJuros);
										System.out.println("vlrMulta: " + vlrMulta);

										if (nsu_Cartao == null || nsu_Cartao.isEmpty()) {
											nubco = this.insertMovBancaria(codConta, vlrBaixa, nufin,
													dataBaixaFormatada, codemp);
											System.out.println("Passou da mov bancaria: " + nubco);
											System.out.println("vlrBaixa: " + vlrBaixa);
											this.updateBaixa(nufin, nubco, vlrBaixa, dataBaixaFormatada, baixaId,
													codemp);

											LogCatcher.logInfo("[UPDATE] Baixa realizada - NUFIN: " + nufin +
													", NUBCO: " + nubco +
													", Valor: " + vlrBaixa +
													", Data: " + dataBaixaFormatada +
													", Baixa ID: " + baixaId +
													", Empresa: " + codemp);

											mapIdBaixaAtual.put(nufin, baixaId);
											mapaInfFinanceiroBaixado.put(nufin, "S");
											movBanc = true;
										}
									} else {
										System.out.println("Titulo ja baixado");
										String baixaIdExist = (String) Optional
												.ofNullable((String) mapaInfIdBaixa.get(nufin)).orElse("");
										String baixaIdAtual = (String) Optional
												.ofNullable((String) mapIdBaixaAtual.get(nufin)).orElse("");
										BigDecimal baixaIdOrig = (BigDecimal) Optional
												.ofNullable(
														(BigDecimal) mapaInfIdBaixaOrig.get(baixaId + "###" + nufin))
												.orElse(BigDecimal.ZERO);
										System.out.println("baixaIdExist: " + baixaIdExist);
										System.out.println("baixaIdAtual: " + baixaIdAtual);
										System.out.println("baixaIdOrig: " + baixaIdOrig);
										if (!baixaIdExist.isEmpty() && !baixaIdExist.equalsIgnoreCase(baixaId)
												&& !baixaIdExist.equalsIgnoreCase("N")
												&& baixaIdOrig.compareTo(BigDecimal.ZERO) == 0
												|| !baixaIdAtual.isEmpty() && !baixaIdAtual.equalsIgnoreCase(baixaId)) {
											LogCatcher.logInfo("[DUPLICIDADE] Baixa duplicada detectada para NUFIN: " + nufin + " e baixa ID: " + baixaId);

											BigDecimal nufinDup = this.insertFin(nufin, vlrBaixa, codTipTit, codemp);

											if (nsu_Cartao == null || nsu_Cartao.isEmpty()) {
												this.updateFinComVlrBaixa(codTipTit, nufinDup, codBanco, codConta,
														vlrBaixa, vlrDesconto, vlrJuros, vlrMulta, vlrOutrosAcrescimos,
														baixaId, codemp);
												nubco = this.insertMovBancaria(codConta, vlrBaixa, nufinDup,
														dataBaixaFormatada, codemp);
												movBanc = true;
												this.updateBaixa(nufinDup, nubco, vlrBaixa, dataBaixaFormatada, baixaId,
														codemp);
												LogCatcher.logInfo("[UPDATE DUPLICADA] Baixa duplicada registrada - NUFIN: " + nufinDup +
														", NUBCO: " + nubco +
														", Valor: " + vlrBaixa +
														", Data: " + dataBaixaFormatada +
														", Baixa ID: " + baixaId +
														", Empresa: " + codemp);
												mapaInfFinanceiroBaixado.put(nufinDup, "S");
												mapIdBaixaAtual.put(nufinDup, baixaId);
												mapaInfIdBaixaOrig.put(baixaId + "###" + nufin, nufinDup);
											}

											System.out.println("Fim baixa dupla");
										}
									}
								} else {
									this.selectsParaInsert.add(
											"SELECT <#NUMUNICO#>, 'Sem \"de para\" de Tipo de Titulo Configurado Para o Metodo de Pagamento (1): "
													+ formaDePagamento + "' , SYSDATE, 'Aviso', " + codemp
													+ ", '' FROM DUAL");
									LogCatcher.logInfo(
											"SELECT <#NUMUNICO#>, 'Sem \"de para\" de Tipo de Titulo Configurado Para o Metodo de Pagamento (1): "
													+ formaDePagamento + "' , SYSDATE, 'Aviso', " + codemp
													+ ", '' FROM DUAL");
								}
							} else {
								this.selectsParaInsert.add(
										"SELECT <#NUMUNICO#>, 'Data Minima de Injeção de Saldo Não Localizada Para a Conta: "
												+ codConta + "' , SYSDATE, 'Aviso', " + codemp + ", '" + idAluno
												+ "' FROM DUAL");

								LogCatcher.logInfo(
										"SELECT <#NUMUNICO#>, 'Data Minima de Injeção de Saldo Não Localizada Para a Conta: "
												+ codConta + "' , SYSDATE, 'Aviso', " + codemp + ", '" + idAluno
												+ "' FROM DUAL");
							}
						} else if (nufin != null && nufin.compareTo(BigDecimal.ZERO) != 0
								&& formas_de_pagamento.size() > 1) {
							System.out.println("entrou em mais de uma forma de pagamento");
							LogCatcher.logInfo("entrou em mais de uma forma de pagamento");
							int countBaixa = 0;

							for (JsonElement formas_de_pagamentoElement : formas_de_pagamento) {
								JsonObject formas_de_pagamentoObject = formas_de_pagamentoElement.getAsJsonObject();
								System.out.println("Forma de pagamento: "
										+ formas_de_pagamentoObject.get("forma_pagamento_id").getAsString());

								LogCatcher.logInfo("Forma de pagamento: "
										+ formas_de_pagamentoObject.get("forma_pagamento_id").getAsString());


								System.out.println("codemp: " + codemp);
								LogCatcher.logInfo("codemp: " + codemp);


								// Trecho modificado para lidar com qtdParcelas ausente em formas_de_pagamento > 1
								BigDecimal qtdParcelas = BigDecimal.ZERO;
								if (formas_de_pagamentoObject.has("forma_pagamento_qtdparcelas") &&
										!formas_de_pagamentoObject.get("forma_pagamento_qtdparcelas").isJsonNull()) {
									qtdParcelas = new BigDecimal(formas_de_pagamentoObject.get("forma_pagamento_qtdparcelas").getAsString());
								}
								String formaPagamento = formas_de_pagamentoObject.get("forma_pagamento_id").getAsString().trim();

								String chaveComParcelas = codemp + "###" + formaPagamento + (qtdParcelas.compareTo(BigDecimal.ZERO) > 0 ? "###" + qtdParcelas : "");
								System.out.println("[DEBUG] Buscando codTipTit com chave: " + chaveComParcelas);
								LogCatcher.logInfo("[DEBUG] Buscando codTipTit com chave: " + chaveComParcelas);
								codTipTit = Optional.ofNullable(mapaInfTipoTitulo.get(chaveComParcelas)).orElse(BigDecimal.ZERO);
								System.out.println("[RESULTADO] codTipTit (com fallback): " + codTipTit);
								LogCatcher.logInfo("[RESULTADO] codTipTit (com fallback): " + codTipTit);
								if (codTipTit.compareTo(BigDecimal.ZERO) != 0) {
									System.out.println("[VALIDAÇÃO] codTipTit é válido: " + codTipTit);
									LogCatcher.logInfo("[VALIDAÇÃO] codTipTit é válido: " + codTipTit);
								} else {
									LogCatcher.logInfo("[VALIDAÇÃO] codTipTit é inválido (ZERO) para chave: " + chaveComParcelas);
									System.out.println("[VALIDAÇÃO] codTipTit é inválido (ZERO) para chave: " + chaveComParcelas);
								}


								LogCatcher.logInfo("[DEBUG] Buscando codTipTit para múltiplas formas com chave: " + chaveComParcelas);
								System.out.println("[DEBUG] Buscando codTipTit para múltiplas formas com chave: " + chaveComParcelas);
								codTipTit = Optional.ofNullable(mapaInfTipoTitulo.get(chaveComParcelas)).orElse(BigDecimal.ZERO);
								LogCatcher.logInfo("[RESULTADO] codTipTit (múltiplas formas): " + codTipTit);
								System.out.println("[RESULTADO] codTipTit (múltiplas formas): " + codTipTit);
								if (codTipTit.compareTo(BigDecimal.ZERO) != 0) {
									System.out.println("[VALIDAÇÃO] codTipTit é válido (múltiplas formas): " + codTipTit);
									LogCatcher.logInfo("[VALIDAÇÃO] codTipTit é válido (múltiplas formas): " + codTipTit);
								} else {
									LogCatcher.logInfo("[VALIDAÇÃO] codTipTit é inválido (ZERO) para forma: " + formaPagamento + " e parcelas: " + qtdParcelas);
									System.out.println("[VALIDAÇÃO] codTipTit é inválido (ZERO) para forma: " + formaPagamento + " e parcelas: " + qtdParcelas);
								}


								LogCatcher.logInfo("qtd Parcelas: " + qtdParcelas);
								System.out.println("qtd Parcelas: " + qtdParcelas);

//								LogCatcher.logInfo("Inicio do Mapa " + codTipTit);
//
//								for (Entry<String, BigDecimal> entry : mapaInfTipoTitulo.entrySet()) {
//									String key = entry.getKey();
//									BigDecimal val = entry.getValue();
//									LogCatcher.logInfo("Entidade: " + key + " " + val);
//								}
//								LogCatcher.logInfo("Fim  do Mapa " + codTipTit);

								LogCatcher.logInfo("String construída: " + codemp + "###"
									    + formas_de_pagamentoObject.get("forma_pagamento_id").getAsString().trim());


								codTipTit = (BigDecimal) Optional
									    .ofNullable(
									        (BigDecimal) mapaInfTipoTitulo.get(
									            codemp + "###" + formas_de_pagamentoObject.get("forma_pagamento_id").getAsString().trim()
									        )
									    )
									    .orElse(BigDecimal.ZERO);



								LogCatcher.logInfo("Tipo de titulo depois do chaveamento:" + codTipTit);
								System.out.println("Tipo de titulo:" + codTipTit);

								taxaCartao = (BigDecimal) Optional
										.ofNullable(
												(BigDecimal) mapaInfTipoTituloTaxa.get(codemp + "###"
														+ formas_de_pagamentoObject.get("forma_pagamento_id")
														.getAsString()
														+ "###" + qtdParcelas))
										.orElse(BigDecimal.ZERO);
								nsu_Cartao = (String) Optional
										.ofNullable(formas_de_pagamentoObject.get("forma_pagamento_nsu"))
										.filter((element) -> !element.isJsonNull()).map(JsonElement::getAsString)
										.orElse("");
								autorizacao = (String) Optional
										.ofNullable(formas_de_pagamentoObject.get("forma_pagamento_autorizacao"))
										.filter((element) -> !element.isJsonNull()).map(JsonElement::getAsString)
										.orElse("");
								BigDecimal codParcCartao = (BigDecimal) Optional
										.ofNullable(
												(BigDecimal) mapaInfTipoTituloCodparcCartao.get(codemp + "###"
														+ formas_de_pagamentoObject.get("forma_pagamento_id")
														.getAsString()
														+ "###" + qtdParcelas))
										.orElse(BigDecimal.ZERO);
								String dtCredito = (String) Optional
										.ofNullable(formas_de_pagamentoObject.get("forma_pagamento_data_credito"))
										.filter((element) -> !element.isJsonNull()).map(JsonElement::getAsString)
										.orElse("");
								vlrBaixa = formas_de_pagamentoObject.get("forma_pagamento_valor").getAsBigDecimal();
								if (taxaCartao.compareTo(BigDecimal.ZERO) != 0) {
									vlrBaixa.subtract(vlrBaixa.multiply(taxaCartao).divide(BigDecimal.valueOf(100L)));
								}


								LogCatcher.logInfo("estorno: " + dataEstorno);
								LogCatcher.logInfo("Data estorno: " + jsonObject.get("baixa_estorno_data"));

								System.out.println("estorno: " + dataEstorno);
								System.out.println("Data estorno: " + jsonObject.get("baixa_estorno_data"));

								Date dtMinMovConta = (Date) mapaInfMenorDataMovBancariaPorConta
										.get(Long.parseLong(codConta.toString()));
								System.out.println("dtMinMovConta: " + dtMinMovConta);
								LogCatcher.logInfo("dtMinMovConta: " + dtMinMovConta);
								if (countBaixa == 0) {
									System.out.println("contagem 1");
									if (dataEstorno == null) {
										if (dtMinMovConta != null) {
											if (!data.equals(dtMinMovConta) && !data.after(dtMinMovConta)) {
												this.selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Baixa Para o Titulo: "
														+ nufin
														+ " Não Efetuada Pois a Data Minima de Movimentação Bancaria "
														+ "Para a Conta " + codConta + " é Superior a Data de Baixa: "
														+ dataBaixaFormatada + "', SYSDATE, 'Aviso', " + codemp + ", '"
														+ idAluno + "' FROM DUAL");

												LogCatcher.logInfo("SELECT <#NUMUNICO#>, 'Baixa Para o Titulo: "
														+ nufin
														+ " Não Efetuada Pois a Data Minima de Movimentação Bancaria "
														+ "Para a Conta " + codConta + " é Superior a Data de Baixa: "
														+ dataBaixaFormatada + "', SYSDATE, 'Aviso', " + codemp + ", '"
														+ idAluno + "' FROM DUAL");
											} else if (codTipTit != null && codTipTit.compareTo(BigDecimal.ZERO) != 0) {
												if ("N".equalsIgnoreCase(
														(String) mapaInfFinanceiroBaixado.get(nufin))) {
													System.out.println("Chegou no update");
													LogCatcher.logInfo("Chegou no update");
													if (vlrBaixa.compareTo(
															(BigDecimal) mapaInfFinanceiroValor.get(nufin)) != 0
															|| nsu_Cartao != null && !nsu_Cartao.isEmpty()) {
														if (nsu_Cartao != null && !nsu_Cartao.isEmpty()) {
															//substituido por
															this.updateFinCartao(codTipTit, nufin, codBanco, codConta, vlrBaixa, vlrDesconto, vlrJuros, vlrMulta, vlrOutrosAcrescimos, baixaId, codemp, codParc, dtCredito, nsu_Cartao, autorizacao);
														} else {
															System.out.println("Entrou no else do valor");
															LogCatcher.logInfo("Entrou no else do valor");
															this.updateFinComVlrBaixa(codTipTit, nufin, codBanco,
																	codConta, vlrBaixa, vlrDesconto, vlrJuros, vlrMulta,
																	vlrOutrosAcrescimos, baixaId, codemp);
														}
													} else {
														LogCatcher.logInfo("Entrou no if do valor");
														System.out.println("Entrou no if do valor");
														this.updateFinComVlrBaixa(codTipTit, nufin, codBanco, codConta,
																vlrBaixa, vlrDesconto, vlrJuros, vlrMulta,
																vlrOutrosAcrescimos, baixaId, codemp);
													}

													System.out.println("vlrDesconto: " + vlrDesconto);
													System.out.println("vlrJuros: " + vlrJuros);
													System.out.println("vlrMulta: " + vlrMulta);

													LogCatcher.logInfo("vlrDesconto: " + vlrDesconto);
													LogCatcher.logInfo("vlrJuros: " + vlrJuros);
													LogCatcher.logInfo("vlrMulta: " + vlrMulta);

													if (nsu_Cartao == null || nsu_Cartao.isEmpty()) {
														nubco = this.insertMovBancaria(codConta, vlrBaixa, nufin,
																dataBaixaFormatada, codemp);
														System.out.println("Passou da mov bancaria: " + nubco);
														System.out.println("vlrBaixa: " + vlrBaixa);

														LogCatcher.logInfo("Passou da mov bancaria: " + nubco);
														LogCatcher.logInfo("vlrBaixa: " + vlrBaixa);

														this.updateBaixaParcial(nufin, nubco, vlrBaixa,
																dataBaixaFormatada, codemp);
														movBanc = true;
														mapaInfFinanceiroBaixado.put(nufin, "S");
														mapIdBaixaAtual.put(nufin, baixaId);
													}

													++countBaixa;
												} else {
													System.out.println("Financeiro " + nufin + " já baixado");
													LogCatcher.logInfo("Financeiro " + nufin + " já baixado");
													String baixaIdExist = (String) Optional
															.ofNullable((String) mapaInfIdBaixa.get(nufin)).orElse("");
													String baixaIdAtual = (String) Optional
															.ofNullable((String) mapIdBaixaAtual.get(nufin)).orElse("");
													BigDecimal baixaIdOrig = (BigDecimal) Optional
															.ofNullable((BigDecimal) mapaInfIdBaixaOrig
																	.get(baixaId + "###" + nufin))
															.orElse(BigDecimal.ZERO);
													System.out.println("baixaIdExist: " + baixaIdExist);
													System.out.println("baixaIdAtual: " + baixaIdAtual);
													System.out.println("baixaIdOrig: " + baixaIdOrig);

													LogCatcher.logInfo("baixaIdExist: " + baixaIdExist);
													LogCatcher.logInfo("baixaIdAtual: " + baixaIdAtual);
													LogCatcher.logInfo("baixaIdOrig: " + baixaIdOrig);

													if (!baixaIdExist.isEmpty()
															&& !baixaIdExist.equalsIgnoreCase(baixaId)
															&& !baixaIdExist.equalsIgnoreCase("N")
															&& baixaIdOrig.compareTo(BigDecimal.ZERO) == 0
															|| !baixaIdAtual.isEmpty()
															&& !baixaIdAtual.equalsIgnoreCase(baixaId)) {
														System.out.println("Baixa Dupla");
														if (nsu_Cartao != null && !nsu_Cartao.isEmpty()) {
															this.insertFinCartao(nufin, vlrBaixa, codTipTit, codemp, codParc, dtCredito, baixaId);
															++countBaixa;


														} else {
															BigDecimal nufinDup = this.insertFin(nufin, vlrBaixa, codTipTit, codemp);
															this.updateFinComVlrBaixa(codTipTit, nufinDup, codBanco,
																	codConta, vlrBaixa, vlrDesconto, vlrJuros, vlrMulta,
																	vlrOutrosAcrescimos, baixaId, codemp);
															nubco = this.insertMovBancaria(codConta, vlrBaixa, nufinDup,
																	dataBaixaFormatada, codemp);
															movBanc = true;
															this.updateBaixa(nufinDup, nubco, vlrBaixa,
																	dataBaixaFormatada, baixaId, codemp);
															LogCatcher.logInfo("[UPDATE PARCIAL] Baixa parcial por múltiplas formas - NUFIN: " + nufinDup +
																	", NUBCO: " + nubco +
																	", Valor: " + vlrBaixa +
																	", Data: " + dataBaixaFormatada +
																	", Baixa ID: " + baixaId +
																	", Empresa: " + codemp);
															mapaInfFinanceiroBaixado.put(nufinDup, "S");
															mapaInfIdBaixaOrig.put(baixaId + "###" + nufin, nufinDup);
															++countBaixa;
															LogCatcher.logInfo("Atualizando baixa com NUBCO: " + nubco + " | NUFIN: " + nufin + " | Baixa ID: " + baixaId);

														}

														System.out.println("Fim baixa dupla");
														LogCatcher.logInfo("Fim baixa dupla");
													}
												}
											} else {
												this.selectsParaInsert.add(
														"SELECT <#NUMUNICO#>, 'Sem \"de para\" de Tipo de Titulo Configurado Para o Metodo de Pagamento (2)"
																+ formas_de_pagamentoObject.get("forma_pagamento_id")
																+ "' , SYSDATE, 'Aviso', " + codemp + ", '" + idAluno + "' FROM DUAL");

												LogCatcher.logInfo(
														"SELECT <#NUMUNICO#>, 'Sem \"de para\" de Tipo de Titulo Configurado Para o Metodo de Pagamento (2): "
																+ formas_de_pagamentoObject.get("forma_pagamento_id")
																+ "' , SYSDATE, 'Aviso', " + codemp + ", '" + idAluno
																+ "' FROM DUAL");
											}
										} else {
											this.selectsParaInsert.add(
													"SELECT <#NUMUNICO#>, 'Data Minima de Injeção de Saldo Não Localizada Para a Conta: "
															+ codConta + "' , SYSDATE, 'Aviso', " + codemp + ", '"
															+ idAluno + "' FROM DUAL");
											LogCatcher.logInfo(
													"SELECT <#NUMUNICO#>, 'Data Minima de Injeção de Saldo Não Localizada Para a Conta: "
															+ codConta + "' , SYSDATE, 'Aviso', " + codemp + ", '"
															+ idAluno + "' FROM DUAL");
										}
									} else if ("S".equalsIgnoreCase((String) mapaInfFinanceiroBaixado.get(nufin))) {
										nubco = (BigDecimal) mapaInfFinanceiroBanco.get(nufin);
										this.estornarTgfFin(nufin, codemp);
									}
								} else if (codTipTit != null && codTipTit.compareTo(BigDecimal.ZERO) != 0
										&& countBaixa > 0) {
									System.out.println("contagem 2");
									if (nsu_Cartao != null && !nsu_Cartao.isEmpty()) {
										this.insertFinCartao(nufin, vlrBaixa, codTipTit, codemp, codParc, dtCredito, baixaId);
									} else {
										BigDecimal nufinDup = this.insertFin(nufin, vlrBaixa, codTipTit, codemp);
										nubco = this.insertMovBancaria(codConta, vlrBaixa, nufinDup, dataBaixaFormatada,
												codemp);
										movBanc = true;
										System.out.println("Passou da mov bancaria duplicada: " + nubco);
										System.out.println("vlrBaixa: " + vlrBaixa);

										LogCatcher.logInfo("Passou da mov bancaria duplicada: " + nubco);
										LogCatcher.logInfo("vlrBaixa: " + vlrBaixa);

										this.updateBaixa(nufinDup, nubco, vlrBaixa, dataBaixaFormatada, baixaId,
												codemp);
										mapaInfFinanceiroBaixado.put(nufinDup, "S");
										mapIdBaixaAtual.put(nufinDup, baixaId);
									}

									++countBaixa;
								}
							}
						} else {
							System.out.println("Não foi possivel encontrar financeiro com id externo " + tituloId);
							LogCatcher.logError("Não foi possivel encontrar financeiro com id externo " + tituloId);
						}
					} else {
						this.selectsParaInsert
								.add("SELECT <#NUMUNICO#>, 'Sem \"de para\" Configurado para o local de pagamento (3): "
										+ idExterno + "' , SYSDATE, 'Aviso', " + codemp + ", '' FROM DUAL");
						LogCatcher.logError("SELECT <#NUMUNICO#>, 'Sem \"de para\" Configurado para o local de pagamento: "
								+ idExterno + "' , SYSDATE, 'Aviso', " + codemp + ", '' FROM DUAL");
					}
				}

				movBanc = false;
				nubco = BigDecimal.ZERO;
			}
		} catch (Exception e) {
			LogCatcher.logError(e);
			e.printStackTrace();
			LogCatcher.logError("[ERRO] Erro durante execução de efetuarBaixa: " + e.getMessage());

			if (movBanc) {
				this.estornarTgfFin(nufin, codemp);
				LogCatcher.logInfo("Apagou mov bank");
				System.out.println("Apagou mov bank");
			}

			this.selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Mensagem de erro nas Baixas: " + e.getMessage()
					+ "', SYSDATE, 'Erro', " + codemp + ", '" + idAluno + "' FROM DUAL");
			LogCatcher.logError("SELECT <#NUMUNICO#>, 'Mensagem de erro nas Baixas: " + e.getMessage()
					+ "', SYSDATE, 'Erro', " + codemp + ", '" + idAluno + "' FROM DUAL");

			System.out.println("Erro de parse JSON: " + e.getMessage());
			System.out.println("Conteúdo que causou o erro: " + response[1]);


			LogCatcher.logError("Erro de parse JSON: " + e.getMessage());
			LogCatcher.logError("Conteúdo que causou o erro: " + response[1]);

			throw e;
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

	public void updateFin(BigDecimal codtiptit, BigDecimal nufin, BigDecimal codBanco, BigDecimal codConta,
						  BigDecimal vlrDesconto, BigDecimal vlrJuros, BigDecimal vlrMulta, BigDecimal vlrOutrosAcrescimos,
						  BigDecimal codemp) throws Exception {
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
		} catch (SQLException e) {
			e.printStackTrace();
			this.selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro Ao Atualizar Financeiro Para baixa: "
					+ e.getMessage() + "' , SYSDATE, 'Erro', " + codemp + ", '' FROM DUAL");
			LogCatcher.logInfo("SELECT <#NUMUNICO#>, 'Erro Ao Atualizar Financeiro Para baixa: "
					+ e.getMessage() + "' , SYSDATE, 'Erro', " + codemp + ", '' FROM DUAL");

		} finally {
			if (pstmt != null) {
				pstmt.close();
			}

			jdbc.closeSession();
		}

	}



	public void estornarTgfFin(BigDecimal nufin, BigDecimal codemp) throws Exception {
	    EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	    JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
	    PreparedStatement stmt = null;

	    try {
	        LogCatcher.logInfo("\nIniciando estorno para NUFIN: " + nufin + " - Empresa: " + codemp);

	        jdbc.openSession();

	        DynamicVO finVO = getTituloVO(nufin);
	        if (finVO == null) {
	            LogCatcher.logInfo("Título financeiro não encontrado: " + nufin);
	            throw new Exception("Título financeiro não encontrado: " + nufin);
	        }

	        BigDecimal nubco = finVO.asBigDecimal("NUBCO");
	        BigDecimal vlrBaixa = finVO.asBigDecimal("VLRBAIXA");
	        String isCartao = finVO.asString("AD_BAIXA_CARTAO");

	        if ((nubco == null || nubco.compareTo(BigDecimal.ZERO) == 0)
	                && !"S".equalsIgnoreCase(isCartao)
	                && (vlrBaixa == null || vlrBaixa.compareTo(BigDecimal.ZERO) > 0)) {

	            String msg = "Movimento Bancário não encontrado para o título: " + nufin;
	            LogCatcher.logInfo("[ALERTA] " + msg);

	            this.selectsParaInsert.add("SELECT <#NUMUNICO#>, '" + msg.replace("'", "''")
	                    + "' , SYSDATE, 'Aviso', " + codemp + ", '' FROM DUAL");

	            LogCatcher.logInfo("SELECT <#NUMUNICO#>, '" + msg.replace("'", "''")
	                    + "' , SYSDATE, 'Aviso', " + codemp + ", '' FROM DUAL");
	        }

	        EstornoHelper.EstornoParam estornoParam = new EstornoHelper.EstornoParam();
	        estornoParam.setNuFin(nufin);
	        estornoParam.setRecompoe(true);
	        estornoParam.setTodosAntecipacao(false);
	        estornoParam.setResourceID("0");
	        estornoParam.setIgnorarValidacaoUsuarioCaixa(true);
	        estornoParam.setContaParaCaixaAberto(BigDecimal.ZERO);

	        AuthenticationInfo auth = AuthenticationInfo.getCurrent();

	        EstornoHelper estornoHelper = new EstornoHelper(entityFacade);
	        estornoHelper.estornarTitulo(auth, entityFacade, jdbc, estornoParam);

	        LogCatcher.logInfo("Chamando estornarTitulo para NUFIN: " + nufin);

	        // Inserção na tabela AD_ESTORNOINT ANTES de deletar
	        String insertSql = "INSERT INTO ad_estornoint (titulo_id, status_estorno, descricao) VALUES (?, ?, ?)";
	        stmt = jdbc.getConnection().prepareStatement(insertSql);
	        stmt.setBigDecimal(1, nufin);
	        stmt.setString(2, "E");
	        stmt.setString(3, "Estorno automático via rotina");

	        int inserted = stmt.executeUpdate();
	        LogCatcher.logInfo("Registro inserido em ad_estornoint para NUFIN: " + nufin + " - Linhas inseridas: " + inserted);

	        deletarTitulo(nufin);
	        LogCatcher.logInfo("Título estornado e removido da TGFFIN com sucesso: " + nufin);
	        LogCatcher.logInfo("Título estornado com sucesso: " + nufin);

	    } catch (Exception e) {
	        LogCatcher.logInfo("[ERRO] Falha ao estornar título NUFIN: " + nufin + " - Erro: " + e.getMessage());
	        e.printStackTrace();
	        this.selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro Ao Estornar Título: "
	                + e.getMessage().replace("'", "''")
	                + "' , SYSDATE, 'Erro', " + codemp + ", '' FROM DUAL");
	        LogCatcher.logError("SELECT <#NUMUNICO#>, 'Erro Ao Estornar Título: "
	                + e.getMessage().replace("'", "''")
	                + "' , SYSDATE, 'Erro', " + codemp + ", '' FROM DUAL");
	        throw e;
	    } finally {
	        JdbcUtils.closeStatement(stmt);
	        jdbc.closeSession();
	    }
	}




	private DynamicVO getTituloVO(BigDecimal nufin) throws Exception {
		EntityFacade dwfFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = dwfFacade.getJdbcWrapper();
		jdbc.openSession();
		try {
			JapeWrapper finDAO = JapeFactory.dao("Financeiro");
			DynamicVO finVO = finDAO.findOne("NUFIN = ?", new Object[] { nufin });
			if (finVO != null) {

				LogCatcher.logInfo("Título localizado - NUFIN: " + nufin + ", NUBCO: " + finVO.asBigDecimal("NUBCO"));

			} else {
				LogCatcher.logInfo("Título não localizado no getTituloVO - NUFIN: " + nufin);

			}
			return finVO;
		} finally {
			jdbc.closeSession();
		}
	}

	private void deletarTitulo(BigDecimal nufin) throws Exception {
		JdbcWrapper jdbc = EntityFacadeFactory.getDWFFacade().getJdbcWrapper();

		jdbc.openSession();
		String sql = "DELETE FROM TGFFIN WHERE NUFIN = ?";

		try (PreparedStatement pstmt = jdbc.getPreparedStatement(sql)) {
			pstmt.setBigDecimal(1, nufin);
			pstmt.executeUpdate();
		}
	}




	public void updateFinComVlrBaixa(BigDecimal codtiptit, BigDecimal nufin, BigDecimal codBanco, BigDecimal codConta,
									 BigDecimal vlrBaixa, BigDecimal vlrDesconto, BigDecimal vlrJuros, BigDecimal vlrMulta,
									 BigDecimal vlrOutrosAcrescimos, String baixaId, BigDecimal codemp) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		try {
			jdbc.openSession();
			String sqlNota = "UPDATE TGFFIN SET CODTIPTIT = ?, CODBCO = ?, CODCTABCOINT = ?, AD_VLRDESCINT = "
					+ vlrDesconto + ", " + "VLRINSS = 0, " + "VLRIRF = 0, " + "VLRISS = 0, " + "AD_VLRJUROSINT = "
					+ vlrJuros + ", " + "AD_VLRMULTAINT = " + vlrMulta + ", "
					+ "TIPJURO = null, AD_VLRORIG = VLRDESDOB, " + "VLRDESDOB = " + vlrBaixa + ", "
					+ "TIPMULTA = null, AD_OUTACRESCIMOS = " + vlrOutrosAcrescimos + ", AD_BAIXAID = " + baixaId
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
			LogCatcher.logError("SELECT <#NUMUNICO#>, 'Erro Ao Atualizar Titulo Para Baixa: " + e.getMessage().replace("'", "''") + "' , SYSDATE, 'Erro', " + codemp + ", '' FROM DUAL");
			this.selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro Ao Atualizar Titulo Para Baixa: " + e.getMessage().replace("'", "''") + "' , SYSDATE, 'Erro', " + codemp + ", '' FROM DUAL");
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}

			jdbc.closeSession();
		}

	}


	public void updateFinCartao(BigDecimal codTipTit, BigDecimal nufin, BigDecimal codBanco, BigDecimal codConta,
								BigDecimal vlrBaixa, BigDecimal vlrDesconto, BigDecimal vlrJuros, BigDecimal vlrMulta,
								BigDecimal vlrOutrosAcrescimos, String baixaId, BigDecimal codemp,
								BigDecimal codParc, String dtCredito, String nsu_Cartao, String autorizacao) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		try {
			jdbc.openSession();
			String sqlNota = "UPDATE TGFFIN SET CODTIPTIT = ?, CODBCO = ?, CODCTABCOINT = ?, AD_VLRDESCINT = " + vlrDesconto + ", " + "VLRINSS = 0, " + "VLRIRF = 0, " + "VLRISS = 0, " + "AD_VLRJUROSINT = " + vlrJuros + ", " + "AD_VLRMULTAINT = " + vlrMulta + ", " + "TIPJURO = null, AD_VLRORIG = VLRDESDOB, " + "VLRDESDOB = " + vlrBaixa + ", " + "TIPMULTA = null, AD_OUTACRESCIMOS = " + vlrOutrosAcrescimos + ", " + "AD_BAIXAID = " + baixaId + ", CODPARC = " + codParc + ", " + "AD_BAIXA_CARTAO = 'S', DTVENC = TO_DATE('" + dtCredito + "', 'YYYY-MM-DD')," + "AD_NSU_CART = '" + nsu_Cartao + "', AD_AUTORIZACAO_CART = '" + autorizacao + "' WHERE nufin = ?";
			pstmt = jdbc.getPreparedStatement(sqlNota);
			pstmt.setBigDecimal(1, codTipTit); // Corrigido de codtiptit para codTipTit
			pstmt.setBigDecimal(2, codBanco);
			pstmt.setBigDecimal(3, codConta);
			pstmt.setBigDecimal(4, nufin);
			pstmt.executeUpdate();
			System.out.println("Passou do update");
		} catch (SQLException e) {
			e.printStackTrace();
			LogCatcher.logError("SELECT <#NUMUNICO#>, 'Erro Ao Atualizar Titulo Para Baixa de Cartão: " + e.getMessage() + "' , SYSDATE, 'Erro', " + codemp + ", '' FROM DUAL");
			this.selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro Ao Atualizar Titulo Para Baixa de Cartão: " + e.getMessage() + "' , SYSDATE, 'Erro', " + codemp + ", '' FROM DUAL");
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}

			jdbc.closeSession();
		}
	}


	public void updateBaixa(BigDecimal nufin, BigDecimal nubco, BigDecimal vlrDesdob, String dataBaixaFormatada,
							String baixaId, BigDecimal codemp) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		try {
			jdbc.openSession();

			String sqlNota = "UPDATE TGFFIN SET VLRBAIXA = " + vlrDesdob + ", " + "DHBAIXA = '" + dataBaixaFormatada
					+ "', " + "NUBCO = " + nubco + ", " + "CODTIPOPERBAIXA = 1400, "
					+ "DHTIPOPERBAIXA = (SELECT MAX(DHALTER) FROM TGFTOP WHERE CODTIPOPER = 1400), "
					+ "CODUSUBAIXA = 0, AD_BAIXAID = " + baixaId + " " + "WHERE NUFIN = " + nufin;

			LogCatcher.logInfo("[UPDATE TGFFIN] Executando atualização da baixa - NUFIN: " + nufin + ", NUBCO: " + nubco
					+ ", VLRBAIXA: " + vlrDesdob + ", DHBAIXA: " + dataBaixaFormatada + ", AD_BAIXAID: " + baixaId
					+ ", Empresa: " + codemp);
			LogCatcher.logInfo("[SQL EXECUTADO] " + sqlNota);

			pstmt = jdbc.getPreparedStatement(sqlNota);
			int linhasAfetadas = pstmt.executeUpdate();

			LogCatcher.logInfo("[UPDATE TGFFIN] Linhas afetadas: " + linhasAfetadas);
			System.out.println("Passou do update");

		} catch (SQLException e) {
			e.printStackTrace();
			String msgErro = "Erro Ao Baixar Titulo: " + e.getMessage();
			LogCatcher
					.logError("SELECT <#NUMUNICO#>, '" + msgErro + "' , SYSDATE, 'Erro', " + codemp + ", '' FROM DUAL");
			this.selectsParaInsert
					.add("SELECT <#NUMUNICO#>, '" + msgErro + "' , SYSDATE, 'Erro', " + codemp + ", '' FROM DUAL");

		} finally {
			if (pstmt != null) {
				pstmt.close();
			}

			jdbc.closeSession();
		}
	}


	public void updateBaixaParcial(BigDecimal nufin, BigDecimal nubco, BigDecimal vlrDesdob, String dataBaixaFormatada,
								   BigDecimal codemp) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		try {
			jdbc.openSession();
			String sqlNota = "UPDATE TGFFIN SET VLRBAIXA = " + vlrDesdob + ", " + "DHBAIXA = '" + dataBaixaFormatada
					+ "', " + "NUBCO = " + nubco + ", " + "CODTIPOPERBAIXA = 1400, "
					+ "DHTIPOPERBAIXA = (SELECT MAX(DHALTER) FROM TGFTOP WHERE CODTIPOPER = 1400), "
					+ "CODUSUBAIXA = 0, AD_BAIXAPARCIAL = 'S'  " + "WHERE NUFIN = " + nufin;
			pstmt = jdbc.getPreparedStatement(sqlNota);
			pstmt.executeUpdate();
			System.out.println("Passou do update");
		} catch (SQLException e) {
			e.printStackTrace();
			LogCatcher.logError("SELECT <#NUMUNICO#>, 'Erro Ao Baixar Parcialmente Um Titulo: " + e.getMessage()
					+ "' , SYSDATE, 'Erro', " + codemp + ", '' FROM DUAL");
			this.selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro Ao Baixar Parcialmente Um Titulo: " + e.getMessage()
					+ "' , SYSDATE, 'Erro', " + codemp + ", '' FROM DUAL");
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

		try {
			jdbc.openSession();
			String sqlUpdate = "INSERT INTO AD_LOGINTEGRACAO (NUMUNICO, DESCRICAO, DTHORA, STATUS)VALUES (((SELECT NVL(MAX(NUMUNICO), 0) + 1 FROM AD_LOGINTEGRACAO)), ?, SYSDATE, ?)";
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

	public BigDecimal insertMovBancaria(BigDecimal contaBancaria, BigDecimal vlrDesdob, BigDecimal nufin,
										String dataBaixaFormatada, BigDecimal codemp) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		EnviromentUtils util = new EnviromentUtils();
		BigDecimal nubco = util.getMaxNumMbc();
		LogCatcher.logInfo("Gerado NUBCO: " + nubco + " | Conta bancária: " + contaBancaria + " | Empresa: " + codemp);

		try {
			jdbc.openSession();
			String sqlUpdate = "INSERT INTO TGFMBC (NUBCO, CODLANC, DTLANC, CODTIPOPER, DHTIPOPER, DTCONTAB, HISTORICO, CODCTABCOINT, NUMDOC, VLRLANC, TALAO, PREDATA, CONCILIADO, DHCONCILIACAO, ORIGMOV, NUMTRANSF, RECDESP, DTALTER, DTINCLUSAO, CODUSU, VLRMOEDA, SALDO, CODCTABCOCONTRA, NUBCOCP, CODPDV)  VALUES ("
					+ nubco + ", " + "1, " + "'" + dataBaixaFormatada + "'" + ", " + "1400, "
					+ "(SELECT MAX(DHALTER) FROM TGFTOP WHERE CODTIPOPER = 1400), " + "NULL, "
					+ "(SELECT HISTORICO FROM TGFFIN WHERE NUFIN = " + nufin + "), " + contaBancaria + ", " + "0, "
					+ vlrDesdob + ", " + "NULL, " + "'" + dataBaixaFormatada + "', " + "'N', " + "NULL, " + "'F', "
					+ "NULL, " + "1, " + "SYSDATE, " + "SYSDATE, " + "0, " + "0, " + vlrDesdob + ", " + "NULL,  "
					+ "NULL, " + "NULL) ";
			pstmt = jdbc.getPreparedStatement(sqlUpdate);
			pstmt.executeUpdate();
			LogCatcher.logInfo("Inserido NUBCO: " + nubco + " na TGFMBC para NUFIN: " + nufin + " | Conta: " + contaBancaria + " | Empresa: " + codemp);

		} catch (Exception se) {
			se.printStackTrace();
			LogCatcher.logError("SELECT <#NUMUNICO#>, 'Erro Ao Inserir Mov. Bancaria: "
					+ se.getMessage().replace("'", "\"") + "' , SYSDATE, 'Erro', " + codemp + ", '' FROM DUAL");
			this.selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro Ao Inserir Mov. Bancaria: "
					+ se.getMessage().replace("'", "\"") + "' , SYSDATE, 'Erro', " + codemp + ", '' FROM DUAL");
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
			String sqlUpdate = "UPDATE AD_ALUNOS SET INTEGRADO_BAIXA = 'S' WHERE ID_EXTERNO = '" + idAluno + "'";
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

	public long printLogDebug(long tempoAnterior, String msgRetornoLog) {
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
		List<Object[]> listRet = new ArrayList();

		try {
			jdbc.openSession();
			String sql = "\tSELECT \tCODCTABCOINT, CODEMP, IDEXTERNO, CODBCO ";
			sql = sql + "\t\tFROM  \tad_infobankbaixa ";
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
		List<Object[]> listRet = new ArrayList();

		try {
			jdbc.openSession();
			String sql = "\tSELECT \tCODPARC, ID_EXTERNO ";
			sql = sql + "\t\tFROM  \tAD_ALUNOS ";
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
		List<Object[]> listRet = new ArrayList();

		try {
			jdbc.openSession();
			String sql = "\tSELECT \tCODEMP, NUFIN, AD_IDEXTERNO, (CASE WHEN DHBAIXA IS NOT NULL THEN 'S' ELSE 'N' END) BAIXADO, VLRDESDOB, NUBCO ";
			sql = sql + "\t\tFROM  \tTGFFIN ";
			sql = sql + "\t\tWHERE  \tRECDESP = 1 ";
			sql = sql + "\t\t    AND PROVISAO = 'N' ";
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

	//TIPO TITULO
	public List<Object[]> retornarInformacoesTipoTitulo() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		List<Object[]> listRet = new ArrayList();

		try {
			jdbc.openSession();
			String sql = "\tSELECT \tCODEMP, CODTIPTIT, IDEXTERNO, TAXACART, NVL(QTD_PARCELAS, 0) AS QTD_PARCELA,(SELECT TIT.CODPARCTEF FROM TGFTIT TIT WHERE TIT.CODTIPTIT = AD_TIPTITINTEGRACAO.CODTIPTIT) AS CODPARCTEF ";
			sql = sql + "\t\tFROM  \tAD_TIPTITINTEGRACAO WHERE IDEXTERNO IS NOT NULL";
			pstmt = jdbc.getPreparedStatement(sql);
			rs = pstmt.executeQuery();

			while (rs.next()) {
				Object[] ret = new Object[6];
				ret[0] = rs.getBigDecimal("CODTIPTIT");
				ret[1] = rs.getLong("CODEMP");
				ret[2] = rs.getString("IDEXTERNO").trim();
				ret[3] = rs.getBigDecimal("TAXACART");
				ret[4] = rs.getBigDecimal("QTD_PARCELA");
				ret[5] = rs.getBigDecimal("CODPARCTEF");
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


	public BigDecimal insertFin(BigDecimal nufinOrig, BigDecimal vlrDesdob, BigDecimal codTipTit, BigDecimal codemp) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		EnviromentUtils util = new EnviromentUtils();
		System.out.println("Chegou no insert do financeiro segundo");
		BigDecimal nufin = util.getMaxNumFin(true);

		try {
			jdbc.openSession();
			String sqlUpdate = "INSERT INTO TGFFIN         (NUFIN,          NUNOTA,          NUMNOTA,          ORIGEM,          RECDESP,          CODEMP,          CODCENCUS,          CODNAT,          CODTIPOPER,          DHTIPOPER,          CODTIPOPERBAIXA,          DHTIPOPERBAIXA,          CODPARC,          CODTIPTIT,          VLRDESDOB,          VLRDESC,          VLRBAIXA,          CODBCO,          CODCTABCOINT,          DTNEG,          DHMOV,          DTALTER,          DTVENC,          DTPRAZO,          DTVENCINIC,          TIPJURO,          TIPMULTA,          HISTORICO,          TIPMARCCHEQ,          AUTORIZADO,          BLOQVAR,          INSSRETIDO,          ISSRETIDO,          PROVISAO,          RATEADO,          TIMBLOQUEADA,          IRFRETIDO,          TIMTXADMGERALU,          VLRDESCEMBUT,          VLRINSS,          VLRIRF,          VLRISS,          VLRJURO,          VLRJUROEMBUT,          VLRJUROLIB,          VLRJURONEGOC,          VLRMOEDA,          VLRMOEDABAIXA,          VLRMULTA,          VLRMULTAEMBUT,          VLRMULTALIB,          VLRMULTANEGOC,          VLRPROV,          VLRVARCAMBIAL,          VLRVENDOR,          ALIQICMS,          BASEICMS,          CARTAODESC,          CODMOEDA,          CODPROJ,          CODVEICULO,          CODVEND,          DESPCART,          NUMCONTRATO,          ORDEMCARGA,          CODUSU,         AD_IDEXTERNO,         AD_IDALUNO, AD_NUFINORIG, AD_BAIXAPARCIAL)          (SELECT " + nufin + ", NULL, 0, 'F', recDesp ,codemp ,codCenCus ,codNat ,codTipOper ,(SELECT MAX(DHALTER) FROM TGFTOP WHERE CODTIPOPER = TGFFIN.codTipOper), 0, (SELECT MAX(DHALTER) FROM TGFTOP WHERE CODTIPOPER = 0), codparc ," + codTipTit + ", " + vlrDesdob + ", 0, 0, CODBCO, CODCTABCOINT, DTNEG , SYSDATE, SYSDATE, DTVENC , SYSDATE, DTVENCINIC , 1 , 1 , null , 'I' , 'N' , 'N' , 'N' , 'N' , 'N' , 'N' , 'N' , 'S' , 'S' , 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, NULL, AD_IDALUNO, " + nufinOrig + ", 'S' FROM TGFFIN WHERE NUFIN = " + nufinOrig + ")";
			pstmt = jdbc.getPreparedStatement(sqlUpdate);
			pstmt.executeUpdate();
		} catch (Exception se) {
			se.printStackTrace();

			LogCatcher.logError("SELECT <#NUMUNICO#>, 'Erro Ao Gerar Titulo Parcial: " + se.getMessage() + "' , SYSDATE, 'Erro', " + codemp + ", '' FROM DUAL");
			this.selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro Ao Gerar Titulo Parcial: " + se.getMessage() + "' , SYSDATE, 'Erro', " + codemp + ", '' FROM DUAL");
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



	public BigDecimal insertFinCartao(BigDecimal nufinOrig, BigDecimal vlrDesdob, BigDecimal codTipTit, BigDecimal codemp,
									  BigDecimal codParc, String dtCredito, String baixaId) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		EnviromentUtils util = new EnviromentUtils();
		System.out.println("Chegou no insert do financeiro cartão");
		LogCatcher.logInfo("Chegou no insert do financeiro cartão");
		BigDecimal nufinNovo = util.getMaxNumFin(true); // Alterado o nome da variável para evitar conflito

		try {
			jdbc.openSession();
			String sqlUpdate = "INSERT INTO TGFFIN (NUFIN, NUNOTA, NUMNOTA, ORIGEM, RECDESP, CODEMP, CODCENCUS, CODNAT, CODTIPOPER, DHTIPOPER, CODTIPOPERBAIXA, DHTIPOPERBAIXA, CODPARC, CODTIPTIT, VLRDESDOB, VLRDESC, VLRBAIXA, CODBCO, CODCTABCOINT, DTNEG, DHMOV, DTALTER, DTVENC, DTPRAZO, DTVENCINIC, TIPJURO, TIPMULTA, HISTORICO, TIPMARCCHEQ, AUTORIZADO, BLOQVAR, INSSRETIDO, ISSRETIDO, PROVISAO, RATEADO, TIMBLOQUEADA, IRFRETIDO, TIMTXADMGERALU, VLRDESCEMBUT, VLRINSS, VLRIRF, VLRISS, VLRJURO, VLRJUROEMBUT, VLRJUROLIB, VLRJURONEGOC, VLRMOEDA, VLRMOEDABAIXA, VLRMULTA, VLRMULTAEMBUT, VLRMULTALIB, VLRMULTANEGOC, VLRPROV, VLRVARCAMBIAL, VLRVENDOR, ALIQICMS, BASEICMS, CARTAODESC, CODMOEDA, CODPROJ, CODVEICULO, CODVEND, DESPCART, NUMCONTRATO, ORDEMCARGA, CODUSU, AD_IDEXTERNO, AD_IDALUNO, AD_NUFINORIG, AD_BAIXAPARCIAL, AD_BAIXA_CARTAO, AD_BAIXAID, AD_AUTORIZACAO_CART, AD_NSU_CART) " +
					"(SELECT " + nufinNovo + ", NULL, 0, 'F', recDesp, codemp, codCenCus, codNat, codTipOper, " +
					"(SELECT MAX(DHALTER) FROM TGFTOP WHERE CODTIPOPER = TGFFIN.codTipOper), 0, " +
					"(SELECT MAX(DHALTER) FROM TGFTOP WHERE CODTIPOPER = 0), " + codParc + ", " + // Substituído codparc por codParc
					codTipTit + ", " + vlrDesdob + ", 0, 0, CODBCO, CODCTABCOINT, DTNEG, SYSDATE, SYSDATE, TO_DATE('" +
					dtCredito + "', 'YYYY-MM-DD'), SYSDATE, DTVENCINIC, 1, 1, null, 'I', 'N', 'N', 'N', 'N', 'N', 'N', 'N', " +
					"'S', 'S', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, NULL, AD_IDALUNO, " +
					nufinOrig + ", 'S', 'S', '" + baixaId + "', AD_AUTORIZACAO_CART, AD_NSU_CART FROM TGFFIN WHERE NUFIN = " + nufinOrig + ")";
			pstmt = jdbc.getPreparedStatement(sqlUpdate);
			pstmt.executeUpdate();
		} catch (Exception se) {
			se.printStackTrace();
			LogCatcher.logError("SELECT <#NUMUNICO#>, 'Erro Ao Gerar Titulo Parcial: " + se.getMessage() +
					"' , SYSDATE, 'Erro', " + codemp + ", '' FROM DUAL");
			this.selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro Ao Gerar Titulo Parcial: " + se.getMessage() +
					"' , SYSDATE, 'Erro', " + codemp + ", '' FROM DUAL");
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

		return nufinNovo;
	}


	public BigDecimal getMaxNumFin() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		BigDecimal id = BigDecimal.ZERO;

		try {
			this.updateNumFin();
			jdbc.openSession();
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
		List<Object[]> listRet = new ArrayList();

		try {
			jdbc.openSession();
			String sql = "\tSELECT \tF.NUFIN, NVL(F.AD_BAIXAID, 'N') as BAIXAID";
			sql = sql + "\t\tFROM  \tTGFFIN F ";
			sql = sql + "\t\tWHERE  \tF.RECDESP = 1 ";
			sql = sql
					+ "\t\t    AND F.PROVISAO = 'N'             AND F.DHBAIXA IS NOT NULL            AND F.AD_IDALUNO IS NOT NULL";
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


	//retornarInformacoesContagemIdBaixa com ALIAS corrigido
	public List<Object[]> retornarInformacoesContagemIdBaixa() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		List<Object[]> listRet = new ArrayList();

		try {
			jdbc.openSession();
			// CORREÇÃO: Adicionado alias para AD_BAIXAID
			String sql = "\tSELECT COUNT(0) AS C, CODEMP, AD_BAIXAID AS BAIXAID FROM TGFFIN WHERE RECDESP = 1 AND PROVISAO = 'N' AND DHBAIXA IS NOT NULL AND AD_IDALUNO IS NOT NULL GROUP BY CODEMP, AD_BAIXAID";
			pstmt = jdbc.getPreparedStatement(sql);
			rs = pstmt.executeQuery();

			while (rs.next()) {
				Object[] ret = new Object[3];
				ret[0] = rs.getBigDecimal("CODEMP");
				ret[1] = rs.getString("BAIXAID"); // Agora usando o alias correto
				ret[2] = rs.getBigDecimal("C");
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


	public List<Object[]> retornarInformacoesIdBaixaOrig() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		List<Object[]> listRet = new ArrayList();

		try {
			jdbc.openSession();
			String sql = "\tSELECT \tNUFIN, NVL(AD_BAIXAID, 'N') as BAIXAID, AD_NUFINORIG";
			sql = sql + "\t\tFROM  \tTGFFIN ";
			sql = sql + "\t\tWHERE  \tRECDESP = 1 ";
			sql = sql
					+ "\t\t    AND PROVISAO = 'N'             AND AD_NUFINORIG IS NOT NULL            AND AD_IDALUNO IS NOT NULL";
			pstmt = jdbc.getPreparedStatement(sql);
			rs = pstmt.executeQuery();

			while (rs.next()) {
				Object[] ret = new Object[3];
				ret[0] = rs.getBigDecimal("NUFIN");
				ret[1] = rs.getString("BAIXAID");
				ret[2] = rs.getString("AD_NUFINORIG");
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
			String sqlUpdate = "INSERT INTO AD_LOGINTEGRACAO (NUMUNICO, DESCRICAO, DTHORA, \tSTATUS, CODEMP, MATRICULA_IDFORN) "
					+ listInsert;
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

	public void insertLogList(String listInsert, BigDecimal codemp) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		try {
			jdbc.openSession();
			String sqlUpdate = "INSERT INTO AD_LOGINTEGRACAO (NUMUNICO, DESCRICAO, DTHORA, \tSTATUS, CODEMP, MATRICULA_IDFORN) "
					+ listInsert;
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
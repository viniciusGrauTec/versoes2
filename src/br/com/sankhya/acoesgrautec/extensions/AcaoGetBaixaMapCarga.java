package br.com.sankhya.acoesgrautec.extensions;

import br.com.sankhya.acoesgrautec.util.EnviromentUtils;
import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.extensions.actionbutton.Registro;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.core.JapeSession;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.util.JapeSessionContext;
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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import br.com.sankhya.modelcore.financeiro.helper.BaixaHelper;
import br.com.sankhya.modelcore.financeiro.util.DadosBaixa;
import br.com.sankhya.modelcore.dwfdata.vo.tgf.FinanceiroVO;
import br.com.sankhya.modelcore.financeiro.util.TipoJurosMulta;

import org.activiti.engine.impl.util.json.JSONArray;
import org.cuckoo.core.ScheduledAction;
import org.cuckoo.core.ScheduledActionContext;



public class AcaoGetBaixaMapCarga implements AcaoRotinaJava, ScheduledAction {
	private List<String> selectsParaInsert = new ArrayList();
	private EnviromentUtils util = new EnviromentUtils();
	private String resumoExecucao;
	private int titulosBaixadosComSucesso;
    private int titulosJaBaixadosIgnorados;
    private int titulosComOutrosErros;
	
	static {
		LogConfiguration.setPath(SWRepositoryUtils.getBaseFolder() + "/logAcao/logs");
	}

	public void doAction(ContextoAcao contexto) throws Exception {

		this.titulosBaixadosComSucesso = 0;
	    this.titulosJaBaixadosIgnorados = 0;
	    this.titulosComOutrosErros = 0;
	    this.resumoExecucao = "";

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


			LogCatcher.logInfo("--- RESUMO FINAL DO PROCESSAMENTO DA EMPRESA " + codEmp + " ---");
			LogCatcher.logInfo(this.resumoExecucao);

			if (this.resumoExecucao != null && !this.resumoExecucao.isEmpty()) {
				contexto.setMensagemRetorno(this.resumoExecucao);
			} else {
				contexto.setMensagemRetorno("Processo concluído!");
			}
			
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
		LogConfiguration.setPath(SWRepositoryUtils.getBaseFolder() + "/logAcao/logs");
		long tempoAnterior = System.currentTimeMillis();
		long tempoInicio = System.currentTimeMillis();
		
		
		JapeSession.SessionHandle hnd = null;
		
		
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		BigDecimal codEmp = BigDecimal.ZERO;
		String url = "";
		String token = "";
		int count = 0;
		LogCatcher.logInfo("\n/*************** Inicio - onTime GetBaixaMap *****************/ ");

		try {
			hnd = JapeSession.open();

	        JapeWrapper usuDAO = JapeFactory.dao("Usuario");
	        DynamicVO usuVO = usuDAO.findOne("CODUSU = ?", BigDecimal.ZERO);
	        
	        if (usuVO == null) {
	            throw new Exception("Usuário SUP (código 0) não foi encontrado no sistema.");
	        }
	        
	        String nomeUsu = usuVO.asString("NOMEUSU");
	        BigDecimal codGrupo = usuVO.asBigDecimal("CODGRUPO");
	        BigDecimal codUsu = usuVO.asBigDecimal("CODUSU");

	        AuthenticationInfo authInfo = new AuthenticationInfo(nomeUsu, codUsu, codGrupo, 0);

	        authInfo.makeCurrent();
	        JapeSessionContext.putProperty("usuario_logado", authInfo.getUserID());

	        LogCatcher.logInfo("Sessão autenticada com sucesso para " + nomeUsu + " via new AuthenticationInfo() e makeCurrent().");
	        
	        
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
			String query = "SELECT CODEMP, URL, TOKEN, INTEGRACAO FROM AD_LINKSINTEGRACAO";
			pstmt = jdbc.getPreparedStatement(query);
			rs = pstmt.executeQuery();

			for (tempoAnterior = this.printLogDebug(tempoAnterior,
					"Consulta para capturar o link de integração: AD_LINKSINTEGRACAO"); rs
						 .next(); tempoAnterior = this.printLogDebug(tempoAnterior,
					"onTime - updateCarga da empresa(" + codEmp + ")")) {
				++count;
				LogCatcher.logInfo("Contagem: " + count);
				codEmp = rs.getBigDecimal("CODEMP");
				url = rs.getString("URL");
				token = rs.getString("TOKEN");
				String statusIntegracao = rs.getString("INTEGRACAO");

				if (!"S".equals(statusIntegracao)) {
					System.out.println("Integração desativada para a empresa " + codEmp + " - pulando processamento");
					continue;
				}

				this.iterarEndpoint(url, token, codEmp, mapaInfIdBaixaOrig, mapaInfIdBaixa, mapaInfTipoTituloTaxa,
						mapaInfBanco, mapaInfConta, mapaInfAlunos, mapaInfFinanceiro, mapaInfTipoTitulo,
						mapaInfMenorDataMovBancariaPorConta, mapaInfFinanceiroBaixado, mapaInfFinanceiroValor,
						mapaInfFinanceiroBanco, mapaInfTipoTituloCodparcCartao, mapaInfIdBaixaParcelas,mapaBaixaIdParaNufin);
				tempoAnterior = this.printLogDebug(tempoAnterior, "onTime - efetuarBaixa da empresa(" + codEmp + ")");
			}

			LogCatcher.logInfo("Chegou ao final da baixa");
			LogCatcher.logInfo("\n/*************** Fim - JobGetBaixaMap *****************/");
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
			  JdbcUtils.closeResultSet(rs);
		        JdbcUtils.closeStatement(pstmt);
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
				LogCatcher.logInfo("Lista de selects: " + this.selectsParaInsert.toString());
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
					LogCatcher.logInfo("Iteração: " + i + " de " + qtdInsert);
					if (i < qtdInsert) {
						msgError.append(" \nUNION ALL ");
					}

					++i;
				}

				LogCatcher.logInfo("Consulta de log: \n" + msgError);

				try {
					this.insertLogList(msgError.toString(), codEmp);
				} catch (Exception e) {
					LogCatcher.logError(e);
					e.printStackTrace();
				}

				AuthenticationInfo.unregistry(); // Essencial para limpar a sessão do processo
		        JapeSession.close(hnd);
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
			LogCatcher.logInfo("\nIniciando processDateRange Baixa de Alunos- Empresa: " + codemp + ", Período: " + dataInicio + " a "
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
					LogCatcher.logInfo(
							"Chamando API - Data: " + currentDate + ", Página: " + pagina + ", URL: " + urlCompleta);

					String[] response1 = apiGet(urlCompleta, token);
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
					} else {
						LogCatcher.logInfo("Erro na requisição: Status " + status);
						break;
					}
				}

				if (todosRegistrosDoDia.length() > 0) {
					LogCatcher.logInfo("Processando " + todosRegistrosDoDia.length()
							+ " registros de baixas para o dia " + currentDate);

					String[] response = new String[] { "200", todosRegistrosDoDia.toString() };

					LogCatcher.logInfo("Dados sendo enviados para efetuarBaixa:");
					LogCatcher.logInfo("Tamanho do array: " + todosRegistrosDoDia.length());
					LogCatcher.logInfo("Conteúdo do todosRegistrosDoDia: " + todosRegistrosDoDia.toString());
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
			Map<String, BigDecimal> mapaInfIdBaixaParcelas, Map<String, BigDecimal> mapaBaixaIdParaNufin)
			throws Exception {
		Date dataAtual = new Date();
		SimpleDateFormat formato = new SimpleDateFormat("yyyy-MM-dd");
		String dataFormatada = formato.format(dataAtual);

		LogCatcher.logInfo("=== iterarEndpoint do JOB de Alunos Baixas iniciado ===");
		LogCatcher.logInfo("codEmp: " + codemp);
		LogCatcher.logInfo("\nURL base: " + url);

		try {
			String[] response = this.apiGet(url + "/financeiro" + "/baixas" + "?quantidade=0" + "&dataInicial="
					+ dataFormatada + " 00:00:00&dataFinal=" + dataFormatada + " 23:59:59", token);

			int status = Integer.parseInt(response[0]);
			String responseString = response[1];

			//só printa a partir daqui
			LogCatcher.logInfo("Status da requisição: " + status);
			LogCatcher.logInfo("Resposta da API (baixas): " + responseString);

			if (status == 200) {
				LogCatcher.logInfo("Sucesso (200): Requisição bem-sucedida.");
				this.efetuarBaixa(response, url, token, codemp, mapaInfIdBaixaOrig, mapaInfIdBaixa,
						mapaInfTipoTituloTaxa, mapaInfBanco, mapaInfConta, mapaInfAlunos, mapaInfFinanceiro,
						mapaInfTipoTitulo, mapaInfMenorDataMovBancariaPorConta, mapaInfFinanceiroBaixado,
						mapaInfFinanceiroValor, mapaInfFinanceiroBanco, mapaInfTipoTituloCodparcCartao,
						mapaInfIdBaixaParcelas, mapaBaixaIdParaNufin);
			} else if (status >= 400 && status < 500) {

				String erroMsg = "Erro do Cliente (" + status + "): A requisição para buscar baixas falhou. Resposta: "
						+ responseString;
				LogCatcher.logError(erroMsg);
				selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro do Cliente (" + status
						+ ") ao buscar baixas.', SYSDATE, 'Erro', " + codemp + ", NULL FROM DUAL");
			} else if (status >= 500) {

				String erroMsg = "Erro do Servidor (" + status
						+ "): Ocorreu um problema no servidor da API ao buscar baixas. Resposta: " + responseString;
				LogCatcher.logError(erroMsg);
				selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro do Servidor (" + status
						+ ") ao buscar baixas.', SYSDATE, 'Erro', " + codemp + ", NULL FROM DUAL");
			} else {

				String erroMsg = "Status inesperado (" + status
						+ "): A API retornou um código não previsto ao buscar baixas. Resposta: " + responseString;
				LogCatcher.logError(erroMsg);
				selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Status inesperado (" + status
						+ ") ao buscar baixas.', SYSDATE, 'Erro', " + codemp + ", NULL FROM DUAL");
			}
		} catch (NumberFormatException e) {
			String erroMsg = "Erro de Formato de Número: Não foi possível converter o status da resposta da API para um número inteiro.";
			LogCatcher.logError(erroMsg);
			LogCatcher.logError(e);
			e.printStackTrace();
		} catch (Exception e) {
			String erroMsg = "Exceção não tratada ao chamar a API de baixas: " + e.getMessage();
			LogCatcher.logError(erroMsg);
			LogCatcher.logError(e); 
			e.printStackTrace();
			selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Exceção ao buscar baixas: " + e.getMessage().replace("'", "''")
					+ "', SYSDATE, 'Erro', " + codemp + ", NULL FROM DUAL");
		}
	}







	//BAIXA DOS CARTOES  ATUALIZADO COM ESTORNO
	public void efetuarBaixa(String[] response, String url, String token, BigDecimal codemp,
			Map<String, BigDecimal> mapaInfIdBaixaOrig, Map<BigDecimal, String> mapaInfIdBaixa,
			Map<String, BigDecimal> mapaInfTipoTituloTaxa, Map<String, BigDecimal> mapaInfBanco,
			Map<String, BigDecimal> mapaInfConta, Map<String, BigDecimal> mapaInfAlunos,
			Map<String, BigDecimal> mapaInfFinanceiro, Map<String, BigDecimal> mapaInfTipoTitulo,
			Map<Long, Date> mapaInfMenorDataMovBancariaPorConta, Map<BigDecimal, String> mapaInfFinanceiroBaixado,
			Map<BigDecimal, BigDecimal> mapaInfFinanceiroValor, Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco,
			Map<String, BigDecimal> mapaInfTipoTituloCodparcCartao, Map<String, BigDecimal> mapaInfIdBaixaParcelas,
			Map<String, BigDecimal> mapaBaixaIdParaNufin) throws Exception {
		Map<String, BigDecimal> mapaBaixaEstornada = new HashMap<>();

		LogCatcher.logInfo("Iniciando efetuarBaixa refatorado - Empresa: " + codemp);

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

		if (!"200".equalsIgnoreCase(response[0])) {
			String erroMsg = "API retornou status não-200: " + response[0] + " - Resposta: " + response[1];
			LogCatcher.logError(erroMsg);
			selectsParaInsert.add("SELECT <#NUMUNICO#>, '" + erroMsg.replace("'", "''") + "', SYSDATE, 'Erro', "
					+ codemp + ", '' FROM DUAL");
			return;
		}
		

		JsonArray jsonArray = new JsonParser().parse(response[1]).getAsJsonArray();
		SimpleDateFormat formatoApi = new SimpleDateFormat("yyyy-MM-dd");
		EntityFacade dwfFacade = EntityFacadeFactory.getDWFFacade(); // Obter o Facade para buscar o VO

		for (JsonElement jsonElement : jsonArray) {
			LogCatcher.logInfo("**INÍCIO DO LAÇO REFATORADO DO EFETUAR BAIXA DE ALUNO**");
			JsonObject jsonObject = jsonElement.getAsJsonObject();
			String idAluno = jsonObject.get("aluno_id").getAsString().trim();
			String tituloId = jsonObject.get("titulo_id").getAsString();
			String baixaId = jsonObject.get("baixa_id").getAsString();

			BigDecimal nufin = mapaInfFinanceiro.get(codemp + "###" + tituloId);
			
			LogCatcher.logInfo("Titulo ID: " + jsonObject.get("titulo_id").getAsInt());
			LogCatcher.logInfo("Valor da Baixa: " + jsonObject.get("baixa_valor").getAsString());

			idAluno = jsonObject.get("aluno_id").getAsString().trim();

			LogCatcher.logInfo("IdAluno: " + idAluno);

			if (nufin == null) {
				LogCatcher.logError("Não foi possível encontrar o financeiro com id externo " + tituloId
						+ " para a empresa " + codemp);
				this.titulosComOutrosErros++;
				continue;
			}

			if (jsonObject.has("baixa_estorno_data") && !jsonObject.get("baixa_estorno_data").isJsonNull()) {
				LogCatcher.logInfo("[ESTORNO] Iniciando processo de estorno para NUFIN: " + nufin);
				

				BigDecimal nufinDaBaixa =
						mapaBaixaIdParaNufin.get(codemp + "###" + baixaId);

				boolean jaBaixado =
						nufinDaBaixa != null &&
								"S".equalsIgnoreCase(
										mapaInfFinanceiroBaixado.getOrDefault(nufinDaBaixa, "N"));

				if (jaBaixado) {
					LogCatcher.logInfo("[ESTORNO] BaixaId " + baixaId +
							" já estava baixada. NUFIN = " + nufinDaBaixa);
					
					estornarTgfFin(nufinDaBaixa, codemp);
					LogCatcher.logInfo("[ESTORNO] Estornando NUFIN: " + nufin + ", Empresa: " + codemp);
					mapaBaixaEstornada.put(codemp + "###" + baixaId, nufinDaBaixa);

				} else {
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

				continue;
			}

			try {
				Timestamp dataBaixa = new Timestamp(
						formatoApi.parse(jsonObject.get("baixa_data").getAsString()).getTime());
				BigDecimal codUsuLogado = AuthenticationInfo.getCurrent().getUserID();

				FinanceiroVO financeiroOriginalVO = (FinanceiroVO) dwfFacade.findEntityByPrimaryKeyAsVO("Financeiro",
						nufin, FinanceiroVO.class);
				if (financeiroOriginalVO == null) {
					throw new Exception(
							"Registro Financeiro com NUFIN " + nufin + " não encontrado no banco de dados.");
				}

				try {
				LogCatcher.logInfo("[INFO] Ativando bypass de validação do dono do caixa para esta transação...");
			    JapeSession.putProperty("ignorar.validacao.usuario.caixa", Boolean.TRUE);
			    
				BaixaHelper baixaHelper = new BaixaHelper(nufin, codUsuLogado, dataBaixa);
				baixaHelper.addListener(new CustomFieldsListener());
				baixaHelper.addListener(new AcompanhamentoBaixaListener());
				
				//calculo de juros desligado evitando a validação do campo TIPTIT
				DadosBaixa dadosBaixa = baixaHelper.montaDadosBaixa(dataBaixa, false, false);

			    BigDecimal valorPagoPelaApi = jsonObject.get("baixa_valor").getAsBigDecimal();
			    BigDecimal valorDevidoCalculado = BigDecimal.valueOf(dadosBaixa.getValoresBaixa().getVlrTotal());

			    int comparacao = valorPagoPelaApi.compareTo(valorDevidoCalculado);

			    if (comparacao < 0) {
			        LogCatcher.logInfo("Valor pago (" + valorPagoPelaApi + ") é MENOR que o devido (" + valorDevidoCalculado + "). Ação: Gerar Pendência.");
			        dadosBaixa.getDescisaoBaixa().setDescisao(DadosBaixa.DescisaoBaixa.VALOR_MENOR_PENDENCIA);
			    } else if (comparacao > 0) {
			        LogCatcher.logInfo("Valor pago (" + valorPagoPelaApi + ") é MAIOR que o devido (" + valorDevidoCalculado + "). Ação: Gerar Crédito.");
			        dadosBaixa.getDescisaoBaixa().setDescisao(DadosBaixa.DescisaoBaixa.VALOR_MAIOR_CREDITO);
			    }
			    
			    dadosBaixa.getValoresBaixa().setVlrTotal(valorPagoPelaApi.doubleValue());
				
				
				
				dadosBaixa.getValoresBaixa().setVlrJuros(jsonObject.get("baixa_juros").getAsBigDecimal().doubleValue());
				dadosBaixa.getValoresBaixa().setVlrMulta(jsonObject.get("baixa_multa").getAsBigDecimal().doubleValue());
				dadosBaixa.getValoresBaixa().setVlrDesconto(jsonObject.get("baixa_desconto").getAsBigDecimal().doubleValue());
				
				dadosBaixa.getValoresBaixa().setTipoJuros(TipoJurosMulta.INCLUSO);
	            dadosBaixa.getValoresBaixa().setTipoMulta(TipoJurosMulta.INCLUSO);
				
				String idExterno = jsonObject.get("local_pagamento_id").getAsString();
				dadosBaixa.getDadosBancarios().setCodConta(mapaInfConta.get(codemp + "###" + idExterno));
				dadosBaixa.getDadosBancarios().setCodBanco(mapaInfBanco.get(codemp + "###" + idExterno));


				JsonArray formasPagamento = jsonObject.getAsJsonArray("formas_de_pagamento");
				
				
				dadosBaixa.getVariosTiposTitulos().clear();

				FinanceiroVO modeloPagamentoVO = (FinanceiroVO) financeiroOriginalVO.buildClone().wrapInterface(FinanceiroVO.class);
				modeloPagamentoVO.setAceptTransientProperties(true);
				modeloPagamentoVO.setProperty("AD_VLRDESCINT", jsonObject.get("baixa_desconto").getAsBigDecimal());
				modeloPagamentoVO.setProperty("AD_VLRMULTAINT", jsonObject.get("baixa_multa").getAsBigDecimal());
				modeloPagamentoVO.setProperty("AD_VLRJUROSINT", jsonObject.get("baixa_juros").getAsBigDecimal());
				modeloPagamentoVO.setProperty("AD_BAIXAID", baixaId);
                
				for (JsonElement pagamentoEl : formasPagamento) {
					JsonObject pagamentoObj = pagamentoEl.getAsJsonObject();
					String formaPagamentoId = pagamentoObj.get("forma_pagamento_id").getAsString().trim();

					BigDecimal codTipTit = mapaInfTipoTitulo.get(codemp + "###" + formaPagamentoId);
					if (codTipTit == null || codTipTit.compareTo(BigDecimal.ZERO) == 0) {
						throw new Exception(
								"Tipo de Título não configurado para a forma de pagamento: " + formaPagamentoId);
					}


				    FinanceiroVO voPagamento = (FinanceiroVO) modeloPagamentoVO.buildClone().wrapInterface(FinanceiroVO.class);
				    voPagamento.setCODTIPTIT(codTipTit);
				    voPagamento.setVLRBAIXA(pagamentoObj.get("forma_pagamento_valor").getAsBigDecimal());
					

	                if ("CARTAO_CREDITO".equals(formaPagamentoId) || "CARTAO_DEBITO".equals(formaPagamentoId)) {
	                     voPagamento.setProperty("AD_BAIXA_CARTAO", "S");
	                     voPagamento.setProperty("AD_NSU_CART", pagamentoObj.get("forma_pagamento_nsu").getAsString());
	                     voPagamento.setProperty("AD_AUTORIZACAO_CART", pagamentoObj.get("forma_pagamento_autorizacao").getAsString());
	                } else {
	                     voPagamento.setProperty("AD_BAIXA_CARTAO", "N");
	                }


	                dadosBaixa.addTipoTitulo(voPagamento);
	            }

			    } finally {
			        LogCatcher.logInfo("[INFO] Desativando bypass de validação do dono do caixa.");
			        JapeSession.putProperty("ignorar.validacao.usuario.caixa", null);
			    }

				LogCatcher.logInfo("[SUCESSO] Baixa para o NUFIN " + nufin + " (Baixa ID: " + baixaId
						+ ") realizada com sucesso via BaixaHelper.");
				mapaInfFinanceiroBaixado.put(nufin, "S");
				
				this.titulosBaixadosComSucesso++;

			} catch (Exception e) {
				String rawErroMsg = e.getMessage();

				if (rawErroMsg != null && rawErroMsg.contains("Título já baixado")) {
					this.titulosJaBaixadosIgnorados++;
					LogCatcher.logInfo("[AVISO] Tentativa de baixa para NUFIN " + nufin + " (Baixa ID: " + baixaId + ") ignorada. Motivo: Título já baixado!");
				} else {			
					this.titulosComOutrosErros++;
					String erroDetalhado = "Falha ao processar baixa para NUFIN " + nufin + " (Baixa ID: " + baixaId + "). Motivo: " + rawErroMsg;
					LogCatcher.logError(erroDetalhado);
					selectsParaInsert.add("SELECT <#NUMUNICO#>, '" + erroDetalhado.replace("'", "''") + "', SYSDATE, 'Erro', " + codemp + ", '" + idAluno + "' FROM DUAL");
				}
			}
		}
		String resumo = String.format("Títulos baixados com sucesso: %d, Títulos já baixados (ignorados): %d, Títulos com outros erros: %d.",
	            this.titulosBaixadosComSucesso,
	            this.titulosJaBaixadosIgnorados,
	            this.titulosComOutrosErros);
	    
	    this.resumoExecucao = resumo; 

	    LogCatcher.logInfo("Finalizando um lote do efetuarBaixa.");
	}
	

	private static final int MAX_REQUESTS_PER_MINUTE = 60;
	private static final long ONE_MINUTE_IN_MS = 60 * 1000;
	private static final Queue<Long> requestTimestamps = new LinkedList<>();
	private static final int MAX_RETRIES = 3; 
	private static final long INITIAL_RETRY_DELAY_MS = 2000; 

	public synchronized String[] apiGet(String ur, String token) throws Exception {
	    int attempt = 0;
	    while (attempt < MAX_RETRIES) {
	        try {
	            long currentTime = System.currentTimeMillis();
	            requestTimestamps.removeIf(timestamp -> currentTime - timestamp > ONE_MINUTE_IN_MS);

	            if (requestTimestamps.size() >= MAX_REQUESTS_PER_MINUTE) {
	                long oldestRequestTime = requestTimestamps.peek();
	                long waitTime = ONE_MINUTE_IN_MS - (currentTime - oldestRequestTime);
	                if (waitTime > 0) {
	                    LogCatcher.logInfo("Limite de 60 requisições por minuto atingido. Aguardando " + waitTime + "ms");
	                    Thread.sleep(waitTime);
	                }
	            }
	            requestTimestamps.offer(System.currentTimeMillis());

	            URL obj = new URL(ur.replace(" ", "%20"));
	            HttpURLConnection https = (HttpURLConnection) obj.openConnection();
	            https.setRequestMethod("GET");
	            https.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)");
	            https.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
	            https.setRequestProperty("Accept", "application/json");
	            https.setRequestProperty("Authorization", "Bearer " + token);
	            https.setConnectTimeout(15000); 
	            https.setReadTimeout(30000); 
	            https.setDoInput(true);

	            int status = https.getResponseCode();
	            StringBuilder responseContent = new StringBuilder();
	            BufferedReader reader = (status >= 300)
	                ? new BufferedReader(new InputStreamReader(https.getErrorStream()))
	                : new BufferedReader(new InputStreamReader(https.getInputStream()));

	            String line;
	            while ((line = reader.readLine()) != null) {
	                responseContent.append(line);
	            }
	            reader.close();
	            https.disconnect();

	            LogCatcher.logInfo("Output from Server .... \n" + status);

	            return new String[] { Integer.toString(status), responseContent.toString() };

	        } catch (java.net.SocketException | javax.net.ssl.SSLHandshakeException e) {
	            attempt++;
	            LogCatcher.logError("Tentativa " + attempt + " falhou com erro de rede: " + e.getMessage());
	            if (attempt < MAX_RETRIES) {
	                long delay = INITIAL_RETRY_DELAY_MS * (long) Math.pow(2, attempt - 1);
	                LogCatcher.logInfo("Aguardando " + delay + "ms antes da próxima tentativa.");
	                Thread.sleep(delay);
	            } else {
	                LogCatcher.logError("Número máximo de tentativas atingido. Desistindo.");
	                throw e;
	            }
	        }
	    }

	    return new String[] { "500", "{\"message\":\"Falha na comunicação com o servidor após " + MAX_RETRIES + " tentativas.\"}" };
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
			LogCatcher.logInfo("Passou do updateFin");
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
	        LogCatcher.logInfo("Iniciando estorno para NUFIN: " + nufin + " - Empresa: " + codemp);

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
	            
	            return;
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
			LogCatcher.logInfo("Passou do updateFinComVlrBaixa");
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
			pstmt.setBigDecimal(1, codTipTit); 
			pstmt.setBigDecimal(2, codBanco);
			pstmt.setBigDecimal(3, codConta);
			pstmt.setBigDecimal(4, nufin);
			pstmt.executeUpdate();
			LogCatcher.logInfo("Passou do updateFinCartao");
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
			LogCatcher.logInfo("Passou do updateBaixa");

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
			LogCatcher.logInfo("Passou do updateBaixaParcial");
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
			LogCatcher.logInfo("Entrou no updateResetarAlunos da flag dos alunos baixa");
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
			LogCatcher.logInfo(msgRetornoLog + " : " + diffInSeconds);
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
		LogCatcher.logInfo("Chegou no insertFin do financeiro segundo");
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
		LogCatcher.logInfo("Chegou no insertFinCartao do financeiro cartão");
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


	public List<Object[]> retornarInformacoesContagemIdBaixa() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		List<Object[]> listRet = new ArrayList();

		try {
			jdbc.openSession();
			String sql = "\tSELECT COUNT(0) AS C, CODEMP, AD_BAIXAID AS BAIXAID FROM TGFFIN WHERE RECDESP = 1 AND PROVISAO = 'N' AND DHBAIXA IS NOT NULL AND AD_IDALUNO IS NOT NULL GROUP BY CODEMP, AD_BAIXAID";
			pstmt = jdbc.getPreparedStatement(sql);
			rs = pstmt.executeQuery();

			while (rs.next()) {
				Object[] ret = new Object[3];
				ret[0] = rs.getBigDecimal("CODEMP");
				ret[1] = rs.getString("BAIXAID"); 
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



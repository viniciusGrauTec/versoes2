package br.com.sankhya.acoesgrautec.services;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.w3c.dom.Document;
import org.w3c.dom.Node;

import br.com.sankhya.acoesgrautec.util.XMLUtil;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.modelcore.comercial.CentralFinanceiro;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

public class SkwServicoFinanceiro {

	private SWServiceInvoker serviceInvoker;

	private ContextoAcao contexto;
	private SimpleDateFormat fmt;
	public static String codLanDestino = "007";
	public static String origemMovimento = "T";
	public static String codTipOperSangriaSuprimento = "1201";
	public static String historicoSangria = "SANGRIA";

	public String md5;
	public String nomeUsu;
	public String domain;

	public SkwServicoFinanceiro(String domain, String nomeUsu, String md5) {
		this.md5 = md5;
		this.nomeUsu = nomeUsu;
		this.domain = domain;
		this.serviceInvoker = new SWServiceInvoker(domain, nomeUsu, md5);
	}

	public SkwServicoFinanceiro(String domain, String nomeUsu, String md5,
			boolean criptedPass) {
		this.md5 = md5;
		this.nomeUsu = nomeUsu;
		this.domain = domain;
		this.serviceInvoker = new SWServiceInvoker(domain, nomeUsu, md5,
				criptedPass);
	}

	public SkwServicoFinanceiro(ContextoAcao contexto) throws Exception {
		this.contexto = contexto;
		this.serviceInvoker = new SWServiceInvoker(contexto);
		serviceInvoker.setDebugMode();
		this.fmt = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
	}

	public String saveMovimentoBancario(String codUsu, String codContaOrigem,
			String codContaDestino, int codLanOrigem, String codLanDestino,
			String vlrLan, String origemMovimento, String historico,
			String codTipOper, StringBuffer mensagem) throws Exception {

		// Composicao da consulta
		StringBuilder xmlRequestBody = new StringBuilder();
		xmlRequestBody
				.append("		<dataSet rootEntity='MovimentoBancario' includePresentationFields='S' crudListener='br.com.sankhya.mgefin.model.listeners.MovimentacaoBancariaCrudListener' datasetid='1616278131175_1'> ");
		xmlRequestBody.append("			<entity path=''> ");
		xmlRequestBody.append("				<fieldset list='*'/> ");
		xmlRequestBody.append("			</entity> ");
		xmlRequestBody.append("			<entity path='HistoricoBancario'> ");
		xmlRequestBody.append("				<field name='DESCRLANCBCO'/> ");
		xmlRequestBody.append("			</entity> ");
		xmlRequestBody.append("			<entity path='ImplantacaoSaldoConta'> ");
		xmlRequestBody.append("				<field name='DESCRICAO'/> ");
		xmlRequestBody.append("			</entity> ");
		xmlRequestBody.append("			<entity path='TipoOperacao'> ");
		xmlRequestBody.append("				<field name='DESCROPER'/> ");
		xmlRequestBody.append("			</entity> ");
		xmlRequestBody.append("			<entity path='Usuario'> ");
		xmlRequestBody.append("				<field name='NOMEUSU'/> ");
		xmlRequestBody.append("			</entity> ");
		xmlRequestBody.append("			<entity path='ContaDestino'> ");
		xmlRequestBody.append("				<field name='DESCRICAO'/> ");
		xmlRequestBody.append("			</entity> ");
		xmlRequestBody.append("			<entity path='ContaBancaria'> ");
		xmlRequestBody.append("				<field name='DESCRICAO'/> ");
		xmlRequestBody.append("			</entity> ");
		xmlRequestBody.append("			<entity path='HistoricoBancarioDestino'> ");
		xmlRequestBody.append("				<fieldset list='DESCRLANCBCO'/> ");
		xmlRequestBody.append("			</entity> ");
		xmlRequestBody.append("			<txProperties> ");
		xmlRequestBody.append("				<prop name='isChanged' value='true'/> ");
		xmlRequestBody
				.append("					<prop name='isChangedTipoOpe' value='true'/> ");
		xmlRequestBody.append("			</txProperties> ");
		xmlRequestBody.append("			<dataRow> ");
		xmlRequestBody.append("				<localFields> ");
		xmlRequestBody.append("					<CODUSU><![CDATA[" + codUsu
				+ "]]></CODUSU> ");
		xmlRequestBody.append("					<CODCTABCOINTDEST><![CDATA["
				+ codContaDestino + "]]></CODCTABCOINTDEST> ");
		xmlRequestBody.append("					<CODLANCDEST><![CDATA[" + codLanDestino
				+ "]]></CODLANCDEST> ");
		xmlRequestBody.append("					<CODCTABCOINT><![CDATA[" + codContaOrigem
				+ "]]></CODCTABCOINT> ");
		xmlRequestBody.append("					<HISTORICO><![CDATA[" + historico
				+ "]]></HISTORICO> ");
		xmlRequestBody.append("					<VLRLANC><![CDATA[" + vlrLan
				+ "]]></VLRLANC> ");
		xmlRequestBody.append("					<ORIGMOV><![CDATA[" + origemMovimento
				+ "]]></ORIGMOV> ");
		xmlRequestBody.append("					<CODTIPOPER><![CDATA[" + codTipOper
				+ "]]></CODTIPOPER> ");
		xmlRequestBody
				.append("						<CONCILIADODEST><![CDATA[N]]></CONCILIADODEST> ");
		xmlRequestBody.append("					<CONCILIADO><![CDATA[N]]></CONCILIADO> ");
		xmlRequestBody.append("					<CODLANC><![CDATA[" + codLanOrigem
				+ "]]></CODLANC> ");
		xmlRequestBody.append("				</localFields> ");
		xmlRequestBody.append("			</dataRow> ");
		xmlRequestBody.append("		</dataSet> ");
		xmlRequestBody.append("		<clientEventList/> ");

		// Chamada ao servi�o da Sankhya
		Document document = serviceInvoker.call(
				"CRUDServiceProvider.saveRecord", "mge",
				xmlRequestBody.toString(), codUsu);

		Node item = document.getChildNodes().item(0);

		if ("1".equals(item.getAttributes().getNamedItem("status")
				.getTextContent())) {
			return document.getElementsByTagName("NUBCO").item(0)
					.getTextContent();

		} else {
			return XMLUtil.xmlToString(document);
		}
	}

	public String baixarTitulo(String codUsu, String nuFin, String codEmp,
			String vlrDesdob, String codContaBaixa, int codLancamento,
			String numDocumento, String codTopBaixa, StringBuffer mensagem)
			throws Exception {
		// Composi��o da consulta
		StringBuilder xmlRequestBody = new StringBuilder();
		SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
		Date dataAtual = new Date();

		xmlRequestBody.append(" 	<dadosBaixa dtbaixa='"
				+ formatter.format(dataAtual) + "' nufin='" + nuFin
				+ "' decisao='0' reterImpostosRetroativos='true' ");
		xmlRequestBody
				.append(" 		resourceID='br.com.sankhya.fin.cad.movimentacaoFinanceira'> ");
		xmlRequestBody.append(" 		<variosTiposTitulos/> ");
		xmlRequestBody
				.append(" 			<valoresBaixa tipoJuros='I' tipoMulta='I' taxaAdm='0' vlrDesconto='0' vlrCalculado='"
						+ vlrDesdob + "' vlrDesdob='" + vlrDesdob + "' ");
		xmlRequestBody
				.append(" 			vlrDespesasCartorio='0' vlrJuros='0' vlrMulta='0' vlrTotal='"
						+ vlrDesdob
						+ "' vlrMultaNeg='0' vlrJurosNeg='0' jurosLib='0'  ");
		xmlRequestBody
				.append(" 			multaLib='0' vlrMoeda='0' vlrVarCambial='0'/> ");
		xmlRequestBody
				.append(" 			<dadosBancarios codConta='"
						+ codContaBaixa
						+ "' codLancamento='"
						+ codLancamento
						+ "' numDocumento='"
						+ numDocumento
						+ "' historico='Q0xJRU5URSAx' dataConciliacao='' codTipTit='' ");
		xmlRequestBody
				.append(" 			vlrMoedaBaixa='0,00000' contaParaCaixaAberto='0'/> ");
		xmlRequestBody.append(" 		<dadosAdicionais codEmpresa='" + codEmp
				+ "' codTipoOperacao='" + codTopBaixa + "'/> ");
		xmlRequestBody
				.append(" 			<impostos inssRetido='false' irrfRetido='false' issRetido='false' outrosImpostos='0' outrosImpostosMensais='0'  ");
		xmlRequestBody
				.append(" 				vlrInss='0' vlrIrrf='0' vlrIss='0' vlrVendor='0' baseDevida='0'/> ");
		xmlRequestBody
				.append(" 			<imprimeRecibo paramImprimeBoleto='false'/> ");
		xmlRequestBody.append(" 	</dadosBaixa> ");
		xmlRequestBody.append(" 	<clientEventList> ");
		xmlRequestBody
				.append(" 				<clientEvent>br.com.sankhya.mgefin.avisa.validacao.numero.financeiro</clientEvent> ");
		xmlRequestBody
				.append(" 				<clientEvent>br.com.sankhya.mgefin.financeiro.Autentica</clientEvent> ");
		xmlRequestBody
				.append("		 		<clientEvent>br.com.sankhya.mgefin.solicitacao.liberacao.orcamento</clientEvent> ");
		xmlRequestBody
				.append(" 				<clientEvent>br.com.sankhya.mgefin.alteracao.boleto.agrupado.registrado</clientEvent> ");
		xmlRequestBody
				.append(" 				<clientEvent>br.com.sankhya.mgecom.pesquisamov.mensagem</clientEvent> ");
		xmlRequestBody
				.append("		 		<clientEvent>br.com.sankhya.mgefin.mensagem.metas</clientEvent> ");
		xmlRequestBody
				.append(" 				<clientEvent>br.com.sankhya.mgefin.financeiro.LibLimite</clientEvent> ");
		xmlRequestBody.append(" 	</clientEventList> ");

		// Chamada ao servi�o da Sankhya
		serviceInvoker.call("BaixaFinanceiroSP.baixarTitulo", "mge",
				xmlRequestBody.toString(), codUsu);
		return xmlRequestBody.toString();

	}

	public void refazerFinanceiro(final BigDecimal nuNota) throws Exception {
		JdbcWrapper jdbc = null;
		jdbc = EntityFacadeFactory.getDWFFacade().getJdbcWrapper();
		jdbc.openSession();
		final CentralFinanceiro financeiro = new CentralFinanceiro();
		final NativeSql nativeSql = new NativeSql(jdbc);
		nativeSql.appendSql("DELETE TGFFIN WHERE NUNOTA = :NUNOTA");
		nativeSql.addParameter((Object) nuNota);
		nativeSql.executeQuery();
		financeiro.inicializaNota(nuNota);
		financeiro.refazerFinanceiro();
	}
}

package br.com.sankhya.acoesgrautec.services;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.w3c.dom.Document;
import org.w3c.dom.Node;

import br.com.sankhya.acoesgrautec.util.EnviromentUtils;
import br.com.sankhya.acoesgrautec.util.XMLUtil;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.extensions.actionbutton.QueryExecutor;

public class SkwServicoProducao {

	private SWServiceInvoker serviceInvoker;

	private ContextoAcao contexto;
	private SimpleDateFormat fmt;

	public SkwServicoProducao(ContextoAcao contexto) throws Exception {
		this.contexto = contexto;
		this.serviceInvoker = new SWServiceInvoker(contexto);
		serviceInvoker.setDebugMode();
		this.fmt = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
	}

	public long printLogDebug(long tempoAnterior, String msgRetornoLog) {
		return serviceInvoker.printLogDebug(tempoAnterior, msgRetornoLog);
	}

	/*
	 * A ação de criar a Novo Lançamento da Ordem de produção Retornar o
	 * número da OP
	 */
	public String criarNovoLancamentoOP(int codProd) throws Exception {
		StringBuilder xmlRequestBody = new StringBuilder();

		Date dtHoje = new Date();
		String textoLanc = "OP Aut: Item(" + codProd + ") - "
				+ dtHoje.getTime();
		xmlRequestBody.append(" 	<params descricao='" + textoLanc
				+ "' reutilizar='N'/> ");
		xmlRequestBody.append(" 	<clientEventList> ");
		xmlRequestBody
				.append(" 				<clientEvent>br.com.sankhya.actionbutton.clientconfirm</clientEvent> ");
		xmlRequestBody
				.append(" 				<clientEvent>br.com.sankhya.mgeprod.redimensionar.op.pa.avisos</clientEvent> ");
		xmlRequestBody
				.append(" 				<clientEvent>br.com.sankhya.mgeprod.confirmacao.substituir.pa</clientEvent> ");
		xmlRequestBody
				.append(" 				<clientEvent>br.com.sankhya.mgeProd.libera.wc</clientEvent> ");
		xmlRequestBody
				.append(" 				<clientEvent>br.com.sankhya.mgeprod.redimensionar.op.pa</clientEvent> ");
		xmlRequestBody
				.append(" 				<clientEvent>br.com.mgeprod.clientEvent.cancelaOP.deleteNotaConfirmada</clientEvent>  ");
		xmlRequestBody.append(" 	</clientEventList> ");

		Document document = serviceInvoker.call(
				"LancamentoOrdemProducaoSP.getNovoLancamentoOP", "mgeprod",
				xmlRequestBody.toString());

		Node item = document.getChildNodes().item(0);

		if ("1".equals(item.getAttributes().getNamedItem("status")
				.getTextContent())) {
			return document.getElementsByTagName("lancamento").item(0)
					.getAttributes().getNamedItem("nulop").getTextContent();

		} else {
			return XMLUtil.xmlToString(document);
		}
	}

	/*
	 * A ação de criar a Ordem de produção
	 */
	public void criarLancamentoOP(String nulop) throws Exception {
		StringBuilder xmlRequestBody = new StringBuilder();

		xmlRequestBody.append(" <params nulop='" + nulop
				+ "' useconfigpi='true'/> ");
		xmlRequestBody.append(" <clientEventList> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.actionbutton.clientconfirm</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgeprod.redimensionar.op.pa.avisos</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgeprod.confirmacao.substituir.pa</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgeProd.libera.wc</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgeprod.redimensionar.op.pa</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.mgeprod.clientEvent.cancelaOP.deleteNotaConfirmada</clientEvent> ");
		xmlRequestBody.append(" </clientEventList> ");

		serviceInvoker.call("LancamentoOrdemProducaoSP.getLancamentoOP",
				"mgeprod", xmlRequestBody.toString());
	}

	/*
	 * A ação de inserir o produto na OP
	 */
	public void inserirProdutoOP(String nulop, int codProd, int codPlp,
			String controle) throws Exception {
		StringBuilder xmlRequestBody = new StringBuilder();
		xmlRequestBody
				.append(" 		<params nulop='"
						+ nulop
						+ "' codplp='"
						+ codPlp
						+ "' codprod='"
						+ codProd
						+ "' codParc='' agruparEmUnicaOP='N' opDesmonte='N' opReparo='N'> ");
		xmlRequestBody.append(" 	<controle controle='" + controle + "'/> ");
		xmlRequestBody.append(" </params> ");
		xmlRequestBody.append(" <clientEventList> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.actionbutton.clientconfirm</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgeprod.redimensionar.op.pa.avisos</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgeprod.confirmacao.substituir.pa</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgeProd.libera.wc</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgeprod.redimensionar.op.pa</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.mgeprod.clientEvent.cancelaOP.deleteNotaConfirmada</clientEvent> ");
		xmlRequestBody.append(" </clientEventList> ");

		serviceInvoker.call("LancamentoOrdemProducaoSP.inserirProduto",
				"mgeprod", xmlRequestBody.toString());
	}

	/*
	 * A ação de lançar OP
	 */
	public void alterarAtributoProduto(String nulop, String seqop,
			String chave, String valor, String codProd, String nroLote)
			throws Exception {
		StringBuilder xmlRequestBody = new StringBuilder();
		xmlRequestBody.append(" <params nulop='" + nulop + "' seqop='" + seqop
				+ "'");

		if (codProd != null) {
			xmlRequestBody.append(" codprod='" + codProd + "' ");
		}
		xmlRequestBody.append(" controle='" + nroLote + "'  atributo='" + chave
				+ "' valor='" + valor + "'/> ");
		xmlRequestBody.append(" <clientEventList> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.actionbutton.clientconfirm</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgeprod.redimensionar.op.pa.avisos</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgeprod.confirmacao.substituir.pa</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgeProd.libera.wc</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgeprod.redimensionar.op.pa</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.mgeprod.clientEvent.cancelaOP.deleteNotaConfirmada</clientEvent> ");
		xmlRequestBody.append(" </clientEventList> ");

		serviceInvoker.call("LancamentoOrdemProducaoSP.alterarAtributoProduto",
				"mgeprod", xmlRequestBody.toString());
	}

	/*
	 * A ação de lançar OP
	 */
	public void lancarOrdensDeProducao(String nulop, int codplp, int codProd)
			throws Exception {
		StringBuilder xmlRequestBody = new StringBuilder();
		xmlRequestBody.append(" <params nulop='" + nulop
				+ "' ignorarWarnings='S' /> ");
		xmlRequestBody.append(" <clientEventList> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.actionbutton.clientconfirm</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgeprod.redimensionar.op.pa.avisos</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgeprod.confirmacao.substituir.pa</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgeProd.libera.wc</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgeprod.redimensionar.op.pa</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.mgeprod.clientEvent.cancelaOP.deleteNotaConfirmada</clientEvent> ");
		xmlRequestBody.append(" </clientEventList> ");

		serviceInvoker.call("LancamentoOrdemProducaoSP.lancarOrdensDeProducao",
				"mgeprod", xmlRequestBody.toString());
	}

	/*
	 * A ação de iniciar uma atividade consiste em sinalizar ao sistema que a
	 * atividade em questão foi inicializada por um operador e/ou uma máquina.
	 */
	public void iniciarAtividade(Long idiatv) throws Exception {
		// Inicializando a intância

		StringBuilder xmlRequestBody = new StringBuilder();
		xmlRequestBody.append("  <instancias> ");
		xmlRequestBody.append("       <instancia>	 ");
		xmlRequestBody.append("                 <IDIATV>" + idiatv
				+ "</IDIATV>	 ");
		xmlRequestBody.append("       </instancia>	 ");
		xmlRequestBody.append("  </instancias> ");
		xmlRequestBody.append("  <clientEventList> ");
		xmlRequestBody
				.append("  			<clientEvent>br.com.sankhya.mgeprod.redimensionar.op.pa.perda</clientEvent> ");
		xmlRequestBody
				.append("  			<clientEvent>br.com.sankhya.mgeProd.apontamento.ultimo</clientEvent> ");
		xmlRequestBody
				.append("  			<clientEvent>br.com.sankhya.mgeprod.finalizar.liberacao.desvio.pa</clientEvent> ");
		xmlRequestBody
				.append("  			<clientEvent>br.com.sankhya.actionbutton.clientconfirm</clientEvent> ");
		xmlRequestBody
				.append("  			<clientEvent>br.com.sankhya.mgeProd.apontamento.liberaNroSerie</clientEvent> ");
		xmlRequestBody
				.append("  			<clientEvent>br.com.sankhya.mgeprod.trocaturno.avisos</clientEvent> ");
		xmlRequestBody
				.append("  			<clientEvent>br.com.sankhya.prod.remove.apontamento.pesagemvolume</clientEvent> ");
		xmlRequestBody
				.append("  			<clientEvent>br.com.sankhya.mgeprod.operacaoproducao.mpalt.proporcao.apontamento.invalida</clientEvent> ");
		xmlRequestBody
				.append("  			<clientEvent>br.com.sankhya.mgeprod.redimensionar.op.pa.avisos</clientEvent> ");
		xmlRequestBody
				.append("  			<clientEvent>br.com.sankhya.mgeProd.wc.indisponivel</clientEvent> ");
		xmlRequestBody
				.append("  			<clientEvent>br.com.sankhya.mgeprod.apontamentos.divergentes</clientEvent> ");
		xmlRequestBody.append("  </clientEventList> ");

		serviceInvoker.call("OperacaoProducaoSP.iniciarInstanciaAtividades",
				"mgeprod", xmlRequestBody.toString());
	}

	/*
	 * A ação de iniciar uma atividade consiste em sinalizar ao sistema que a
	 * atividade em questão foi inicializada por um operador e/ou uma máquina.
	 * IDIATV -> Integer -> Código da instancia de atividade
	 * 
	 * CODPROD -> Integer -> Código do produto (PA)
	 * 
	 * CONTROLE -> String -> Controle do PA
	 * 
	 * QTDPRODUZIR -> Float -> Qtd. boa
	 * 
	 * QTDPERDA -> Float -> Qtd. ruim
	 * 
	 * CODMPE -> Integer -> Código do motivo de perda para a ‘Qtd. ruim’
	 */
	public String criarApontamento(Long idiatv, Long codProd, String controle,
			BigDecimal qtdProduzir, BigDecimal qtdPerca, Long codMPE)
			throws Exception {
		StringBuilder xmlRequestBody = new StringBuilder();
		// xmlRequestBody.append(" <params IDIATV='" + idiatv + "' CODPROD='"
		// + codProd + "' CONTROLE='" + controle + "' ");
		// xmlRequestBody.append("   QTDPRODUZIR='" + qtdProduzir +
		// "' QTDPERDA='"
		// + qtdPerca + "' CODMPE=''/> ");

		xmlRequestBody.append(" <params IDIATV='" + idiatv + "'/> ");
		xmlRequestBody.append(" <clientEventList> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgeprod.redimensionar.op.pa.perda</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgeProd.apontamento.ultimo</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgeprod.finalizar.liberacao.desvio.pa</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.actionbutton.clientconfirm</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgeProd.apontamento.liberaNroSerie</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgeprod.trocaturno.avisos</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.prod.remove.apontamento.pesagemvolume</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgeprod.operacaoproducao.mpalt.proporcao.apontamento.invalida</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgeprod.redimensionar.op.pa.avisos</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgeProd.wc.indisponivel</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgeprod.apontamentos.divergentes</clientEvent> ");
		xmlRequestBody.append(" </clientEventList> ");

		Document document = serviceInvoker.call(
				"OperacaoProducaoSP.criarApontamento", "mgeprod",
				xmlRequestBody.toString());

		Node item = document.getChildNodes().item(0);

		if ("1".equals(item.getAttributes().getNamedItem("status")
				.getTextContent())) {
			return document.getElementsByTagName("apontamento").item(0)
					.getAttributes().getNamedItem("NUAPO").getTextContent();
		} else {
			return XMLUtil.xmlToString(document);
		}
	}

	/*
	 * Após a conclusão do apontamento, é necessário confirma-lo para
	 * representar sistemicamente que aquele apontamento já está
	 * certo/finalizado e que é necessário efetivar as movimentações de
	 * estoque (operações de estoque) configuradas. NUAPO -> Integer ->
	 * Código do cabeçalho do apontamento
	 * 
	 * IDIATV -> Integer -> Código da instancia de atividade
	 * 
	 * ULTIMOAPONTAMENTO -> Boolean -> Paramêtros para informar se será o
	 * último apontamento
	 * 
	 * RESPOSTA_ULTIMO_APONTAMENTO -> Boolean -> Paramêtros para informar se
	 * será o último apontamento
	 * 
	 * RESPOSTA_SERIE_LIBERADO -> Boolean -> Não implementado, sempre enviar
	 * valor "true"
	 * 
	 * RESPOSTA_SERIE_LIBERADO_PERDA -> Boolean -> Não implementado, sempre
	 * enviar valor "true"
	 * 
	 * ACEITA_PROPORCAO_INVALIDA_MPALTERNATIVA -> Boolean -> Não implementado,
	 * sempre enviar valor "true"
	 */
	public void confirmarApontamentoPA(Long nuApo, Long idiatv,
			boolean aceitaQtdMaior, boolean ultimoApontamento,
			boolean respostaUltimoApontamento, boolean respostaSerieLiberado,
			boolean respostaSerieLiberadoPerda,
			boolean aceitaProporcaoInvalidaMpAlternativa) throws Exception {
		StringBuilder xmlRequestBody = new StringBuilder();
		xmlRequestBody.append(" <params 	NUAPO='" + nuApo + "' IDIATV='"
				+ idiatv + "' 				ACEITARQTDMAIOR='" + aceitaQtdMaior + "' ");
		xmlRequestBody.append("          	ULTIMOAPONTAMENTO='"
				+ ultimoApontamento + "'/> ");
		xmlRequestBody.append(" <clientEventList> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgeprod.trocaturno.avisos</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgeprod.operacaoproducao.mpalt.proporcao.apontamento.invalida</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgeprod.redimensionar.op.pa.avisos</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.apontamentomp.naoreproporcionalizado</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgeProd.apontamento.ultimo</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgeprod.finalizar.liberacao.desvio.pa</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgeProd.apontamento.liberaNroSerie</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.prod.remove.apontamento.pesagemvolume</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgeProd.wc.indisponivel</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.actionbutton.clientconfirm</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgeprod.apontamentos.divergentes</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgeprod.redimensionar.op.pa.perda</clientEvent> ");
		xmlRequestBody.append(" </clientEventList> ");

		serviceInvoker.call("OperacaoProducaoSP.confirmarApontamento",
				"mgeprod", xmlRequestBody.toString());

	}

	/*
	 * O ato de finalizar uma atividade consiste em sinalizar à aplicação que
	 * o trabalho executado em uma atividade foi concluído pelo operador e/ou
	 * pela máquina.
	 * 
	 * A seguir tem-se o serviço
	 * OperacaoProducaoSP.finalizarInstanciaAtividades utilizado para comandar
	 * esta ação:
	 * 
	 * Finalizar a atividade
	 * 
	 * IDIATV -> Integer -> Código da instancia da atividade
	 * 
	 * IDEFX -> Integer -> Código da atividade no processo produtivo
	 * 
	 * IDIPROC -> Integer -> Número da Ordem de produção
	 */
	public void finalizarInstanciaAtividades(Long idiatv, Long idefx,
			Long idiproc) throws Exception {
		StringBuilder xmlRequestBody = new StringBuilder();
		xmlRequestBody
				.append(" 			<instancias confirmarApontamentosDivergentes='true' ");
		xmlRequestBody.append("         ignoraValidacaoFormulario='true'> ");
		xmlRequestBody.append("              <instancia> ");
		xmlRequestBody.append("              			<IDIATV>" + idiatv
				+ "</IDIATV> ");
		xmlRequestBody.append("                 <IDEFX>" + idefx + "</IDEFX> ");
		xmlRequestBody.append("                 <IDIPROC>" + idiproc
				+ "</IDIPROC> ");
		xmlRequestBody.append("               </instancia> ");
		xmlRequestBody.append("     </instancias> ");

		serviceInvoker.call("OperacaoProducaoSP.finalizarInstanciaAtividades",
				"mgeprod", xmlRequestBody.toString());

	}

	public void cancelarOrdens(Long idProc, Long idiproc) throws Exception {
		StringBuilder xmlRequestBody = new StringBuilder();
		xmlRequestBody.append(" <ordens idicop='0'> ");
		xmlRequestBody.append("     <ordem IDIPROC='" + idiproc + "' IDPROC='"
				+ idProc + "' STATUSPROC='A'/> ");
		xmlRequestBody.append(" </ordens> ");
		xmlRequestBody.append("     <clientEventList> ");
		xmlRequestBody
				.append("     	<clientEvent>br.com.sankhya.mgeprod.redimensionar.op.pa.avisos</clientEvent> ");
		xmlRequestBody
				.append("     	<clientEvent>br.com.mgeprod.clientEvent.cancelaOP.deleteNotaConfirmada</clientEvent> ");
		xmlRequestBody
				.append("     	<clientEvent>br.com.sankhya.mgeprod.redimensionar.op.pa</clientEvent> ");
		xmlRequestBody
				.append("     	<clientEvent>br.com.sankhya.mgeprod.confirmacao.substituir.pa</clientEvent> ");
		xmlRequestBody
				.append("     	<clientEvent>br.com.sankhya.mgeProd.libera.wc</clientEvent> ");
		xmlRequestBody
				.append("     	<clientEvent>br.com.sankhya.actionbutton.clientconfirm</clientEvent> ");
		xmlRequestBody.append("     </clientEventList> ");

		serviceInvoker.call("OrdemProducaoSP.cancelarOrdens", "mgeprod",
				xmlRequestBody.toString());

	}

	public void confirmarNota(Long nuNota) throws Exception {
		// Composi��o da consulta
		StringBuilder xmlRequestBody = new StringBuilder();
		xmlRequestBody
				.append(" 		<nota confirmacaoCentralNota='true' ehPedidoWeb='false' atualizaPrecoItemPedCompra='false' ownerServiceCall='CentralNotas'> ");
		xmlRequestBody.append(" 	<NUNOTA>" + nuNota + "</NUNOTA> ");
		xmlRequestBody.append(" </nota> ");
		xmlRequestBody.append(" <clientEventList> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.utiliza.dtneg.servidor</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.valida.ChaveNFeCompraTerceiros</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgeprod.producao.terceiro.inclusao.item.nota</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>central.save.grade.itens.mostrar.popup.info.lote</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.expedicao.SolicitarUsuarioConferente</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.event.troca.item.por.produto.alternativo</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.nota.adicional.SolicitarUsuarioGerente</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.cancelamento.notas.remessa</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.central.itens.KitRevenda</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecomercial.event.estoque.componentes</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.exibe.msg.variacao.preco</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.central.itens.VendaCasada</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecomercial.event.cadastrarDistancia</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.parcelas.financeiro</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.checkout.obter.peso</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecomercial.event.estoque.insuficiente.produto</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>central.save.grade.itens.mostrar.popup.serie</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.imobilizado</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecomercial.event.baixaPortal</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecomercial.event.compensacao.credito.debito</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecomercial.event.faturamento.confirmacao</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgefin.event.fixa.vencimento</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.coleta.entrega.recalculado</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.central.itens.KitRevenda.msgValidaFormula</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.importacaoxml.cfi.para.produto</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgefin.solicitacao.liberacao.orcamento</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.exibir.variacao.valor.item</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.event.troca.item.por.produto.substituto</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.actionbutton.clientconfirm</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.compra.SolicitacaoComprador</clientEvent> ");
		xmlRequestBody.append(" </clientEventList> ");

		// Chamada ao servi�o da Sankhya
		serviceInvoker.call("CACSP.confirmarNota", "mgecom",
				xmlRequestBody.toString());
	}

	public void gerarLote(Long nuNota) throws Exception {
		// Composi��o da consulta
		StringBuilder xmlRequestBody = new StringBuilder();
		xmlRequestBody
				.append(" 		<notas retNotasReprovadas='true' habilitaClientEvent='S'> ");
		xmlRequestBody.append(" 	<nunota>" + nuNota + "</nunota> ");
		xmlRequestBody.append(" </notas> ");
		xmlRequestBody.append(" <clientEventList> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.modelcore.comercial.cancela.nfce.baixa.caixa.fechado</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgewms.expedicao.selecaoDocas</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.msg.nao.possui.itens.pendentes</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.comercial.solicitaContingencia</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.utiliza.dtneg.servidor</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.compra.SolicitacaoComprador</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgewms.expedicao.validarPedidos</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgewms.expedicao.cortePedidos</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.cancelamento.nfeAcimaTolerancia</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecomercial.event.baixaPortal</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.nota.adicional.SolicitarUsuarioGerente</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.cancelamento.notas.remessa</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.expedicao.SolicitarUsuarioConferente</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.gera.lote.xmlRejeitado</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.fat.preco.produtos.alterado</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>comercial.status.nfe.situacao.diferente</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.valida.ChaveNFeCompraTerceiros</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.cancelamento.processo.wms.andamento</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.actionbutton.clientconfirm</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecomercial.event.compensacao.credito.debito</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.modelcore.comercial.cancela.nota.devolucao.wms</clientEvent> ");
		xmlRequestBody.append(" </clientEventList> ");

		// Chamada ao servi�o da Sankhya
		serviceInvoker.call("ServicosNfeSP.gerarLote", "mgecom",
				xmlRequestBody.toString());
	}

	public void refazerFinanceiro(Long nuNota) throws Exception {
		// Composi��o da consulta
		StringBuilder xmlRequestBody = new StringBuilder();
		xmlRequestBody.append(" <nota NUNOTA='" + nuNota
				+ "' ownerServiceCall='CentralNotas'> ");
		xmlRequestBody.append(" 	<txProperties> ");
		xmlRequestBody
				.append(" 		<prop name='br.com.sankhya.mgefin.recalculo.custopreco.Automatico' value='false'/> ");
		xmlRequestBody
				.append(" 		<prop name='cabecalhoNota.inserindo.pedidoWeb' value='false'/> ");
		xmlRequestBody
				.append(" 		<prop name='br.com.sankhya.mgefin.checarfinanceiro.VlrEntrada' value='0'/> ");
		xmlRequestBody
				.append(" 		<prop name='br.com.sankhya.mgefin.checarfinanceiro.RecalcularVencimento' value='false'/> ");
		xmlRequestBody.append(" 	</txProperties> ");
		xmlRequestBody.append(" </nota> ");
		xmlRequestBody
				.append(" <clientEventList><clientEvent>br.com.sankhya.mgecom.event.troca.item.por.produto.substituto</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecomercial.event.cadastrarDistancia</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.parcelas.financeiro</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.exibe.msg.variacao.preco</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.event.troca.item.por.produto.alternativo</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecomercial.event.baixaPortal</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.checkout.obter.peso</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecomercial.event.estoque.insuficiente.produto</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgefin.event.fixa.vencimento</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.coleta.entrega.recalculado</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.cancelamento.notas.remessa</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.compra.SolicitacaoComprador</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgefin.solicitacao.liberacao.orcamento</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.importacaoxml.cfi.para.produto</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>central.save.grade.itens.mostrar.popup.info.lote</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.imobilizado</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.expedicao.SolicitarUsuarioConferente</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecomercial.event.faturamento.confirmacao</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.exibir.variacao.valor.item</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecomercial.event.compensacao.credito.debito</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.actionbutton.clientconfirm</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.utiliza.dtneg.servidor</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecomercial.event.estoque.componentes</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.central.itens.VendaCasada</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.central.itens.KitRevenda.msgValidaFormula</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.valida.ChaveNFeCompraTerceiros</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>central.save.grade.itens.mostrar.popup.serie</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgeprod.producao.terceiro.inclusao.item.nota</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.nota.adicional.SolicitarUsuarioGerente</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.central.itens.KitRevenda</clientEvent> ");
		xmlRequestBody.append(" </clientEventList> ");

		// Chamada ao servi�o da Sankhya
		serviceInvoker.call("CACSP.refazerFinanceiro", "mgecom",
				xmlRequestBody.toString());
	}

	public void gerarNotaRemessa(Long nuNota) throws Exception {
		// Composi��o da consulta
		StringBuilder xmlRequestBody = new StringBuilder();
		xmlRequestBody.append(" 	<notas> ");
		xmlRequestBody.append(" 		<numeroNota>" + nuNota + "</numeroNota> ");
		xmlRequestBody.append(" 	</notas> ");
		xmlRequestBody.append(" 	<clientEventList> ");
		xmlRequestBody
				.append(" 				<clientEvent>br.com.sankhya.mgecom.expedicao.SolicitarUsuarioConferente</clientEvent> ");
		xmlRequestBody
				.append(" 				<clientEvent>br.com.sankhya.mgecom.gera.lote.xmlRejeitado</clientEvent> ");
		xmlRequestBody
				.append(" 				<clientEvent>br.com.sankhya.mgecom.valida.ChaveNFeCompraTerceiros</clientEvent> ");
		xmlRequestBody
				.append(" 				<clientEvent>br.com.sankhya.actionbutton.clientconfirm</clientEvent> ");
		xmlRequestBody
				.append(" 				<clientEvent>br.com.sankhya.mgecom.nota.adicional.SolicitarUsuarioGerente</clientEvent> ");
		xmlRequestBody
				.append(" 				<clientEvent>br.com.sankhya.mgecomercial.event.baixaPortal</clientEvent> ");
		xmlRequestBody
				.append(" 				<clientEvent>br.com.sankhya.mgecom.cancelamento.processo.wms.andamento</clientEvent> ");
		xmlRequestBody
				.append(" 				<clientEvent>br.com.sankhya.mgecomercial.event.compensacao.credito.debito</clientEvent> ");
		xmlRequestBody
				.append(" 				<clientEvent>br.com.sankhya.modelcore.comercial.cancela.nfce.baixa.caixa.fechado</clientEvent> ");
		xmlRequestBody
				.append(" 				<clientEvent>br.com.sankhya.mgecom.compra.SolicitacaoComprador</clientEvent> ");
		xmlRequestBody
				.append(" 				<clientEvent>br.com.sankhya.mgecom.cancelamento.nfeAcimaTolerancia</clientEvent> ");
		xmlRequestBody
				.append(" 				<clientEvent>br.com.utiliza.dtneg.servidor</clientEvent> ");
		xmlRequestBody
				.append(" 				<clientEvent>br.com.sankhya.mgecom.msg.nao.possui.itens.pendentes</clientEvent> ");
		xmlRequestBody
				.append(" 				<clientEvent>br.com.sankhya.mgewms.expedicao.cortePedidos</clientEvent> ");
		xmlRequestBody
				.append(" 				<clientEvent>br.com.sankhya.mgewms.expedicao.selecaoDocas</clientEvent> ");
		xmlRequestBody
				.append(" 				<clientEvent>br.com.sankhya.comercial.solicitaContingencia</clientEvent> ");
		xmlRequestBody
				.append(" 				<clientEvent>br.com.sankhya.mgecom.fat.preco.produtos.alterado</clientEvent> ");
		xmlRequestBody
				.append(" 				<clientEvent>br.com.sankhya.mgecom.cancelamento.notas.remessa</clientEvent> ");
		xmlRequestBody
				.append(" 				<clientEvent>br.com.sankhya.mgewms.expedicao.validarPedidos</clientEvent> ");
		xmlRequestBody
				.append(" 				<clientEvent>comercial.status.nfe.situacao.diferente</clientEvent> ");
		xmlRequestBody
				.append(" 				<clientEvent>br.com.sankhya.modelcore.comercial.cancela.nota.devolucao.wms</clientEvent> ");
		xmlRequestBody.append(" 	</clientEventList> ");

		// Chamada ao servi�o da Sankhya
		serviceInvoker.call("CACSP.gerarNotaRemessa", "mgecom",
				xmlRequestBody.toString());
	}

	public void excluirNotas(Long nuNota) throws Exception {
		// Composicao da consulta
		StringBuilder xmlRequestBody = new StringBuilder();
		xmlRequestBody.append(" <notas ownerServiceCall='CentralNotas'> ");
		xmlRequestBody.append(" 	<nota NUNOTA='" + nuNota + "'/> ");
		xmlRequestBody.append(" </notas> ");
		xmlRequestBody.append(" <clientEventList> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.exibe.msg.variacao.preco</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.central.itens.VendaCasada</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.importacaoxml.cfi.para.produto</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.cancelamento.notas.remessa</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.checkout.obter.peso</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.imobilizado</clientEvent> ");
		xmlRequestBody
				.append("		 	<clientEvent>br.com.sankhya.actionbutton.clientconfirm</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecomercial.event.estoque.componentes</clientEvent> ");
		xmlRequestBody
				.append("		 	<clientEvent>br.com.sankhya.mgecom.nota.adicional.SolicitarUsuarioGerente</clientEvent> ");
		xmlRequestBody
				.append("		 	<clientEvent>br.com.sankhya.mgecomercial.event.faturamento.confirmacao</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.valida.ChaveNFeCompraTerceiros</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecomercial.event.estoque.insuficiente.produto</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>central.save.grade.itens.mostrar.popup.info.lote</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.utiliza.dtneg.servidor</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.exibir.variacao.valor.item</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.expedicao.SolicitarUsuarioConferente</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.central.itens.KitRevenda.msgValidaFormula</clientEvent> ");
		xmlRequestBody
				.append("		 	<clientEvent>br.com.sankhya.mgecom.coleta.entrega.recalculado</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgefin.event.fixa.vencimento</clientEvent> ");
		xmlRequestBody
				.append("		 	<clientEvent>br.com.sankhya.mgecomercial.event.cadastrarDistancia</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.event.troca.item.por.produto.substituto</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgefin.solicitacao.liberacao.orcamento</clientEvent> ");
		xmlRequestBody
				.append("		 	<clientEvent>br.com.sankhya.mgecom.compra.SolicitacaoComprador</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecomercial.event.baixaPortal</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.central.itens.KitRevenda</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.event.troca.item.por.produto.alternativo</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.parcelas.financeiro</clientEvent> ");
		xmlRequestBody
				.append("		 	<clientEvent>br.com.sankhya.mgecomercial.event.compensacao.credito.debito</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>central.save.grade.itens.mostrar.popup.serie</clientEvent> ");
		xmlRequestBody.append(" </clientEventList> ");

		// Chamada ao servi�o da Sankhya
		serviceInvoker.call("CACSP.excluirNotas", "mgecom",
				xmlRequestBody.toString());

	}

	public void cancelarNota(Long nuNota, String justificativa)
			throws Exception {
		// Composicao da consulta
		StringBuilder xmlRequestBody = new StringBuilder();
		xmlRequestBody.append(" 	<notasCanceladas justificativa='"
				+ justificativa + "' validarProcessosWmsEmAndamento='true'> ");
		xmlRequestBody.append(" 			<nunota>" + nuNota + "</nunota> ");
		xmlRequestBody
				.append("				<txProperties><prop name='pode.cancelar.notas.remessa' value='true'/></txProperties> ");
		xmlRequestBody.append(" 	</notasCanceladas> ");
		xmlRequestBody.append(" 	<clientEventList> ");
		xmlRequestBody
				.append(" 				<clientEvent>br.com.utiliza.dtneg.servidor</clientEvent><clientEvent>comercial.status.nfe.situacao.diferente</clientEvent><clientEvent>br.com.sankhya.mgewms.expedicao.cortePedidos</clientEvent><clientEvent>br.com.sankhya.mgecom.cancelamento.nfeAcimaTolerancia</clientEvent><clientEvent>br.com.sankhya.mgecomercial.event.compensacao.credito.debito</clientEvent><clientEvent>br.com.sankhya.actionbutton.clientconfirm</clientEvent><clientEvent>br.com.sankhya.modelcore.comercial.cancela.nota.devolucao.wms</clientEvent><clientEvent>br.com.sankhya.mgecom.msg.nao.possui.itens.pendentes</clientEvent><clientEvent>br.com.sankhya.mgecom.cancelamento.processo.wms.andamento</clientEvent><clientEvent>br.com.sankhya.mgecom.compra.SolicitacaoComprador</clientEvent><clientEvent>br.com.sankhya.mgewms.expedicao.selecaoDocas</clientEvent><clientEvent>br.com.sankhya.mgecom.valida.ChaveNFeCompraTerceiros</clientEvent><clientEvent>br.com.sankhya.mgecom.cancelamento.notas.remessa</clientEvent><clientEvent>br.com.sankhya.mgecom.expedicao.SolicitarUsuarioConferente</clientEvent><clientEvent>br.com.sankhya.mgewms.expedicao.validarPedidos</clientEvent><clientEvent>br.com.sankhya.comercial.solicitaContingencia</clientEvent><clientEvent>br.com.sankhya.mgecom.fat.preco.produtos.alterado</clientEvent><clientEvent>br.com.sankhya.mgecom.nota.adicional.SolicitarUsuarioGerente</clientEvent><clientEvent>br.com.sankhya.modelcore.comercial.cancela.nfce.baixa.caixa.fechado</clientEvent> ");
		xmlRequestBody.append(" 	</clientEventList> ");

		// Chamada ao servi�o da Sankhya
		serviceInvoker.call("CACSP.cancelarNota", "mgecom",
				xmlRequestBody.toString());

	}

	public void gerarInutilizacaoDeNumeracaoNFE(Long codEmp, String modeloNota,
			String serieNota, Long numNota, String justificativa)
			throws Exception {
		// Composicao da consulta
		StringBuilder xmlRequestBody = new StringBuilder();
		SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
		Date date = new Date();
		xmlRequestBody.append(" <inutilizacaoNfe codemp='" + codEmp
				+ "' datamov='" + formatter.format(date) + "' modelo='"
				+ modeloNota + "' serienota='" + serieNota + "' nuIni='"
				+ numNota + "' nuFim='" + numNota + "' justificativa='"
				+ justificativa + "'/> ");
		xmlRequestBody.append(" <clientEventList> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.actionbutton.clientconfirm</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.compra.SolicitacaoComprador</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecomercial.event.compensacao.credito.debito</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.expedicao.SolicitarUsuarioConferente</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.modelcore.comercial.cancela.nfce.baixa.caixa.fechado</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgewms.expedicao.validarPedidos</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.cancelamento.processo.wms.andamento</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.modelcore.comercial.cancela.nota.devolucao.wms</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.comercial.solicitaContingencia</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>comercial.status.nfe.situacao.diferente</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.utiliza.dtneg.servidor</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.cancelamento.notas.remessa</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgewms.expedicao.selecaoDocas</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.gera.lote.xmlRejeitado</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.nota.adicional.SolicitarUsuarioGerente</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.cancelamento.nfeAcimaTolerancia</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecomercial.event.baixaPortal</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.valida.ChaveNFeCompraTerceiros</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.fat.preco.produtos.alterado</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.msg.nao.possui.itens.pendentes</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgewms.expedicao.cortePedidos</clientEvent> ");
		xmlRequestBody.append(" </clientEventList> ");

		// Chamada ao servi�o da Sankhya
		Document document = serviceInvoker.call("ServicosNfeSP.gerarInutilizacaoDeNumeracaoNFE", "mgecom", xmlRequestBody.toString());
		
		
		Node item = document.getChildNodes().item(0);

		if ("1".equals(item.getAttributes().getNamedItem("status").getTextContent())) {
System.out.println(XMLUtil.xmlToString(document));
		}
	}

	public void retornarColetaEstoqueRondonopolis(Long nuNotaVenda,
			Long idiproc, Long idproc) throws Exception {
		// Roolback - Cancelar a OP
		QueryExecutor queryNotaParaExcluir = contexto.getQuery();
		QueryExecutor queryNotaParaCancelar = contexto.getQuery();
		QueryExecutor queryNotaParaCancelarInicio = contexto.getQuery();
		QueryExecutor queryNotaParaCancelarCheck = contexto.getQuery();
		QueryExecutor queryNotaParaCancelarCheckInicio = contexto.getQuery();
		QueryExecutor queryNotaParaInutilizar = contexto.getQuery();
		QueryExecutor queryStatusOP = contexto.getQuery();
		QueryExecutor queryUpdOP = contexto.getQuery();

		if (idiproc != null && idproc != null) {

			// Capturar a nota associada para cancelar
			String sqlNotaParaCancelarInicio = " 	SELECT nunota ";
			sqlNotaParaCancelarInicio += "	  		FROM TGFCAB c ";
			sqlNotaParaCancelarInicio += "			WHERE c.idiproc = {IDIPROC} ";
			sqlNotaParaCancelarInicio += "	   			AND codemp IN (3,9) ";
			if (nuNotaVenda != null) {
				sqlNotaParaCancelarInicio += "   		AND nuNota <> "
						+ nuNotaVenda;
			}
			sqlNotaParaCancelarInicio += "	   			AND c.codtipoper IN(742,702) ";
			sqlNotaParaCancelarInicio += "		    	AND c.statusnfe = 'A' ";

			queryNotaParaCancelarInicio.setParam("IDIPROC", idiproc);
			queryNotaParaCancelarInicio.nativeSelect(sqlNotaParaCancelarInicio);

			while (queryNotaParaCancelarInicio.next()) {
				Long nuNotaCancelar = new Long(
						queryNotaParaCancelarInicio.getInt("nunota"));
				cancelarNota(nuNotaCancelar,
						"Cancelamento automatico processo beneficiamento OP:"
								+ idiproc);
				String sqlExistsNotaParaCancelar = "SELECT c.nunota FROM TGFCAB c WHERE c.nunota = {NUNOTA}";
				queryNotaParaCancelarCheckInicio.setParam("NUNOTA",
						nuNotaCancelar);
				queryNotaParaCancelarCheckInicio
						.nativeSelect(sqlExistsNotaParaCancelar);
				boolean existsNotaParaCancelar = false;
				while (queryNotaParaCancelarCheckInicio.next()) {
					existsNotaParaCancelar = true;
				}
				if (existsNotaParaCancelar) {
					cancelarNota(nuNotaCancelar,
							"Cancelamento automatico processo beneficiamento OP:"
									+ idiproc);
				}
			}
			queryNotaParaCancelarCheckInicio.close();

			queryNotaParaCancelarInicio.close();

			// Capturar a nota associada para excluir
			String sqlNotaParaExcluir = " 	SELECT 	nunota ";
			sqlNotaParaExcluir += "  		FROM 	TGFCAB c  ";
			sqlNotaParaExcluir += " 		WHERE 	c.idiproc = {IDIPROC} ";
			sqlNotaParaExcluir += "   		    AND c.codemp IN(1,3,9) ";
			if (nuNotaVenda != null) {
				sqlNotaParaExcluir += "   		AND c.nuNota <> " + nuNotaVenda;
			}
			sqlNotaParaExcluir += "   		    AND c.codtipoper IN(380, 332, 1401, 1310, 307, 335, 2107) ";
			// sqlNotaParaExcluir +=
			// "			AND c.statusnfe IS NULL OR c.statusnfe IN('T','M','R') ";

			queryNotaParaExcluir.setParam("IDIPROC", idiproc);
			queryNotaParaExcluir.nativeSelect(sqlNotaParaExcluir);

			while (queryNotaParaExcluir.next()) {
				Long nuNotaAssociada = new Long(
						queryNotaParaExcluir.getInt("nunota"));
				excluirNotas(nuNotaAssociada);
			}

			queryNotaParaExcluir.close();

			// Capturar a nota associada para cancelar
			String sqlNotaParaCancelar = " 	SELECT 	nunota ";
			sqlNotaParaCancelar += "  		FROM 	TGFCAB c ";
			sqlNotaParaCancelar += " 		WHERE 	c.idiproc = {IDIPROC} ";
			sqlNotaParaCancelar += "   		    AND codemp IN(1,3,9) ";
			if (nuNotaVenda != null) {
				sqlNotaParaCancelar += "   		AND nuNota <> " + nuNotaVenda;
			}
			sqlNotaParaCancelar += "			AND c.statusnfe = 'A' ";
			sqlNotaParaCancelar += "		UNION ";
			sqlNotaParaCancelar += "		SELECT nunota ";
			sqlNotaParaCancelar += "	  	FROM TGFCAB c ";
			sqlNotaParaCancelar += "		WHERE c.idiproc = {IDIPROC} ";
			sqlNotaParaCancelar += "	   		AND codemp IN (1, 3, 9) ";
			if (nuNotaVenda != null) {
				sqlNotaParaCancelar += "   		AND nuNota <> " + nuNotaVenda;
			}
			sqlNotaParaCancelar += "	   		AND c.codtipoper IN(411, 419) ";
			sqlNotaParaCancelar += "		UNION  ";
			sqlNotaParaCancelar += "		SELECT c.NUNOTA ";
			sqlNotaParaCancelar += "		FROM TGFVAR var, TGFCAB c ";
			sqlNotaParaCancelar += "		WHERE var.nunotaorig IN(SELECT NUNOTA FROM TGFCAB WHERE idiproc = {IDIPROC})";
			sqlNotaParaCancelar += "		 AND c.nunota=var.nunota ";
			sqlNotaParaCancelar += "		 AND codemp = 1  ";
			sqlNotaParaCancelar += "		 AND c.statusnfe = 'A' ";

			queryNotaParaCancelar.setParam("IDIPROC", idiproc);
			queryNotaParaCancelar.nativeSelect(sqlNotaParaCancelar);

			while (queryNotaParaCancelar.next()) {
				Long nuNotaCancelar = new Long(
						queryNotaParaCancelar.getInt("nunota"));
				cancelarNota(nuNotaCancelar,
						"Cancelamento automatico processo beneficiamento OP:"
								+ idiproc);
				String sqlExistsNotaParaCancelar = "SELECT c.nunota FROM TGFCAB c WHERE c.nunota = {NUNOTA}";
				queryNotaParaCancelarCheck.setParam("NUNOTA", nuNotaCancelar);
				queryNotaParaCancelarCheck
						.nativeSelect(sqlExistsNotaParaCancelar);
				boolean existsNotaParaCancelar = false;
				while (queryNotaParaCancelarCheck.next()) {
					existsNotaParaCancelar = true;
				}
				if (existsNotaParaCancelar) {
					cancelarNota(nuNotaCancelar,
							"Cancelamento automatico processo beneficiamento OP:"
									+ idiproc);
				}
			}
			queryNotaParaCancelarCheck.close();

			queryNotaParaCancelar.close();

			// Capturar a nota associada para inutilizar
			String sqlNotaParaInutilizar = " SELECT 	nunota, codemp, serienota, numnota ";
			sqlNotaParaInutilizar += "  		FROM 	TGFCAB c ";
			sqlNotaParaInutilizar += " 			WHERE 	c.idiproc = {IDIPROC} ";
			sqlNotaParaInutilizar += "   		    AND codemp IN(1,3,9) ";
			if (nuNotaVenda != null) {
				sqlNotaParaInutilizar += "   		AND nuNota <> " + nuNotaVenda;
			}
			sqlNotaParaInutilizar += "				AND c.statusnfe = 'V' ";

			queryNotaParaInutilizar.setParam("IDIPROC", idiproc);
			queryNotaParaInutilizar.nativeSelect(sqlNotaParaInutilizar);

			while (queryNotaParaInutilizar.next()) {
				Long codemp = new Long(queryNotaParaInutilizar.getInt("codemp"));
				String modeloNota = "55";
				String serieNota = queryNotaParaInutilizar
						.getString("serienota");
				Long numNota = new Long(
						queryNotaParaInutilizar.getInt("numnota"));
				gerarInutilizacaoDeNumeracaoNFE(codemp, modeloNota, serieNota,
						numNota,
						"Cancelamento automatico processo beneficiamento");
			}

			queryNotaParaInutilizar.close();

			// Cancelar a OP
			String statusOP = "";

			String sqlOP = " SELECT STATUSPROC	FROM TPRIPROC  WHERE IDIPROC = {IDIPROC} ";
			queryStatusOP.setParam("IDIPROC", idiproc);
			queryStatusOP.nativeSelect(sqlOP);

			while (queryStatusOP.next()) {
				statusOP = queryStatusOP.getString("STATUSPROC");
			}

			if ("F".equals(statusOP) || "C".equals(statusOP)) {
				// Apagando as movimentações das notas
				String sqlUpd = " 	UPDATE TPRIPROC SET STATUSPROC = 'C' WHERE IDIPROC = "
						+ idiproc;
				// queryUpdOP.setParam("IDIPROC", idiproc);
				// queryUpdOP.update(sqlUpd);
				// queryUpdOP.close();

				EnviromentUtils.updateQueryConnection(sqlUpd);

			} else {
				cancelarOrdens(idproc, idiproc);
			}
			queryStatusOP.close();

		}
	}	
	
	public String reimprimirPesagemVolume(Long idEmbalagem) throws Exception {
		StringBuilder xmlRequestBody = new StringBuilder();

		xmlRequestBody.append(" <params IDINICIAL=" + idEmbalagem + " IDFINAL='" + idEmbalagem + "'/> ");
		xmlRequestBody.append(" 	<clientEventList> ");
		xmlRequestBody.append(" 	</clientEventList> ");

		Document document = serviceInvoker.call("OperacaoProducaoSP.reimprimirPesagemVolume", "mgeprod",	xmlRequestBody.toString());

		Node item = document.getChildNodes().item(0);

		if ("1".equals(item.getAttributes().getNamedItem("status").getTextContent())) {
			System.out.println(XMLUtil.xmlToString(document));
			return XMLUtil.xmlToString(document);
		}
		return null;
	}
	
	public String printLogErro(Class<?> clazz, long tempoAnterior) { 
		return serviceInvoker.printLogErro(clazz, tempoAnterior);
	}
}
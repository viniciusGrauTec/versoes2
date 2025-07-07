package br.com.sankhya.acoesgrautec.services;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.List;

import org.w3c.dom.Document;
import org.w3c.dom.Node;

import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.acoesgrautec.util.XMLUtil;

public class SkwServicoVendas {

	private SWServiceInvoker serviceInvoker;

	private ContextoAcao contexto;
	private SimpleDateFormat fmt;

	public SkwServicoVendas(ContextoAcao contexto) throws Exception {
		this.contexto = contexto;
		this.serviceInvoker = new SWServiceInvoker(contexto);
		serviceInvoker.setDebugMode();
		this.fmt = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
	}

	/*
	 * A ação de criar a Novo Lançamento da Ordem de produção Retornar o número
	 * da OP
	 */

	public void liberarNegarLimites(String userId, String vlrLib,
			String obsLib, String liberar, String nuNota, String evento)
			throws Exception {
		// Composi��o da consulta
		StringBuilder xmlRequestBody = new StringBuilder();
		xmlRequestBody
				.append(" 		<params usuario='"
						+ userId
						+ "' liberar='"
						+ liberar
						+ "' obsLib='"
						+ obsLib
						+ "' vlrLib='"
						+ vlrLib
						+ "' confirmarNotaAutomaticamente='true' compensarNotaAutomaticamente='true'> ");
		xmlRequestBody
				.append(" 			<item nuChave='"
						+ nuNota
						+ "' evento='"
						+ evento
						+ "' nucll='0' seqCascata='0' sequencia='0' tabela='TGFCAB'/> ");
		xmlRequestBody.append(" </params> ");
		xmlRequestBody.append(" <clientEventList> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.actionbutton.clientconfirm</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.compra.SolicitacaoComprador</clientEvent> ");
		xmlRequestBody.append(" </clientEventList> ");

		// Chamada ao servi�o da Sankhya
		serviceInvoker.call("LiberacaoLimitesSP.liberarNegarLimites", "mge",
				xmlRequestBody.toString());

	}

	/*
	 * A ação de criar a editar as liberações
	 */

	public void editarLiberacoes(String userId, String nuNota, String evento)
			throws Exception {
		// Composi��o da consulta
		StringBuilder xmlRequestBody = new StringBuilder();
		xmlRequestBody.append(" 		<liberacoes> ");
		xmlRequestBody.append(" 			<liberacao 	chave='" + nuNota + "'  ");
		xmlRequestBody.append(" 				tabela='TGFCAB'  ");
		xmlRequestBody.append(" 				evento='" + evento + "'  ");
		xmlRequestBody.append(" 				sequencia='0'  ");
		xmlRequestBody.append(" 				descricao='' ");
		xmlRequestBody.append(" 				solicitante=''  ");
		xmlRequestBody.append(" 				liberador='" + userId + "'  ");
		xmlRequestBody.append(" 				seqCascata='0'  ");
		xmlRequestBody.append(" 				enviaNotif='S'  ");
		xmlRequestBody
				.append(" 						hashLiberacao='be7c491e4f64419d4237d7a11e07c1ae'  ");
		xmlRequestBody.append(" 				nucll='0'/> ");
		xmlRequestBody.append(" </liberacoes> ");
		xmlRequestBody.append(" <clientEventList> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.actionbutton.clientconfirm</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.compra.SolicitacaoComprador</clientEvent> ");
		xmlRequestBody.append(" </clientEventList> ");

		// Chamada ao servi�o da Sankhya
		serviceInvoker.call("LiberacaoAlcadaSP.editarLiberacoes", "mge",
				xmlRequestBody.toString());

	}

	public void aprovarNota(Long nuNota) throws Exception {
		// Composi��o da consulta
		StringBuilder xmlRequestBody = new StringBuilder();
		xmlRequestBody.append(" <nota nuNota='" + nuNota
				+ "' ownerServiceCall='CentralNotas'/> ");
		xmlRequestBody.append(" <clientEventList> ");
		xmlRequestBody
				.append(" 			<clientEvent>central.save.grade.itens.mostrar.popup.info.lote</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgefin.solicitacao.liberacao.orcamento</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecomercial.event.compensacao.credito.debito</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.cancelamento.notas.remessa</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.valida.ChaveNFeCompraTerceiros</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.coleta.entrega.recalculado</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>central.save.grade.itens.mostrar.popup.serie</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecomercial.event.baixaPortal</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgefin.event.fixa.vencimento</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.central.itens.KitRevenda</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecomercial.event.estoque.insuficiente.produto</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.aprovar.nota.apos.baixa</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.parcelas.financeiro</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.central.itens.KitRevenda.msgValidaFormula</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.event.troca.item.por.produto.alternativo</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.exibir.variacao.valor.item</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.compra.SolicitacaoComprador</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.exibe.msg.variacao.preco</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecomercial.event.estoque.componentes</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgeprod.producao.terceiro.inclusao.item.nota</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.imobilizado</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.central.itens.VendaCasada</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecomercial.event.cadastrarDistancia</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.actionbutton.clientconfirm</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.nota.adicional.SolicitarUsuarioGerente</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecomercial.event.faturamento.confirmacao</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.expedicao.SolicitarUsuarioConferente</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.checkout.obter.peso</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.mgecom.event.troca.item.por.produto.substituto</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.utiliza.dtneg.servidor</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.importacaoxml.cfi.para.produto</clientEvent> ");
		xmlRequestBody
				.append(" 			<clientEvent>br.com.sankhya.comercial.solicitaContingencia</clientEvent> ");
		xmlRequestBody.append(" </clientEventList> ");

		// Chamada ao servi�o da Sankhya
		serviceInvoker.call("CACSP.aprovarNota", "mgecom",
				xmlRequestBody.toString());
	}

	public String imprimeDocumentos(List<Long> listNotas) throws Exception {
		// Composi��o da consulta
		StringBuilder xmlRequestBody = new StringBuilder();
		xmlRequestBody
				.append(" <notas pedidoWeb='false' ownerServiceCall='CentralNotas'> ");
		for (Long nuNota : listNotas) {
			xmlRequestBody.append(" 	<nota nuNota='" + nuNota
					+ "' tipoImp='1'/> ");
		}
		xmlRequestBody.append(" </notas> ");
		xmlRequestBody.append(" <clientEventList> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.modelcore.comercial.cancela.nfce.baixa.caixa.fechado</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>comercial.status.nfe.situacao.diferente</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgewms.expedicao.validarPedidos</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.utiliza.dtneg.servidor</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.modelcore.comercial.cancela.nota.devolucao.wms</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.cancelamento.nfeAcimaTolerancia</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.fat.preco.produtos.alterado</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.actionbutton.clientconfirm</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgewms.expedicao.cortePedidos</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecomercial.event.compensacao.credito.debito</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgewms.expedicao.selecaoDocas</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecomercial.event.baixaPortal</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.comercial.solicitaContingencia</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.compra.SolicitacaoComprador</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.expedicao.SolicitarUsuarioConferente</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.gera.lote.xmlRejeitado</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.msg.nao.possui.itens.pendentes</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.nota.adicional.SolicitarUsuarioGerente</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.cancelamento.processo.wms.andamento</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.cancelamento.notas.remessa</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.valida.ChaveNFeCompraTerceiros</clientEvent> ");
		xmlRequestBody.append(" </clientEventList> ");

		// Chamada ao servi�o da Sankhya

		Document document = serviceInvoker.call(
				"ImpressaoNotasSP.imprimeDocumentos", "mge",
				xmlRequestBody.toString());

		Node item = document.getChildNodes().item(0);

		if ("1".equals(item.getAttributes().getNamedItem("status")
				.getTextContent())) {
			return item.getAttributes().getNamedItem("transactionId")
					.getTextContent();

		} else {
			return XMLUtil.xmlToString(document);
		}

	}

	public void findPendingPrinters(String transactionId) throws Exception {
		// Composi��o da consulta
		StringBuilder xmlRequestBody = new StringBuilder();
		xmlRequestBody.append(" <transactionIds> ");
		xmlRequestBody.append(" 	<transactionId>" + transactionId
				+ "</transactionId> ");
		xmlRequestBody.append(" </transactionIds> ");
		xmlRequestBody.append(" <clientEventList> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.actionbutton.clientconfirm</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.comercial.solicitaContingencia</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.cancelamento.processo.wms.andamento</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgewms.expedicao.validarPedidos</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.fat.preco.produtos.alterado</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.msg.nao.possui.itens.pendentes</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecomercial.event.compensacao.credito.debito</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.cancelamento.notas.remessa</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgewms.expedicao.cortePedidos</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecomercial.event.baixaPortal</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.expedicao.SolicitarUsuarioConferente</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.modelcore.comercial.cancela.nota.devolucao.wms</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.cancelamento.nfeAcimaTolerancia</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>comercial.status.nfe.situacao.diferente</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.compra.SolicitacaoComprador</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.modelcore.comercial.cancela.nfce.baixa.caixa.fechado</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgewms.expedicao.selecaoDocas</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.valida.ChaveNFeCompraTerceiros</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.utiliza.dtneg.servidor</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.gera.lote.xmlRejeitado</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.nota.adicional.SolicitarUsuarioGerente</clientEvent> ");
		xmlRequestBody.append(" </clientEventList> ");

		// Chamada ao servi�o da Sankhya
		serviceInvoker.call("PrintServiceSP.findPendingPrinters", "mge",
				xmlRequestBody.toString());

	}

	public void saveParceiro(String razaoSocial, String nomeParceiro,
			String tipoPessoa, String cnpj, String inscricaoEstadual,
			String codVendedor, String email, Long codParc,
			Long idFichaCadastral, boolean isCliente, boolean isFornecedor,
			Long codCid, Long codBai, Long codEnd, String sexo)
			throws Exception {
		// Composi��o da consulta
		StringBuilder xmlRequestBody = new StringBuilder();

		// Chamada ao servi�o da Sankhya
		xmlRequestBody
				.append(" 		<dataSet rootEntity='Parceiro' includePresentationFields='S' crudListener='br.com.sankhya.modelcore.crudlisteners.ParceiroCrudListener' datasetid='1572179327214_1'> ");
		xmlRequestBody.append("		<entity path=''> ");
		xmlRequestBody.append("			<fieldset list='*'/> ");
		xmlRequestBody
				.append("				<fieldset except='true' list='CODTABST,ENTREGAENDCONTATO,RETEMCSL,RETEMPIS,CODROTA,USATABCRFORN,SERIENFDES,VALDESCGRDCAR,PERCDESCESPECIAL,CODUSUCOBR,NATUREZAOPERDES,DIASEM,DESCRROTA,SITESPECIALRESP,MOTNAORETERISSQN,MODELONFDES,RETEMCOFINS'/> ");
		xmlRequestBody.append("		</entity> ");
		xmlRequestBody.append("		<entity path='LocalFinanceiro'> ");
		xmlRequestBody.append("		  <field name='DESCRLOCAL'/> ");
		xmlRequestBody.append("		</entity> ");
		xmlRequestBody.append("		<entity path='Assessor'> ");
		xmlRequestBody.append("		  <field name='APELIDO'/> ");
		xmlRequestBody.append("		</entity> ");
		xmlRequestBody.append("		<entity path='PlanoConta4'> ");
		xmlRequestBody.append("		  <field name='DESCRCTA'/> ");
		xmlRequestBody.append("		</entity> ");
		xmlRequestBody.append("		<entity path='PlanoConta3'> ");
		xmlRequestBody.append("		  <field name='DESCRCTA'/> ");
		xmlRequestBody.append("		</entity> ");
		xmlRequestBody.append("		<entity path='PlanoConta2'> ");
		xmlRequestBody.append("		  <field name='DESCRCTA'/> ");
		xmlRequestBody.append("		</entity> ");
		xmlRequestBody.append("		<entity path='Cidade'> ");
		xmlRequestBody.append("		  <field name='NOMECID'/> ");
		xmlRequestBody.append("		</entity> ");
		xmlRequestBody.append("		<entity path='Vendedor'> ");
		xmlRequestBody.append("		  <field name='APELIDO'/> ");
		xmlRequestBody.append("		</entity> ");
		xmlRequestBody.append("		<entity path='ParceiroMatriz'> ");
		xmlRequestBody.append("		  <field name='NOMEPARC'/> ");
		xmlRequestBody.append("		</entity> ");
		xmlRequestBody.append("		<entity path='Usuario'> ");
		xmlRequestBody.append("		  <field name='NOMEUSU'/> ");
		xmlRequestBody.append("		</entity> ");
		xmlRequestBody.append("		<entity path='Regiao'> ");
		xmlRequestBody.append("		  <field name='NOMEREG'/> ");
		xmlRequestBody.append("		</entity> ");
		xmlRequestBody.append("		<entity path='NomeTabelasPreco'> ");
		xmlRequestBody.append("		  <field name='NOMETAB'/> ");
		xmlRequestBody.append("		</entity> ");
		xmlRequestBody.append("		<entity path='EmpresaPreferencial'> ");
		xmlRequestBody.append("		  <field name='NOMEFANTASIA'/> ");
		xmlRequestBody.append("		</entity> ");
		xmlRequestBody.append("		<entity path='Perfil'> ");
		xmlRequestBody.append("		  <field name='DESCRTIPPARC'/> ");
		xmlRequestBody.append("		</entity> ");
		xmlRequestBody.append("		<entity path='Cidade_AD001'> ");
		xmlRequestBody.append("		  <field name='NOMECID'/> ");
		xmlRequestBody.append("		</entity> ");
		xmlRequestBody.append("		<entity path='UnidadeFederativa'> ");
		xmlRequestBody.append("		  <field name='UF'/> ");
		xmlRequestBody.append("		</entity> ");
		xmlRequestBody.append("		<entity path='Banco'> ");
		xmlRequestBody.append("		  <field name='NOMEBCO'/> ");
		xmlRequestBody.append("		</entity> ");
		xmlRequestBody.append("		<entity path='Empresa'> ");
		xmlRequestBody.append("		  <field name='NOMEFANTASIA'/> ");
		xmlRequestBody.append("		</entity> ");
		xmlRequestBody.append("		<entity path='Bairro'> ");
		xmlRequestBody.append("		  <field name='NOMEBAI'/> ");
		xmlRequestBody.append("		</entity> ");
		xmlRequestBody.append("		<entity path='GrupoCobranca'> ");
		xmlRequestBody.append("		  <field name='DESCRICAO'/> ");
		xmlRequestBody.append("		</entity> ");
		xmlRequestBody.append("		<entity path='Parceiro'> ");
		xmlRequestBody.append("		  <field name='NOMEPARC'/> ");
		xmlRequestBody.append("		</entity> ");
		xmlRequestBody.append("		<entity path='TipoNegociacao'> ");
		xmlRequestBody.append("		  <field name='DESCRTIPVENDA'/> ");
		xmlRequestBody.append("		</entity> ");
		xmlRequestBody.append("		<entity path='Endereco'> ");
		xmlRequestBody.append("		  <field name='NOMEEND'/> ");
		xmlRequestBody.append("		</entity> ");
		xmlRequestBody.append("		<entity path='ContaBancaria'> ");
		xmlRequestBody.append("		  <field name='DESCRICAO'/> ");
		xmlRequestBody.append("		</entity> ");
		xmlRequestBody.append("		<entity path='ContatoEntrega'> ");
		xmlRequestBody.append("		  <field name='NOMECONTATO'/> ");
		xmlRequestBody.append("		</entity> ");
		xmlRequestBody.append("		<entity path='FinalidadeOperacao'> ");
		xmlRequestBody.append("		  <field name='DESCRICAO'/> ");
		xmlRequestBody.append("		</entity> ");
		xmlRequestBody.append("		<entity path='PlanoConta'> ");
		xmlRequestBody.append("		  <field name='DESCRCTA'/> ");
		xmlRequestBody.append("		</entity> ");
		xmlRequestBody
				.append("			<entity path='ComplementoParc.TipoNegociacaoFilha'> ");
		xmlRequestBody.append("		  <field name='DESCRTIPVENDA'/> ");
		xmlRequestBody.append("		</entity> ");
		xmlRequestBody
				.append("			<entity path='ComplementoParc.NacionalidadeParceiro'> ");
		xmlRequestBody.append("		  <field name='DESCRICAO'/> ");
		xmlRequestBody.append("		</entity> ");
		xmlRequestBody
				.append("			<entity path='ComplementoParc.TipoNegociacao'> ");
		xmlRequestBody.append("		  <field name='DESCRTIPVENDA'/> ");
		xmlRequestBody.append("		</entity> ");
		xmlRequestBody
				.append("			<entity path='ComplementoParc.BairroTrabalho'> ");
		xmlRequestBody.append("		  <field name='NOMEBAI'/> ");
		xmlRequestBody.append("		</entity> ");
		xmlRequestBody
				.append("			<entity path='ComplementoParc.Transportadora'> ");
		xmlRequestBody.append("		  <field name='NOMEPARC'/> ");
		xmlRequestBody.append("		</entity> ");
		xmlRequestBody
				.append("			<entity path='ComplementoParc.BairroRecebimento'> ");
		xmlRequestBody.append("		  <field name='NOMEBAI'/> ");
		xmlRequestBody.append("		</entity> ");
		xmlRequestBody
				.append("			<entity path='ComplementoParc.EnderecoTrabalho'> ");
		xmlRequestBody.append("		  <field name='NOMEEND'/> ");
		xmlRequestBody.append("		</entity> ");
		xmlRequestBody.append("		<entity path='ComplementoParc.Moeda'> ");
		xmlRequestBody.append("		  <field name='NOMEMOEDA'/> ");
		xmlRequestBody.append("		</entity> ");
		xmlRequestBody.append("		<entity path='ComplementoParc.Cidade'> ");
		xmlRequestBody.append("		  <field name='NOMECID'/> ");
		xmlRequestBody.append("		</entity> ");
		xmlRequestBody
				.append("			<entity path='ComplementoParc.CidadeTrabalho'> ");
		xmlRequestBody.append("		  <field name='NOMECID'/> ");
		xmlRequestBody.append("		</entity> ");
		xmlRequestBody
				.append("			<entity path='ComplementoParc.NaturalidadeParceiro'> ");
		xmlRequestBody.append("		  <field name='NOMECID'/> ");
		xmlRequestBody.append("		</entity> ");
		xmlRequestBody
				.append("			<entity path='ComplementoParc.NomeTabelasPreco'> ");
		xmlRequestBody.append("		  <field name='NOMETAB'/> ");
		xmlRequestBody.append("		</entity> ");
		xmlRequestBody
				.append("			<entity path='ComplementoParc.EnderecoRecebimento'> ");
		xmlRequestBody.append("		  <field name='NOMEEND'/> ");
		xmlRequestBody.append("		</entity> ");
		xmlRequestBody.append("		<entity path='ComplementoParc.Bairro'> ");
		xmlRequestBody.append("		  <field name='NOMEBAI'/> ");
		xmlRequestBody.append("		</entity> ");
		xmlRequestBody
				.append("			<entity path='ComplementoParc.CidadeRecebimento'> ");
		xmlRequestBody.append("		  <field name='NOMECID'/> ");
		xmlRequestBody.append("		</entity> ");
		xmlRequestBody.append("			<entity path='ComplementoParc.Endereco'> ");
		xmlRequestBody.append("		  <field name='NOMEEND'/> ");
		xmlRequestBody.append("		</entity> ");
		xmlRequestBody.append("		<entity path='ComplementoParc'> ");
		xmlRequestBody.append("			<fieldset list='*'/> ");
		xmlRequestBody.append("		</entity> ");
		xmlRequestBody.append(" 	<txProperties> ");
		xmlRequestBody
				.append(" 				<prop name='parceiro.isAutoNum' value='true'/> ");
		xmlRequestBody.append(" 	</txProperties> ");
		xmlRequestBody.append(" 	<dataRow> ");
		xmlRequestBody.append(" 		<localFields> ");
		xmlRequestBody.append(" 			<RAZAOSOCIAL><![CDATA[" + razaoSocial
				+ "]]></RAZAOSOCIAL> ");
		if (codCid != null) {
			xmlRequestBody.append(" 		<CODCID><![CDATA[" + codCid
					+ "]]></CODCID> ");
		}
		if (codBai != null) {
			xmlRequestBody.append(" 		<CODBAI><![CDATA[" + codBai
					+ "]]></CODBAI> ");
		}
		if (codEnd != null) {
			xmlRequestBody.append(" 		<CODEND><![CDATA[" + codEnd
					+ "]]></CODEND> ");
		}

		xmlRequestBody.append(" 			<CODREG><![CDATA[0]]></CODREG> ");
		xmlRequestBody.append(" 			<CLASSIFICMS><![CDATA[R]]></CLASSIFICMS> ");
		xmlRequestBody.append(" 			<AD_COMVENDA><![CDATA[0]]></AD_COMVENDA> ");
		xmlRequestBody.append(" 			<NOMEPARC><![CDATA[" + nomeParceiro
				+ "]]></NOMEPARC> ");
		xmlRequestBody.append(" 			<TIPPESSOA><![CDATA[" + tipoPessoa
				+ "]]></TIPPESSOA> ");
		xmlRequestBody.append(" 			<CGC_CPF><![CDATA[" + cnpj
				+ "]]></CGC_CPF> ");
		if (idFichaCadastral != null) {
			xmlRequestBody.append(" 		<AD_IDFICHACADASTRAL><![CDATA["
					+ idFichaCadastral + "]]></AD_IDFICHACADASTRAL> ");
		}
		// if (!FacesUtil.isNullOrEmpty(parceiro.getIdentInscEstad())) {
		// xmlRequestBody.append(" 		<IDENTINSCESTAD><![CDATA["
		// + parceiro.getIdentInscEstad()
		// + "]]></IDENTINSCESTAD> ");
		// }
		if (inscricaoEstadual != null) {
			xmlRequestBody.append(" 		<IDENTINSCESTAD><![CDATA["
					+ inscricaoEstadual + "]]></IDENTINSCESTAD> ");
		}
		if (codVendedor != null) {
			xmlRequestBody.append(" 		<CODVEND><![CDATA[" + codVendedor
					+ "]]></CODVEND> ");
		}

		if (email != null) {
			xmlRequestBody.append(" 		<EMAIL><![CDATA[" + email
					+ "]]></EMAIL> ");
		}
		if (sexo != null) {
			xmlRequestBody.append(" 		<SEXO><![CDATA[" + sexo + "]]></SEXO> ");
		}
		if (isFornecedor) {
			xmlRequestBody.append(" 			<ATIVO><![CDATA[S]]></ATIVO> ");
		} else {
			xmlRequestBody.append(" 			<ATIVO><![CDATA[N]]></ATIVO> ");
		}

		if (isCliente) {
			xmlRequestBody.append(" 			<CLIENTE><![CDATA[S]]></CLIENTE> ");
		} else {
			xmlRequestBody.append(" 			<CLIENTE><![CDATA[]]></CLIENTE> ");
		}
		if (isFornecedor) {
			xmlRequestBody
					.append(" 			<FORNECEDOR><![CDATA[S]]></FORNECEDOR> ");
		} else {
			xmlRequestBody.append(" 			<FORNECEDOR><![CDATA[]]></FORNECEDOR> ");
		}
		xmlRequestBody.append(" 		</localFields> ");
		if (codParc != null) {
			xmlRequestBody.append(" 	<key> ");
			xmlRequestBody.append(" 		<CODPARC><![CDATA[" + codParc
					+ "]]></CODPARC> ");
			xmlRequestBody.append(" 	</key> ");
		}
		xmlRequestBody.append(" 	</dataRow> ");
		xmlRequestBody.append(" </dataSet> ");
		xmlRequestBody.append(" <clientEventList> ");
		xmlRequestBody
				.append(" 			<clientEvent>parceiro.mostra.mensagem.criticaie</clientEvent> ");
		xmlRequestBody.append(" </clientEventList> ");

		System.out.println(xmlRequestBody.toString());

		// Chamada ao servi�o da Sankhya
		serviceInvoker.call("CRUDServiceProvider.saveRecord", "mge",
				xmlRequestBody.toString());

	}

	public String saveProduto(String codVol, String nomeProduto,
			String usoProd, String codGrupoProd, String usaLocal, String marca,
			String referencia, String ncm, BigDecimal estoqueMinimo,
			BigDecimal estoqueMaximo, String codLocalPadrao, String codNat,
			String codCenCus, String consMat, String codCtaCtbEfd,
			String codFormPrec, String codEnqIPIEnt, String codEnqIPISai,
			String temIPICompra, String temIPIVenda, String codIPI,
			String codEspecST, String tipSubst, String identImob,
			String grupoCSLL, String grupoCofins, String grupoPis,
			String cstIpiEnt, String cstIpiSai, String solCompra,
			String calculoGiro, String decQtd, String decVlr, String temCiap)
			throws Exception {
		// Composi��o da consulta
		StringBuilder xmlRequestBody = new StringBuilder();

		// Chamada ao servi�o da Sankhya
		xmlRequestBody
				.append(" <dataSet rootEntity='Produto' includePresentationFields='S' crudListener='br.com.sankhya.modelcore.crudlisteners.ProdutoServicoCrudListener' datasetid='1624454372631_1'> ");
		xmlRequestBody.append(" 	<entity path=''> ");
		xmlRequestBody.append(" 		<fieldset list='*'/>   ");
		xmlRequestBody
				.append(" 		<fieldset except='true' list='TEMMEDICAO'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='LocalFinanceiro'> ");
		xmlRequestBody.append(" 	  <field name='DESCRLOCAL'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='PlanoContaConta4'> ");
		xmlRequestBody.append(" 	  <field name='DESCRCTA'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='PlanoContaEmpresa3'> ");
		xmlRequestBody.append(" 	  <field name='DESCRCTA'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='UnidadeMinArmazenagemWMS'> ");
		xmlRequestBody.append(" 	  <field name='DESCRVOL'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='PlanoContaEmpresa2'> ");
		xmlRequestBody.append(" 	  <field name='DESCRCTA'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='Produto_AD001'> ");
		xmlRequestBody.append(" 	  <field name='DESCRPROD'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='VolumeResumoEntrega'> ");
		xmlRequestBody.append(" 	  <field name='DESCRVOL'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='AD_ADGRUPOICMSPROD'> ");
		xmlRequestBody.append(" 	  <field name='RESGRUPOICMS'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='CategoriaMedicamento'> ");
		xmlRequestBody.append(" 	  <field name='DESCRICAO'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='Volume'> ");
		xmlRequestBody.append(" 	  <field name='DESCRVOL'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='ConfiguracaoKit'> ");
		xmlRequestBody.append(" 	  <field name='DESCRCONFKIT'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='ModeloEtiqueta'> ");
		xmlRequestBody.append(" 	  <field name='DESCRICAO'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='FormulaPrecificacao'> ");
		xmlRequestBody.append(" 	  <field name='DESCRFORM'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='CentroResultado'> ");
		xmlRequestBody.append(" 	  <field name='DESCRCENCUS'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='UnidadeCompra'> ");
		xmlRequestBody.append(" 	  <field name='DESCRVOL'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='VolumeFETHAB'> ");
		xmlRequestBody.append(" 	  <field name='DESCRVOL'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='MarcaProduto'> ");
		xmlRequestBody.append(" 	  <field name='DESCRICAO'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='RelatorioEtiquetaSeparacao'> ");
		xmlRequestBody.append(" 	  <field name='DESCRICAO'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='Produto_AD002'> ");
		xmlRequestBody.append(" 	  <field name='DESCRPROD'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='Parceiro'> ");
		xmlRequestBody.append(" 	  <field name='NOMEPARC'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='PlanoConta'> ");
		xmlRequestBody.append(" 	  <field name='DESCRCTA'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='Servico'> ");
		xmlRequestBody.append(" 	  <field name='DESCRPROD'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='Moeda'> ");
		xmlRequestBody.append(" 	  <field name='NOMEMOEDA'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='AD_PADRAOTARA'> ");
		xmlRequestBody.append(" 	  <field name='DESCR'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='ViaTransporte'> ");
		xmlRequestBody.append(" 	  <field name='DESCRICAO'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='ParceiroFornecedor'> ");
		xmlRequestBody.append(" 	  <field name='NOMEPARC'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='Projeto'> ");
		xmlRequestBody.append(" 	  <field name='IDENTIFICACAO'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='ParceiroFabricante'> ");
		xmlRequestBody.append(" 	  <field name='NOMEPARC'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='ProdutoApontamento1'> ");
		xmlRequestBody.append(" 	  <field name='DESCRPROD'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='ConfiguracaoGradeProduto'> ");
		xmlRequestBody.append(" 	  <field name='NOMGRD'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='FormulaRequisicao'> ");
		xmlRequestBody.append(" 	  <field name='DESCRFORM'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='SubCategoriaMedicamento'> ");
		xmlRequestBody.append(" 	  <field name='DESCRICAO'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody
				.append(" 	<entity path='TipoOperacaoGrupoFaturamento'> ");
		xmlRequestBody.append(" 	  <field name='DESCROPER'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='Formula'> ");
		xmlRequestBody.append(" 	  <field name='DESCRFORM'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='ProdutoSubstitutoKit'> ");
		xmlRequestBody.append(" 	  <field name='DESCRPROD'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='GeneroProduto'> ");
		xmlRequestBody.append(" 	  <field name='DESCRICAO'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='AreaSeparacao'> ");
		xmlRequestBody.append(" 	  <field name='NOMEAREASEP'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='NomeTabelasPrecoFilha'> ");
		xmlRequestBody.append(" 	  <field name='NOMETAB'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='ProdutoApontamento2'> ");
		xmlRequestBody.append(" 	  <field name='DESCRPROD'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='GrupoProducaoProduto'> ");
		xmlRequestBody.append(" 	  <field name='DESCRGPROD'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='Produto3'> ");
		xmlRequestBody.append(" 	  <field name='DESCRPROD'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='AD_COR'> ");
		xmlRequestBody.append(" 	  <field name='DESCRICAO'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='TabelaCPRB'> ");
		xmlRequestBody.append(" 	  <field name='DESCATIVIDADE'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='ClassificacaoMedicamento'> ");
		xmlRequestBody.append(" 	  <field name='DESCRICAO'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='SubClassificacaoMedicamento'> ");
		xmlRequestBody.append(" 	  <field name='DESCRICAO'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='PrincipioAtivo'> ");
		xmlRequestBody.append(" 	  <field name='DESCRICAO'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='AliquotaIPI'> ");
		xmlRequestBody.append(" 	  <field name='DESCRICAO'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='ClasseTerapeutica'> ");
		xmlRequestBody.append(" 	  <field name='DESCRICAO'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='Natureza'> ");
		xmlRequestBody.append(" 	  <field name='DESCRNAT'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='GrupoProduto'> ");
		xmlRequestBody.append(" 	  <field name='DESCRGRUPOPROD'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='SubClasseTerapeutica'> ");
		xmlRequestBody.append(" 	  <field name='DESCRICAO'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='PlanoContaEmpresa'> ");
		xmlRequestBody.append(" 	  <field name='DESCRCTA'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='RegraArmazenagem'> ");
		xmlRequestBody.append(" 	  <field name='DESCRICAO'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody
				.append(" 	<entity path='UnidadeSeparacaoPesoVariavel'> ");
		xmlRequestBody.append(" 		<field name='DESCRVOL'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<dataRow> ");
		xmlRequestBody.append(" 		<localFields> ");
		xmlRequestBody.append(" 			<CODVOL><![CDATA[" + codVol
				+ "]]></CODVOL> ");
		xmlRequestBody.append(" 			<USALOCAL><![CDATA[" + usaLocal
				+ "]]></USALOCAL> ");
		xmlRequestBody.append(" 			<USOPROD><![CDATA[" + usoProd
				+ "]]></USOPROD> ");
		xmlRequestBody.append(" 			<CODGRUPOPROD><![CDATA[" + codGrupoProd
				+ "]]></CODGRUPOPROD> ");
		xmlRequestBody.append(" 			<MARCA><![CDATA[" + marca + "]]></MARCA> ");
		if (referencia != null) {
			xmlRequestBody.append(" 		<REFERENCIA><![CDATA[" + referencia
					+ "]]></REFERENCIA> ");
		}
		xmlRequestBody.append(" 			<NCM><![CDATA[" + ncm + "]]></NCM> ");
		if (estoqueMinimo != null) {
			xmlRequestBody.append(" 		<ESTMIN><![CDATA[" + estoqueMinimo
					+ "]]></ESTMIN> ");
		}
		if (estoqueMaximo != null) {
			xmlRequestBody.append(" 		<ESTMAX><![CDATA[" + estoqueMaximo
					+ "]]></ESTMAX> ");
		}
		if (codLocalPadrao != null) {
			xmlRequestBody.append(" 		<CODLOCALPADRAO><![CDATA["
					+ codLocalPadrao + "]]></CODLOCALPADRAO> ");
		}
		if (codNat != null) {
			xmlRequestBody.append(" 		<CODNAT><![CDATA[" + codNat
					+ "]]></CODNAT> ");
		}
		if (codCenCus != null) {
			xmlRequestBody.append(" 		<CODCENCUS><![CDATA[" + codCenCus
					+ "]]></CODCENCUS> ");
		}
		if (consMat != null) {
			xmlRequestBody.append(" 		<AD_CONSMAT><![CDATA[" + consMat
					+ "]]></AD_CONSMAT> ");
		}
		if (codCtaCtbEfd != null) {
			xmlRequestBody.append(" 		<CODCTACTBEFD><![CDATA[" + codCtaCtbEfd
					+ "]]></CODCTACTBEFD> ");
		}
		if (codFormPrec != null) {
			xmlRequestBody.append(" 		<CODFORMPREC><![CDATA[" + codFormPrec
					+ "]]></CODFORMPREC> ");
		}
		if (codEnqIPIEnt != null) {
			xmlRequestBody.append(" 		<CODENQIPIENT><![CDATA[" + codEnqIPIEnt
					+ "]]></CODENQIPIENT> ");
		}
		if (codEnqIPISai != null) {
			xmlRequestBody.append(" 		<CODENQIPISAI><![CDATA[" + codEnqIPISai
					+ "]]></CODENQIPISAI> ");
		}
		if (temIPICompra != null) {
			xmlRequestBody.append(" 		<TEMIPICOMPRA><![CDATA[" + temIPICompra
					+ "]]></TEMIPICOMPRA> ");
		}
		if (temIPIVenda != null) {
			xmlRequestBody.append(" 		<TEMIPIVENDA><![CDATA[" + temIPIVenda
					+ "]]></TEMIPIVENDA> ");
		}
		if (temCiap != null) {
			xmlRequestBody.append(" 		<TEMCIAP><![CDATA[" + temCiap
					+ "]]></TEMCIAP> ");
		}
		if (codIPI != null) {
			xmlRequestBody.append(" 		<CODIPI><![CDATA[" + codIPI
					+ "]]></CODIPI> ");
		}
		if (codEspecST != null) {
			xmlRequestBody.append(" 		<CODESPECST><![CDATA[" + codEspecST
					+ "]]></CODESPECST> ");
		}
		if (tipSubst != null) {
			xmlRequestBody.append(" 		<TIPSUBST><![CDATA[" + tipSubst
					+ "]]></TIPSUBST> ");
		}
		if (identImob != null) {
			xmlRequestBody.append(" 		<IDENTIMOB><![CDATA[" + identImob
					+ "]]></IDENTIMOB> ");
		}
		if (grupoCSLL != null) {
			xmlRequestBody.append(" 		<GRUPOCSSL><![CDATA[" + grupoCSLL
					+ "]]></GRUPOCSSL> ");
		}
		if (grupoCofins != null) {
			xmlRequestBody.append(" 		<GRUPOCOFINS><![CDATA[" + grupoCofins
					+ "]]></GRUPOCOFINS> ");
		}
		if (grupoPis != null) {
			xmlRequestBody.append(" 		<GRUPOPIS><![CDATA[" + grupoPis
					+ "]]></GRUPOPIS> ");
		}
		if (cstIpiEnt != null) {
			xmlRequestBody.append(" 		<CSTIPIENT><![CDATA[" + cstIpiEnt
					+ "]]></CSTIPIENT> ");
		}
		if (cstIpiSai != null) {
			xmlRequestBody.append(" 		<CSTIPISAI><![CDATA[" + cstIpiSai
					+ "]]></CSTIPISAI> ");
		}

		if (solCompra != null) {
			xmlRequestBody.append(" 		<SOLCOMPRA><![CDATA[" + solCompra
					+ "]]></SOLCOMPRA> ");
		}

		if (calculoGiro != null) {
			if (calculoGiro.equals("S")) {
				calculoGiro = "G";
			} else {
				calculoGiro = "E";
			}
			xmlRequestBody.append(" 		<CALCULOGIRO><![CDATA[" + calculoGiro
					+ "]]></CALCULOGIRO> ");
		}

		if (decQtd != null) {
			xmlRequestBody.append(" 		<DECQTD><![CDATA[" + decQtd
					+ "]]></DECQTD> ");
		}

		if (decVlr != null) {
			xmlRequestBody.append(" 		<DECVLR><![CDATA[" + decVlr
					+ "]]></DECVLR> ");
		}
		xmlRequestBody
				.append(" 					<EXIGELASTROCAMADAS><![CDATA[N]]></EXIGELASTROCAMADAS> ");
		xmlRequestBody.append(" 			<DESCRPROD><![CDATA[" + nomeProduto
				+ "]]></DESCRPROD> ");
		xmlRequestBody.append(" 		</localFields> ");
		xmlRequestBody.append(" 	</dataRow> ");
		xmlRequestBody.append(" </dataSet> ");
		xmlRequestBody.append(" <clientEventList> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.actionbutton.clientconfirm</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mge.info.metro.cubico</clientEvent> ");
		xmlRequestBody.append(" </clientEventList> ");

		// Chamada ao servi�o da Sankhya
		Document document = serviceInvoker.call(
				"CRUDServiceProvider.saveRecord", "mge",
				xmlRequestBody.toString());

		Node item = document.getChildNodes().item(0);

		if ("1".equals(item.getAttributes().getNamedItem("status")
				.getTextContent())) {
			return document.getElementsByTagName("CODPROD").item(0)
					.getTextContent();
		} else {
			return XMLUtil.xmlToString(document);
		}

	}

	public void saveEmpresaProdutoImpostos(String codProd, String codEmp,
			String grupoIcms, String temIcms, String tipSubstDet,
			String usoProdDet, String tipoItemSped, BigDecimal aliqIcmsIntEfd,
			String calcDifal, String origProd, String temIpiCompra,
			String temIpiVenda, String cstIpiEntDet, String cstIpiSaiDet,
			String codEnqIpiEnt, String codEnqIpiSai, String cat1799spres,
			String codEspeCst, String codCtaCtbEfdDet, String codFci)
			throws Exception {
		// Composi��o da consulta
		StringBuilder xmlRequestBody = new StringBuilder();

		// Chamada ao serviço da Sankhya
		xmlRequestBody
				.append(" <dataSet rootEntity='EmpresaProdutoImpostos' includePresentationFields='S' datasetid='1624481689052_6'> ");
		xmlRequestBody.append(" 	<entity path=''> ");
		xmlRequestBody.append(" 	  <fieldset list='*'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='ConfiguracaoGradeProduto'> ");
		xmlRequestBody.append(" 	  <field name='NOMGRD'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='LocalFinanceiro'> ");
		xmlRequestBody.append(" 	  <field name='DESCRLOCAL'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='Empresa'> ");
		xmlRequestBody.append(" 	  <field name='NOMEFANTASIA'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='PlanoConta'> ");
		xmlRequestBody.append(" 	  <field name='DESCRCTA'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<dataRow> ");
		xmlRequestBody.append(" 		<localFields> ");
		xmlRequestBody.append(" 			<CODEMP><![CDATA[" + codEmp
				+ "]]></CODEMP> ");
		if (grupoIcms != null && !"".equals(grupoIcms)) {
			xmlRequestBody.append(" 		<GRUPOICMS><![CDATA[" + grupoIcms
					+ "]]></GRUPOICMS> ");
		}
		if (origProd != null && !"".equals(origProd)) {
			xmlRequestBody.append(" 		<ORIGPROD><![CDATA[" + origProd
					+ "]]></ORIGPROD> ");
		}
		if (calcDifal != null && !"".equals(calcDifal)) {
			xmlRequestBody.append(" 		<CALCDIFAL><![CDATA[" + calcDifal
					+ "]]></CALCDIFAL> ");
		}
		if (tipSubstDet != null && !"".equals(tipSubstDet)) {
			xmlRequestBody.append(" 		<TIPSUBST><![CDATA[" + tipSubstDet
					+ "]]></TIPSUBST> ");
		}
		if (cstIpiEntDet != null && !"".equals(cstIpiEntDet)) {
			xmlRequestBody.append(" 		<CSTIPIENT><![CDATA[" + cstIpiEntDet
					+ "]]></CSTIPIENT> ");
		}
		if (cstIpiSaiDet != null && !"".equals(cstIpiSaiDet)) {
			xmlRequestBody.append(" 		<CSTIPISAI><![CDATA[" + cstIpiSaiDet
					+ "]]></CSTIPISAI> ");
		}
		if (codEnqIpiSai != null && !"".equals(codEnqIpiSai)) {
			xmlRequestBody.append(" 		<CODENQIPISAI><![CDATA[" + codEnqIpiSai
					+ "]]></CODENQIPISAI> ");
		}
		if (usoProdDet != null && !"".equals(usoProdDet)) {
			xmlRequestBody.append(" 		<USOPROD><![CDATA[" + usoProdDet
					+ "]]></USOPROD> ");
		}
		if (codEnqIpiEnt != null && !"".equals(codEnqIpiEnt)) {
			xmlRequestBody.append(" 		<CODENQIPIENT><![CDATA[" + codEnqIpiEnt
					+ "]]></CODENQIPIENT> ");
		}
		if (temIpiVenda != null && !"".equals(temIpiVenda)) {
			xmlRequestBody.append(" 		<TEMIPIVENDA><![CDATA[" + temIpiVenda
					+ "]]></TEMIPIVENDA> ");
		}
		if (temIpiCompra != null && !"".equals(temIpiCompra)) {
			xmlRequestBody.append(" 		<TEMIPICOMPRA><![CDATA[" + temIpiCompra
					+ "]]></TEMIPICOMPRA> ");
		}
		if (temIcms != null && !"".equals(temIcms)) {
			xmlRequestBody.append(" 		<TEMICMS><![CDATA[" + temIcms
					+ "]]></TEMICMS> ");
		}
		if (tipoItemSped != null && !"".equals(tipoItemSped)) {
			xmlRequestBody.append(" 		<TIPOITEMSPED><![CDATA[" + tipoItemSped
					+ "]]></TIPOITEMSPED> ");
		}
		if (aliqIcmsIntEfd != null && !"".equals(aliqIcmsIntEfd)) {
			xmlRequestBody.append(" 		<ALIQICMSINTEFD><![CDATA["
					+ aliqIcmsIntEfd + "]]></ALIQICMSINTEFD> ");
		}
		if (cat1799spres != null && !"".equals(cat1799spres)) {
			xmlRequestBody.append(" 		<CAT1799SPRES><![CDATA[" + cat1799spres
					+ "]]></CAT1799SPRES> ");
		}
		if (codEspeCst != null && !"".equals(codEspeCst)) {
			xmlRequestBody.append(" 		<CODESPECST><![CDATA[" + codEspeCst
					+ "]]></CODESPECST> ");
		}
		if (codCtaCtbEfdDet != null && !"".equals(codCtaCtbEfdDet)) {
			xmlRequestBody.append(" 		<CODCTACTBEFD><![CDATA["
					+ codCtaCtbEfdDet + "]]></CODCTACTBEFD> ");
		}
		if (codFci != null && !"".equals(codFci)) {
			xmlRequestBody.append(" 		<CODFCI><![CDATA[" + codFci
					+ "]]></CODFCI> ");
		}

		xmlRequestBody.append(" 		</localFields> ");
		xmlRequestBody.append(" 		<foreingKey> ");
		xmlRequestBody.append(" 			<CODPROD><![CDATA[" + codProd
				+ "]]></CODPROD> ");
		xmlRequestBody.append(" 		</foreingKey> ");
		xmlRequestBody.append(" 	</dataRow> ");
		xmlRequestBody.append(" </dataSet> ");
		xmlRequestBody.append(" <clientEventList> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.actionbutton.clientconfirm</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mge.volume.alternativo.utilizado.tgfpap</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mge.info.metro.cubico</clientEvent> ");
		xmlRequestBody.append(" </clientEventList> ");

		// Chamada ao servi�o da Sankhya
		serviceInvoker.call("CRUDServiceProvider.saveRecord", "mge",
				xmlRequestBody.toString());

	}

	public String enviaCartaCorrecaoSefaz(BigDecimal nuNota,
			String textoCorrecao) throws Exception {
		StringBuilder xmlRequestBody = new StringBuilder();

		xmlRequestBody.append(" <carta> ");
		xmlRequestBody.append(" 	<nota>" + nuNota + "</nota> ");
		xmlRequestBody.append(" 	<texto>" + textoCorrecao.toString()	+ "</texto> ");
		xmlRequestBody.append(" </carta> ");
		xmlRequestBody.append(" <clientEventList> ");
		xmlRequestBody.append(" 	<clientEvent>br.com.sankhya.mgewms.expedicao.cortePedidos</clientEvent> ");
		xmlRequestBody.append(" 	<clientEvent>br.com.sankhya.mgecom.cancelamento.nfeAcimaTolerancia</clientEvent> ");
		xmlRequestBody.append(" 	<clientEvent>br.com.sankhya.mgecom.nota.adicional.SolicitarUsuarioGerente</clientEvent> ");
		xmlRequestBody.append(" 	<clientEvent>br.com.sankhya.mgecomercial.event.baixaPortal</clientEvent> ");
		xmlRequestBody.append(" 	<clientEvent>br.com.sankhya.mgecom.msg.nao.possui.itens.pendentes</clientEvent> ");
		xmlRequestBody.append(" 	<clientEvent>br.com.sankhya.mgecomercial.event.compensacao.credito.debito</clientEvent> ");
		xmlRequestBody.append(" 	<clientEvent>br.com.utiliza.dtneg.servidor</clientEvent> ");
		xmlRequestBody.append(" 	<clientEvent>br.com.sankhya.mgecom.cancelamento.processo.wms.andamento</clientEvent> ");
		xmlRequestBody.append(" 	<clientEvent>br.com.sankhya.mgecom.gera.lote.xmlRejeitado</clientEvent> ");
		xmlRequestBody.append(" 	<clientEvent>br.com.sankhya.modelcore.comercial.cancela.nfce.baixa.caixa.fechado</clientEvent> ");
		xmlRequestBody.append(" 	<clientEvent>br.com.sankhya.mgecom.expedicao.SolicitarUsuarioConferente</clientEvent> ");
		xmlRequestBody.append(" 	<clientEvent>br.com.sankhya.mgecom.compra.SolicitacaoComprador</clientEvent> ");
		xmlRequestBody.append(" 	<clientEvent>comercial.status.nfe.situacao.diferente</clientEvent> ");
		xmlRequestBody.append(" 	<clientEvent>br.com.sankhya.mgecom.cancelamento.notas.remessa</clientEvent> ");
		xmlRequestBody.append(" 	<clientEvent>br.com.sankhya.mgecom.fat.preco.produtos.alterado</clientEvent> ");
		xmlRequestBody.append(" 	<clientEvent>br.com.sankhya.comercial.solicitaContingencia</clientEvent> ");
		xmlRequestBody.append(" 	<clientEvent>br.com.sankhya.mgewms.expedicao.validarPedidos</clientEvent> ");
		xmlRequestBody.append(" 	<clientEvent>br.com.sankhya.mgecom.valida.ChaveNFeCompraTerceiros</clientEvent> ");
		xmlRequestBody.append(" 	<clientEvent>br.com.sankhya.mgewms.expedicao.selecaoDocas</clientEvent> ");
		xmlRequestBody.append(" 	<clientEvent>br.com.sankhya.modelcore.comercial.cancela.nota.devolucao.wms</clientEvent> ");
		xmlRequestBody.append(" 	<clientEvent>br.com.sankhya.actionbutton.clientconfirm</clientEvent> ");
		xmlRequestBody.append(" </clientEventList> ");

		/* Chamada ao servi�o da Sankhya */
		Document document = serviceInvoker.call(
				"ServicosNfeSP.enviarCartaCorrecao", "mgecom",
				xmlRequestBody.toString());

		Node item = document.getChildNodes().item(0);

		if ("1".equals(item.getAttributes().getNamedItem("status")
				.getTextContent())) {
			return item.getAttributes().getNamedItem("transactionId")
					.getTextContent();
		} else {
			return XMLUtil.xmlToString(document);
		}

	}

	public String imprimirCartaCorrecao(BigDecimal nuNota) throws Exception {
		StringBuilder xmlRequestBody = new StringBuilder();

		xmlRequestBody.append(" <notas> ");
		xmlRequestBody.append(" 	<nota>" + nuNota + "</nota> ");
		xmlRequestBody.append(" </notas> ");
		xmlRequestBody.append(" <clientEventList> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.expedicao.SolicitarUsuarioConferente</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.nota.adicional.SolicitarUsuarioGerente</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.modelcore.comercial.cancela.nota.devolucao.wms</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>comercial.status.nfe.situacao.diferente</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.compra.SolicitacaoComprador</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgewms.expedicao.selecaoDocas</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.gera.lote.xmlRejeitado</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecomercial.event.compensacao.credito.debito</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecomercial.event.baixaPortal</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.cancelamento.processo.wms.andamento</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.actionbutton.clientconfirm</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.cancelamento.notas.remessa</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.fat.preco.produtos.alterado</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgewms.expedicao.cortePedidos</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.utiliza.dtneg.servidor</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgewms.expedicao.validarPedidos</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.comercial.solicitaContingencia</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.cancelamento.nfeAcimaTolerancia</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.msg.nao.possui.itens.pendentes</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.mgecom.valida.ChaveNFeCompraTerceiros</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.modelcore.comercial.cancela.nfce.baixa.caixa.fechado</clientEvent> ");
		xmlRequestBody.append(" </clientEventList> ");

		/* Chamada ao servi�o da Sankhya */
		Document document = serviceInvoker.call(
				"ServicosNfeSP.imprimeCartaCorrecao", "mgecom",
				xmlRequestBody.toString());

		Node item = document.getChildNodes().item(0);

		if ("1".equals(item.getAttributes().getNamedItem("status")
				.getTextContent())) {
			return item.getAttributes().getNamedItem("transactionId")
					.getTextContent();
		} else {
			return XMLUtil.xmlToString(document);
		}

	}

}

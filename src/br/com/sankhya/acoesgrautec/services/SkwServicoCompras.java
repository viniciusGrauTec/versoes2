package br.com.sankhya.acoesgrautec.services;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.w3c.dom.Document;
import org.w3c.dom.Node;

import br.com.sankhya.acoesgrautec.util.XMLUtil;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.extensions.actionbutton.QueryExecutor;
import br.com.sankhya.extensions.actionbutton.Registro;

import com.google.gson.JsonObject;

public class SkwServicoCompras {

	private SWServiceInvoker serviceInvoker;

	private ContextoAcao contexto;
	private SimpleDateFormat fmt;

	public String md5;
	public String nomeUsu;
	public String domain;

	public SkwServicoCompras(String domain, String nomeUsu, String md5) {
		this.md5 = md5;
		this.nomeUsu = nomeUsu;
		this.domain = domain;
		this.serviceInvoker = new SWServiceInvoker(domain, nomeUsu, md5, true);
	}

	public SkwServicoCompras(ContextoAcao contexto) throws Exception {
		this.contexto = contexto;
		this.serviceInvoker = new SWServiceInvoker(contexto);
		serviceInvoker.setDebugMode();
		this.fmt = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
	}

	public void obterDadosDoCep(String cep) throws Exception {
		// Composicao da consulta
		StringBuilder xmlRequestBody = new StringBuilder();
		xmlRequestBody.append("	<cep>" + cep + "</cep> ");
		xmlRequestBody.append(" <clientEventList>");
		xmlRequestBody
				.append(" 			<clientEvent>clientEvent>parceiro.mostra.mensagem.criticaie</clientEvent>");
		xmlRequestBody.append(" </clientEventList>");

		// Chamada ao servi�o da Sankhyassssss
		serviceInvoker.call("PesquisaCepSP.obterDadosDoCep", "mge",
				xmlRequestBody.toString());
	}

	public String baixarTitulo(String codUsu, String nuFin, String codEmp,
			String vlrDesdob, String codContaBaixa, int codLancamento,
			String numDocumento, String codTopBaixa, StringBuffer mensagem)
			throws Exception {
		// Composiï¿½ï¿½o da consulta
		StringBuilder xmlRequestBody = new StringBuilder();
		SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
		Date dataAtual = new Date();

//		xmlRequestBody.append(" 	<dadosBaixa dtbaixa=\""
//				+ "24/11/2023"/*formatter.format(dataAtual)*/ + "\" nufin=\"" + nuFin
//				+ "\" decisao=\"0\" reterImpostosRetroativos=\"true\" ");
//		xmlRequestBody
//				.append(" 		resourceID=\"br.com.sankhya.fin.cad.movimentacaoFinanceira\"> ");
//		xmlRequestBody.append(" 		<variosTiposTitulos/> ");
//		xmlRequestBody
//				.append(" 			<valoresBaixa tipoJuros=\"I\" tipoMulta=\"I\" taxaAdm=\"0\" vlrDesconto=\"0\" vlrCalculado=\""
//						+ 30/*vlrDesdob*/ + "\" vlrDesdob=\"" + 30/*vlrDesdob*/ + "\" ");
//		xmlRequestBody
//				.append(" 			vlrDespesasCartorio=\"0\" vlrJuros=\"0\" vlrMulta=\"0\" vlrTotal=\""
//						+ 30/*vlrDesdob*/
//						+ "\" vlrMultaNeg=\"0\" vlrJurosNeg=\"0\" jurosLib=\"0\"  ");
//		xmlRequestBody
//				.append(" 			multaLib=\"0\" vlrMoeda=\"0\" vlrVarCambial=\"0\"/> ");
//		xmlRequestBody
//				.append(" 			<dadosBancarios codConta=\""
//						+ codContaBaixa
//						+ "\" codLancamento=\""
//						+ codLancamento
//						+ "\" numDocumento=\""
//						+ numDocumento
//						+ "\" historico=\"RkFCSU8gR09NRVMgU0lMVkE=\" dataConciliacao=\"\" codTipTit=\"21\" ");
//		xmlRequestBody
//				.append(" 			vlrMoedaBaixa=\"0,00000\" contaParaCaixaAberto=\"0\"/> ");
//		xmlRequestBody.append(" 		<dadosAdicionais codEmpresa=\"" + codEmp
//				+ "\" codTipoOperacao=\"" + codTopBaixa + "\"/> ");
//		xmlRequestBody
//				.append(" 			<impostos inssRetido=\"false\" irrfRetido=\"true\" issRetido=\"false\" outrosImpostos=\"0\" outrosImpostosMensais=\"0\"  ");
//		xmlRequestBody
//				.append(" 				vlrInss=\"0\" vlrIrrf=\"0\" vlrIss=\"0\" vlrVendor=\"0\" baseDevida=\"0\"/> ");
//		xmlRequestBody
//				.append(" 			<imprimeRecibo paramImprimeBoleto=\"false\"/> ");
//		xmlRequestBody.append(" 	</dadosBaixa> ");
//		xmlRequestBody.append(" 	<clientEventList> ");
//		xmlRequestBody
//				.append(" 				<clientEvent>br.com.sankhya.mgefin.solicitacao.liberacao.orcamento</clientEvent> ");
//		xmlRequestBody
//				.append(" 				<clientEvent>br.com.sankhya.mgefin.financeiro.Autentica</clientEvent> ");
//		xmlRequestBody
//				.append("		 		<clientEvent>br.com.sankhya.mgefin.mensagem.metas</clientEvent> ");
//		xmlRequestBody
//				.append(" 				<clientEvent>br.com.sankhya.mgefin.avisa.validacao.numero.financeiro</clientEvent> ");
//		xmlRequestBody
//				.append(" 				<clientEvent>br.com.sankhya.mgecom.pesquisamov.mensagem</clientEvent> ");
//		xmlRequestBody
//				.append("		 		<clientEvent>br.com.sankhya.mgefin.alteracao.boleto.agrupado.registrado</clientEvent> ");
//		xmlRequestBody
//				.append(" 				<clientEvent>br.com.sankhya.mgefin.financeiro.LibLimite</clientEvent> ");
//		xmlRequestBody.append(" 	</clientEventList> ");
		
		xmlRequestBody.append("<dadosBaixa dtbaixa=\"27/11/2023\" nufin=\"111095\" decisao=\"0\" reterImpostosRetroativos=\"true\" resourceID=\"br.com.sankhya.fin.cad.movimentacaoFinanceira\"> "
			+"		<variosTiposTitulos/> "
			+"		<valoresBaixa tipoJuros=\"I\" tipoMulta=\"I\" taxaAdm=\"0\" vlrDesconto=\"0\" vlrCalculado=\"30.63\" vlrDesdob=\"30\" vlrDespesasCartorio=\"0\" vlrJuros=\"0.03\" vlrMulta=\"0.6\" vlrTotal=\"30.63\" vlrMultaNeg=\"0\" vlrJurosNeg=\"0\" jurosLib=\"0\" multaLib=\"0\" vlrMoeda=\"0\" vlrVarCambial=\"0\"/> "
			+"		<dadosBancarios codConta=\"15\" codLancamento=\"1\" numDocumento=\"0\" historico=\"RkFCSU8gR09NRVMgU0lMVkE=\" dataConciliacao=\"\" codTipTit=\"21\" vlrMoedaBaixa=\"0,00000\" contaParaCaixaAberto=\"0\"/> "
			+"		<dadosAdicionais codEmpresa=\"4\" codTipoOperacao=\"1400\"/> "
			+"		<impostos inssRetido=\"false\" irrfRetido=\"false\" issRetido=\"false\" outrosImpostos=\"0\" outrosImpostosMensais=\"0\" vlrInss=\"0\" vlrIrrf=\"0\" vlrIss=\"0\" vlrVendor=\"0\" baseDevida=\"0\"/>"
			+"		<imprimeRecibo paramImprimeBoleto=\"false\"/> "
			+"	</dadosBaixa>"
			+"	<clientEventList>"
			+"		<clientEvent>br.com.sankhya.mgefin.financeiro.LibLimite</clientEvent> "
			+"		<clientEvent>br.com.sankhya.mgefin.alteracao.boleto.agrupado.registrado</clientEvent> "
			+"		<clientEvent>br.com.sankhya.mgefin.solicitacao.liberacao.orcamento</clientEvent> "
			+"		<clientEvent>br.com.sankhya.mgefin.financeiro.Autentica</clientEvent> "
			+"		<clientEvent>br.com.sankhya.mgecom.pesquisamov.mensagem</clientEvent> "
			+"		<clientEvent>br.com.sankhya.mgefin.mensagem.metas</clientEvent> "
			+"		<clientEvent>br.com.sankhya.mgefin.avisa.validacao.numero.financeiro</clientEvent> "
			+"	</clientEventList>");
		
		System.out.println("Corpo da requisição de baixa: \n" + xmlRequestBody);
		
		// Chamada ao serviï¿½o da Sankhya
		serviceInvoker.call("BaixaFinanceiroSP.baixarTitulo", "mge",
				xmlRequestBody.toString(), codUsu, "br.com.sankhya.fin.cad.movimentacaoFinanceira", "MovimentacaoFinanceira");
		return xmlRequestBody.toString();

	}
	
	public void inserirIntegracaoPesagem(String texto)
			throws Exception {
		// Composicao da consulta
		StringBuilder xmlRequestBody = new StringBuilder();
		xmlRequestBody.append(" <dataSet rootEntity='AD_ADINTEPESA' includePresentationFields='S' datasetid='1701104526101_1'>    ");
		xmlRequestBody.append(" 	<entity path=''>                                                                            ");
		xmlRequestBody.append(" 		<fieldset list='*'/>                                                                    ");
		xmlRequestBody.append(" 	</entity>                                                                                   ");
		xmlRequestBody.append(" 	<dataRow>                                                                                   ");
		xmlRequestBody.append(" 		<localFields>                                                                           ");
		xmlRequestBody.append(" 			<TESTE><![CDATA["+ texto +"]]></TESTE>                                              ");
		xmlRequestBody.append(" 		</localFields>                                                                          ");
		xmlRequestBody.append(" 	</dataRow>                                                                                  ");
		xmlRequestBody.append(" </dataSet>                                                                                      ");
		xmlRequestBody.append(" <clientEventList/>                                                                              ");

		serviceInvoker.call("CRUDServiceProvider.saveRecord", "mge",
				xmlRequestBody.toString());

	}
	
	public String parcelarTitulo(String vencimento, String codUsu, 
			int parcelas, String tipTit)
					throws Exception {
		// Composiï¿½ï¿½o da consulta
		StringBuilder xmlRequestBody = new StringBuilder();
		
		xmlRequestBody.append("<parcel save='S' nroparcel='"+parcelas+"' cpyrat='N' "
				+ "txjur='' txmul='' hist='S' calc='N' "
						+ "considerarEntradaCalcJuroMulta='N' "
								+ "tip='"+tipTit+"' "
										+ "jur='1' "
												+ "mul='1' "
														+ "calcDesc='0' "
																+ "usaParamJUROSPCP='N' "
																		+ "desdobInicial='1' "
																				+ "varCambialJurMult='N' "
																						+ "zerarJuroMulta='N' "
																								+ "chkComEntrada='N' "
																										+ "recalcularTaxaAdministradora='S' "
																												+ "priceJurMulDesc='N'> ");
		xmlRequestBody.append("		<freq value='5' dias='0'/> ");
		xmlRequestBody.append("		<venc value='3' nova='"+vencimento+"'/> ");
		xmlRequestBody.append("		<tits nufins='10243'/> ");
		xmlRequestBody.append("		<negoc value='1'/> ");
		xmlRequestBody.append("	</parcel> ");
		xmlRequestBody.append("	<clientEventList> ");
		xmlRequestBody.append("		<clientEvent>br.com.sankhya.mgefin.financeiro.Autentica</clientEvent> ");
		xmlRequestBody.append("		<clientEvent>br.com.sankhya.mgefin.alteracao.boleto.agrupado.registrado</clientEvent> ");
		xmlRequestBody.append("		<clientEvent>br.com.sankhya.mgefin.financeiro.LibLimite</clientEvent> ");
		xmlRequestBody.append("		<clientEvent>br.com.sankhya.mgefin.solicitacao.liberacao.orcamento</clientEvent> ");
		xmlRequestBody.append("		<clientEvent>br.com.sankhya.mgefin.avisa.validacao.numero.financeiro</clientEvent> ");
		xmlRequestBody.append("		<clientEvent>br.com.sankhya.mgefin.mensagem.metas</clientEvent> ");
		xmlRequestBody.append("	</clientEventList>");
		
		// Chamada ao serviï¿½o da Sankhya
		serviceInvoker.call("ParcelamentoSP.parcelar", "mge",
				xmlRequestBody.toString(), codUsu);
		return xmlRequestBody.toString();
		
	}

	/*
	 * A ação de criar os documentos relacionados
	 */
	public void saveCompraVendaVariosPedido(int nuNota, String sequencia,
			BigDecimal qtdNeg, int nuNotaOrigem, int sequenciaOrigem)
			throws Exception {

		// Composicao da consulta
		StringBuilder xmlRequestBody = new StringBuilder();
		xmlRequestBody
				.append("		<dataSet rootEntity='CompraVendavariosPedido' includePresentationFields='S' crudListener='br.com.sankhya.modelcore.dwfdata.listeners.tgf.CompraVendavariosPedidoAdapter' datasetid='1568319105335_10'> ");
		xmlRequestBody.append("			<entity path=''> ");
		xmlRequestBody.append("				<fieldset list='*'/> ");
		xmlRequestBody.append("			</entity> ");
		xmlRequestBody.append("			<entity path='ItemNotaOrigem'> ");
		xmlRequestBody.append("				<field name='PENDENTE'/> ");
		xmlRequestBody.append("			</entity> ");
		xmlRequestBody
				.append("				<entity path='ItemNotaOrigem.CabecalhoNota'> ");
		xmlRequestBody.append("					<fieldset list='NUMNOTA,TIPMOV,DTNEG'/> ");
		xmlRequestBody.append("			</entity> ");
		xmlRequestBody.append("			<dataRow> ");
		xmlRequestBody.append("				<localFields> ");
		xmlRequestBody.append("					<STATUSNOTA><![CDATA[A]]></STATUSNOTA> ");
		xmlRequestBody.append("					<QTDATENDIDA><![CDATA[" + qtdNeg
				+ "]]></QTDATENDIDA> ");
		xmlRequestBody.append("					<SEQUENCIA><![CDATA[" + sequencia
				+ "]]></SEQUENCIA> ");
		xmlRequestBody.append("					<NUNOTAORIG><![CDATA[" + nuNotaOrigem
				+ "]]></NUNOTAORIG> ");
		xmlRequestBody.append("					<SEQUENCIAORIG><![CDATA[" + sequenciaOrigem
				+ "]]></SEQUENCIAORIG> ");
		xmlRequestBody.append("					<NUNOTA><![CDATA[" + nuNota
				+ "]]></NUNOTA> ");
		xmlRequestBody.append("				</localFields> ");
		xmlRequestBody.append("			</dataRow> ");
		xmlRequestBody.append("		</dataSet> ");
		xmlRequestBody.append("		<clientEventList> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.actionbutton.clientconfirm</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.central.itens.VendaCasada</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.expedicao.SolicitarUsuarioConferente</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.importacaoxml.cfi.para.produto</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.exibe.msg.variacao.preco</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.event.troca.item.por.produto.substituto</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.checkout.obter.peso</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.valida.ChaveNFeCompraTerceiros</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.parcelas.financeiro</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.event.troca.item.por.produto.alternativo</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.nota.adicional.SolicitarUsuarioGerente</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.compra.SolicitacaoComprador</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecomercial.event.estoque.insuficiente.produto</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.central.itens.KitRevenda</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecomercial.event.baixaPortal</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.coleta.entrega.recalculado</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>central.save.grade.itens.mostrar.popup.serie</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecomercial.event.cadastrarDistancia</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgefin.event.fixa.vencimento</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgefin.solicitacao.liberacao.orcamento</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.exibir.variacao.valor.item</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.cancelamento.notas.remessa</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.central.itens.KitRevenda.msgValidaFormula</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecomercial.event.estoque.componentes</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.utiliza.dtneg.servidor</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>central.save.grade.itens.mostrar.popup.info.lote</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecomercial.event.compensacao.credito.debito</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecomercial.event.faturamento.confirmacao</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.imobilizado</clientEvent> ");
		xmlRequestBody.append("		</clientEventList> ");

		// Chamada ao servi�o da Sankhya

		serviceInvoker.call("CRUDServiceProvider.saveRecord", "mge",
				xmlRequestBody.toString());

	}

	/*
	 * A ação de criar os documentos relacionados
	 */
	public String saveItensNota(int codEmp, int nuNota, int codProd,
			BigDecimal qtdNeg, BigDecimal vltUnitLiq, BigDecimal vltTotLiq,
			int qtdPeca, int codLocal, String controle) throws Exception {
		DecimalFormat decimalFormat = new DecimalFormat();
		decimalFormat.setMaximumFractionDigits(2);
		decimalFormat.setMinimumFractionDigits(2);
		// Composicao da consulta
		StringBuilder xmlRequestBody = new StringBuilder();
		xmlRequestBody.append("		<nota NUNOTA='" + nuNota
				+ "' ownerServiceCall='GradeItens684'> ");
		xmlRequestBody.append("				<itens ATUALIZACAO_ONLINE='false'> ");

		String codVol = "";
		String usoProd = "";
		QueryExecutor queryProduto = contexto.getQuery();
		String sqlNota = " 	SELECT codvol, usoprod FROM tgfpro WHERE codprod = {CODPROD} ";
		queryProduto.setParam("CODPROD", codProd);
		queryProduto.nativeSelect(sqlNota);
		while (queryProduto.next()) {
			codVol = queryProduto.getString("codvol");
			usoProd = queryProduto.getString("usoprod");
		}
		queryProduto.close();

		QueryExecutor queryCab = contexto.getQuery();
		String sqlCab = " 	   select codtipoper from tgfcab where nunota = {NUNOTA} ";
		queryCab.setParam("NUNOTA", nuNota);
		queryCab.nativeSelect(sqlCab);
		String codtipoper = "";
		while (queryCab.next()) {
			codtipoper = queryCab.getString("codtipoper");
		}
		queryCab.close();

		// if (vltUnitLiq != null) {
		// vltUnitLiq = vltUnitLiq.setScale(5, BigDecimal.ROUND_UP);
		// }

		// if (qtdNeg != null) {
		// qtdNeg = qtdNeg.setScale(2, BigDecimal.ROUND_UP);
		// }

		BigDecimal pesoBruto = qtdNeg;
		// if (pesoBruto != null) {
		// pesoBruto = pesoBruto.setScale(2, BigDecimal.ROUND_UP);
		// }

		BigDecimal pesoLiq = qtdNeg;
		// if (pesoLiq != null) {
		// pesoLiq = pesoLiq.setScale(2, BigDecimal.ROUND_UP);
		// }

		// vltTotLiq = vltTotLiq.setScale(2, BigDecimal.ROUND_UP);

		xmlRequestBody.append("			<item> ");
		xmlRequestBody.append("				<NUNOTA>" + nuNota + "</NUNOTA> ");
		xmlRequestBody.append("				<SEQUENCIA/> ");
		xmlRequestBody.append("				<VLRUNIT>" + vltUnitLiq + "</VLRUNIT> ");
		xmlRequestBody.append("				<VLRSUBST>0</VLRSUBST> ");
		xmlRequestBody.append("				<BASESUBSTIT>0</BASESUBSTIT> ");
		xmlRequestBody.append("				<VLRDESCMOE>0</VLRDESCMOE> ");
		xmlRequestBody.append("				<VLRICMSUFDEST>0</VLRICMSUFDEST> ");
		xmlRequestBody.append("				<VLRUNITLIQ>" + vltUnitLiq
				+ "</VLRUNITLIQ> ");
		xmlRequestBody.append("				<ALIQICMS>0</ALIQICMS> ");
		xmlRequestBody.append("				<PRECOBASE>0</PRECOBASE> ");
		xmlRequestBody.append("				<BASEIPI>0</BASEIPI> ");
		xmlRequestBody.append("				<VLRIPI>0</VLRIPI> ");
		xmlRequestBody.append("				<VLRICMS>0</VLRICMS> ");
		xmlRequestBody.append("				<VLRCUS>0</VLRCUS> ");
		xmlRequestBody.append("				<BASESTUFDEST>0</BASESTUFDEST> ");
		xmlRequestBody.append("				<BASESTANT>0</BASESTANT> ");
		xmlRequestBody.append("				<PERCDESC>0</PERCDESC> ");
		xmlRequestBody.append("				<ALIQIPI>0</ALIQIPI> ");
		xmlRequestBody.append("				<VLRDESC>0</VLRDESC> ");

		xmlRequestBody.append("				<VLRTOT>" + vltTotLiq + "</VLRTOT> ");

		xmlRequestBody.append("				<VLRUNIDPAD>" + vltUnitLiq
				+ "</VLRUNIDPAD> ");
		xmlRequestBody.append("				<CODPROD>" + codProd + "</CODPROD> ");

		xmlRequestBody.append("				<VLRTOTMOE>0</VLRTOTMOE> ");
		xmlRequestBody.append("				<VLRUNITMOE>0</VLRUNITMOE> ");
		xmlRequestBody.append("				<VLRUNITLIQMOE>0</VLRUNITLIQMOE> ");
		xmlRequestBody.append("				<VLRTOTLIQMOE>0</VLRTOTLIQMOE> ");
		xmlRequestBody.append("				<CODLOCALORIG>" + codLocal
				+ "</CODLOCALORIG> ");
		xmlRequestBody.append("				<M3>0</M3> ");
		xmlRequestBody.append("				<QTDUNIDPAD>" + qtdNeg + "</QTDUNIDPAD> ");
		xmlRequestBody.append("				<STATUSLOTE>N</STATUSLOTE> ");
		xmlRequestBody.append("				<STATUSNOTA>P</STATUSNOTA> ");

		xmlRequestBody.append("				<CODVOL>" + codVol + "</CODVOL> ");
		xmlRequestBody.append("				<CODVOLPAD>" + codVol + "</CODVOLPAD> ");
		xmlRequestBody.append("				<VLRTOTLIQ>" + vltTotLiq + "</VLRTOTLIQ> ");
		xmlRequestBody.append("				<QTDNEG>" + qtdNeg + "</QTDNEG> ");
		xmlRequestBody.append("				<QTDVOL>" + qtdPeca + "</QTDVOL> ");
		xmlRequestBody
				.append("					<PRODUTOALTERNATIVO>N</PRODUTOALTERNATIVO> ");
		xmlRequestBody.append("				<CODEMP>" + codEmp + "</CODEMP> ");
		// Top do algodão não vai controlar por lote
		if ("251".equals(codtipoper)) {
			xmlRequestBody.append("				<CONTROLE> </CONTROLE>");
			if (controle != null) {
				xmlRequestBody.append("			<AD_LOTECONTRATO>" + controle
						+ "</AD_LOTECONTRATO>");
			} else {
				xmlRequestBody
						.append("			<AD_LOTECONTRATO> </AD_LOTECONTRATO>");
			}
		} else {
			if (controle != null) {
				xmlRequestBody.append("			<CONTROLE>" + controle
						+ "</CONTROLE>");
			} else {
				xmlRequestBody.append("			<CONTROLE> </CONTROLE>");
			}
		}

		if ("729".equals(codtipoper)) {
			xmlRequestBody.append("			<ATUALESTTERC>D</ATUALESTTERC> ");
			xmlRequestBody.append("			<TERCEIROS>S</TERCEIROS> ");
			xmlRequestBody.append("			<CODLOCALTERC>" + codLocal
					+ "</CODLOCALTERC>");
			xmlRequestBody.append("			<ATUALESTOQUE>0</ATUALESTOQUE>");
			xmlRequestBody.append("			<RESERVA>N</RESERVA> ");
			xmlRequestBody.append("			<ORIGPROD>0</ORIGPROD> ");
		}
		xmlRequestBody.append("				<PESOBRUTO>" + pesoBruto + "</PESOBRUTO> ");
		xmlRequestBody.append("				<PESOLIQ>" + pesoLiq + "</PESOLIQ> ");
		xmlRequestBody.append("				<QTDPECA>" + qtdPeca + "</QTDPECA> ");
		xmlRequestBody.append("				<USOPROD>" + usoProd + "</USOPROD>");
		xmlRequestBody.append("				<CODVEND>0</CODVEND>");
		xmlRequestBody.append("				<CODUSU>0</CODUSU>");
		xmlRequestBody.append("				<PENDENTE>S</PENDENTE> ");
		xmlRequestBody.append("				<GERAPRODUCAO>S</GERAPRODUCAO>");

		xmlRequestBody.append("				<FATURAR>S</FATURAR>");
		xmlRequestBody.append("				<CUSTO/> ");
		xmlRequestBody.append("				<NUTAB/> ");
		xmlRequestBody.append("				<DTALTER/> ");
		xmlRequestBody.append("				<NRSERIERESERVA/> ");

		xmlRequestBody.append("				<VLRUNITDOLAR/> ");
		xmlRequestBody.append("				<VLRTROCA/> ");
		xmlRequestBody.append("				<NUFOP/> ");
		xmlRequestBody.append("				<PERCSTFCPINTANT/> ");
		xmlRequestBody.append("				<NUMPEDIDO2/> ");
		xmlRequestBody.append("				<QTDTRIBEXPORT/> ");
		xmlRequestBody.append("				<SEQUENCIAFISCAL/> ");
		xmlRequestBody.append("				<PERCDESCFORNECEDOR/> ");
		xmlRequestBody.append("				<RESERVADO/> ");
		xmlRequestBody.append("				<PERCCOMGER/> ");
		xmlRequestBody.append("				<VLRLIQPROM/> ");
		xmlRequestBody.append("				<VLRREPRED/> ");
		xmlRequestBody.append("				<SEQPEDIDO2/> ");
		xmlRequestBody.append("				<PERCCOM/> ");
		xmlRequestBody.append("				<GRUPOTRANSG/> ");
		xmlRequestBody.append("				<CODTRIB/> ");
		xmlRequestBody.append("				<VLRSUBSTUNITORIG/> ");
		xmlRequestBody.append("				<CODCFPS/> ");
		xmlRequestBody.append("				<ALIQSTEXTRANOTA/> ");
		xmlRequestBody.append("				<CODUSU/> ");
		xmlRequestBody.append("				<BASEISS/> ");
		xmlRequestBody.append("				<NUMEROOS/> ");
		xmlRequestBody.append("				<ALIQISS/> ");
		xmlRequestBody.append("				<LARGURA/> ");
		xmlRequestBody.append("				<NUMCONTRATO/> ");
		xmlRequestBody.append("				<ALIQSTFCPSTANT/> ");
		xmlRequestBody.append("				<CODTRIBISS/> ");
		xmlRequestBody.append("				<VLRCOMGER/> ");
		xmlRequestBody.append("				<VLRISS/> ");
		xmlRequestBody.append("				<CODCFO/> ");
		xmlRequestBody.append("				<VARIACAOFCP/> ");
		xmlRequestBody.append("				<VLRSUBSTANT/> ");
		xmlRequestBody.append("				<VLRSUGERIDO/> ");
		xmlRequestBody.append("				<CODENQIPI/> ");
		xmlRequestBody.append("				<BASESTEXTRANOTA/> ");
		xmlRequestBody.append("				<VLRUNITLOC/> ");
		xmlRequestBody.append("				<VLRTOTLIQREF/> ");
		xmlRequestBody.append("				<DTINICIO/> ");
		xmlRequestBody.append("				<QTDPENDENTE/> ");
		xmlRequestBody.append("				<CODEXEC/> ");
		xmlRequestBody.append("				<CODANTECIPST/> ");
		xmlRequestBody.append("				<ESTOQUE/> ");
		xmlRequestBody.append("				<ALTPRECO/> ");
		xmlRequestBody.append("				<CODMOTDESONERAICMS/> ");
		xmlRequestBody.append("				<CODOBSPADRAO/> ");
		xmlRequestBody.append("				<VLRSTEXTRANOTA/> ");
		xmlRequestBody.append("				<VLRICMSANT/> ");
		xmlRequestBody.append("				<VLRRETENCAO/> ");
		xmlRequestBody.append("				<SOLCOMPRA/> ");
		xmlRequestBody.append("				<BASESUBSTITUNITORIG/> ");
		xmlRequestBody.append("				<IDALIQICMS/> ");
		xmlRequestBody.append("				<VLRREPREDSEMDESC/> ");
		xmlRequestBody.append("				<CODAGREGACAO/> ");
		xmlRequestBody.append("				<STATUSPROC/> ");
		xmlRequestBody.append("				<GTINNFE/> ");
		xmlRequestBody.append("				<CODPROMO/> ");
		xmlRequestBody.append("				<PERCDESCPROM/> ");
		xmlRequestBody.append("				<ALIQICMSRED/> ");
		xmlRequestBody.append("				<VLRCOM/> ");
		xmlRequestBody.append("				<VLRPROMO/> ");
		xmlRequestBody.append("				<REFERENCIA/> ");
		xmlRequestBody.append("				<ALTURA/> ");
		xmlRequestBody.append("				<INDESCALA/> ");
		xmlRequestBody.append("				<OPERATUAL/> ");
		xmlRequestBody.append("				<BASESUBSTITANT/> ");
		xmlRequestBody.append("				<GTINTRIBNFE/> ");
		xmlRequestBody.append("				<CODLOCALDEST/> ");
		xmlRequestBody.append("				<QTDFIXADA/> ");
		xmlRequestBody.append("				<CONTROLEDEST/> ");
		xmlRequestBody.append("				<PRODUTOPESQUISADO/> ");
		xmlRequestBody.append("				<QTDWMS/> ");
		xmlRequestBody.append("				<CODBENEFNAUF/> ");
		xmlRequestBody.append("				<CODESPECST/> ");
		xmlRequestBody.append("				<PRODUTONFE/> ");
		xmlRequestBody.append("				<BASESUBSTSEMRED/> ");
		xmlRequestBody.append("				<VLRPTOPUREZA/> ");
		xmlRequestBody.append("				<CODCAV/> ");
		xmlRequestBody.append("				<ENDIMAGEM/> ");
		xmlRequestBody.append("				<BASICMMOD/> ");
		xmlRequestBody.append("				<CODPROC/> ");
		xmlRequestBody.append("				<BASESTFCPINTANT/> ");
		xmlRequestBody.append("				<NROPROCESSO/> ");
		xmlRequestBody.append("				<PERCDESCBONIF/> ");
		xmlRequestBody.append("				<QTDENTREGUE/> ");
		xmlRequestBody.append("				<DTVIGOR/> ");
		xmlRequestBody.append("				<VLRDESCBONIF/> ");
		xmlRequestBody.append("				<PERCPUREZA/> ");
		xmlRequestBody.append("				<PERCGERMIN/> ");
		xmlRequestBody.append("				<CSTIPI/> ");
		xmlRequestBody.append("				<VLRDESCDIGITADO/> ");
		xmlRequestBody.append("				<CODTPA/> ");
		xmlRequestBody.append("				<MARCA/> ");
		xmlRequestBody.append("				<QTDFORMULA/> ");
		xmlRequestBody.append("				<QTDCONFERIDA/> ");
		xmlRequestBody.append("				<PERCDESCBASE/> ");
		xmlRequestBody.append("				<CSOSN/> ");
		xmlRequestBody.append("				<REFFORN/> ");
		xmlRequestBody.append("				<OBSERVACAO/> ");
		xmlRequestBody.append("				<NCM/> ");
		xmlRequestBody.append("				<ESPESSURA/> ");
		xmlRequestBody.append("				<CODPARCEXEC/> ");
		xmlRequestBody.append("				<VLRACRESCDESC/> ");
		xmlRequestBody.append("				<NUPROMOCAO/> ");
		xmlRequestBody.append("				<VLRSTFCPINTANT/> ");
		xmlRequestBody.append("				<PERCDESCDIGITADO/> ");
		xmlRequestBody.append("				<BASICMSTMOD/> ");
		xmlRequestBody.append("				<CODVOLPARC/> ");
		xmlRequestBody.append("				<CNPJFABRICANTE/> ");
		xmlRequestBody.append("				<QTDFAT/> ");
		xmlRequestBody.append("				<BASEICMS/> ");
		xmlRequestBody.append("				<AD_COTACAO/> ");
		xmlRequestBody.append("				<PERCDESCTGFDES/> ");
		xmlRequestBody.append("				<VLRVENDAPROMO/> ");
		xmlRequestBody.append("				<PRECOBASEQTD/> ");
		xmlRequestBody.append("				<ORIGEMBUSCA/> ");
		xmlRequestBody.append("				<COMPLDESC/> ");
		xmlRequestBody.append("			</item> ");
		xmlRequestBody.append("			</itens> ");
		xmlRequestBody
				.append("				<txProperties><prop name='br.com.sankhya.mgefin.mostrar.sugestao.venda' value='true'/> ");
		xmlRequestBody
				.append("					<prop name='br.com.sankhya.mgefin.recalculo.custopreco.Automatico' value='false'/> ");
		xmlRequestBody
				.append("					<prop name='br.com.sankhya.mgefin.checarfinanceiro.VlrEntrada' value='0'/> ");
		xmlRequestBody
				.append("					<prop name='br.com.sankhya.mgecom.gradeItens.pedidoWeb' value='false'/> ");
		xmlRequestBody.append("			</txProperties> ");
		xmlRequestBody.append("		</nota> ");
		xmlRequestBody.append("		<clientEventList> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.mgecom.coleta.entrega.recalculado</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.mgecom.central.itens.VendaCasada</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.mgecom.expedicao.SolicitarUsuarioConferente</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.mgecom.valida.ChaveNFeCompraTerceiros</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.mgecom.central.itens.KitRevenda</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.mgecom.nota.adicional.SolicitarUsuarioGerente</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.mgecomercial.event.compensacao.credito.debito</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.mgecom.central.itens.KitRevenda.msgValidaFormula</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.mgecom.parcelas.financeiro</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.mgecomercial.event.baixaPortal</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>central.save.grade.itens.mostrar.popup.serie</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.mgefin.event.fixa.vencimento</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.mgefin.solicitacao.liberacao.orcamento</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.exibir.variacao.valor.item</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.mgecomercial.event.cadastrarDistancia</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.mgecom.cancelamento.notas.remessa</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.mgecomercial.event.estoque.insuficiente.produto</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.utiliza.dtneg.servidor</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>central.save.grade.itens.mostrar.popup.info.lote</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.mgecom.event.troca.item.por.produto.alternativo</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.mgecomercial.event.faturamento.confirmacao</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.mgecomercial.event.estoque.componentes</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.mgecom.compra.SolicitacaoComprador</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.mgecom.event.troca.item.por.produto.substituto</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.mgecom.imobilizado</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.checkout.obter.peso</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.importacaoxml.cfi.para.produto</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.actionbutton.clientconfirm</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.exibe.msg.variacao.preco</clientEvent> ");
		xmlRequestBody.append("		</clientEventList> ");

		// Chamada ao servi�o da Sankhya

		Document document = serviceInvoker.call("CACSP.incluirAlterarItemNota",
				"mgecom", xmlRequestBody.toString());

		Node item = document.getChildNodes().item(0);

		if ("1".equals(item.getAttributes().getNamedItem("status")
				.getTextContent())) {
			// return
			// document.getElementsByTagName("pk").item(0).getAttributes()
			// .getNamedItem("SEQUENCIA").getTextContent();
			return document.getElementsByTagName("SEQUENCIA").item(0)
					.getTextContent();

		} else {
			return XMLUtil.xmlToString(document);
		}

	}

	/*
	 * A ação de criar os documentos relacionados
	 */
	public String saveCabecalhoNota(Long nuNota, String numNota, String codEmp,
			String codTipoOper, String dhTipOper, String tipoMovimento,
			String codParc, String codParcDest, String codTipVenda,
			String dhTipVenda, String codCencus, String codNat,
			String serieNota, String chaveNFERef, Long qtdVol,
			BigDecimal pesoLiq, BigDecimal pesoBruto, String observacao,
			Long idiproc, String codVend) throws Exception {
		DecimalFormat decimalFormat = new DecimalFormat();
		decimalFormat.setMaximumFractionDigits(2);
		decimalFormat.setMinimumFractionDigits(2);

		SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
		SimpleDateFormat formatter2 = new SimpleDateFormat(
				"dd/MM/yyyy hh:mm:ss");
		Date dhNeg = new Date();

		// Parceiro Entrega
		// String codParcDest = "0";
		// String numNota = "0";
		String dtNeg = formatter.format(dhNeg) + " 0:00:0";

		String tipFrete = "N";
		String cifFob = "F";
		if ("6".equals(codEmp) || "8".equals(codEmp)) {
			cifFob = "C";
		}

		// Empresa
		String codEmpFunc = codEmp;

		// A definir posteriormente
		String tipIPIEmb = "N";
		String indPresNfce = "0";
		String issRetido = "N";
		String irfRetido = "S";

		// Composicao da consulta
		StringBuilder xmlRequestBody = new StringBuilder();
		xmlRequestBody.append(" <nota ownerServiceCall='CentralNotas'> ");
		xmlRequestBody.append(" 	<cabecalho> ");
		if (!"677".equals(codTipoOper)) {
			xmlRequestBody.append(" 	<NUMNOTA>" + numNota + "</NUMNOTA>");
		}

		xmlRequestBody.append(" 	    <TIPMOV>" + tipoMovimento + "</TIPMOV>");
		xmlRequestBody.append(" 	    <CODTIPOPER>" + codTipoOper
				+ "</CODTIPOPER>");
		xmlRequestBody.append("			<DHTIPOPER>" + dhTipOper + "</DHTIPOPER> ");
		xmlRequestBody.append("			<CODPARC>" + codParc + "</CODPARC> ");
		if (codParcDest != null) {
			xmlRequestBody.append(" 		<CODPARCDEST>" + codParcDest
					+ "</CODPARCDEST>");
		} else {
			xmlRequestBody.append(" </CODPARCDEST>");
		}
		xmlRequestBody.append(" 	    <CODTIPVENDA>" + codTipVenda
				+ "</CODTIPVENDA>");
		xmlRequestBody.append(" 	    <CODEMP>" + codEmp + "</CODEMP> ");
		xmlRequestBody.append(" 	    <DTNEG>" + dtNeg + "</DTNEG>");
		xmlRequestBody.append(" 	    <CODEMPFUNC>" + codEmpFunc
				+ "</CODEMPFUNC>");
		xmlRequestBody.append(" 	    <CIF_FOB>" + cifFob + "</CIF_FOB> ");
		xmlRequestBody.append(" 	    <TIPIPIEMB>" + tipIPIEmb + "</TIPIPIEMB>");
		xmlRequestBody.append(" 	    <TIPFRETE>" + tipFrete + "</TIPFRETE>");
		xmlRequestBody.append(" 	    <INDPRESNFCE>" + indPresNfce
				+ "</INDPRESNFCE>");
		xmlRequestBody.append(" 	    		<ISSRETIDO>" + issRetido
				+ "</ISSRETIDO> ");
		xmlRequestBody.append(" 	    		<IRFRETIDO>" + irfRetido
				+ "</IRFRETIDO> ");
		xmlRequestBody.append("					<DHTIPVENDA>" + dhTipVenda
				+ "</DHTIPVENDA> ");
		xmlRequestBody.append(" 		<CODCENCUS>" + codCencus + "</CODCENCUS>");
		xmlRequestBody.append(" 		<CODNAT>" + codNat + "</CODNAT>");
		xmlRequestBody.append(" 		<DTPREVENT>" + dtNeg + "</DTPREVENT>");
		xmlRequestBody.append(" 		<SERIENOTA>" + serieNota + "</SERIENOTA>");
		if ("332".equals(codTipoOper) || "380".equals(codTipoOper)
				|| "339".equals(codTipoOper) || "813".equals(codTipoOper)
				|| "307".equals(codTipoOper) || "335".equals(codTipoOper)) {
			xmlRequestBody.append(" 	<CHAVENFE>" + chaveNFERef + "</CHAVENFE>");
		} else {
			xmlRequestBody.append(" 	<CHAVENFEREF>" + chaveNFERef
					+ "</CHAVENFEREF>");
		}

		xmlRequestBody.append(" 		<QTDVOL>" + qtdVol + "</QTDVOL> ");
		xmlRequestBody.append(" 		<PESO>" + pesoLiq + "</PESO>");
		xmlRequestBody.append(" 		<PESOBRUTO>" + pesoBruto + "</PESOBRUTO>");
		xmlRequestBody.append(" 		<OBSERVACAO>" + observacao + "</OBSERVACAO>");
		if (idiproc != null) {
			xmlRequestBody.append(" 	<IDIPROC>" + idiproc + "</IDIPROC> ");
		} else {
			xmlRequestBody.append(" 	<IDIPROC/> ");
		}

		if (codVend != null) {
			xmlRequestBody.append(" 	<CODVEND>" + codVend + "</CODVEND> ");
		} else {
			xmlRequestBody.append(" 	<CODVEND/> ");
		}

		if (nuNota != null) {
			xmlRequestBody.append(" 	<NUNOTA>" + nuNota + "</NUNOTA> ");
		} else {
			xmlRequestBody.append(" 	<NUNOTA></NUNOTA> ");
			xmlRequestBody.append(" 	<CODMOEDA/> ");
			xmlRequestBody.append(" 	<NUMPEDIDO2/> ");
			xmlRequestBody.append(" 	<AD_AVAL_VENDEDOR/> ");
			xmlRequestBody.append(" 	<AD_OBSERVACAOPEDIDO/> ");
			xmlRequestBody.append(" 	<AD_TIPOPEDIDO/> ");
			xmlRequestBody.append(" 	<VLRMOEDA/> ");
			xmlRequestBody.append(" 	<CODUSUCOMPRADOR/> ");
			xmlRequestBody.append(" 	<STATUSCTE/> ");
			xmlRequestBody.append(" 	<CODHISTAC/> ");
			xmlRequestBody.append(" 	<CODCIDENTREGA/> ");
			xmlRequestBody.append(" 	<CODVENDTEC/> ");
			xmlRequestBody.append(" 	<DTENVIOPMB/> ");
			xmlRequestBody.append(" 	<CODUSU/> ");
			xmlRequestBody.append(" 	<HRENTSAI/> ");
			xmlRequestBody.append(" 	<VLRCOMPENSACAO/> ");
			xmlRequestBody.append(" 	<BASEPIS/> ");
			xmlRequestBody.append("	    <VLRICMS/> ");
			xmlRequestBody.append(" 	<CODOBSPADRAO/> ");
			xmlRequestBody.append(" 	<REGESPTRIBUT/> ");
			xmlRequestBody.append(" 	<BASESUBSTIT/> ");
			xmlRequestBody.append(" 	<VOLUME/> ");
			xmlRequestBody.append(" 	<VLRCOFINS/> ");
			xmlRequestBody.append(" 	<VLRSUBST/> ");
			xmlRequestBody.append(" 	<M3AENTREGAR/> ");
			xmlRequestBody.append(" 	<NUMCF/> ");
			xmlRequestBody.append(" 	<LOCEMBARQ/> ");
			xmlRequestBody.append(" 	<UFEMBARQ/> ");
			xmlRequestBody.append(" 	<VLRSTEXTRANOTATOT/> ");
			xmlRequestBody.append(" 	<REBOQUE1/> ");
			xmlRequestBody.append(" 	<VLRAFRMM/> ");
			xmlRequestBody.append(" 	<CNPJADQUIRENTE/> ");
			xmlRequestBody.append(" 	<PESOBRUTOMANUAL/> ");
			xmlRequestBody.append(" 	<ORDEMCARGA/> ");
			xmlRequestBody.append(" 	<DTCONTAB/> ");
			xmlRequestBody.append(" 	<NUPEDFRETE/> ");
			xmlRequestBody.append(" 	<CODMODDOCNOTA/> ");
			xmlRequestBody.append(" 	<NUPCA/> ");
			xmlRequestBody.append(" 	<TIPOPTAGJNFE/> ");
			xmlRequestBody.append(" 	<RETORNADOAC/> ");
			xmlRequestBody.append(" 	<NURD8/> ");
			xmlRequestBody.append(" 	<CODPARCCONSIGNATARIO/> ");
			xmlRequestBody.append(" 	<NULOTENFSE/> ");
			xmlRequestBody.append(" 	<VLRICMSFCP/> ");
			xmlRequestBody.append(" 	<DTREF3/> ");
			xmlRequestBody.append(" 	<DHPROTOC/> ");
			xmlRequestBody.append(" 	<STATUSCONFERENCIA/> ");
			xmlRequestBody.append(" 	<NOTAEMPENHO/> ");
			xmlRequestBody.append(" 	<PERMALTERCENTRAL/> ");
			xmlRequestBody.append(" 	<PEDIDOIMPRESSO/> ");
			xmlRequestBody.append(" 	<VLRICMSFCPINT/> ");
			xmlRequestBody.append(" 	<BASEISS/> ");
			xmlRequestBody.append(" 	<DTREF2/> ");
			xmlRequestBody.append(" 	<NUFOP/> ");
			xmlRequestBody.append(" 	<VLRTOTLIQITEMMOE/> ");
			xmlRequestBody.append(" 	<VLRDESCTOTITEMMOE/> ");
			xmlRequestBody.append(" 	<BASEICMSFRETE/> ");
			xmlRequestBody.append(" 	<PESOAENTREGAR/> ");
			xmlRequestBody.append(" 	<TERMACORDNOTA/> ");
			xmlRequestBody.append(" 	<VLRIPI/> ");
			xmlRequestBody.append(" 	<KMVEICULO/> ");
			xmlRequestBody.append(" 	<NUMNFSE/> ");
			xmlRequestBody.append(" 	<CLASSIFICMS/> ");
			xmlRequestBody.append(" 	<VLRISS/> ");
			xmlRequestBody.append(" 	<NULOTECTE/> ");
			xmlRequestBody.append(" 	<DANFE/> ");
			xmlRequestBody.append(" 	<TIPNOTAPMB/> ");
			xmlRequestBody.append(" 	<NUCFR/> ");
			xmlRequestBody.append(" 	<CODPARCREDESPACHO/> ");
			xmlRequestBody.append(" 	<IDNAVIO/> ");
			xmlRequestBody.append(" 	<CODTPD/> ");
			xmlRequestBody.append(" 	<VLRCARGAAVERB/> ");
			xmlRequestBody.append(" 	<CODCIDFIMCTE/> ");
			xmlRequestBody.append(" 	<DTENTSAIINFO/> ");
			xmlRequestBody.append(" 	<VLRINDENIZ/> ");
			xmlRequestBody.append(" 	<TPEMISNFE/> ");
			xmlRequestBody.append(" 	<DHREGDPEC/> ");
			xmlRequestBody.append(" 	<VLRPISST/> ");
			xmlRequestBody.append(" 	<IPIEMB/> ");
			xmlRequestBody.append(" 	<BASECOFINSST/> ");
			xmlRequestBody.append(" 	<VLRLIQITEMNFE/> ");
			xmlRequestBody.append(" 	<CODLOCALORIG/> ");
			xmlRequestBody.append(" 	<CONFIRMNOTAFAT/> ");
			xmlRequestBody.append(" 	<TPAMBNFE/> ");
			xmlRequestBody.append(" 	<VLRICMSEMB/> ");
			xmlRequestBody.append(" 	<PESOBRUTOITENS/> ");
			xmlRequestBody.append(" 	<ICMSFRETE/> ");
			xmlRequestBody.append(" 	<CODPARCREMETENTE/> ");
			xmlRequestBody.append(" 	<VENCFRETE/> ");
			xmlRequestBody.append(" 	<DTMOV/> ");
			xmlRequestBody.append(" 	<DTALTER/> ");
			xmlRequestBody.append(" 	<CODCIDINICTE/> ");
			xmlRequestBody.append(" 	<OBSERVACAOAC/> ");
			xmlRequestBody.append(" 	<STATUSWMS/> ");
			xmlRequestBody.append(" 	<TIPPROCIMP/> ");
			xmlRequestBody.append(" 	<STATUSNOTA/> ");
			xmlRequestBody.append(" 	<DTENVSUF/> ");
			xmlRequestBody.append(" 	<NUODP/> ");
			xmlRequestBody.append(" 	<NUMERACAOVOLUMES/> ");
			xmlRequestBody.append(" 	<STATUSNFE/> ");
			xmlRequestBody.append(" 	<CPFCNPJADQUIRENTE/> ");
			xmlRequestBody.append(" 	<NUMCOTACAO/> ");
			xmlRequestBody.append(" 	<BASEIPI/> ");
			xmlRequestBody.append(" 	<DTREMRET/> ");
			xmlRequestBody.append(" 	<CODVTP/> ");
			xmlRequestBody.append(" 	<COMGER/> ");
			xmlRequestBody.append(" 	<TIPLIBERACAO/> ");
			xmlRequestBody.append(" 	<VLRINDENIZDIST/> ");
			xmlRequestBody.append(" 	<VLRICMSDIFALDEST/> ");
			xmlRequestBody.append(" 	<NUNOTAPEDFRET/> ");
			xmlRequestBody.append(" 	<IDBALSA03/> ");
			xmlRequestBody.append(" 	<TPEMISNFSE/> ");
			xmlRequestBody.append(" 	<CTELOTACAO/> ");
			xmlRequestBody.append(" 	<LACRES/> ");
			xmlRequestBody.append(" 	<VLRSTFCPINTANT/> ");
			xmlRequestBody.append(" 	<MODENTREGA/> ");
			xmlRequestBody.append(" 	<LIBCONF/> ");
			xmlRequestBody.append(" 	<DTVAL/> ");
			xmlRequestBody.append(" 	<CTEGLOBAL/> ");
			xmlRequestBody.append(" 	<IDBALSA02/> ");
			xmlRequestBody.append(" 	<FRETEVLRPAGO/> ");
			xmlRequestBody.append(" 	<VENCIPI/> ");
			xmlRequestBody.append(" 	<DTADIAM/> ");
			xmlRequestBody.append(" 	<CODCC/> ");
			xmlRequestBody.append(" 	<PRODPRED/> ");
			xmlRequestBody.append(" 	<BASEIRF/> ");
			xmlRequestBody.append(" 	<REBOQUE2/> ");
			xmlRequestBody.append(" 	<TPEMISCTE/> ");
			xmlRequestBody.append(" 	<CODEMPNEGOC/> ");
			xmlRequestBody.append(" 	<NULOTENFE/> ");
			xmlRequestBody.append(" 	<CODMAQ/> ");
			xmlRequestBody.append(" 	<VLRICMSSEG/> ");
			xmlRequestBody.append(" 	<TPAMBCTE/> ");
			xmlRequestBody.append(" 	<VLRFRETECALC/> ");
			xmlRequestBody.append(" 	<TPASSINANTE/> ");
			xmlRequestBody.append(" 	<CODRASTREAMENTOECT/> ");
			xmlRequestBody.append(" 	<RATEADO/> ");
			xmlRequestBody.append(" 	<CHAVENFSE/> ");
			xmlRequestBody.append(" 	<VLRFRETETOTAL/> ");
			xmlRequestBody.append(" 	<AGRUPFINNOTA/> ");
			xmlRequestBody.append(" 	<ALIQIRF/> ");
			xmlRequestBody.append(" 	<CODUSUINC/> ");
			xmlRequestBody.append(" 	<VLRVENDOR/> ");
			xmlRequestBody.append(" 	<VLRJURODIST/> ");
			xmlRequestBody.append(" 	<CODCONTATOENTREGA/> ");
			xmlRequestBody.append(" 	<CODCIDDESTINO/> ");
			xmlRequestBody.append(" 	<LOTACAO/> ");
			xmlRequestBody.append(" 	<BASEINSS/> ");
			xmlRequestBody.append(" 	<SITUACAOWMS/> ");
			xmlRequestBody.append(" 	<CODCID/> ");
			xmlRequestBody.append(" 	<ANTT/> ");
			xmlRequestBody.append(" 	<BASESUBSTSEMRED/> ");
			xmlRequestBody.append(" 	<TIPOCTE/> ");
			xmlRequestBody.append(" 	<VIATRANSP/> ");
			xmlRequestBody.append(" 	<DTENTSAI/> ");
			xmlRequestBody.append(" 	<VLREMB/> ");
			xmlRequestBody.append(" 	<LOCALCOLETA/> ");
			xmlRequestBody.append(" 	<VLRPIS/> ");
			xmlRequestBody.append(" 	<VLRNOTA/> ");
			xmlRequestBody.append(" 	<VLRDESTAQUE/> ");
			xmlRequestBody.append(" 	<PERCDESCFOB/> ");
			xmlRequestBody.append(" 	<CIOT/> ");
			xmlRequestBody.append(" 	<CODUFDESTINO/> ");
			xmlRequestBody.append(" 	<DTREF/> ");
			xmlRequestBody.append(" 	<CODDOCA/> ");
			xmlRequestBody.append(" 	<VLROUTROS/> ");
			xmlRequestBody.append(" 	<PESOLIQUIMANUAL/> ");
			xmlRequestBody.append(" 	<VLRJURO/> ");
			xmlRequestBody.append(" 	<PERCDESC/> ");
			xmlRequestBody.append(" 	<VLRDESCSERV/> ");
			xmlRequestBody.append(" 	<CODPARCTRANSPFINAL/> ");
			xmlRequestBody.append(" 	<PLACA/> ");
			xmlRequestBody.append(" 	<LIBPENDENTE/> ");
			xmlRequestBody.append(" 	<CODOBRA/> ");
			xmlRequestBody.append(" 	<TOTALCUSTOSERV/> ");
			xmlRequestBody.append(" 	<CODGRUPOTENSAO/> ");
			xmlRequestBody.append(" 	<NUMCONTRATO/> ");
			xmlRequestBody.append(" 	<NUREM/> ");
			xmlRequestBody.append(" 	<LOCALENTREGA/> ");
			xmlRequestBody.append(" 	<DESCRHISTAC/> ");
			xmlRequestBody.append(" 	<NROREDZ/> ");
			xmlRequestBody.append(" 	<CODART/> ");
			xmlRequestBody.append(" 	<PRODUETLOC/> ");
			xmlRequestBody.append(" 	<UFADQUIRENTE/> ");
			xmlRequestBody.append(" 	<VLRFRETECPL/> ");
			xmlRequestBody.append(" 	<CLASCONS/> ");
			xmlRequestBody.append(" 	<COMISSAO/> ");
			xmlRequestBody.append(" 	<VLRFRETE/> ");
			xmlRequestBody.append(" 	<VLRREPREDTOTSEMDESC/> ");
			xmlRequestBody.append(" 	<REBOQUE3/> ");
			xmlRequestBody.append(" 	<BASECOFINS/> ");
			xmlRequestBody.append(" 	<VLRDESCTOTITEM/> ");
			xmlRequestBody.append(" 	<EXIGEISSQN/> ");
			xmlRequestBody.append(" 	<VLRIRF/> ");
			xmlRequestBody.append(" 	<VLRCOFINSST/> ");
			xmlRequestBody.append(" 	<IDBALSA01/> ");
			xmlRequestBody.append(" 	<SITUACAOCTE/> ");
			xmlRequestBody.append(" 	<DIGITAL/> ");
			xmlRequestBody.append(" 	<HRMOV/> ");
			xmlRequestBody.append(" 	<CODVEICULO/> ");
			xmlRequestBody.append(" 	<TIPSERVCTE/> ");
			xmlRequestBody.append(" 	<VLRSEG/> ");
			xmlRequestBody.append(" 	<NUMPROTOCCTE/> ");
			xmlRequestBody.append(" 	<DIRECAOVIAG/> ");
			xmlRequestBody.append(" 	<CODCIDORIGEM/> ");
			xmlRequestBody.append(" 	<CODGUF/> ");
			xmlRequestBody.append(" 	<VLRINSS/> ");
			xmlRequestBody.append(" 	<VLRDESCTOT/> ");
			xmlRequestBody.append(" 	<NUTRANSF/> ");
			xmlRequestBody.append(" 	<CODPARCTRANSP/> ");
			xmlRequestBody.append(" 	<HRADIAM/> ");
			xmlRequestBody.append(" 	<SERIENFDES/> ");
			xmlRequestBody.append(" 	<FUSOEMISSEPEC/> ");
			xmlRequestBody.append(" 	<MODELONFDES/> ");
			xmlRequestBody.append(" 	<TROCO/> ");
			xmlRequestBody.append(" 	<SITESPECIALRESP/> ");
			xmlRequestBody.append(" 	<NUMALEATORIO/> ");
			xmlRequestBody.append(" 	<VLRMERCADORIA/> ");
			xmlRequestBody.append(" 	<NUMREGDPEC/> ");
			xmlRequestBody.append(" 	<NROCAIXA/> ");
			xmlRequestBody.append(" 	<TOTDISPDESC/> ");
			xmlRequestBody.append(" 	<DHEMISSEPEC/> ");
			xmlRequestBody.append(" 	<NUCONFATUAL/> ");
			xmlRequestBody.append(" 	<NUNOTASUB/> ");
			xmlRequestBody.append(" 	<DESCTERMACORD/> ");
			xmlRequestBody.append(" 	<TPLIGACAO/> ");
			xmlRequestBody.append(" 	<MOTNAORETERISSQN/> ");
			xmlRequestBody.append(" 	<VLRICMSDIFALREM/> ");
			xmlRequestBody.append(" 	<TOTALCUSTOPROD/> ");
			xmlRequestBody.append(" 	<APROVADO/> ");
			xmlRequestBody.append(" 	<CHAVECTE/> ");
			xmlRequestBody.append(" 	<DTDECLARA/> ");
			xmlRequestBody.append(" 	<CODFUNC/> ");
			xmlRequestBody.append(" 	<IRINNAVIO/> ");
			xmlRequestBody.append(" 	<PENDENTE/> ");
			xmlRequestBody.append(" 	<NUMOS/> ");
			xmlRequestBody.append(" 	<VLRPRESTAFRMM/> ");
			xmlRequestBody.append(" 	<CODUFENTREGA/> ");
			xmlRequestBody.append(" 	<CODCIDPREST/> ");
			xmlRequestBody.append(" 	<TPAMBNFSE/> ");
			xmlRequestBody.append(" 	<NATUREZAOPERDES/> ");
			xmlRequestBody.append(" 	<UFVEICULO/> ");
			xmlRequestBody.append(" 	<CODCONTATO/> ");
			xmlRequestBody.append(" 	<SEQCARGA/> ");
			xmlRequestBody.append(" 	<DHPROTOCCTE/> ");
			xmlRequestBody.append(" 	<MARCA/> ");
			xmlRequestBody.append(" 	<PESOLIQITENS/> ");
			xmlRequestBody.append(" 	<NUMALEATORIOCTE/> ");
			xmlRequestBody.append(" 	<M3/> ");
			xmlRequestBody.append(" 	<NOTASCF/> ");
			xmlRequestBody.append(" 	<FORMPGTCTE/> ");
			xmlRequestBody.append(" 	<STATUSNFSE/> ");
			xmlRequestBody.append(" 	<CODUFORIGEM/> ");
			xmlRequestBody.append(" 	<NUMPROTOC/> ");
			xmlRequestBody.append(" 	<OCCN48/> ");
			xmlRequestBody.append(" 	<VLRREPREDTOT/> ");
			xmlRequestBody.append(" 	<CODSITE/> ");
			xmlRequestBody.append(" 	<CODPROJ/> ");
			xmlRequestBody.append(" 	<DTFATUR/> ");
			xmlRequestBody.append(" 	<NFEDEVRECUSA/> ");
			xmlRequestBody.append(" 	<VLRSTFCPINT/> ");
			xmlRequestBody.append(" 	<BASEICMS/> ");
			xmlRequestBody.append(" 	<VLRROYALT/> ");
			xmlRequestBody.append(" 	<CODMOTORISTA/> ");
			xmlRequestBody.append(" 	<CONFIRMADA/> ");
			xmlRequestBody.append(" 	<BASEPISST/> ");
			xmlRequestBody.append(" 	<NOMEADQUIRENTE/> ");
		}
		xmlRequestBody.append("		</cabecalho> ");
		xmlRequestBody.append("		<txProperties>");
		xmlRequestBody
				.append("         		<prop name='br.com.sankhya.mgecom.gradeItens.pedidoWeb' value='false'/> ");
		xmlRequestBody
				.append(" 	    		<prop name='br.com.sankhya.mgefin.recalculo.custopreco.Automatico' value='false'/> ");
		xmlRequestBody
				.append(" 	    		<prop name='br.com.sankhya.mgefin.checarfinanceiro.VlrEntrada' value='0'/> ");
		xmlRequestBody
				.append(" 	    		<prop name='br.com.sankhya.mgefin.checarfinanceiro.RecalcularVencimento' value='false'/> ");
		xmlRequestBody
				.append(" 	    		<prop name='cabecalhoNota.inserindo.pedidoWeb' value='false'/> ");
		xmlRequestBody.append("		</txProperties> ");
		xmlRequestBody.append("	</nota> ");
		xmlRequestBody.append("	<clientEventList> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.central.itens.VendaCasada</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.parcelas.financeiro</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.central.itens.KitRevenda.msgValidaFormula</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecomercial.event.compensacao.credito.debito</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecomercial.event.baixaPortal</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecomercial.event.cadastrarDistancia</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgefin.event.fixa.vencimento</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.imobilizado</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgefin.solicitacao.liberacao.orcamento</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.exibir.variacao.valor.item</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.cancelamento.notas.remessa</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecomercial.event.estoque.componentes</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.nota.adicional.SolicitarUsuarioGerente</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.event.troca.item.por.produto.alternativo</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.central.itens.KitRevenda</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.utiliza.dtneg.servidor</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>central.save.grade.itens.mostrar.popup.info.lote</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.expedicao.SolicitarUsuarioConferente</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.valida.ChaveNFeCompraTerceiros</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.actionbutton.clientconfirm</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.compra.SolicitacaoComprador</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.checkout.obter.peso</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecomercial.event.estoque.insuficiente.produto</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.importacaoxml.cfi.para.produto</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.event.troca.item.por.produto.substituto</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecomercial.event.faturamento.confirmacao</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.exibe.msg.variacao.preco</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.coleta.entrega.recalculado</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>central.save.grade.itens.mostrar.popup.serie</clientEvent> ");
		xmlRequestBody.append("	</clientEventList> ");

		// Chamada ao servi�o da Sankhya
		Document document = serviceInvoker.call(
				"CACSP.incluirAlterarCabecalhoNota", "mgecom",
				xmlRequestBody.toString());

		Node item = document.getChildNodes().item(0);

		if ("1".equals(item.getAttributes().getNamedItem("status")
				.getTextContent())) {
			return document.getElementsByTagName("NUNOTA").item(0)
					.getTextContent();

		} else {
			return XMLUtil.xmlToString(document);
		}
	}

	/*
	 * A ação de criar os documentos relacionados
	 */
	public String saveCabecalhoNota(Long nuNota, String numNota, String codEmp,
			String codTipoOper, String dhTipOper, String tipoMovimento,
			String codParc, String codParcDest, String codTipVenda,
			String dhTipVenda, String codCencus, String codNat,
			String serieNota, String chaveNFERef, Long qtdVol,
			BigDecimal pesoLiq, BigDecimal pesoBruto, String observacao,
			Long idiproc, String codVend, BigDecimal codFixacao,
			BigDecimal vlrMoeda, BigDecimal numContrato, int codMoeda)
			throws Exception {
		DecimalFormat decimalFormat = new DecimalFormat();
		decimalFormat.setMaximumFractionDigits(2);
		decimalFormat.setMinimumFractionDigits(2);

		SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
		SimpleDateFormat formatter2 = new SimpleDateFormat(
				"dd/MM/yyyy hh:mm:ss");
		Date dhNeg = new Date();

		// Parceiro Entrega
		// String codParcDest = "0";
		// String numNota = "0";
		String dtNeg = formatter.format(dhNeg) + " 0:00:0";

		String tipFrete = "N";
		String cifFob = "F";
		if ("6".equals(codEmp) || "8".equals(codEmp)) {
			cifFob = "C";
		}

		// Empresa
		String codEmpFunc = codEmp;

		// A definir posteriormente
		String tipIPIEmb = "N";
		String indPresNfce = "0";
		String issRetido = "N";
		String irfRetido = "S";

		// Composicao da consulta
		StringBuilder xmlRequestBody = new StringBuilder();
		xmlRequestBody.append(" <nota ownerServiceCall='CentralNotas'> ");
		xmlRequestBody.append(" 	<cabecalho> ");
		if (!"677".equals(codTipoOper)) {
			xmlRequestBody.append(" 	<NUMNOTA>" + numNota + "</NUMNOTA>");
		}

		xmlRequestBody.append(" 	    <TIPMOV>" + tipoMovimento + "</TIPMOV>");
		xmlRequestBody.append(" 	    <CODTIPOPER>" + codTipoOper
				+ "</CODTIPOPER>");
		xmlRequestBody.append("			<DHTIPOPER>" + dhTipOper + "</DHTIPOPER> ");
		xmlRequestBody.append("			<CODPARC>" + codParc + "</CODPARC> ");
		if (codParcDest != null) {
			xmlRequestBody.append(" 		<CODPARCDEST>" + codParcDest
					+ "</CODPARCDEST>");
		} else {
			xmlRequestBody.append(" <CODPARCDEST></CODPARCDEST>");
		}
		xmlRequestBody.append(" 	    <CODTIPVENDA>" + codTipVenda
				+ "</CODTIPVENDA>");
		xmlRequestBody.append(" 	    <CODEMP>" + codEmp + "</CODEMP> ");
		xmlRequestBody.append(" 	    <DTNEG>" + dtNeg + "</DTNEG>");
		xmlRequestBody.append(" 	    <CODEMPFUNC>" + codEmpFunc
				+ "</CODEMPFUNC>");
		xmlRequestBody.append(" 	    <CIF_FOB>" + cifFob + "</CIF_FOB> ");
		xmlRequestBody.append(" 	    <TIPIPIEMB>" + tipIPIEmb + "</TIPIPIEMB>");
		xmlRequestBody.append(" 	    <TIPFRETE>" + tipFrete + "</TIPFRETE>");
		xmlRequestBody.append(" 	    <INDPRESNFCE>" + indPresNfce
				+ "</INDPRESNFCE>");
		xmlRequestBody.append(" 	    		<ISSRETIDO>" + issRetido
				+ "</ISSRETIDO> ");
		xmlRequestBody.append(" 	    		<IRFRETIDO>" + irfRetido
				+ "</IRFRETIDO> ");
		xmlRequestBody.append("					<DHTIPVENDA>" + dhTipVenda
				+ "</DHTIPVENDA> ");
		xmlRequestBody.append(" 		<CODCENCUS>" + codCencus + "</CODCENCUS>");
		xmlRequestBody.append(" 		<CODNAT>" + codNat + "</CODNAT>");
		xmlRequestBody.append(" 		<DTPREVENT>" + dtNeg + "</DTPREVENT>");
		xmlRequestBody.append(" 		<SERIENOTA>" + serieNota + "</SERIENOTA>");
		if ("332".equals(codTipoOper) || "380".equals(codTipoOper)
				|| "339".equals(codTipoOper) || "813".equals(codTipoOper)
				|| "307".equals(codTipoOper) || "335".equals(codTipoOper)) {
			xmlRequestBody.append(" 	<CHAVENFE>" + chaveNFERef + "</CHAVENFE>");
		} else {
			xmlRequestBody.append(" 	<CHAVENFEREF></CHAVENFEREF>");
		}

		xmlRequestBody.append(" 		<QTDVOL>" + qtdVol + "</QTDVOL> ");
		xmlRequestBody.append(" 		<PESO>" + pesoLiq + "</PESO>");
		xmlRequestBody.append(" 		<PESOBRUTO>" + pesoBruto + "</PESOBRUTO>");
		if (observacao != null) {
			xmlRequestBody.append(" 		<OBSERVACAO>" + observacao
					+ "</OBSERVACAO>");
		} else {
			xmlRequestBody.append(" 		<OBSERVACAO></OBSERVACAO>");
		}

		if (idiproc != null) {
			xmlRequestBody.append(" 	<IDIPROC>" + idiproc + "</IDIPROC> ");
		} else {
			xmlRequestBody.append(" 	<IDIPROC/> ");
		}

		if (codVend != null) {
			xmlRequestBody.append(" 	<CODVEND>" + codVend + "</CODVEND> ");
		} else {
			xmlRequestBody.append(" 	<CODVEND/> ");
		}

		if (vlrMoeda != null)
			xmlRequestBody.append(" <VLRMOEDA>" + vlrMoeda + "</VLRMOEDA> ");
		else
			xmlRequestBody.append(" 	<VLRMOEDA/> ");

		if (codFixacao != null)
			xmlRequestBody.append(" <AD_CODFIXAMOED>" + codFixacao
					+ "</AD_CODFIXAMOED> ");
		else
			xmlRequestBody.append(" 	<AD_CODFIXAMOED/> ");

		if (numContrato != null)
			xmlRequestBody.append(" <NUMCONTRATO>" + numContrato
					+ "</NUMCONTRATO> ");
		else
			xmlRequestBody.append(" 	<NUMCONTRATO/> ");

		if (numContrato != null)
			xmlRequestBody.append(" <CODMOEDA>" + codMoeda + "</CODMOEDA> ");
		else
			xmlRequestBody.append(" 	<CODMOEDA/> ");

		if (nuNota != null) {
			xmlRequestBody.append(" 	<NUNOTA>" + nuNota + "</NUNOTA> ");
		} else {
			xmlRequestBody.append(" 	<NUNOTA></NUNOTA> ");
			xmlRequestBody.append(" 	<NUMPEDIDO2/> ");
			xmlRequestBody.append(" 	<AD_AVAL_VENDEDOR/> ");
			xmlRequestBody.append(" 	<AD_OBSERVACAOPEDIDO/> ");
			xmlRequestBody.append(" 	<AD_TIPOPEDIDO/> ");
			xmlRequestBody.append(" 	<CODUSUCOMPRADOR/> ");
			xmlRequestBody.append(" 	<STATUSCTE/> ");
			xmlRequestBody.append(" 	<CODHISTAC/> ");
			xmlRequestBody.append(" 	<CODCIDENTREGA/> ");
			xmlRequestBody.append(" 	<CODVENDTEC/> ");
			xmlRequestBody.append(" 	<DTENVIOPMB/> ");
			xmlRequestBody.append(" 	<CODUSU/> ");
			xmlRequestBody.append(" 	<HRENTSAI/> ");
			xmlRequestBody.append(" 	<VLRCOMPENSACAO/> ");
			xmlRequestBody.append(" 	<BASEPIS/> ");
			xmlRequestBody.append("	    <VLRICMS/> ");
			xmlRequestBody.append(" 	<CODOBSPADRAO/> ");
			xmlRequestBody.append(" 	<REGESPTRIBUT/> ");
			xmlRequestBody.append(" 	<BASESUBSTIT/> ");
			xmlRequestBody.append(" 	<VOLUME/> ");
			xmlRequestBody.append(" 	<VLRCOFINS/> ");
			xmlRequestBody.append(" 	<VLRSUBST/> ");
			xmlRequestBody.append(" 	<M3AENTREGAR/> ");
			xmlRequestBody.append(" 	<NUMCF/> ");
			xmlRequestBody.append(" 	<LOCEMBARQ/> ");
			xmlRequestBody.append(" 	<UFEMBARQ/> ");
			xmlRequestBody.append(" 	<VLRSTEXTRANOTATOT/> ");
			xmlRequestBody.append(" 	<REBOQUE1/> ");
			xmlRequestBody.append(" 	<VLRAFRMM/> ");
			xmlRequestBody.append(" 	<CNPJADQUIRENTE/> ");
			xmlRequestBody.append(" 	<PESOBRUTOMANUAL/> ");
			xmlRequestBody.append(" 	<ORDEMCARGA/> ");
			xmlRequestBody.append(" 	<DTCONTAB/> ");
			xmlRequestBody.append(" 	<NUPEDFRETE/> ");
			xmlRequestBody.append(" 	<CODMODDOCNOTA/> ");
			xmlRequestBody.append(" 	<NUPCA/> ");
			xmlRequestBody.append(" 	<TIPOPTAGJNFE/> ");
			xmlRequestBody.append(" 	<RETORNADOAC/> ");
			xmlRequestBody.append(" 	<NURD8/> ");
			xmlRequestBody.append(" 	<CODPARCCONSIGNATARIO/> ");
			xmlRequestBody.append(" 	<NULOTENFSE/> ");
			xmlRequestBody.append(" 	<VLRICMSFCP/> ");
			xmlRequestBody.append(" 	<DTREF3/> ");
			xmlRequestBody.append(" 	<DHPROTOC/> ");
			xmlRequestBody.append(" 	<STATUSCONFERENCIA/> ");
			xmlRequestBody.append(" 	<NOTAEMPENHO/> ");
			xmlRequestBody.append(" 	<PERMALTERCENTRAL/> ");
			xmlRequestBody.append(" 	<PEDIDOIMPRESSO/> ");
			xmlRequestBody.append(" 	<VLRICMSFCPINT/> ");
			xmlRequestBody.append(" 	<BASEISS/> ");
			xmlRequestBody.append(" 	<DTREF2/> ");
			xmlRequestBody.append(" 	<NUFOP/> ");
			xmlRequestBody.append(" 	<VLRTOTLIQITEMMOE/> ");
			xmlRequestBody.append(" 	<VLRDESCTOTITEMMOE/> ");
			xmlRequestBody.append(" 	<BASEICMSFRETE/> ");
			xmlRequestBody.append(" 	<PESOAENTREGAR/> ");
			xmlRequestBody.append(" 	<TERMACORDNOTA/> ");
			xmlRequestBody.append(" 	<VLRIPI/> ");
			xmlRequestBody.append(" 	<KMVEICULO/> ");
			xmlRequestBody.append(" 	<NUMNFSE/> ");
			xmlRequestBody.append(" 	<CLASSIFICMS/> ");
			xmlRequestBody.append(" 	<VLRISS/> ");
			xmlRequestBody.append(" 	<NULOTECTE/> ");
			xmlRequestBody.append(" 	<DANFE/> ");
			xmlRequestBody.append(" 	<TIPNOTAPMB/> ");
			xmlRequestBody.append(" 	<NUCFR/> ");
			xmlRequestBody.append(" 	<CODPARCREDESPACHO/> ");
			xmlRequestBody.append(" 	<IDNAVIO/> ");
			xmlRequestBody.append(" 	<CODTPD/> ");
			xmlRequestBody.append(" 	<VLRCARGAAVERB/> ");
			xmlRequestBody.append(" 	<CODCIDFIMCTE/> ");
			xmlRequestBody.append(" 	<DTENTSAIINFO/> ");
			xmlRequestBody.append(" 	<VLRINDENIZ/> ");
			xmlRequestBody.append(" 	<TPEMISNFE/> ");
			xmlRequestBody.append(" 	<DHREGDPEC/> ");
			xmlRequestBody.append(" 	<VLRPISST/> ");
			xmlRequestBody.append(" 	<IPIEMB/> ");
			xmlRequestBody.append(" 	<BASECOFINSST/> ");
			xmlRequestBody.append(" 	<VLRLIQITEMNFE/> ");
			xmlRequestBody.append(" 	<CODLOCALORIG/> ");
			xmlRequestBody.append(" 	<CONFIRMNOTAFAT/> ");
			xmlRequestBody.append(" 	<TPAMBNFE/> ");
			xmlRequestBody.append(" 	<VLRICMSEMB/> ");
			xmlRequestBody.append(" 	<PESOBRUTOITENS/> ");
			xmlRequestBody.append(" 	<ICMSFRETE/> ");
			xmlRequestBody.append(" 	<CODPARCREMETENTE/> ");
			xmlRequestBody.append(" 	<VENCFRETE/> ");
			xmlRequestBody.append(" 	<DTMOV/> ");
			xmlRequestBody.append(" 	<DTALTER/> ");
			xmlRequestBody.append(" 	<CODCIDINICTE/> ");
			xmlRequestBody.append(" 	<OBSERVACAOAC/> ");
			xmlRequestBody.append(" 	<STATUSWMS/> ");
			xmlRequestBody.append(" 	<TIPPROCIMP/> ");
			xmlRequestBody.append(" 	<STATUSNOTA/> ");
			xmlRequestBody.append(" 	<DTENVSUF/> ");
			xmlRequestBody.append(" 	<NUODP/> ");
			xmlRequestBody.append(" 	<NUMERACAOVOLUMES/> ");
			xmlRequestBody.append(" 	<STATUSNFE/> ");
			xmlRequestBody.append(" 	<CPFCNPJADQUIRENTE/> ");
			xmlRequestBody.append(" 	<NUMCOTACAO/> ");
			xmlRequestBody.append(" 	<BASEIPI/> ");
			xmlRequestBody.append(" 	<DTREMRET/> ");
			xmlRequestBody.append(" 	<CODVTP/> ");
			xmlRequestBody.append(" 	<COMGER/> ");
			xmlRequestBody.append(" 	<TIPLIBERACAO/> ");
			xmlRequestBody.append(" 	<VLRINDENIZDIST/> ");
			xmlRequestBody.append(" 	<VLRICMSDIFALDEST/> ");
			xmlRequestBody.append(" 	<NUNOTAPEDFRET/> ");
			xmlRequestBody.append(" 	<IDBALSA03/> ");
			xmlRequestBody.append(" 	<TPEMISNFSE/> ");
			xmlRequestBody.append(" 	<CTELOTACAO/> ");
			xmlRequestBody.append(" 	<LACRES/> ");
			xmlRequestBody.append(" 	<VLRSTFCPINTANT/> ");
			xmlRequestBody.append(" 	<MODENTREGA/> ");
			xmlRequestBody.append(" 	<LIBCONF/> ");
			xmlRequestBody.append(" 	<DTVAL/> ");
			xmlRequestBody.append(" 	<CTEGLOBAL/> ");
			xmlRequestBody.append(" 	<IDBALSA02/> ");
			xmlRequestBody.append(" 	<FRETEVLRPAGO/> ");
			xmlRequestBody.append(" 	<VENCIPI/> ");
			xmlRequestBody.append(" 	<DTADIAM/> ");
			xmlRequestBody.append(" 	<CODCC/> ");
			xmlRequestBody.append(" 	<PRODPRED/> ");
			xmlRequestBody.append(" 	<BASEIRF/> ");
			xmlRequestBody.append(" 	<REBOQUE2/> ");
			xmlRequestBody.append(" 	<TPEMISCTE/> ");
			xmlRequestBody.append(" 	<CODEMPNEGOC/> ");
			xmlRequestBody.append(" 	<NULOTENFE/> ");
			xmlRequestBody.append(" 	<CODMAQ/> ");
			xmlRequestBody.append(" 	<VLRICMSSEG/> ");
			xmlRequestBody.append(" 	<TPAMBCTE/> ");
			xmlRequestBody.append(" 	<VLRFRETECALC/> ");
			xmlRequestBody.append(" 	<TPASSINANTE/> ");
			xmlRequestBody.append(" 	<CODRASTREAMENTOECT/> ");
			xmlRequestBody.append(" 	<RATEADO/> ");
			xmlRequestBody.append(" 	<CHAVENFSE/> ");
			xmlRequestBody.append(" 	<VLRFRETETOTAL/> ");
			xmlRequestBody.append(" 	<AGRUPFINNOTA/> ");
			xmlRequestBody.append(" 	<ALIQIRF/> ");
			xmlRequestBody.append(" 	<CODUSUINC/> ");
			xmlRequestBody.append(" 	<VLRVENDOR/> ");
			xmlRequestBody.append(" 	<VLRJURODIST/> ");
			xmlRequestBody.append(" 	<CODCONTATOENTREGA/> ");
			xmlRequestBody.append(" 	<CODCIDDESTINO/> ");
			xmlRequestBody.append(" 	<LOTACAO/> ");
			xmlRequestBody.append(" 	<BASEINSS/> ");
			xmlRequestBody.append(" 	<SITUACAOWMS/> ");
			xmlRequestBody.append(" 	<CODCID/> ");
			xmlRequestBody.append(" 	<ANTT/> ");
			xmlRequestBody.append(" 	<BASESUBSTSEMRED/> ");
			xmlRequestBody.append(" 	<TIPOCTE/> ");
			xmlRequestBody.append(" 	<VIATRANSP/> ");
			xmlRequestBody.append(" 	<DTENTSAI/> ");
			xmlRequestBody.append(" 	<VLREMB/> ");
			xmlRequestBody.append(" 	<LOCALCOLETA/> ");
			xmlRequestBody.append(" 	<VLRPIS/> ");
			xmlRequestBody.append(" 	<VLRNOTA/> ");
			xmlRequestBody.append(" 	<VLRDESTAQUE/> ");
			xmlRequestBody.append(" 	<PERCDESCFOB/> ");
			xmlRequestBody.append(" 	<CIOT/> ");
			xmlRequestBody.append(" 	<CODUFDESTINO/> ");
			xmlRequestBody.append(" 	<DTREF/> ");
			xmlRequestBody.append(" 	<CODDOCA/> ");
			xmlRequestBody.append(" 	<VLROUTROS/> ");
			xmlRequestBody.append(" 	<PESOLIQUIMANUAL/> ");
			xmlRequestBody.append(" 	<VLRJURO/> ");
			xmlRequestBody.append(" 	<PERCDESC/> ");
			xmlRequestBody.append(" 	<VLRDESCSERV/> ");
			xmlRequestBody.append(" 	<CODPARCTRANSPFINAL/> ");
			xmlRequestBody.append(" 	<PLACA/> ");
			xmlRequestBody.append(" 	<LIBPENDENTE/> ");
			xmlRequestBody.append(" 	<CODOBRA/> ");
			xmlRequestBody.append(" 	<TOTALCUSTOSERV/> ");
			xmlRequestBody.append(" 	<CODGRUPOTENSAO/> ");
			xmlRequestBody.append(" 	<NUREM/> ");
			xmlRequestBody.append(" 	<LOCALENTREGA/> ");
			xmlRequestBody.append(" 	<DESCRHISTAC/> ");
			xmlRequestBody.append(" 	<NROREDZ/> ");
			xmlRequestBody.append(" 	<CODART/> ");
			xmlRequestBody.append(" 	<PRODUETLOC/> ");
			xmlRequestBody.append(" 	<UFADQUIRENTE/> ");
			xmlRequestBody.append(" 	<VLRFRETECPL/> ");
			xmlRequestBody.append(" 	<CLASCONS/> ");
			xmlRequestBody.append(" 	<COMISSAO/> ");
			xmlRequestBody.append(" 	<VLRFRETE/> ");
			xmlRequestBody.append(" 	<VLRREPREDTOTSEMDESC/> ");
			xmlRequestBody.append(" 	<REBOQUE3/> ");
			xmlRequestBody.append(" 	<BASECOFINS/> ");
			xmlRequestBody.append(" 	<VLRDESCTOTITEM/> ");
			xmlRequestBody.append(" 	<EXIGEISSQN/> ");
			xmlRequestBody.append(" 	<VLRIRF/> ");
			xmlRequestBody.append(" 	<VLRCOFINSST/> ");
			xmlRequestBody.append(" 	<IDBALSA01/> ");
			xmlRequestBody.append(" 	<SITUACAOCTE/> ");
			xmlRequestBody.append(" 	<DIGITAL/> ");
			xmlRequestBody.append(" 	<HRMOV/> ");
			xmlRequestBody.append(" 	<CODVEICULO/> ");
			xmlRequestBody.append(" 	<TIPSERVCTE/> ");
			xmlRequestBody.append(" 	<VLRSEG/> ");
			xmlRequestBody.append(" 	<NUMPROTOCCTE/> ");
			xmlRequestBody.append(" 	<DIRECAOVIAG/> ");
			xmlRequestBody.append(" 	<CODCIDORIGEM/> ");
			xmlRequestBody.append(" 	<CODGUF/> ");
			xmlRequestBody.append(" 	<VLRINSS/> ");
			xmlRequestBody.append(" 	<VLRDESCTOT/> ");
			xmlRequestBody.append(" 	<NUTRANSF/> ");
			xmlRequestBody.append(" 	<CODPARCTRANSP/> ");
			xmlRequestBody.append(" 	<HRADIAM/> ");
			xmlRequestBody.append(" 	<SERIENFDES/> ");
			xmlRequestBody.append(" 	<FUSOEMISSEPEC/> ");
			xmlRequestBody.append(" 	<MODELONFDES/> ");
			xmlRequestBody.append(" 	<TROCO/> ");
			xmlRequestBody.append(" 	<SITESPECIALRESP/> ");
			xmlRequestBody.append(" 	<NUMALEATORIO/> ");
			xmlRequestBody.append(" 	<VLRMERCADORIA/> ");
			xmlRequestBody.append(" 	<NUMREGDPEC/> ");
			xmlRequestBody.append(" 	<NROCAIXA/> ");
			xmlRequestBody.append(" 	<TOTDISPDESC/> ");
			xmlRequestBody.append(" 	<DHEMISSEPEC/> ");
			xmlRequestBody.append(" 	<NUCONFATUAL/> ");
			xmlRequestBody.append(" 	<NUNOTASUB/> ");
			xmlRequestBody.append(" 	<DESCTERMACORD/> ");
			xmlRequestBody.append(" 	<TPLIGACAO/> ");
			xmlRequestBody.append(" 	<MOTNAORETERISSQN/> ");
			xmlRequestBody.append(" 	<VLRICMSDIFALREM/> ");
			xmlRequestBody.append(" 	<TOTALCUSTOPROD/> ");
			xmlRequestBody.append(" 	<APROVADO/> ");
			xmlRequestBody.append(" 	<CHAVECTE/> ");
			xmlRequestBody.append(" 	<DTDECLARA/> ");
			xmlRequestBody.append(" 	<CODFUNC/> ");
			xmlRequestBody.append(" 	<IRINNAVIO/> ");
			xmlRequestBody.append(" 	<PENDENTE/> ");
			xmlRequestBody.append(" 	<NUMOS/> ");
			xmlRequestBody.append(" 	<VLRPRESTAFRMM/> ");
			xmlRequestBody.append(" 	<CODUFENTREGA/> ");
			xmlRequestBody.append(" 	<CODCIDPREST/> ");
			xmlRequestBody.append(" 	<TPAMBNFSE/> ");
			xmlRequestBody.append(" 	<NATUREZAOPERDES/> ");
			xmlRequestBody.append(" 	<UFVEICULO/> ");
			xmlRequestBody.append(" 	<CODCONTATO/> ");
			xmlRequestBody.append(" 	<SEQCARGA/> ");
			xmlRequestBody.append(" 	<DHPROTOCCTE/> ");
			xmlRequestBody.append(" 	<MARCA/> ");
			xmlRequestBody.append(" 	<PESOLIQITENS/> ");
			xmlRequestBody.append(" 	<NUMALEATORIOCTE/> ");
			xmlRequestBody.append(" 	<M3/> ");
			xmlRequestBody.append(" 	<NOTASCF/> ");
			xmlRequestBody.append(" 	<FORMPGTCTE/> ");
			xmlRequestBody.append(" 	<STATUSNFSE/> ");
			xmlRequestBody.append(" 	<CODUFORIGEM/> ");
			xmlRequestBody.append(" 	<NUMPROTOC/> ");
			xmlRequestBody.append(" 	<OCCN48/> ");
			xmlRequestBody.append(" 	<VLRREPREDTOT/> ");
			xmlRequestBody.append(" 	<CODSITE/> ");
			xmlRequestBody.append(" 	<CODPROJ/> ");
			xmlRequestBody.append(" 	<DTFATUR/> ");
			xmlRequestBody.append(" 	<NFEDEVRECUSA/> ");
			xmlRequestBody.append(" 	<VLRSTFCPINT/> ");
			xmlRequestBody.append(" 	<BASEICMS/> ");
			xmlRequestBody.append(" 	<VLRROYALT/> ");
			xmlRequestBody.append(" 	<CODMOTORISTA/> ");
			xmlRequestBody.append(" 	<CONFIRMADA/> ");
			xmlRequestBody.append(" 	<BASEPISST/> ");
			xmlRequestBody.append(" 	<NOMEADQUIRENTE/> ");
		}
		xmlRequestBody.append("		</cabecalho> ");
		xmlRequestBody.append("		<txProperties>");
		xmlRequestBody
				.append("         		<prop name='br.com.sankhya.mgecom.gradeItens.pedidoWeb' value='false'/> ");
		xmlRequestBody
				.append(" 	    		<prop name='br.com.sankhya.mgefin.recalculo.custopreco.Automatico' value='false'/> ");
		xmlRequestBody
				.append(" 	    		<prop name='br.com.sankhya.mgefin.checarfinanceiro.VlrEntrada' value='0'/> ");
		xmlRequestBody
				.append(" 	    		<prop name='br.com.sankhya.mgefin.checarfinanceiro.RecalcularVencimento' value='false'/> ");
		xmlRequestBody
				.append(" 	    		<prop name='cabecalhoNota.inserindo.pedidoWeb' value='false'/> ");
		xmlRequestBody.append("		</txProperties> ");
		xmlRequestBody.append("	</nota> ");
		xmlRequestBody.append("	<clientEventList> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.central.itens.VendaCasada</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.parcelas.financeiro</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.central.itens.KitRevenda.msgValidaFormula</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecomercial.event.compensacao.credito.debito</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecomercial.event.baixaPortal</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecomercial.event.cadastrarDistancia</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgefin.event.fixa.vencimento</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.imobilizado</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgefin.solicitacao.liberacao.orcamento</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.exibir.variacao.valor.item</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.cancelamento.notas.remessa</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecomercial.event.estoque.componentes</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.nota.adicional.SolicitarUsuarioGerente</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.event.troca.item.por.produto.alternativo</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.central.itens.KitRevenda</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.utiliza.dtneg.servidor</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>central.save.grade.itens.mostrar.popup.info.lote</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.expedicao.SolicitarUsuarioConferente</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.valida.ChaveNFeCompraTerceiros</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.actionbutton.clientconfirm</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.compra.SolicitacaoComprador</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.checkout.obter.peso</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecomercial.event.estoque.insuficiente.produto</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.importacaoxml.cfi.para.produto</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.event.troca.item.por.produto.substituto</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecomercial.event.faturamento.confirmacao</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.exibe.msg.variacao.preco</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.coleta.entrega.recalculado</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>central.save.grade.itens.mostrar.popup.serie</clientEvent> ");
		xmlRequestBody.append("	</clientEventList> ");
		// Chamada ao servi�o da Sankhya
		Document document = serviceInvoker.call(
				"CACSP.incluirAlterarCabecalhoNota", "mgecom",
				xmlRequestBody.toString());

		Node item = document.getChildNodes().item(0);

		if ("1".equals(item.getAttributes().getNamedItem("status")
				.getTextContent())) {
			return document.getElementsByTagName("NUNOTA").item(0)
					.getTextContent();

		} else {
			return XMLUtil.xmlToString(document);
		}
	}

	public void saveVolumesPorItemNota(Long idCabecalho, Long sequencia,
			Long idAvo) throws Exception {
		// Composicao da consulta
		StringBuilder xmlRequestBody = new StringBuilder();
		xmlRequestBody
				.append("	<dataSet rootEntity='AD_TGFCABITEAVO' includePresentationFields='S' datasetid='1568382449933_1'> ");
		xmlRequestBody.append("		<entity path=''> ");
		xmlRequestBody.append("			<fieldset list='*'/> ");
		xmlRequestBody.append("		</entity> ");
		xmlRequestBody.append("		<dataRow> ");
		xmlRequestBody.append("			<localFields> ");
		xmlRequestBody.append("				<NUNOTA><![CDATA[" + idCabecalho
				+ "]]></NUNOTA> ");
		xmlRequestBody.append("				<ITESEQ><![CDATA[" + sequencia
				+ "]]></ITESEQ> ");
		xmlRequestBody.append("				<IDAVO><![CDATA[" + idAvo + "]]></IDAVO> ");
		xmlRequestBody.append("			</localFields> ");
		xmlRequestBody.append("		</dataRow> ");
		xmlRequestBody.append("	</dataSet> ");

		// Chamada ao servi�o da Sankhyassssss
		serviceInvoker.call("CRUDServiceProvider.saveRecord", "mge",
				xmlRequestBody.toString());

	}

	public void saveTgfCabIteAvo(int nuNota, int sequencia, int idAvo)
			throws Exception {
		Registro ad_tgfcabiteavo = contexto.novaLinha("AD_TGFCABITEAVO");
		ad_tgfcabiteavo.setCampo("NUNOTA", nuNota);
		ad_tgfcabiteavo.setCampo("ITESEQ", sequencia);
		ad_tgfcabiteavo.setCampo("IDAVO", idAvo);
		ad_tgfcabiteavo.save();
	}

	public void saveTprAvoMov(int nuNota, int codEmp, int codLocal, int idSta,
			String usuario, int idAvo, Long codLocalEnd) throws Exception {
		Registro ad_tpravomov = contexto.novaLinha("AD_TPRAVOMOV");
		ad_tpravomov.setCampo("ID", idAvo);
		ad_tpravomov.setCampo("SEQUENCIA", null);
		ad_tpravomov.setCampo("IDSTA", idSta);
		ad_tpravomov.setCampo("DATAMOV", new Date());
		ad_tpravomov.setCampo("CODEMP", codEmp);
		ad_tpravomov.setCampo("NUNOTA", nuNota);
		ad_tpravomov.setCampo("CODLOCAL", codLocal);
		if (codLocalEnd != null) {
			ad_tpravomov.setCampo("CODLOCEND", codLocalEnd);
		}
		ad_tpravomov.setCampo("USUARIOULTALTERACAO", usuario);
		ad_tpravomov.setCampo("DATAULTALTERACAO", new Date());
		ad_tpravomov.save();
	}

	public void atualizarVolumesNota(int nuNota) throws Exception {
		QueryExecutor queryEstoqueNotaItemAvo = contexto.getQuery();
		String sqlEstoqueNota = " 	SELECT 	COUNT(cia.IDAVO) TOTAL_VOLUMES, ";
		sqlEstoqueNota += "					SUM(avo.pesobruto) PESO_BRUTO, ";
		sqlEstoqueNota += "	  				SUM(avo.pesoliq) PESO_LIQ ";
		sqlEstoqueNota += "	 		FROM 	AD_TGFCABITEAVO cia, TPRAVO avo ";
		sqlEstoqueNota += "	 		WHERE 	cia.nunota = {NUNOTA} ";
		sqlEstoqueNota += "	  			AND cia.idavo = avo.id ";
		queryEstoqueNotaItemAvo.setParam("NUNOTA", nuNota);
		queryEstoqueNotaItemAvo.nativeSelect(sqlEstoqueNota);
		int qtdVolumes = 0;
		BigDecimal pesoBrutoTotal = BigDecimal.ZERO;
		BigDecimal pesoLiquidoTotal = BigDecimal.ZERO;
		while (queryEstoqueNotaItemAvo.next()) {
			qtdVolumes = queryEstoqueNotaItemAvo.getInt("TOTAL_VOLUMES");
			pesoBrutoTotal = queryEstoqueNotaItemAvo
					.getBigDecimal("PESO_BRUTO");
			pesoLiquidoTotal = queryEstoqueNotaItemAvo
					.getBigDecimal("PESO_LIQ");
		}
		queryEstoqueNotaItemAvo.close();

		// Ajustar a quantidade no pedido
		QueryExecutor queryUpdCab = contexto.getQuery();
		queryUpdCab.setParam("nunota", nuNota);
		queryUpdCab
				.update(" UPDATE TGFCAB c SET pesobrutomanual='S', pesoliquimanual='S', qtdvol = "
						+ qtdVolumes
						+ ",pesobruto = "
						+ pesoBrutoTotal
						+ ",peso= "
						+ pesoLiquidoTotal
						+ ",VLRNOTA = ROUND((SELECT NVL(SUM((QTDNEG*VLRUNIT) + NVL(VLRSUBST,0)),0) FROM TGFITE i WHERE i.NUNOTA = c.NUNOTA),2) Where nunota = {nunota}");
		queryUpdCab.close();
	}

	/*
	 * NOTAS FISCAIS DE ENTRADA Mudan�a devido necessidade de ser gravado as
	 * duas simultaneamente (CHAVENFE, CHAVENFEREF) Oc�lio Pinho - 16/06/2020
	 */
	public String saveCabecalhoNotaEntrada(Long nuNota, String numNota,
			String codEmp, String codTipoOper, String dhTipOper,
			String tipoMovimento, String codParc, String codParcDest,
			String codTipVenda, String dhTipVenda, String codCencus,
			String codNat, String serieNota, String chaveNfe,
			String chaveNfeRef, Long qtdVol, BigDecimal pesoLiq,
			BigDecimal pesoBruto, String observacao, Long idiproc,
			String codVend) throws Exception {
		DecimalFormat decimalFormat = new DecimalFormat();
		decimalFormat.setMaximumFractionDigits(2);
		decimalFormat.setMinimumFractionDigits(2);

		SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
		SimpleDateFormat formatter2 = new SimpleDateFormat(
				"dd/MM/yyyy hh:mm:ss");
		Date dhNeg = new Date();

		// Parceiro Entrega
		// String codParcDest = "0";
		// String numNota = "0";
		String dtNeg = formatter.format(dhNeg) + " 0:00:0";

		String tipFrete = "N";
		String cifFob = "F";
		if ("6".equals(codEmp) || "8".equals(codEmp)) {
			cifFob = "C";
		}

		// Empresa
		String codEmpFunc = codEmp;

		// A definir posteriormente
		String tipIPIEmb = "N";
		String indPresNfce = "0";
		String issRetido = "N";
		String irfRetido = "S";

		// Composicao da consulta
		StringBuilder xmlRequestBody = new StringBuilder();
		xmlRequestBody.append(" <nota ownerServiceCall='CentralNotas'> ");
		xmlRequestBody.append(" 	<cabecalho> ");
		if (!"677".equals(codTipoOper)) {
			xmlRequestBody.append(" 	<NUMNOTA>" + numNota + "</NUMNOTA>");
		}

		xmlRequestBody.append(" 	    <TIPMOV>" + tipoMovimento + "</TIPMOV>");
		xmlRequestBody.append(" 	    <CODTIPOPER>" + codTipoOper
				+ "</CODTIPOPER>");
		xmlRequestBody.append("			<DHTIPOPER>" + dhTipOper + "</DHTIPOPER> ");
		xmlRequestBody.append("			<CODPARC>" + codParc + "</CODPARC> ");
		if (codParcDest != null) {
			xmlRequestBody.append(" 		<CODPARCDEST>" + codParcDest
					+ "</CODPARCDEST>");
		} else {
			xmlRequestBody.append("    <CODPARCDEST/> ");
		}

		xmlRequestBody.append(" 	    <CODTIPVENDA>" + codTipVenda
				+ "</CODTIPVENDA>");
		xmlRequestBody.append(" 	    <CODEMP>" + codEmp + "</CODEMP> ");
		xmlRequestBody.append(" 	    <DTNEG>" + dtNeg + "</DTNEG>");
		xmlRequestBody.append(" 	    <CODEMPFUNC>" + codEmpFunc
				+ "</CODEMPFUNC>");
		xmlRequestBody.append(" 	    <CIF_FOB>" + cifFob + "</CIF_FOB> ");
		xmlRequestBody.append(" 	    <TIPIPIEMB>" + tipIPIEmb + "</TIPIPIEMB>");
		xmlRequestBody.append(" 	    <TIPFRETE>" + tipFrete + "</TIPFRETE>");
		xmlRequestBody.append(" 	    <INDPRESNFCE>" + indPresNfce
				+ "</INDPRESNFCE>");
		xmlRequestBody
				.append(" 	    <ISSRETIDO>" + issRetido + "</ISSRETIDO> ");
		xmlRequestBody
				.append(" 	    <IRFRETIDO>" + irfRetido + "</IRFRETIDO> ");
		xmlRequestBody
				.append("			<DHTIPVENDA>" + dhTipVenda + "</DHTIPVENDA> ");
		xmlRequestBody.append(" 		<CODCENCUS>" + codCencus + "</CODCENCUS>");
		xmlRequestBody.append(" 		<CODNAT>" + codNat + "</CODNAT>");
		xmlRequestBody.append(" 		<DTPREVENT>" + dtNeg + "</DTPREVENT>");
		xmlRequestBody.append(" 		<SERIENOTA>" + serieNota + "</SERIENOTA>");
		if (chaveNfe != null) {
			xmlRequestBody.append("    <CHAVENFE>" + chaveNfe + "</CHAVENFE>");
		} else {
			xmlRequestBody.append("    <CHAVENFE/> ");
		}

		if (chaveNfeRef != null) {
			xmlRequestBody.append("    <CHAVENFEREF>" + chaveNfeRef
					+ "</CHAVENFEREF>");
		} else {
			xmlRequestBody.append("    <CHAVENFEREF/> ");
		}

		if ("339".equals(codTipoOper)) {
			xmlRequestBody.append("    <CHAVENFE>" + chaveNfe + "</CHAVENFE>");
			xmlRequestBody.append("    <CHAVENFEREF>" + chaveNfe
					+ "</CHAVENFEREF>");
		}

		xmlRequestBody.append(" 		<QTDVOL>" + qtdVol + "</QTDVOL> ");
		xmlRequestBody.append(" 		<PESO>" + pesoLiq + "</PESO>");
		xmlRequestBody.append(" 		<PESOBRUTO>" + pesoBruto + "</PESOBRUTO>");
		xmlRequestBody.append(" 		<OBSERVACAO>" + observacao + "</OBSERVACAO>");
		if (idiproc != null) {
			xmlRequestBody.append(" 	<IDIPROC>" + idiproc + "</IDIPROC> ");
		} else {
			xmlRequestBody.append(" 	<IDIPROC/> ");
		}

		if (codVend != null) {
			xmlRequestBody.append(" 	<CODVEND>" + codVend + "</CODVEND> ");
		} else {
			xmlRequestBody.append(" 	<CODVEND/> ");
		}

		if (nuNota != null) {
			xmlRequestBody.append(" 	<NUNOTA>" + nuNota + "</NUNOTA> ");
		} else {
			xmlRequestBody.append(" 	<NUNOTA></NUNOTA> ");
			xmlRequestBody.append(" 	<CODMOEDA/> ");
			xmlRequestBody.append(" 	<NUMPEDIDO2/> ");
			xmlRequestBody.append(" 	<AD_AVAL_VENDEDOR/> ");
			xmlRequestBody.append(" 	<AD_OBSERVACAOPEDIDO/> ");
			xmlRequestBody.append(" 	<AD_TIPOPEDIDO/> ");
			xmlRequestBody.append(" 	<VLRMOEDA/> ");
			xmlRequestBody.append(" 	<CODUSUCOMPRADOR/> ");
			xmlRequestBody.append(" 	<STATUSCTE/> ");
			xmlRequestBody.append(" 	<CODHISTAC/> ");
			xmlRequestBody.append(" 	<CODCIDENTREGA/> ");
			xmlRequestBody.append(" 	<CODVENDTEC/> ");
			xmlRequestBody.append(" 	<DTENVIOPMB/> ");
			xmlRequestBody.append(" 	<CODUSU/> ");
			xmlRequestBody.append(" 	<HRENTSAI/> ");
			xmlRequestBody.append(" 	<VLRCOMPENSACAO/> ");
			xmlRequestBody.append(" 	<BASEPIS/> ");
			xmlRequestBody.append("	    <VLRICMS/> ");
			xmlRequestBody.append(" 	<CODOBSPADRAO/> ");
			xmlRequestBody.append(" 	<REGESPTRIBUT/> ");
			xmlRequestBody.append(" 	<BASESUBSTIT/> ");
			xmlRequestBody.append(" 	<VOLUME/> ");
			xmlRequestBody.append(" 	<VLRCOFINS/> ");
			xmlRequestBody.append(" 	<VLRSUBST/> ");
			xmlRequestBody.append(" 	<M3AENTREGAR/> ");
			xmlRequestBody.append(" 	<NUMCF/> ");
			xmlRequestBody.append(" 	<LOCEMBARQ/> ");
			xmlRequestBody.append(" 	<UFEMBARQ/> ");
			xmlRequestBody.append(" 	<VLRSTEXTRANOTATOT/> ");
			xmlRequestBody.append(" 	<REBOQUE1/> ");
			xmlRequestBody.append(" 	<VLRAFRMM/> ");
			xmlRequestBody.append(" 	<CNPJADQUIRENTE/> ");
			xmlRequestBody.append(" 	<PESOBRUTOMANUAL/> ");
			xmlRequestBody.append(" 	<ORDEMCARGA/> ");
			xmlRequestBody.append(" 	<DTCONTAB/> ");
			xmlRequestBody.append(" 	<NUPEDFRETE/> ");
			xmlRequestBody.append(" 	<CODMODDOCNOTA/> ");
			xmlRequestBody.append(" 	<NUPCA/> ");
			xmlRequestBody.append(" 	<TIPOPTAGJNFE/> ");
			xmlRequestBody.append(" 	<RETORNADOAC/> ");
			xmlRequestBody.append(" 	<NURD8/> ");
			xmlRequestBody.append(" 	<CODPARCCONSIGNATARIO/> ");
			xmlRequestBody.append(" 	<NULOTENFSE/> ");
			xmlRequestBody.append(" 	<VLRICMSFCP/> ");
			xmlRequestBody.append(" 	<DTREF3/> ");
			xmlRequestBody.append(" 	<DHPROTOC/> ");
			xmlRequestBody.append(" 	<STATUSCONFERENCIA/> ");
			xmlRequestBody.append(" 	<NOTAEMPENHO/> ");
			xmlRequestBody.append(" 	<PERMALTERCENTRAL/> ");
			xmlRequestBody.append(" 	<PEDIDOIMPRESSO/> ");
			xmlRequestBody.append(" 	<VLRICMSFCPINT/> ");
			xmlRequestBody.append(" 	<BASEISS/> ");
			xmlRequestBody.append(" 	<DTREF2/> ");
			xmlRequestBody.append(" 	<NUFOP/> ");
			xmlRequestBody.append(" 	<VLRTOTLIQITEMMOE/> ");
			xmlRequestBody.append(" 	<VLRDESCTOTITEMMOE/> ");
			xmlRequestBody.append(" 	<BASEICMSFRETE/> ");
			xmlRequestBody.append(" 	<PESOAENTREGAR/> ");
			xmlRequestBody.append(" 	<TERMACORDNOTA/> ");
			xmlRequestBody.append(" 	<VLRIPI/> ");
			xmlRequestBody.append(" 	<KMVEICULO/> ");
			xmlRequestBody.append(" 	<NUMNFSE/> ");
			xmlRequestBody.append(" 	<CLASSIFICMS/> ");
			xmlRequestBody.append(" 	<VLRISS/> ");
			xmlRequestBody.append(" 	<NULOTECTE/> ");
			xmlRequestBody.append(" 	<DANFE/> ");
			xmlRequestBody.append(" 	<TIPNOTAPMB/> ");
			xmlRequestBody.append(" 	<NUCFR/> ");
			xmlRequestBody.append(" 	<CODPARCREDESPACHO/> ");
			xmlRequestBody.append(" 	<IDNAVIO/> ");
			xmlRequestBody.append(" 	<CODTPD/> ");
			xmlRequestBody.append(" 	<VLRCARGAAVERB/> ");
			xmlRequestBody.append(" 	<CODCIDFIMCTE/> ");
			xmlRequestBody.append(" 	<DTENTSAIINFO/> ");
			xmlRequestBody.append(" 	<VLRINDENIZ/> ");
			xmlRequestBody.append(" 	<TPEMISNFE/> ");
			xmlRequestBody.append(" 	<DHREGDPEC/> ");
			xmlRequestBody.append(" 	<VLRPISST/> ");
			xmlRequestBody.append(" 	<IPIEMB/> ");
			xmlRequestBody.append(" 	<BASECOFINSST/> ");
			xmlRequestBody.append(" 	<VLRLIQITEMNFE/> ");
			xmlRequestBody.append(" 	<CODLOCALORIG/> ");
			xmlRequestBody.append(" 	<CONFIRMNOTAFAT/> ");
			xmlRequestBody.append(" 	<TPAMBNFE/> ");
			xmlRequestBody.append(" 	<VLRICMSEMB/> ");
			xmlRequestBody.append(" 	<PESOBRUTOITENS/> ");
			xmlRequestBody.append(" 	<ICMSFRETE/> ");
			xmlRequestBody.append(" 	<CODPARCREMETENTE/> ");
			xmlRequestBody.append(" 	<VENCFRETE/> ");
			xmlRequestBody.append(" 	<DTMOV/> ");
			xmlRequestBody.append(" 	<DTALTER/> ");
			xmlRequestBody.append(" 	<CODCIDINICTE/> ");
			xmlRequestBody.append(" 	<OBSERVACAOAC/> ");
			xmlRequestBody.append(" 	<STATUSWMS/> ");
			xmlRequestBody.append(" 	<TIPPROCIMP/> ");
			xmlRequestBody.append(" 	<STATUSNOTA/> ");
			xmlRequestBody.append(" 	<DTENVSUF/> ");
			xmlRequestBody.append(" 	<NUODP/> ");
			xmlRequestBody.append(" 	<NUMERACAOVOLUMES/> ");
			xmlRequestBody.append(" 	<STATUSNFE/> ");
			xmlRequestBody.append(" 	<CPFCNPJADQUIRENTE/> ");
			xmlRequestBody.append(" 	<NUMCOTACAO/> ");
			xmlRequestBody.append(" 	<BASEIPI/> ");
			xmlRequestBody.append(" 	<DTREMRET/> ");
			xmlRequestBody.append(" 	<CODVTP/> ");
			xmlRequestBody.append(" 	<COMGER/> ");
			xmlRequestBody.append(" 	<TIPLIBERACAO/> ");
			xmlRequestBody.append(" 	<VLRINDENIZDIST/> ");
			xmlRequestBody.append(" 	<VLRICMSDIFALDEST/> ");
			xmlRequestBody.append(" 	<NUNOTAPEDFRET/> ");
			xmlRequestBody.append(" 	<IDBALSA03/> ");
			xmlRequestBody.append(" 	<TPEMISNFSE/> ");
			xmlRequestBody.append(" 	<CTELOTACAO/> ");
			xmlRequestBody.append(" 	<LACRES/> ");
			xmlRequestBody.append(" 	<VLRSTFCPINTANT/> ");
			xmlRequestBody.append(" 	<MODENTREGA/> ");
			xmlRequestBody.append(" 	<LIBCONF/> ");
			xmlRequestBody.append(" 	<DTVAL/> ");
			xmlRequestBody.append(" 	<CTEGLOBAL/> ");
			xmlRequestBody.append(" 	<IDBALSA02/> ");
			xmlRequestBody.append(" 	<FRETEVLRPAGO/> ");
			xmlRequestBody.append(" 	<VENCIPI/> ");
			xmlRequestBody.append(" 	<DTADIAM/> ");
			xmlRequestBody.append(" 	<CODCC/> ");
			xmlRequestBody.append(" 	<PRODPRED/> ");
			xmlRequestBody.append(" 	<BASEIRF/> ");
			xmlRequestBody.append(" 	<REBOQUE2/> ");
			xmlRequestBody.append(" 	<TPEMISCTE/> ");
			xmlRequestBody.append(" 	<CODEMPNEGOC/> ");
			xmlRequestBody.append(" 	<NULOTENFE/> ");
			xmlRequestBody.append(" 	<CODMAQ/> ");
			xmlRequestBody.append(" 	<VLRICMSSEG/> ");
			xmlRequestBody.append(" 	<TPAMBCTE/> ");
			xmlRequestBody.append(" 	<VLRFRETECALC/> ");
			xmlRequestBody.append(" 	<TPASSINANTE/> ");
			xmlRequestBody.append(" 	<CODRASTREAMENTOECT/> ");
			xmlRequestBody.append(" 	<RATEADO/> ");
			xmlRequestBody.append(" 	<CHAVENFSE/> ");
			xmlRequestBody.append(" 	<VLRFRETETOTAL/> ");
			xmlRequestBody.append(" 	<AGRUPFINNOTA/> ");
			xmlRequestBody.append(" 	<ALIQIRF/> ");
			xmlRequestBody.append(" 	<CODUSUINC/> ");
			xmlRequestBody.append(" 	<VLRVENDOR/> ");
			xmlRequestBody.append(" 	<VLRJURODIST/> ");
			xmlRequestBody.append(" 	<CODCONTATOENTREGA/> ");
			xmlRequestBody.append(" 	<CODCIDDESTINO/> ");
			xmlRequestBody.append(" 	<LOTACAO/> ");
			xmlRequestBody.append(" 	<BASEINSS/> ");
			xmlRequestBody.append(" 	<SITUACAOWMS/> ");
			xmlRequestBody.append(" 	<CODCID/> ");
			xmlRequestBody.append(" 	<ANTT/> ");
			xmlRequestBody.append(" 	<BASESUBSTSEMRED/> ");
			xmlRequestBody.append(" 	<TIPOCTE/> ");
			xmlRequestBody.append(" 	<VIATRANSP/> ");
			xmlRequestBody.append(" 	<DTENTSAI/> ");
			xmlRequestBody.append(" 	<VLREMB/> ");
			xmlRequestBody.append(" 	<LOCALCOLETA/> ");
			xmlRequestBody.append(" 	<VLRPIS/> ");
			xmlRequestBody.append(" 	<VLRNOTA/> ");
			xmlRequestBody.append(" 	<VLRDESTAQUE/> ");
			xmlRequestBody.append(" 	<PERCDESCFOB/> ");
			xmlRequestBody.append(" 	<CIOT/> ");
			xmlRequestBody.append(" 	<CODUFDESTINO/> ");
			xmlRequestBody.append(" 	<DTREF/> ");
			xmlRequestBody.append(" 	<CODDOCA/> ");
			xmlRequestBody.append(" 	<VLROUTROS/> ");
			xmlRequestBody.append(" 	<PESOLIQUIMANUAL/> ");
			xmlRequestBody.append(" 	<VLRJURO/> ");
			xmlRequestBody.append(" 	<PERCDESC/> ");
			xmlRequestBody.append(" 	<VLRDESCSERV/> ");
			xmlRequestBody.append(" 	<CODPARCTRANSPFINAL/> ");
			xmlRequestBody.append(" 	<PLACA/> ");
			xmlRequestBody.append(" 	<LIBPENDENTE/> ");
			xmlRequestBody.append(" 	<CODOBRA/> ");
			xmlRequestBody.append(" 	<TOTALCUSTOSERV/> ");
			xmlRequestBody.append(" 	<CODGRUPOTENSAO/> ");
			xmlRequestBody.append(" 	<NUMCONTRATO/> ");
			xmlRequestBody.append(" 	<NUREM/> ");
			xmlRequestBody.append(" 	<LOCALENTREGA/> ");
			xmlRequestBody.append(" 	<DESCRHISTAC/> ");
			xmlRequestBody.append(" 	<NROREDZ/> ");
			xmlRequestBody.append(" 	<CODART/> ");
			xmlRequestBody.append(" 	<PRODUETLOC/> ");
			xmlRequestBody.append(" 	<UFADQUIRENTE/> ");
			xmlRequestBody.append(" 	<VLRFRETECPL/> ");
			xmlRequestBody.append(" 	<CLASCONS/> ");
			xmlRequestBody.append(" 	<COMISSAO/> ");
			xmlRequestBody.append(" 	<VLRFRETE/> ");
			xmlRequestBody.append(" 	<VLRREPREDTOTSEMDESC/> ");
			xmlRequestBody.append(" 	<REBOQUE3/> ");
			xmlRequestBody.append(" 	<BASECOFINS/> ");
			xmlRequestBody.append(" 	<VLRDESCTOTITEM/> ");
			xmlRequestBody.append(" 	<EXIGEISSQN/> ");
			xmlRequestBody.append(" 	<VLRIRF/> ");
			xmlRequestBody.append(" 	<VLRCOFINSST/> ");
			xmlRequestBody.append(" 	<IDBALSA01/> ");
			xmlRequestBody.append(" 	<SITUACAOCTE/> ");
			xmlRequestBody.append(" 	<DIGITAL/> ");
			xmlRequestBody.append(" 	<HRMOV/> ");
			xmlRequestBody.append(" 	<CODVEICULO/> ");
			xmlRequestBody.append(" 	<TIPSERVCTE/> ");
			xmlRequestBody.append(" 	<VLRSEG/> ");
			xmlRequestBody.append(" 	<NUMPROTOCCTE/> ");
			xmlRequestBody.append(" 	<DIRECAOVIAG/> ");
			xmlRequestBody.append(" 	<CODCIDORIGEM/> ");
			xmlRequestBody.append(" 	<CODGUF/> ");
			xmlRequestBody.append(" 	<VLRINSS/> ");
			xmlRequestBody.append(" 	<VLRDESCTOT/> ");
			xmlRequestBody.append(" 	<NUTRANSF/> ");
			xmlRequestBody.append(" 	<CODPARCTRANSP/> ");
			xmlRequestBody.append(" 	<HRADIAM/> ");
			xmlRequestBody.append(" 	<SERIENFDES/> ");
			xmlRequestBody.append(" 	<FUSOEMISSEPEC/> ");
			xmlRequestBody.append(" 	<MODELONFDES/> ");
			xmlRequestBody.append(" 	<TROCO/> ");
			xmlRequestBody.append(" 	<SITESPECIALRESP/> ");
			xmlRequestBody.append(" 	<NUMALEATORIO/> ");
			xmlRequestBody.append(" 	<VLRMERCADORIA/> ");
			xmlRequestBody.append(" 	<NUMREGDPEC/> ");
			xmlRequestBody.append(" 	<NROCAIXA/> ");
			xmlRequestBody.append(" 	<TOTDISPDESC/> ");
			xmlRequestBody.append(" 	<DHEMISSEPEC/> ");
			xmlRequestBody.append(" 	<NUCONFATUAL/> ");
			xmlRequestBody.append(" 	<NUNOTASUB/> ");
			xmlRequestBody.append(" 	<DESCTERMACORD/> ");
			xmlRequestBody.append(" 	<TPLIGACAO/> ");
			xmlRequestBody.append(" 	<MOTNAORETERISSQN/> ");
			xmlRequestBody.append(" 	<VLRICMSDIFALREM/> ");
			xmlRequestBody.append(" 	<TOTALCUSTOPROD/> ");
			xmlRequestBody.append(" 	<APROVADO/> ");
			xmlRequestBody.append(" 	<CHAVECTE/> ");
			xmlRequestBody.append(" 	<DTDECLARA/> ");
			xmlRequestBody.append(" 	<CODFUNC/> ");
			xmlRequestBody.append(" 	<IRINNAVIO/> ");
			xmlRequestBody.append(" 	<PENDENTE/> ");
			xmlRequestBody.append(" 	<NUMOS/> ");
			xmlRequestBody.append(" 	<VLRPRESTAFRMM/> ");
			xmlRequestBody.append(" 	<CODUFENTREGA/> ");
			xmlRequestBody.append(" 	<CODCIDPREST/> ");
			xmlRequestBody.append(" 	<TPAMBNFSE/> ");
			xmlRequestBody.append(" 	<NATUREZAOPERDES/> ");
			xmlRequestBody.append(" 	<UFVEICULO/> ");
			xmlRequestBody.append(" 	<CODCONTATO/> ");
			xmlRequestBody.append(" 	<SEQCARGA/> ");
			xmlRequestBody.append(" 	<DHPROTOCCTE/> ");
			xmlRequestBody.append(" 	<MARCA/> ");
			xmlRequestBody.append(" 	<PESOLIQITENS/> ");
			xmlRequestBody.append(" 	<NUMALEATORIOCTE/> ");
			xmlRequestBody.append(" 	<M3/> ");
			xmlRequestBody.append(" 	<NOTASCF/> ");
			xmlRequestBody.append(" 	<FORMPGTCTE/> ");
			xmlRequestBody.append(" 	<STATUSNFSE/> ");
			xmlRequestBody.append(" 	<CODUFORIGEM/> ");
			xmlRequestBody.append(" 	<NUMPROTOC/> ");
			xmlRequestBody.append(" 	<OCCN48/> ");
			xmlRequestBody.append(" 	<VLRREPREDTOT/> ");
			xmlRequestBody.append(" 	<CODSITE/> ");
			xmlRequestBody.append(" 	<CODPROJ/> ");
			xmlRequestBody.append(" 	<DTFATUR/> ");
			xmlRequestBody.append(" 	<NFEDEVRECUSA/> ");
			xmlRequestBody.append(" 	<VLRSTFCPINT/> ");
			xmlRequestBody.append(" 	<BASEICMS/> ");
			xmlRequestBody.append(" 	<VLRROYALT/> ");
			xmlRequestBody.append(" 	<CODMOTORISTA/> ");
			xmlRequestBody.append(" 	<CONFIRMADA/> ");
			xmlRequestBody.append(" 	<BASEPISST/> ");
			xmlRequestBody.append(" 	<NOMEADQUIRENTE/> ");
		}
		xmlRequestBody.append("		</cabecalho> ");
		xmlRequestBody.append("		<txProperties>");
		xmlRequestBody
				.append("         		<prop name='br.com.sankhya.mgecom.gradeItens.pedidoWeb' value='false'/> ");
		xmlRequestBody
				.append(" 	    		<prop name='br.com.sankhya.mgefin.recalculo.custopreco.Automatico' value='false'/> ");
		xmlRequestBody
				.append(" 	    		<prop name='br.com.sankhya.mgefin.checarfinanceiro.VlrEntrada' value='0'/> ");
		xmlRequestBody
				.append(" 	    		<prop name='br.com.sankhya.mgefin.checarfinanceiro.RecalcularVencimento' value='false'/> ");
		xmlRequestBody
				.append(" 	    		<prop name='cabecalhoNota.inserindo.pedidoWeb' value='false'/> ");
		xmlRequestBody.append("		</txProperties> ");
		xmlRequestBody.append("	</nota> ");
		xmlRequestBody.append("	<clientEventList> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.central.itens.VendaCasada</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.parcelas.financeiro</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.central.itens.KitRevenda.msgValidaFormula</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecomercial.event.compensacao.credito.debito</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecomercial.event.baixaPortal</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecomercial.event.cadastrarDistancia</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgefin.event.fixa.vencimento</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.imobilizado</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgefin.solicitacao.liberacao.orcamento</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.exibir.variacao.valor.item</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.cancelamento.notas.remessa</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecomercial.event.estoque.componentes</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.nota.adicional.SolicitarUsuarioGerente</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.event.troca.item.por.produto.alternativo</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.central.itens.KitRevenda</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.utiliza.dtneg.servidor</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>central.save.grade.itens.mostrar.popup.info.lote</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.expedicao.SolicitarUsuarioConferente</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.valida.ChaveNFeCompraTerceiros</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.actionbutton.clientconfirm</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.compra.SolicitacaoComprador</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.checkout.obter.peso</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecomercial.event.estoque.insuficiente.produto</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.importacaoxml.cfi.para.produto</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.event.troca.item.por.produto.substituto</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecomercial.event.faturamento.confirmacao</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.exibe.msg.variacao.preco</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.coleta.entrega.recalculado</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>central.save.grade.itens.mostrar.popup.serie</clientEvent> ");
		xmlRequestBody.append("	</clientEventList> ");
		// Chamada ao servi�o da Sankhya
		Document document = serviceInvoker.call(
				"CACSP.incluirAlterarCabecalhoNota", "mgecom",
				xmlRequestBody.toString());

		Node item = document.getChildNodes().item(0);

		if ("1".equals(item.getAttributes().getNamedItem("status")
				.getTextContent())) {
			return document.getElementsByTagName("NUNOTA").item(0)
					.getTextContent();
		} else {
			return XMLUtil.xmlToString(document);
		}
	}

	public Document cabecalhoCotacao(BigDecimal nuNota, int codEmp, int codNat,
			int centroCusto, int pesoPreco, int codComprador,
			String dataInicio, int codUsuario) throws Exception {
		StringBuilder xmlRequestBody = new StringBuilder();
		xmlRequestBody
				.append(" <dataSet rootEntity='CabecalhoCotacao' includePresentationFields='S' datasetid='1598096643911_3'> ");
		xmlRequestBody.append(" 	<entity path=''> ");
		xmlRequestBody.append(" 		<fieldset list='*'/> ");
		xmlRequestBody
				.append(" 		<fieldset except='true' list='NUNOTAORIG,GERPEDREAL,CODTIPVENDA,PRAZOENTREGA,VALPROPOSTA,DTALTER,CODUSU,MODFRETE,NUMNOTAORIG'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='Projeto'> ");
		xmlRequestBody.append(" 		<field name='IDENTIFICACAO'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='UsuarioResponsavel'> ");
		xmlRequestBody.append(" 		<field name='NOMEUSU'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='Natureza'> ");
		xmlRequestBody.append(" 		<field name='DESCRNAT'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='UsuarioComprador'> ");
		xmlRequestBody.append(" 		<field name='NOMEUSU'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 	<entity path='Empresa'> ");
		xmlRequestBody.append(" 		<field name='NOMEFANTASIA'/> ");
		xmlRequestBody.append(" 	</entity> ");
		xmlRequestBody.append(" 		<entity path='CentroResultado'> ");
		xmlRequestBody.append(" 			<field name='DESCRCENCUS'/> ");
		xmlRequestBody.append(" 		</entity> ");
		xmlRequestBody.append("			<dataRow> ");
		xmlRequestBody.append("				<localFields> ");
		xmlRequestBody.append("					<CODUSU><![CDATA[" + codUsuario
				+ "]]></CODUSU> ");
		xmlRequestBody.append("					<CODUSURESP><![CDATA[" + codUsuario
				+ "]]></CODUSURESP> ");
		xmlRequestBody.append("					<CODUSUREQ><![CDATA[" + codComprador
				+ "]]></CODUSUREQ> ");
		xmlRequestBody.append("					<CODNAT><![CDATA[" + codNat
				+ "]]></CODNAT> ");
		xmlRequestBody.append("					<DHINIC><![CDATA[" + dataInicio
				+ "]]></DHINIC> ");
		xmlRequestBody.append("					<CODCENCUS><![CDATA[" + centroCusto
				+ "]]></CODCENCUS> ");
		xmlRequestBody.append("					<PESOPRECO><![CDATA[" + pesoPreco
				+ "]]></PESOPRECO> ");
		xmlRequestBody.append("					<CODEMP><![CDATA[" + codEmp
				+ "]]></CODEMP> ");
		xmlRequestBody.append("					<NUNOTAORIG><![CDATA[" + nuNota
				+ "]]></NUNOTAORIG> ");
		xmlRequestBody.append("				</localFields> ");
		xmlRequestBody.append(" 	    </dataRow> ");
		xmlRequestBody.append(" </dataSet> ");
		xmlRequestBody.append(" 	<clientEventList> ");
		xmlRequestBody
				.append(" 	 	<clientEvent>br.com.sankhya.cotacao.novos.enviar.email</clientEvent> ");
		xmlRequestBody
				.append(" 	 	<clientEvent>br.com.sankhya.actionbutton.clientconfirm</clientEvent> ");
		xmlRequestBody.append(" 	</clientEventList> ");
		// Chamada ao servi�o da Sankhya

		System.out.println(xmlRequestBody.toString());

		Document document = serviceInvoker.call(
				"CRUDServiceProvider.saveRecord", "mge",
				xmlRequestBody.toString());

		Node item = document.getChildNodes().item(0);

		if ("1".equals(item.getAttributes().getNamedItem("status")
				.getTextContent())) {
			return document;
		} else {
			return document;
		}
	}

	public String itemCotacao(String descNutereza, String dataInicio,
			int codEmp, String descProj, String empFantasia,
			String descCentroCusto, Integer numCotacao, int codUsuario,
			String descUsuario, String observacao, int pesoPreco,
			int centroCusto, int codProj, BigDecimal nuNota, int codTop,
			String desctipOper, int codNat) throws Exception {
		QueryExecutor queryItem = contexto.getQuery();
		String requisicao = "";

		StringBuilder xmlRequestBody = new StringBuilder();
		xmlRequestBody.append(" <parametros> ");
		xmlRequestBody.append(" 	<cabecalhoCotacao> ");
		xmlRequestBody.append(" 		<DHFINAL/> ");
		xmlRequestBody.append(" 		<PESOPRAZOMED/> ");
		xmlRequestBody.append(" 		<PESOQUALPROD/> ");
		xmlRequestBody.append(" 		<PESOQUALATEND/> ");
		xmlRequestBody.append(" 		<PESOCONDPAG/> ");
		xmlRequestBody.append(" 		<PESOTAXAJURO/> ");
		xmlRequestBody.append(" 		<VALPROPOSTA/> ");
		xmlRequestBody.append(" 		<PESOCONFIABFORN/> ");
		xmlRequestBody.append(" 		<PESOGARANTIA/> ");
		xmlRequestBody.append(" 		<PESOPRAZOENTREG/> ");

		xmlRequestBody.append("			<Natureza_DESCRNAT><![CDATA[" + descNutereza
				+ "]]></Natureza_DESCRNAT> ");
		xmlRequestBody.append("			<DHINIC><![CDATA[" + dataInicio
				+ "]]></DHINIC> ");
		xmlRequestBody
				.append("			<CODEMP><![CDATA[" + codEmp + "]]></CODEMP> ");
		xmlRequestBody.append("			<Projeto_IDENTIFICACAO><![CDATA[" + descProj
				+ "]]></Projeto_IDENTIFICACAO> ");
		xmlRequestBody.append("			<Empresa_NOMEFANTASIA><![CDATA["
				+ empFantasia + "]]></Empresa_NOMEFANTASIA> ");
		xmlRequestBody.append("			<CentroResultado_DESCRCENCUS><![CDATA["
				+ descCentroCusto + "]]></CentroResultado_DESCRCENCUS> ");
		xmlRequestBody.append("			<NUMCOTACAO><![CDATA[" + numCotacao
				+ "]]></NUMCOTACAO> ");
		xmlRequestBody
				.append("			<CODNAT><![CDATA[" + codNat + "]]></CODNAT> ");
		xmlRequestBody.append("			<CODUSURESP><![CDATA[" + codUsuario
				+ "]]></CODUSURESP> ");
		xmlRequestBody.append("			<CODUSU><![CDATA[" + codUsuario
				+ "]]></CODUSU> ");
		xmlRequestBody.append("			<UsuarioResponsavel_NOMEUSU><![CDATA["
				+ descUsuario + "]]></UsuarioResponsavel_NOMEUSU> ");

		if (observacao == null) {
			xmlRequestBody.append(" 		<OBSERVACAO/> ");
		} else {
			xmlRequestBody.append("			<OBSERVACAO><![CDATA[" + observacao
					+ "]]></OBSERVACAO> ");
		}
		xmlRequestBody.append("			<PESOPRECO><![CDATA[" + pesoPreco
				+ "]]></PESOPRECO> ");
		xmlRequestBody.append("			<CODCENCUS><![CDATA[" + centroCusto
				+ "]]></CODCENCUS> ");
		xmlRequestBody.append("			<CODUSUREQ><![CDATA[" + codUsuario
				+ "]]></CODUSUREQ> ");
		xmlRequestBody.append("			<CODPROJ><![CDATA[" + codProj
				+ "]]></CODPROJ> ");
		xmlRequestBody.append("			<UsuarioComprador_NOMEUSU><![CDATA["
				+ descUsuario + "]]></UsuarioComprador_NOMEUSU> ");
		xmlRequestBody.append(" 	</cabecalhoCotacao> ");
		xmlRequestBody.append("  	<itensCotacao>");

		System.out.println(xmlRequestBody.toString());

		String sqlItem = " SELECT distinct ";
		sqlItem += "              pro.DESCRPROD, ";
		sqlItem += "              pro.CODPROD, ";
		sqlItem += "              pro.CODGRUPOPROD, ";
		sqlItem += "              ite.CONTROLE, ";
		sqlItem += "              ite.OBSERVACAO, ";
		sqlItem += "              pro.CODVOL, ";
		sqlItem += "              ite.QTDNEG, ";
		sqlItem += "              loc.DESCRLOCAL, ";
		sqlItem += "              ite.CODLOCALORIG, ";
		sqlItem += "              ite.NUNOTA, ";
		sqlItem += "              ite.SEQUENCIA ";
		sqlItem += "       From TGFITE ite, ";
		sqlItem += "            TGFPRO pro, ";
		sqlItem += "            TGFLOC loc ";
		sqlItem += " Where ite.NUNOTA = {NUNOTA} ";
		sqlItem += " and   pro.codprod = ite.codprod ";
		sqlItem += " and   loc.codlocal = ite.codlocalorig ";

		queryItem.setParam("NUNOTA", nuNota);
		System.out.println(" sqlItem " + sqlItem);
		queryItem.nativeSelect(sqlItem);

		while (queryItem.next()) {
			requisicao = queryItem.getString("NUNOTA");
			String descProd = queryItem.getString("DESCRPROD");
			String observacaoItem = queryItem.getString("OBSERVACAO");
			int codProd = queryItem.getInt("CODPROD");
			int codGrupoProd = queryItem.getInt("CODGRUPOPROD");
			String controle = queryItem.getString("CONTROLE");
			String unidMedida = queryItem.getString("CODVOL");
			BigDecimal qtdNeg = queryItem.getBigDecimal("QTDNEG");
			String descLocalEstoque = queryItem.getString("DESCRLOCAL");
			int codLocal = queryItem.getInt("CODLOCALORIG");
			int seqItem = queryItem.getInt("SEQUENCIA");

			int diferenciador = 0;
			int codParc = 0;
			String cabecalho = "N";
			Boolean avoidFilter = true;

			xmlRequestBody.append("  		<itemCotacao> ");
			xmlRequestBody.append(" 		  	<SITUACAO/> ");
			xmlRequestBody.append(" 			<Parceiro_NOMEPARC/> ");
			xmlRequestBody.append(" 			<AD_NRNOTA/> ");
			xmlRequestBody.append(" 			<DTLIMITE/> ");
			xmlRequestBody.append(" 			<STATUSPRODCOT/> ");
			xmlRequestBody.append(" 			<AD_NRPEDIDO/> ");
			xmlRequestBody.append(" 			<AD_REQCOMPRAS/> ");

			xmlRequestBody.append("				<Produto_DESCRPROD><![CDATA[" + descProd
					+ "]]></Produto_DESCRPROD> ");
			xmlRequestBody.append("				<DIFERENCIADOR><![CDATA["
					+ diferenciador + "]]></DIFERENCIADOR> ");
			xmlRequestBody.append("				<AD_CODTIPOPER><![CDATA[" + codTop
					+ "]]></AD_CODTIPOPER> ");
			xmlRequestBody.append("				<CODPROD><![CDATA[" + codProd
					+ "]]></CODPROD> ");
			xmlRequestBody.append("				<CONTROLE><![CDATA[" + controle
					+ "]]></CONTROLE> ");
			xmlRequestBody.append("				<CABECALHO><![CDATA[" + cabecalho
					+ "]]></CABECALHO> ");
			if (observacaoItem == null) {
				xmlRequestBody.append(" 		<OBS/> ");
			} else {
				xmlRequestBody.append("				<OBS><![CDATA[" + observacaoItem
						+ "]]></OBS> ");
			}
			xmlRequestBody.append("				<AD_DESCROPER><![CDATA[" + desctipOper
					+ "]]></AD_DESCROPER> ");
			xmlRequestBody.append("				<Volume_DESCRVOL><![CDATA[" + unidMedida
					+ "]]></Volume_DESCRVOL> ");
			xmlRequestBody.append("				<QTDCOTADA><![CDATA[" + qtdNeg
					+ "]]></QTDCOTADA> ");
			xmlRequestBody.append("				<LocalEstoque_DESCRLOCAL><![CDATA["
					+ descLocalEstoque + "]]></LocalEstoque_DESCRLOCAL> ");
			xmlRequestBody.append("				<CODVOL><![CDATA[" + unidMedida
					+ "]]></CODVOL> ");
			xmlRequestBody.append("				<avoidFilter><![CDATA[" + avoidFilter
					+ "]]></avoidFilter> ");
			xmlRequestBody.append("				<CODLOCAL><![CDATA[" + codLocal
					+ "]]></CODLOCAL> ");
			xmlRequestBody.append("				<NUMCOTACAO><![CDATA[" + numCotacao
					+ "]]></NUMCOTACAO> ");
			xmlRequestBody.append("				<CODPARC><![CDATA[" + codParc
					+ "]]></CODPARC> ");
			xmlRequestBody.append("  		</itemCotacao> ");
		} // queryItem.next()

		xmlRequestBody.append("  	</itensCotacao>");
		xmlRequestBody.append(" </parametros> ");
		xmlRequestBody.append(" <clientEventList> ");
		xmlRequestBody
				.append("  	<clientEvent>br.com.sankhya.cotacao.novos.enviar.email</clientEvent> ");
		xmlRequestBody
				.append(" 	<clientEvent>br.com.sankhya.actionbutton.clientconfirm</clientEvent> ");
		xmlRequestBody.append(" </clientEventList> ");
		System.out.println(xmlRequestBody.toString());
		serviceInvoker.call("CotacaoSP.salvarCotacao", "mgecot",
				xmlRequestBody.toString());
		return requisicao;
	}

	public String cabecalhoRequisicao(int codEmp, String issRetido,
			Long nuNota, String irfRetido, int codComprador, int codNat,
			int centroResultado, String tipFrete, String tipoFrete,
			String dataNeg, String tipoMov, int codTop, String dataTop,
			int IndNFCe, String tipIpiEmb, String codUsu) throws Exception {
		StringBuilder xmlRequestBody = new StringBuilder();
		xmlRequestBody.append(" <nota ownerServiceCall='CentralNotas'> ");
		xmlRequestBody.append(" 	<cabecalho> ");
		xmlRequestBody.append("			<CLASCONS/> ");
		xmlRequestBody.append("			<BASEIRF/> ");
		xmlRequestBody.append("			<NULOTENFE/> ");
		xmlRequestBody.append("			<AD_PREVENTREGA/> ");
		xmlRequestBody.append("			<TERMACORDNOTA/> ");
		xmlRequestBody.append("			<PESOBRUTOMANUAL/> ");
		xmlRequestBody.append("			<CODCONTATO/> ");
		xmlRequestBody.append("			<BASECOFINS/> ");
		xmlRequestBody.append("			<TIMNUFINORIG/> ");
		xmlRequestBody.append("			<TOTALCUSTOSERV/> ");
		xmlRequestBody.append("			<NUMDOCARRECAD/> ");
		xmlRequestBody.append("			<BASESUBSTSEMRED/> ");
		xmlRequestBody.append("			<NURD8/> ");
		xmlRequestBody.append("			<VLRICMSFCPINT/> ");
		xmlRequestBody.append("			<CODMAQ/> ");
		xmlRequestBody.append("			<AD_TRANSP_OC/> ");
		xmlRequestBody.append("			<CODCIDORIGEM/> ");
		xmlRequestBody.append("			<DESCRHISTAC/> ");
		xmlRequestBody.append("			<CTEGLOBAL/> ");
		xmlRequestBody.append("			<IDBALSA03/> ");
		xmlRequestBody.append("			<VLRVENDOR/> ");
		xmlRequestBody.append("			<PESOLIQITENS/> ");
		xmlRequestBody.append("			<VLRIRF/> ");
		xmlRequestBody.append("			<VLRPRESTAFRMM/> ");
		xmlRequestBody.append("			<VLRDESCTOT/> ");
		xmlRequestBody.append("			<MODENTREGA/> ");
		xmlRequestBody.append("			<NUMPROTOCCTE/> ");
		xmlRequestBody.append("			<NOMEADQUIRENTE/> ");
		xmlRequestBody.append("			<CHAVECTE/> ");
		xmlRequestBody.append("			<PESOBRUTOITENS/> ");
		xmlRequestBody.append("			<NUMPROTOC/> ");
		xmlRequestBody.append("			<VLRCOFINSST/> ");
		xmlRequestBody.append("			<AD_REFERENCE/> ");
		xmlRequestBody.append("			<CODVEND/> ");
		xmlRequestBody.append("			<CODCIDENTREGA/> ");
		// xmlRequestBody.append("			<CODUSUINC/> ");
		xmlRequestBody.append("			<CODPARCDEST/> ");
		xmlRequestBody.append("			<MARCA/> ");
		xmlRequestBody.append("			<NUODP/> ");
		xmlRequestBody.append("			<NUREM/> ");
		xmlRequestBody.append("			<COMISSAO/> ");
		xmlRequestBody.append("			<CODPARCREMETENTE/> ");
		xmlRequestBody.append("			<PEDIDOIMPRESSO/> ");
		xmlRequestBody.append("			<AD_RAZAO_SOCIAL/> ");
		xmlRequestBody.append("			<CODTIPVENDA/> ");
		xmlRequestBody.append("			<AD_TIPOPEDIDO/> ");
		xmlRequestBody.append("			<VLRSTFCPINTANT/> ");
		xmlRequestBody.append("			<TIMCONTRATOVENDA/> ");
		xmlRequestBody.append("			<CODGUF/> ");
		xmlRequestBody.append("			<NOTASCF/> ");
		xmlRequestBody.append("			<CODPARCREDESPACHO/> ");
		xmlRequestBody.append("			<DTREF2/> ");
		xmlRequestBody.append("			<AD_INUTILIZADA/> ");
		xmlRequestBody.append("			<M3/> ");
		xmlRequestBody.append("			<DIGITAL/> ");
		xmlRequestBody.append("			<OBSERVACAO/> ");
		xmlRequestBody.append("			<VLRMOEDA/> ");
		xmlRequestBody.append("			<VENCFRETE/> ");
		xmlRequestBody.append("			<CONFIRMNOTAFAT/> ");
		xmlRequestBody.append("			<TOTDISPDESC/> ");
		xmlRequestBody.append("			<MODELONFDES/> ");
		xmlRequestBody.append("			<LATITUDE/> ");
		xmlRequestBody.append("			<IPIEMB/> ");
		xmlRequestBody.append("			<FORMPGTCTE/> ");
		xmlRequestBody.append("			<NATUREZAOPERDES/> ");
		xmlRequestBody.append("			<MOTNAORETERISSQN/> ");
		xmlRequestBody.append("			<AD_REMESSA/> ");
		xmlRequestBody.append("			<CODPARCTRANSP/> ");
		xmlRequestBody.append("			<AD_PARCEIRO_MATRIZ/> ");
		xmlRequestBody.append("			<CTELOTACAO/> ");
		xmlRequestBody.append("			<NFEDEVRECUSA/> ");
		xmlRequestBody.append("			<CODPARCDESCARGAMDFE/> ");
		xmlRequestBody.append("			<PENDENTE/> ");
		xmlRequestBody.append("			<NUMCF/> ");
		xmlRequestBody.append("			<HRENTSAI/> ");
		xmlRequestBody.append("			<VLRINDENIZDIST/> ");
		xmlRequestBody.append("			<AD_CONFERIDO/> ");
		xmlRequestBody.append("			<AD_DIFERNCACONT/> ");
		xmlRequestBody.append("			<STATUSWMS/> ");
		xmlRequestBody.append("			<AD_CIDADEDEST/> ");
		xmlRequestBody.append("			<BASEINSS/> ");
		xmlRequestBody.append("			<VLRICMSDIFALREM/> ");
		xmlRequestBody.append("			<LONGITUDE/> ");
		xmlRequestBody.append("		    <SITESPECIALRESP/> ");
		xmlRequestBody.append("			<IDBALSA02/> ");
		xmlRequestBody.append("			<DTALTER/> ");
		xmlRequestBody.append("			<LIBPENDENTE/> ");
		xmlRequestBody.append("			<SITUACAOWMS/> ");
		xmlRequestBody.append("			<AD_CODPLP/> ");
		xmlRequestBody.append("			<NUFOP/> ");
		xmlRequestBody.append("			<TIMORIGEM/> ");
		xmlRequestBody.append("			<NROREDZ/> ");
		xmlRequestBody.append("			<BASEPISST/> ");
		xmlRequestBody.append("			<NFSEID/> ");
		xmlRequestBody.append("			<NULOTENFSE/> ");
		xmlRequestBody.append("			<UFADQUIRENTE/> ");
		xmlRequestBody.append("			<VLRICMSDIFERIDO/> ");
		xmlRequestBody.append("			<DTVAL/> ");
		xmlRequestBody.append("			<SERIENFDES/> ");
		xmlRequestBody.append("			<DTENTSAIINFO/> ");
		xmlRequestBody.append("			<CODUFORIGEM/> ");
		xmlRequestBody.append("			<CODHISTAC/> ");
		xmlRequestBody.append("			<CODCIDFIMCTE/> ");
		xmlRequestBody.append("			<NUMERACAOVOLUMES/> ");
		xmlRequestBody.append("			<LOCALCOLETA/> ");
		xmlRequestBody.append("			<PRODPRED/> ");
		xmlRequestBody.append("			<AGRUPFINNOTA/> ");
		xmlRequestBody.append("			<AD_EVENTOS_PEND_APROV/> ");
		xmlRequestBody.append("			<AD_PARC_SEPARACAO/> ");
		xmlRequestBody.append("			<DTENVIOPMB/> ");
		xmlRequestBody.append("			<APROVADO/> ");
		xmlRequestBody.append("			<AD_CODEMP_SAIDA/> ");
		xmlRequestBody.append("			<TPASSINANTE/> ");
		xmlRequestBody.append("			<CODUFENTREGA/> ");
		xmlRequestBody.append("			<NUMNFSE/> ");
		xmlRequestBody.append("			<IDBALSA01/> ");
		xmlRequestBody.append("			<IDNAVIO/> ");
		// xmlRequestBody.append("			<CODUSU/> ");
		xmlRequestBody.append("			<CHAVENFEREF/> ");
		xmlRequestBody.append("			<DHREGDPEC/> ");
		xmlRequestBody.append("			<VLRDESCTOTITEM/> ");
		xmlRequestBody.append("			<TIPNOTAPMB/> ");
		xmlRequestBody.append("			<DTENVSUF/> ");
		xmlRequestBody.append("			<AD_CODPARC/> ");
		xmlRequestBody.append("			<VLRFRETECPL/> ");
		xmlRequestBody.append("			<CODCIDDESTINO/> ");
		xmlRequestBody.append("			<CPFCNPJADQUIRENTE/> ");
		xmlRequestBody.append("			<BASEICMS/> ");
		xmlRequestBody.append("			<QTDVOL/> ");
		xmlRequestBody.append("			<CODDOCA/> ");
		xmlRequestBody.append("			<VLRFRETECALC/> ");
		xmlRequestBody.append("			<AD_DTEMBARQUE/> ");
		xmlRequestBody.append("		    <CODMODDOCNOTA/> ");
		xmlRequestBody.append("			<NOTAEMPENHO/> ");
		xmlRequestBody.append("			<DTDECLARA/> ");
		xmlRequestBody.append("			<AD_PEDIDOSUSPENSO/> ");
		xmlRequestBody.append("			<TIPOCTE/> ");
		xmlRequestBody.append("			<REGESPTRIBUT/> ");
		xmlRequestBody.append("			<FRETEVLRPAGO/> ");
		xmlRequestBody.append("			<SEQCARGA/> ");
		xmlRequestBody.append("			<CODCC/> ");
		xmlRequestBody.append("			<VLRLIQITEMNFE/> ");
		xmlRequestBody.append("			<CHAVENFE/> ");
		xmlRequestBody.append("			<CODFUNC/> ");
		xmlRequestBody.append("			<VLRSUBST/> ");
		xmlRequestBody.append("			<DTENTSAI/> ");
		xmlRequestBody.append("			<NUPCA/> ");
		xmlRequestBody.append("			<VLRICMS/> ");
		xmlRequestBody.append("			<PERMALTERCENTRAL/> ");
		xmlRequestBody.append("			<UFEMBARQ/> ");
		xmlRequestBody.append("			<DTREMRET/> ");
		xmlRequestBody.append("			<NUTRANSF/> ");
		xmlRequestBody.append("			<VLRPISST/> ");
		xmlRequestBody.append("			<TIPLIBERACAO/> ");
		xmlRequestBody.append("			<CHAVENFSE/> ");
		xmlRequestBody.append("			<VLRFRETETOTAL/> ");
		xmlRequestBody.append("			<TPAMBNFE/> ");
		xmlRequestBody.append("			<CODPROJ/> ");
		xmlRequestBody.append("			<CODPARCTRANSPFINAL/> ");
		xmlRequestBody.append("			<CODPARC/> ");
		xmlRequestBody.append("			<TIMCONTRATOLTV/> ");
		xmlRequestBody.append("			<EXIGEISSQN/> ");
		xmlRequestBody.append("			<VLRICMSFCP/> ");
		xmlRequestBody.append("			<VLRICMSDIFALDEST/> ");
		xmlRequestBody.append("			<SITUACAOCTE/> ");
		xmlRequestBody.append("			<BASECOFINSST/> ");
		xmlRequestBody.append("			<VLRREPREDTOTSEMDESC/> ");
		xmlRequestBody.append("			<NUPEDFRETE/> ");
		xmlRequestBody.append("			<VLREMB/> ");
		xmlRequestBody.append("			<BASEICMSFRETE/> ");
		xmlRequestBody.append("			<TOTALCUSTOPROD/> ");
		xmlRequestBody.append("			<CODVENDTEC/> ");
		xmlRequestBody.append("			<VLRICMSEMB/> ");
		xmlRequestBody.append("			<TPEMISNFE/> ");
		xmlRequestBody.append("			<IDIPROC/> ");
		xmlRequestBody.append("			<BASEIPI/> ");
		xmlRequestBody.append("			<PESOBRUTO/> ");
		xmlRequestBody.append("			<TIPPROCIMP/> ");
		xmlRequestBody.append("			<AD_COLETADO/> ");
		xmlRequestBody.append("			<NULOTECTE/> ");
		xmlRequestBody.append("			<AD_UF/> ");
		xmlRequestBody.append("			<DTCONTAB/> ");
		xmlRequestBody.append("			<VLRIPI/> ");
		xmlRequestBody.append("			<CLASSIFICMS/> ");
		xmlRequestBody.append("			<VLRCOFINS/> ");
		xmlRequestBody.append("			<VLRTOTLIQITEMMOE/> ");
		xmlRequestBody.append("			<AD_NRPEDIOD/> ");
		xmlRequestBody.append("			<BASESUBSTIT/> ");
		xmlRequestBody.append("			<VLRCARGAAVERB/> ");
		xmlRequestBody.append("			<TIMCODPROD/> ");
		xmlRequestBody.append("			<VLRAFRMM/> ");
		xmlRequestBody.append("			<CODMOEDA/> ");
		xmlRequestBody.append("			<VLRICMSSEG/> ");
		xmlRequestBody.append("			<AD_PESO_BRUTO_OC/> ");
		xmlRequestBody.append("			<CODCONTATOENTREGA/> ");
		xmlRequestBody.append("			<NUMREGDPEC/> ");
		xmlRequestBody.append("			<CODUFDESTINO/> ");
		xmlRequestBody.append("			<VLRNOTA/> ");
		xmlRequestBody.append("			<PRODUETLOC/> ");
		xmlRequestBody.append("			<NUCFR/> ");
		xmlRequestBody.append("			<VLRSTFCPINT/> ");
		xmlRequestBody.append("			<NUMOS/> ");
		xmlRequestBody.append("			<AD_CNPJ/> ");
		xmlRequestBody.append("			<STATUSCONFERENCIA/> ");
		xmlRequestBody.append("			<STATUSNOTA/> ");
		xmlRequestBody.append("			<VLRSTEXTRANOTATOT/> ");
		xmlRequestBody.append("			<VLRCOMPENSACAO/> ");
		xmlRequestBody.append("			<LIBCONF/> ");
		xmlRequestBody.append("			<HRADIAM/> ");
		xmlRequestBody.append("			<BASEISS/> ");
		xmlRequestBody.append("			<AD_PESO_BRUTO/> ");
		xmlRequestBody.append("			<VIATRANSP/> ");
		xmlRequestBody.append("			<CODLOCALORIG/> ");
		xmlRequestBody.append("			<VLRINDENIZ/> ");
		xmlRequestBody.append("			<CODEMPNEGOC/> ");
		xmlRequestBody.append("			<AD_NUMCONTRATO/> ");
		xmlRequestBody.append("			<LOTACAO/> ");
		xmlRequestBody.append("			<COMGER/> ");
		xmlRequestBody.append("			<DIRECAOVIAG/> ");
		xmlRequestBody.append("			<TPAMBCTE/> ");
		xmlRequestBody.append("			<NUNOTA/> ");
		xmlRequestBody.append("			<REBOQUE3/> ");
		xmlRequestBody.append("			<ANTT/> ");
		xmlRequestBody.append("			<CODVTP/> ");
		xmlRequestBody.append("			<NUNOTASUB/> ");
		xmlRequestBody.append("			<VLRFRETE/> ");
		xmlRequestBody.append("			<RETORNADOAC/> ");
		xmlRequestBody.append("			<AD_CODFIXAMOED/> ");
		xmlRequestBody.append("			<CODSITE/> ");
		xmlRequestBody.append("			<OCCN48/> ");
		xmlRequestBody.append("			<BASEPIS/> ");
		xmlRequestBody.append("			<UFVEICULO/> ");
		xmlRequestBody.append("			<TPLIGACAO/> ");
		xmlRequestBody.append("			<VLRDESCTOTITEMMOE/> ");
		xmlRequestBody.append("			<DANFE/> ");
		xmlRequestBody.append("			<CODCIDPREST/> ");
		xmlRequestBody.append("			<CODCID/> ");
		xmlRequestBody.append("			<TIMNUNOTAMOD/> ");
		xmlRequestBody.append("			<NUMALEATORIO/> ");
		xmlRequestBody.append("			<TPEMISNFSE/> ");
		xmlRequestBody.append("			<LOCALENTREGA/> ");
		xmlRequestBody.append("			<CODOBSPADRAO/> ");
		xmlRequestBody.append("			<AD_IGNORARCONFERENCIA/> ");
		xmlRequestBody.append("			<PLACA/> ");
		xmlRequestBody.append("			<DTMOV/> ");
		xmlRequestBody.append("			<VOLUME/> ");
		xmlRequestBody.append("			<TPAMBNFSE/> ");
		xmlRequestBody.append("			<ICMSFRETE/> ");
		xmlRequestBody.append("			<DTREF3/> ");
		xmlRequestBody.append("			<M3AENTREGAR/> ");
		xmlRequestBody.append("			<ORDEMCARGA/> ");
		xmlRequestBody.append("			<VLRPIS/> ");
		xmlRequestBody.append("			<CIOT/> ");
		xmlRequestBody.append("			<STATUSCTE/> ");
		xmlRequestBody.append("			<TIMDESCPROD/> ");
		xmlRequestBody.append("			<VLRISS/> ");
		xmlRequestBody.append("			<PERCDESCFOB/> ");
		xmlRequestBody.append("			<AD_OBSERVACAOPEDIDO/> ");
		xmlRequestBody.append("			<VENCIPI/> ");
		xmlRequestBody.append("			<KMVEICULO/> ");
		xmlRequestBody.append("			<TIPOPTAGJNFE/> ");
		xmlRequestBody.append("			<STATUSNFSE/> ");
		xmlRequestBody.append("			<PESOLIQUIMANUAL/> ");
		xmlRequestBody.append("			<TPEMISCTE/> ");
		xmlRequestBody.append("			<CODCIDINICTE/> ");
		xmlRequestBody.append("			<CODTPD/> ");
		xmlRequestBody.append("			<STATUSNFE/> ");
		xmlRequestBody.append("			<CODGRUPOTENSAO/> ");
		xmlRequestBody.append("			<NUMCOTACAO/> ");
		xmlRequestBody.append("			<DTPREVENT/> ");
		xmlRequestBody.append("			<CODOBRA/> ");
		xmlRequestBody.append("			<SERIENOTA/> ");
		xmlRequestBody.append("			<DTFATUR/> ");
		xmlRequestBody.append("			<CODART/> ");
		xmlRequestBody.append("			<DESCTERMACORD/> ");
		xmlRequestBody.append("			<NUNOTAPEDFRET/> ");
		xmlRequestBody.append("			<FUSOEMISSEPEC/> ");
		xmlRequestBody.append("			<CODMOTORISTA/> ");
		xmlRequestBody.append("			<AD_UF_DESTINATARIO/> ");
		xmlRequestBody.append("			<NROCAIXA/> ");
		xmlRequestBody.append("			<PESO/> ");
		xmlRequestBody.append("			<DTADIAM/> ");
		xmlRequestBody.append("			<LACRES/> ");
		xmlRequestBody.append("			<CODRASTREAMENTOECT/> ");
		xmlRequestBody.append("			<DHPROTOCCTE/> ");
		xmlRequestBody.append("			<VLRROYALT/> ");
		xmlRequestBody.append("			<VLROUTROS/> ");
		xmlRequestBody.append("			<VLRJURODIST/> ");
		xmlRequestBody.append("			<AD_PEDIDOCANCELADO/> ");
		xmlRequestBody.append("			<TIPSERVCTE/> ");
		xmlRequestBody.append("			<NUMPEDIDO2/> ");
		xmlRequestBody.append("			<REBOQUE1/> ");
		xmlRequestBody.append("			<HRMOV/> ");
		xmlRequestBody.append("			<VLRINSS/> ");
		xmlRequestBody.append("			<CONFIRMADA/> ");
		xmlRequestBody.append("			<IRINNAVIO/> ");
		xmlRequestBody.append("			<DTREF/> ");
		xmlRequestBody.append("			<ALIQIRF/> ");
		xmlRequestBody.append("			<NUCONFATUAL/> ");
		xmlRequestBody.append("			<LOCEMBARQ/> ");
		xmlRequestBody.append("			<CNPJADQUIRENTE/> ");
		xmlRequestBody.append("			<AD_DTPREVENT/> ");
		xmlRequestBody.append("			<AD_AVAL_VENDEDOR/> ");
		xmlRequestBody.append("			<CODVEICULO/> ");
		xmlRequestBody.append("			<VLRSEG/> ");
		xmlRequestBody.append("			<VLRREPREDTOT/> ");
		xmlRequestBody.append("			<DHPROTOC/> ");
		xmlRequestBody.append("			<NUMALEATORIOCTE/> ");
		xmlRequestBody.append("			<PESOAENTREGAR/> ");
		xmlRequestBody.append("			<PREMIACAOESTADUAL/> ");
		xmlRequestBody.append("			<AD_COMISSAO_TBM/> ");
		xmlRequestBody.append("			<TROCO/> ");
		xmlRequestBody.append("			<DHEMISSEPEC/> ");
		xmlRequestBody.append("			<RATEADO/> ");
		xmlRequestBody.append("			<CODPARCCONSIGNATARIO/> ");
		xmlRequestBody.append("			<VLRMERCADORIA/> ");
		xmlRequestBody.append("			<VLRJURO/> ");
		xmlRequestBody.append("			<VLRDESTAQUE/> ");
		xmlRequestBody.append("			<VLRDESCSERV/> ");
		xmlRequestBody.append("			<AD_CUPOMFISCAL/> ");
		xmlRequestBody.append("			<NUMCONTRATO/> ");
		xmlRequestBody.append("			<OBSERVACAOAC/> ");
		xmlRequestBody.append("			<REBOQUE2/> ");
		xmlRequestBody.append("			<DHTIPVENDA/> ");
		xmlRequestBody.append("			<CODDOCARRECAD/> ");
		xmlRequestBody.append("			<CODUSU>" + codUsu + "</CODUSU> ");
		xmlRequestBody.append("			<CODUSUINC>" + codUsu + "</CODUSUINC> ");
		xmlRequestBody.append("			<CODEMP>" + codEmp + "</CODEMP> ");
		xmlRequestBody.append("			<ISSRETIDO>" + issRetido + "</ISSRETIDO> ");
		xmlRequestBody.append("			<NUMNOTA>" + nuNota + "</NUMNOTA> ");
		xmlRequestBody.append("			<CODEMPFUNC>" + codEmp + "</CODEMPFUNC> ");
		xmlRequestBody.append("			<IRFRETIDO>" + irfRetido + "</IRFRETIDO> ");
		xmlRequestBody.append("			<CODUSUCOMPRADOR>" + codComprador
				+ "</CODUSUCOMPRADOR> ");
		xmlRequestBody.append("			<CODNAT>" + codNat + "</CODNAT> ");
		xmlRequestBody.append("			<CODCENCUS>" + centroResultado
				+ "</CODCENCUS> ");
		xmlRequestBody.append("			<TIPFRETE>" + tipFrete + "</TIPFRETE> ");
		xmlRequestBody.append("			<DTNEG>" + dataNeg + "</DTNEG> ");
		xmlRequestBody.append("			<CIF_FOB>" + tipoFrete + "</CIF_FOB> ");
		xmlRequestBody.append("			<TIPMOV>" + tipoMov + "</TIPMOV> ");
		xmlRequestBody.append("			<CODTIPOPER>" + codTop + "</CODTIPOPER> ");
		xmlRequestBody.append("			<INDPRESNFCE>" + IndNFCe + "</INDPRESNFCE> ");
		xmlRequestBody.append("			<TIPIPIEMB>" + tipIpiEmb + "</TIPIPIEMB> ");
		xmlRequestBody.append("			<DHTIPOPER>" + dataTop + "</DHTIPOPER> ");
		xmlRequestBody.append(" 	</cabecalho> ");
		xmlRequestBody.append(" 	<txProperties> ");
		xmlRequestBody
				.append(" 		<prop name='cabecalhoNota.inserindo.pedidoWeb' value='false'/> ");
		xmlRequestBody
				.append(" 		<prop name='br.com.sankhya.mgefin.recalculo.custopreco.Automatico' value='false'/> ");
		xmlRequestBody
				.append(" 	 	<prop name='br.com.sankhya.mgefin.checarfinanceiro.RecalcularVencimento' value='false'/> ");
		xmlRequestBody
				.append(" 		<prop name='br.com.sankhya.mgefin.checarfinanceiro.VlrEntrada' value='0'/> ");
		xmlRequestBody.append(" 	</txProperties> ");
		xmlRequestBody.append(" </nota> ");
		xmlRequestBody.append(" 	<clientEventList> ");
		xmlRequestBody
				.append(" 		<clientEvent>br.com.sankhya.aprovar.nota.apos.baixa</clientEvent> ");
		xmlRequestBody
				.append(" 		<clientEvent>br.com.sankhya.mgecomercial.event.estoque.componentes</clientEvent> ");
		xmlRequestBody
				.append(" 		<clientEvent>br.com.sankhya.mgecomercial.event.compensacao.credito.debito</clientEvent> ");
		xmlRequestBody
				.append(" 		<clientEvent>br.com.sankhya.mgeprod.producao.terceiro.inclusao.item.nota</clientEvent> ");
		xmlRequestBody
				.append(" 		<clientEvent>br.com.sankhya.actionbutton.clientconfirm</clientEvent> ");
		xmlRequestBody
				.append(" 		<clientEvent>central.save.grade.itens.mostrar.popup.serie</clientEvent> ");
		xmlRequestBody
				.append(" 		<clientEvent>br.com.sankhya.mgecom.parcelas.financeiro</clientEvent> ");
		xmlRequestBody
				.append(" 		<clientEvent>br.com.sankhya.mgefin.event.fixa.vencimento</clientEvent> ");
		xmlRequestBody
				.append(" 		<clientEvent>br.com.sankhya.mgecom.compra.SolicitacaoComprador</clientEvent> ");
		xmlRequestBody
				.append(" 		<clientEvent>br.com.sankhya.mgecomercial.event.cadastrarDistancia</clientEvent> ");
		xmlRequestBody
				.append(" 		<clientEvent>br.com.sankhya.exibir.variacao.valor.item</clientEvent> ");
		xmlRequestBody
				.append(" 		<clientEvent>br.com.sankhya.mgecom.event.troca.item.por.produto.alternativo</clientEvent> ");
		xmlRequestBody
				.append(" 		<clientEvent>br.com.utiliza.dtneg.servidor</clientEvent> ");
		xmlRequestBody
				.append(" 		<clientEvent>br.com.sankhya.mgecom.expedicao.SolicitarUsuarioConferente</clientEvent> ");
		xmlRequestBody
				.append(" 		<clientEvent>br.com.sankhya.mgecom.imobilizado</clientEvent> ");
		xmlRequestBody
				.append(" 		<clientEvent>br.com.sankhya.checkout.obter.peso</clientEvent> ");
		xmlRequestBody
				.append(" 		<clientEvent>br.com.sankhya.mgecom.valida.ChaveNFeCompraTerceiros</clientEvent> ");
		xmlRequestBody
				.append(" 		<clientEvent>br.com.sankhya.mgecom.nota.adicional.SolicitarUsuarioGerente</clientEvent> ");
		xmlRequestBody
				.append(" 		<clientEvent>br.com.sankhya.mgecom.central.itens.VendaCasada</clientEvent> ");
		xmlRequestBody
				.append(" 		<clientEvent>br.com.sankhya.mgecom.coleta.entrega.recalculado</clientEvent> ");
		xmlRequestBody
				.append(" 		<clientEvent>br.com.sankhya.mgecom.cancelamento.notas.remessa</clientEvent> ");
		xmlRequestBody
				.append(" 		<clientEvent>br.com.sankhya.mgecomercial.event.faturamento.confirmacao</clientEvent> ");
		xmlRequestBody
				.append(" 		<clientEvent>br.com.sankhya.exibe.msg.variacao.preco</clientEvent> ");
		xmlRequestBody
				.append(" 		<clientEvent>br.com.sankhya.mgecomercial.event.estoque.insuficiente.produto</clientEvent> ");
		xmlRequestBody
				.append(" 		<clientEvent>br.com.sankhya.mgecom.central.itens.KitRevenda.msgValidaFormula</clientEvent> ");
		xmlRequestBody
				.append(" 		<clientEvent>central.save.grade.itens.mostrar.popup.info.lote</clientEvent> ");
		xmlRequestBody
				.append(" 		<clientEvent>br.com.sankhya.mgecom.event.troca.item.por.produto.substituto</clientEvent> ");
		xmlRequestBody
				.append(" 		<clientEvent>br.com.sankhya.mgefin.solicitacao.liberacao.orcamento</clientEvent> ");
		xmlRequestBody
				.append(" 		<clientEvent>br.com.sankhya.mgecom.central.itens.KitRevenda</clientEvent> ");
		xmlRequestBody
				.append(" 		<clientEvent>br.com.sankhya.mgecomercial.event.baixaPortal</clientEvent> ");
		xmlRequestBody
				.append(" 		<clientEvent>br.com.sankhya.comercial.solicitaContingencia</clientEvent> ");
		xmlRequestBody
				.append(" 		<clientEvent>br.com.sankhya.importacaoxml.cfi.para.produto</clientEvent> ");
		xmlRequestBody.append(" 	</clientEventList> ");

		// Retorno do Servi�o
		Document document = serviceInvoker.call(
				"CACSP.incluirAlterarCabecalhoNota", "mgecom",
				xmlRequestBody.toString());
		Node item = document.getChildNodes().item(0);

		if ("1".equals(item.getAttributes().getNamedItem("status")
				.getTextContent())) {
			return document.getElementsByTagName("NUNOTA").item(0)
					.getTextContent();
		} else {
			return XMLUtil.xmlToString(document);
		}
	}

	public Document liberarLimitesOrcamentarios(String nuTransf,
			String sequencia, String sequenciaItem, String user, String senha,
			String liberador, StringBuffer mensagem) throws Exception {

		String requestBody = "<params>" + "	<NUTRANSF>"
				+ nuTransf
				+ "</NUTRANSF>                                                                                                     "
				+ "	<SEQUENCIA>"
				+ sequencia
				+ "</SEQUENCIA>                                                                                                     "
				+ "	<SEQITEM>"
				+ sequenciaItem
				+ "</SEQITEM>                                                                                                     "
				+ "	<USUARIO>"
				+ user
				+ "</USUARIO>                                                                                                     "
				+ "	<SENHA>"
				+ senha
				+ "</SENHA>                                                                                                     "
				+ "	<SOULIBERADOR>"
				+ liberador
				+ "</SOULIBERADOR>                                                                                                     "
				+ "</params>                                                                                                                    "
				+ "<clientEventList>                                                                                                          "
				+ "	<clientEvent>br.com.sankhya.mge.alterar.usuario.liberador</clientEvent>                                                "
				+ "	<clientEvent>br.com.sankhya.mgecom.valida.ChaveNFeCompraTerceiros</clientEvent>                                        "
				+ "	<clientEvent>br.com.sankhya.actionbutton.clientconfirm</clientEvent>                                    "
				+ "</clientEventList>                                                                                                        ";
		mensagem.append("requestBody: " + requestBody);

		return serviceInvoker.call("LiberacaoLimitesOrcamentariosSP.liberar",
				"mge", requestBody.toString());
	}

	public JsonObject liberarLimitesOrcamentarios2(String nuTransf,
			String sequencia, String sequenciaItem, String user, String senha,
			String liberador) throws Exception {

		StringBuilder xmlRequestBody = new StringBuilder();

		xmlRequestBody
				.append("params: {nuTransf: "
						+ nuTransf
						+ ","
						+ "sequencia: "
						+ sequencia
						+ ","
						+ "seqItem: "
						+ sequenciaItem
						+ ","
						+ "usuario: "
						+ user
						+ ","
						+ "senha: "
						+ senha
						+ ","
						+ "souLiberador: "
						+ liberador
						+ ","
						+ "clientEventList:"
						+ "{clientEvent:[{$:br.com.sankhya.mge.alterar.usuario.liberador},"
						+ "{$:br.com.sankhya.mgecom.valida.ChaveNFeCompraTerceiros},"
						+ "{$:br.com.sankhya.actionbutton.clientconfirm}]}}");

		JsonObject docResp = serviceInvoker.callAsJson(
				"LiberacaoLimitesOrcamentariosSP.liberar", "mge",
				xmlRequestBody.toString());
		System.out.println(docResp.toString());

		return docResp;

	}

	public JsonObject confirmarNotas2(String nuNota) throws Exception {

		StringBuilder xmlRequestBody = new StringBuilder();

		// Composicao da Consulta
		xmlRequestBody
				.append("notas:{nunota:[{$:"
						+ nuNota
						+ "}]},"
						+ "clientEventList:"
						+ "{clientEvent:[{$:br.com.sankhya.mge.alterar.usuario.liberador},"
						+ "{$:br.com.sankhya.mgecom.valida.ChaveNFeCompraTerceiros},"
						+ "{$:br.com.sankhya.actionbutton.clientconfirm}]}");

		JsonObject docResp = serviceInvoker.callAsJson(
				"ServicosNfeSP.confirmarNotas", "mgecom",
				xmlRequestBody.toString());
		System.out.println(docResp.toString());

		return docResp;

	}

	public Document confirmarNota(BigDecimal nuNota) throws Exception {

		String requestBody = "<nota confirmacaoCentralNota='true' ehPedidoWeb='false' atualizaPrecoItemPedCompra='false' ownerServiceCall='CentralNotas'>"
				+ "	<NUNOTA>"
				+ nuNota
				+ "</NUNOTA>                                                                                                     "
				+ "</nota>                                                                                                                    "
				+ "<clientEventList>                                                                                                          "
				+ "	<clientEvent>br.com.sankhya.mgecomercial.event.baixaPortal</clientEvent>                                                "
				+ "	<clientEvent>br.com.sankhya.mgecomercial.event.estoque.componentes</clientEvent>                                        "
				+ "	<clientEvent>br.com.sankhya.mgecomercial.event.faturamento.confirmacao</clientEvent>                                    "
				+ "	<clientEvent>central.save.grade.itens.mostrar.popup.serie</clientEvent>                                                 "
				+ "	<clientEvent>br.com.sankhya.mgecom.central.itens.KitRevenda</clientEvent>                                               "
				+ "	<clientEvent>br.com.sankhya.mgecom.valida.ChaveNFeCompraTerceiros</clientEvent>                                         "
				+ "	<clientEvent>br.com.sankhya.mgecomercial.event.cadastrarDistancia</clientEvent>                                         "
				+ "	<clientEvent>br.com.sankhya.actionbutton.clientconfirm</clientEvent>                                                    "
				+ "	<clientEvent>br.com.sankhya.mgefin.solicitacao.liberacao.orcamento</clientEvent>                                        "
				+ "	<clientEvent>central.save.grade.itens.mostrar.popup.info.lote</clientEvent>                                             "
				+ "	<clientEvent>br.com.sankhya.mgecom.event.troca.item.por.produto.alternativo</clientEvent>                               "
				+ "	<clientEvent>br.com.sankhya.mgecomercial.event.estoque.insuficiente.produto</clientEvent>                               "
				+ "	<clientEvent>br.com.sankhya.mgecom.expedicao.SolicitarUsuarioConferente</clientEvent>                                   "
				+ "	<clientEvent>br.com.sankhya.mgecom.event.troca.item.por.produto.substituto</clientEvent>                                "
				+ "	<clientEvent>br.com.sankhya.mgecom.cancelamento.notas.remessa</clientEvent>                                             "
				+ "	<clientEvent>br.com.sankhya.exibir.variacao.valor.item</clientEvent>                                                    "
				+ "	<clientEvent>br.com.sankhya.mgeprod.producao.terceiro.inclusao.item.nota</clientEvent>                                  "
				+ "	<clientEvent>br.com.sankhya.mgecom.compra.SolicitacaoComprador</clientEvent>                                            "
				+ "	<clientEvent>br.com.sankhya.mgefin.event.fixa.vencimento</clientEvent>                                                  "
				+ "	<clientEvent>br.com.sankhya.exibe.msg.variacao.preco</clientEvent>                                                      "
				+ "	<clientEvent>br.com.sankhya.mgecom.coleta.entrega.recalculado</clientEvent>                                             "
				+ "	<clientEvent>br.com.sankhya.mgecom.parcelas.financeiro</clientEvent>                                                    "
				+ "	<clientEvent>br.com.sankhya.mgecom.imobilizado</clientEvent>                                                            "
				+ "	<clientEvent>br.com.sankhya.mgecom.central.itens.VendaCasada</clientEvent>                                              "
				+ "	<clientEvent>br.com.sankhya.mgecom.nota.adicional.SolicitarUsuarioGerente</clientEvent>                                 "
				+ "	<clientEvent>br.com.utiliza.dtneg.servidor</clientEvent>                                                                "
				+ "	<clientEvent>br.com.sankhya.checkout.obter.peso</clientEvent>                                                           "
				+ "	<clientEvent>br.com.sankhya.mgecom.central.itens.KitRevenda.msgValidaFormula</clientEvent>                              "
				+ "	<clientEvent>br.com.sankhya.importacaoxml.cfi.para.produto</clientEvent>                                                "
				+ "	<clientEvent>br.com.sankhya.mgecomercial.event.compensacao.credito.debito</clientEvent>                                 "
				+ "</clientEventList>                                                                                                        ";

		return serviceInvoker.call("CACSP.confirmarNota", "mgecom",
				requestBody.toString());
	}

	public String itensRequisicao(Long numUnico, int valorPadrao,
			String codEnqIpi, BigDecimal valorTotal, String ncm, int codProd,
			BigDecimal unidPadrao, String statusNota, int codLocal,
			int origemProd, int qtdeUnidPad, String usoProd, String pendente,
			int codComprador, String dataAlter, BigDecimal qtdeNeg,
			String atualizaEstqTerc, int cstIpi, BigDecimal valorUnit,
			String complDescProd, String codVol) throws Exception {
		StringBuilder xmlRequestBody = new StringBuilder();
		xmlRequestBody.append(" <nota NUNOTA='" + numUnico
				+ "' ownerServiceCall='GradeItens684'> ");
		xmlRequestBody.append(" 	<itens ATUALIZACAO_ONLINE='false'> ");
		xmlRequestBody.append(" 		<item> ");
		xmlRequestBody.append("					<SEQUENCIA/> ");
		xmlRequestBody.append("					<NUMDOCARRECAD/> ");
		xmlRequestBody.append("					<PRODUTOALTERNATIVO/> ");
		xmlRequestBody.append("					<VLRUNITLOC/> ");
		xmlRequestBody.append("					<VARIACAOFCP/> ");
		xmlRequestBody.append("					<AD_PRECTAB/> ");
		xmlRequestBody.append("					<PERCGERMIN/> ");
		xmlRequestBody.append("					<GRUPOTRANSG/> ");
		xmlRequestBody.append("					<OPERATUAL/> ");
		xmlRequestBody.append("					<PERCPUREZA/> ");
		xmlRequestBody.append("					<QTDPECA/> ");
		xmlRequestBody.append("					<QTDFIXADA/> ");
		xmlRequestBody.append("					<DTINICIO/> ");
		xmlRequestBody.append("					<PERCCOM/> ");
		xmlRequestBody.append("					<AD_QTDVOLPARACOLETAR/> ");
		xmlRequestBody.append("					<BASEISS/> ");
		xmlRequestBody.append("					<REFERENCIA/> ");
		xmlRequestBody.append("					<AD_QTDORIGINAL/> ");
		xmlRequestBody.append("					<VLRSTFCPINTANT/> ");
		xmlRequestBody.append("					<PERCREDVLRIPI/> ");
		xmlRequestBody.append("					<CODLOCALDEST/> ");
		xmlRequestBody.append("					<CONTROLEDEST/> ");
		xmlRequestBody.append("					<VLRSUGERIDO/> ");
		xmlRequestBody.append("					<ALIQISS/> ");
		xmlRequestBody.append("					<QTDTRIBEXPORT/> ");
		xmlRequestBody.append("					<VLRTOTLIQREF/> ");
		xmlRequestBody.append("					<VLRPROMO/> ");
		xmlRequestBody.append("					<CONTROLE/> ");
		xmlRequestBody.append("					<MARCA/> ");
		xmlRequestBody.append("					<BASESTFCPINTANT/> ");
		xmlRequestBody.append("					<CODMOTDESONERAICMS/> ");
		xmlRequestBody.append("					<CODCFPS/> ");
		xmlRequestBody.append("					<NRSERIERESERVA/> ");
		xmlRequestBody.append("					<VLRISS/> ");
		xmlRequestBody.append("					<BASESUBSTITUNITORIG/> ");
		xmlRequestBody.append("					<AD_COTACAO/> ");
		xmlRequestBody.append("					<REFFORN/> ");
		xmlRequestBody.append("					<OBSERVACAO/> ");
		xmlRequestBody.append("					<CODBENEFNAUF/> ");
		xmlRequestBody.append("					<CODAGREGACAO/> ");
		xmlRequestBody.append("					<PRODUTOPESQUISADO/> ");
		xmlRequestBody.append("					<QTDFAT/> ");
		xmlRequestBody.append("					<CODANTECIPST/> ");
		xmlRequestBody.append("					<QTDFORMULA/> ");
		xmlRequestBody.append("					<CODTRIBISS/> ");
		xmlRequestBody.append("					<CODOBSPADRAO/> ");
		xmlRequestBody.append("					<PERCDESCBONIF/> ");
		xmlRequestBody.append("					<ESTOQUE/> ");
		xmlRequestBody.append("					<SEQPEDIDO2/> ");
		xmlRequestBody.append("					<ORIGEMBUSCA/> ");
		xmlRequestBody.append("					<CODESPECST/> ");
		xmlRequestBody.append("					<CODCAV/> ");
		xmlRequestBody.append("					<BASESUBSTITANT/> ");
		xmlRequestBody.append("					<PERCCOMGER/> ");
		xmlRequestBody.append("					<VLRSUBSTUNITORIG/> ");
		xmlRequestBody.append("					<DTVIGOR/> ");
		xmlRequestBody.append("					<AD_QTDFARDOS/> ");
		xmlRequestBody.append("					<AD_NUMCOTACAO/> ");
		xmlRequestBody.append("					<VLRDESCDIGITADO/> ");
		xmlRequestBody.append("					<AD_PERCCOMCALC/> ");
		xmlRequestBody.append("					<PERCDESCDIGITADO/> ");
		xmlRequestBody.append("					<VLRLIQPROM/> ");
		xmlRequestBody.append("					<PERCDESCTGFDES/> ");
		xmlRequestBody.append("					<AD_IDLOCALENDERECO/> ");
		xmlRequestBody.append("					<ALTPRECO/> ");
		xmlRequestBody.append("					<PERCDESCBASE/> ");
		xmlRequestBody.append("					<SEQUENCIAFISCAL/> ");
		xmlRequestBody.append("					<NUTAB/> ");
		xmlRequestBody.append("					<PERCDESCPROM/> ");
		xmlRequestBody.append("					<CNPJFABRICANTE/> ");
		xmlRequestBody.append("					<VLRTROCA/> ");
		xmlRequestBody.append("					<PERCSTFCPINTANT/> ");
		xmlRequestBody.append("					<CODVOLPARC/> ");
		xmlRequestBody.append("					<AD_LOTECONTRATO/> ");
		xmlRequestBody.append("					<VLRUNITDOLAR/> ");
		xmlRequestBody.append("					<TIPOSEPARACAO/> ");
		xmlRequestBody.append("					<BASEICMS/> ");
		xmlRequestBody.append("					<PRECOBASEQTD/> ");
		xmlRequestBody.append("					<NUMPEDIDO2/> ");
		xmlRequestBody.append("					<NUPROMOCAO/> ");
		xmlRequestBody.append("					<INDESCALA/> ");
		xmlRequestBody.append("					<CSOSN/> ");
		xmlRequestBody.append("					<CODPROMO/> ");
		xmlRequestBody.append("					<VLRCOM/> ");
		xmlRequestBody.append("					<BASICMSTMOD/> ");
		xmlRequestBody.append("					<AD_COD_ORACLE/> ");
		xmlRequestBody.append("					<VLRICMSANT/> ");
		xmlRequestBody.append("					<VLRICMSDIFERIDO/> ");
		xmlRequestBody.append("					<AD_IDIPROC/> ");
		xmlRequestBody.append("					<VLRSUBSTANT/> ");
		xmlRequestBody.append("					<NROPROCESSO/> ");
		xmlRequestBody.append("					<NUFOP/> ");
		xmlRequestBody.append("					<PERCDESCFORNECEDOR/> ");
		xmlRequestBody.append("					<VLRPTOPUREZA/> ");
		xmlRequestBody.append("					<VLRVENDAPROMO/> ");
		xmlRequestBody.append("					<AD_CODCLAFIBRA/> ");
		xmlRequestBody.append("					<CODPROC/> ");
		xmlRequestBody.append("					<CODTPA/> ");
		xmlRequestBody.append("					<NUMEROOS/> ");
		xmlRequestBody.append("					<ENDIMAGEM/> ");
		xmlRequestBody.append("					<CODLOCALTERC/> ");
		xmlRequestBody.append("					<AD_VLRCOM/> ");
		xmlRequestBody.append("					<NUMCONTRATO/> ");
		xmlRequestBody.append("					<STATUSPROC/> ");
		xmlRequestBody.append("					<GTINTRIBNFE/> ");
		xmlRequestBody.append("					<VLRCOMGER/> ");
		xmlRequestBody.append("					<BASICMMOD/> ");
		xmlRequestBody.append("					<CODDOCARRECAD/> ");
		xmlRequestBody.append("					<ALIQSTFCPSTANT/> ");
		xmlRequestBody.append("					<GTINNFE/> ");
		xmlRequestBody.append("				    <NUNOTA>" + numUnico + "</NUNOTA> ");
		xmlRequestBody.append("					<REDBASEST>" + valorPadrao
				+ "</REDBASEST> ");
		xmlRequestBody.append("					<VLRIPI>" + valorPadrao + "</VLRIPI> ");
		xmlRequestBody.append("					<CODENQIPI>" + codEnqIpi + "</CODENQIPI> ");
		xmlRequestBody.append("					<PESOLIQ>" + valorPadrao + "</PESOLIQ> ");
		xmlRequestBody.append("					<PESOBRUTO>" + valorPadrao
				+ "</PESOBRUTO> ");
		xmlRequestBody.append("					<VLRTOT>" + valorTotal + "</VLRTOT> ");
		xmlRequestBody.append("					<VLRCUS>" + valorPadrao + "</VLRCUS> ");
		xmlRequestBody.append("					<VLRTOTMOE>" + valorPadrao
				+ "</VLRTOTMOE> ");
		xmlRequestBody.append("					<BASESUBSTSEMRED>" + valorPadrao
				+ "</BASESUBSTSEMRED> ");
		xmlRequestBody.append("					<NCM>" + ncm + "</NCM> ");
		xmlRequestBody.append("					<CODPROD>" + codProd + "</CODPROD> ");
		xmlRequestBody.append("					<VLRUNIDPAD>" + unidPadrao
				+ "</VLRUNIDPAD> ");
		xmlRequestBody.append("					<STATUSNOTA>" + statusNota
				+ "</STATUSNOTA> ");
		xmlRequestBody.append("					<VLRUNITLIQMOE>" + valorPadrao
				+ "</VLRUNITLIQMOE> ");
		xmlRequestBody.append("					<VLRTOTLIQMOE>" + valorPadrao
				+ "</VLRTOTLIQMOE> ");
		xmlRequestBody.append("					<CODLOCALORIG>" + codLocal
				+ "</CODLOCALORIG> ");
		xmlRequestBody.append("					<ORIGPROD>" + origemProd + "</ORIGPROD> ");
		xmlRequestBody.append("					<QTDUNIDPAD>" + qtdeNeg + "</QTDUNIDPAD> ");
		xmlRequestBody.append("					<ALTURA>" + valorPadrao + "</ALTURA> ");
		xmlRequestBody.append("					<VLRREPREDSEMDESC>" + valorPadrao
				+ "</VLRREPREDSEMDESC> ");
		xmlRequestBody.append("					<VLRRETENCAO>" + valorPadrao
				+ "</VLRRETENCAO> ");
		xmlRequestBody.append("					<CODCFO>" + valorPadrao + "</CODCFO> ");
		xmlRequestBody.append("					<CODEXEC>" + valorPadrao + "</CODEXEC> ");
		xmlRequestBody.append("					<QTDCONFERIDA>" + valorPadrao
				+ "</QTDCONFERIDA> ");
		xmlRequestBody.append("					<CODPARCEXEC>" + valorPadrao
				+ "</CODPARCEXEC> ");
		xmlRequestBody.append("					<BASECALCSTEXTRANOTA>" + valorPadrao
				+ "</BASECALCSTEXTRANOTA> ");
		xmlRequestBody.append("					<M3>" + valorPadrao + "</M3> ");
		xmlRequestBody.append("					<USOPROD>" + usoProd + "</USOPROD> ");
		xmlRequestBody.append("					<VLRDESCBONIF>" + valorPadrao
				+ "</VLRDESCBONIF> ");
		xmlRequestBody.append("					<PENDENTE>" + pendente + "</PENDENTE> ");
		xmlRequestBody.append("					<CODVEND>" + codComprador + "</CODVEND> ");
		xmlRequestBody.append("					<VLRSTEXTRANOTA>" + valorPadrao
				+ "</VLRSTEXTRANOTA> ");
		xmlRequestBody.append("					<QTDENTREGUE>" + valorPadrao
				+ "</QTDENTREGUE> ");
		xmlRequestBody.append("					<DTALTER>" + dataAlter + "</DTALTER> ");
		xmlRequestBody.append("					<RESERVADO>" + valorPadrao
				+ "</RESERVADO> ");
		xmlRequestBody.append("					<ATUALESTTERC>" + atualizaEstqTerc
				+ "</ATUALESTTERC> ");
		xmlRequestBody.append("					<CSTIPI>" + cstIpi + "</CSTIPI> ");
		xmlRequestBody.append("				    <QTDNEG>" + qtdeNeg + "</QTDNEG> ");
		xmlRequestBody.append("					<ESPESSURA>" + valorPadrao
				+ "</ESPESSURA> ");
		xmlRequestBody.append("					<VLRUNIT>" + valorUnit + "</VLRUNIT> ");
		xmlRequestBody.append("					<COMPLDESC>" + complDescProd
				+ "</COMPLDESC> ");
		xmlRequestBody.append("					<CODVOL>" + codVol + "</CODVOL> ");
		xmlRequestBody.append("					<CODVOLPAD>" + codVol + "</CODVOLPAD> ");
		xmlRequestBody.append("					<QTDPENDENTE>" + qtdeNeg
				+ "</QTDPENDENTE> ");
		xmlRequestBody.append("					<PERCDESC>" + valorPadrao + "</PERCDESC> ");
		xmlRequestBody.append("					<VLRDESC>" + valorPadrao + "</VLRDESC> ");
		xmlRequestBody.append(" 		</item> ");
		xmlRequestBody.append(" 	</itens> ");
		xmlRequestBody.append(" 	<txProperties> ");
		xmlRequestBody
				.append(" 		<prop name='br.com.sankhya.mgefin.mostrar.sugestao.venda' value='true'/> ");
		xmlRequestBody
				.append(" 		<prop name='br.com.sankhya.mgecom.gradeItens.pedidoWeb' value='false'/> ");
		xmlRequestBody
				.append(" 		<prop name='br.com.sankhya.mgefin.recalculo.custopreco.Automatico' value='false'/> ");
		xmlRequestBody
				.append(" 		<prop name='br.com.sankhya.mgefin.checarfinanceiro.VlrEntrada' value='0'/> ");
		xmlRequestBody.append(" 	</txProperties> ");
		xmlRequestBody.append(" </nota> ");
		xmlRequestBody.append("		<clientEventList> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.mgecom.coleta.entrega.recalculado</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.mgecom.central.itens.VendaCasada</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.mgecom.expedicao.SolicitarUsuarioConferente</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.mgecom.valida.ChaveNFeCompraTerceiros</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.mgecom.central.itens.KitRevenda</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.mgecom.nota.adicional.SolicitarUsuarioGerente</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.mgecomercial.event.compensacao.credito.debito</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.mgecom.central.itens.KitRevenda.msgValidaFormula</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.mgecom.parcelas.financeiro</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.mgecomercial.event.baixaPortal</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>central.save.grade.itens.mostrar.popup.serie</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.mgefin.event.fixa.vencimento</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.mgefin.solicitacao.liberacao.orcamento</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.exibir.variacao.valor.item</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.mgecomercial.event.cadastrarDistancia</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.mgecom.cancelamento.notas.remessa</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.mgecomercial.event.estoque.insuficiente.produto</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.utiliza.dtneg.servidor</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>central.save.grade.itens.mostrar.popup.info.lote</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.mgecom.event.troca.item.por.produto.alternativo</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.mgecomercial.event.faturamento.confirmacao</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.mgecomercial.event.estoque.componentes</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.mgecom.compra.SolicitacaoComprador</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.mgecom.event.troca.item.por.produto.substituto</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.mgecom.imobilizado</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.checkout.obter.peso</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.importacaoxml.cfi.para.produto</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.actionbutton.clientconfirm</clientEvent> ");
		xmlRequestBody
				.append("					<clientEvent>br.com.sankhya.exibe.msg.variacao.preco</clientEvent> ");
		xmlRequestBody.append("		</clientEventList> ");
		// Retorno do Servi�o
		Document document = serviceInvoker.call("CACSP.incluirAlterarItemNota",
				"mgecom", xmlRequestBody.toString());
		Node item = document.getChildNodes().item(0);

		if ("1".equals(item.getAttributes().getNamedItem("status")
				.getTextContent())) {
			return document.getElementsByTagName("SEQUENCIA").item(0)
					.getTextContent();
		} else {
			return XMLUtil.xmlToString(document);
		}

	}

	public String saveCabecalhoPedido(Long nuNota, String numNota,
			String codEmp, String codTipoOper, String dhTipOper,
			String tipoMovimento, String codParc, String codParcDest,
			String codTipVenda, String dhTipVenda, String codCencus,
			String codNat, String serieNota, Long qtdVol, BigDecimal pesoLiq,
			BigDecimal pesoBruto, String observacao, Long idiproc,
			String codVend, String dtFatur, String dtFaturPrev,
			String tipoPedido, String numPedido2, String observacaoAdicional,
			String cifFob, String prevEntrega, String avalVend,
			String possuiItemPet) throws Exception {
		DecimalFormat decimalFormat = new DecimalFormat();
		decimalFormat.setMaximumFractionDigits(2);
		decimalFormat.setMinimumFractionDigits(2);

		SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
		SimpleDateFormat formatter2 = new SimpleDateFormat(
				"dd/MM/yyyy hh:mm:ss");
		Date dhNeg = new Date();

		// Parceiro Entrega
		// String codParcDest = "0";
		// String numNota = "0";
		String dtNeg = formatter.format(dhNeg) + " 0:00:0";

		String tipFrete = "N";

		// Empresa
		String codEmpFunc = codEmp;

		// A definir posteriormente
		String tipIPIEmb = "N";
		String indPresNfce = "0";
		String issRetido = "N";
		String irfRetido = "S";

		// Composicao da consulta
		StringBuilder xmlRequestBody = new StringBuilder();
		xmlRequestBody.append(" <nota ownerServiceCall='CentralNotas'> ");
		xmlRequestBody.append(" 	<cabecalho> ");
		if (!"677".equals(codTipoOper)) {
			xmlRequestBody.append(" 	<NUMNOTA>" + numNota + "</NUMNOTA>");
		}

		if (numPedido2 != null) {
			xmlRequestBody.append(" 	    <NUMPEDIDO2>" + numPedido2
					+ "</NUMPEDIDO2>");
		} else {
			xmlRequestBody.append(" 	<NUMPEDIDO2/> ");
		}

		xmlRequestBody.append(" 	    <AD_POSSUIITEMPET>" + possuiItemPet
				+ "</AD_POSSUIITEMPET>");
		xmlRequestBody.append(" 	    <AD_AVAL_VENDEDOR>" + avalVend
				+ "</AD_AVAL_VENDEDOR>");
		xmlRequestBody.append(" 	    <AD_TIPOPEDIDO>" + tipoPedido
				+ "</AD_TIPOPEDIDO>");
		xmlRequestBody.append(" 	    <TIPMOV>" + tipoMovimento + "</TIPMOV>");
		xmlRequestBody.append(" 	    <CODTIPOPER>" + codTipoOper
				+ "</CODTIPOPER>");
		xmlRequestBody.append("			<DHTIPOPER>" + dhTipOper + "</DHTIPOPER> ");
		xmlRequestBody.append("			<CODPARC>" + codParc + "</CODPARC> ");
		xmlRequestBody.append(" 		<CODPARCDEST>" + codParcDest
				+ "</CODPARCDEST>");
		xmlRequestBody.append(" 	    <CODTIPVENDA>" + codTipVenda
				+ "</CODTIPVENDA>");
		xmlRequestBody.append(" 	    <CODEMP>" + codEmp + "</CODEMP> ");
		xmlRequestBody.append(" 	    <DTNEG>" + dtNeg + "</DTNEG>");
		xmlRequestBody.append(" 	    <DTFATUR>" + dtFatur + "</DTFATUR>");
		if (dtFaturPrev != null) {
			xmlRequestBody.append(" 	    <AD_DTPREVFATUR>" + dtFaturPrev
					+ "</AD_DTPREVFATUR>");
		} else {
			xmlRequestBody.append(" 	    <AD_DTPREVFATUR/>");
		}
		xmlRequestBody.append(" 	    <CODEMPFUNC>" + codEmpFunc
				+ "</CODEMPFUNC>");
		xmlRequestBody.append(" 	    <CIF_FOB>" + cifFob + "</CIF_FOB> ");
		xmlRequestBody.append(" 	    <TIPIPIEMB>" + tipIPIEmb + "</TIPIPIEMB>");
		xmlRequestBody.append(" 	    <TIPFRETE>" + tipFrete + "</TIPFRETE>");
		xmlRequestBody.append(" 	    <INDPRESNFCE>" + indPresNfce
				+ "</INDPRESNFCE>");
		xmlRequestBody.append(" 	    		<ISSRETIDO>" + issRetido
				+ "</ISSRETIDO> ");
		xmlRequestBody.append(" 	    		<IRFRETIDO>" + irfRetido
				+ "</IRFRETIDO> ");
		xmlRequestBody.append("					<DHTIPVENDA>" + dhTipVenda
				+ "</DHTIPVENDA> ");
		xmlRequestBody.append(" 		<CODCENCUS>" + codCencus + "</CODCENCUS>");
		xmlRequestBody.append(" 		<CODNAT>" + codNat + "</CODNAT>");
		xmlRequestBody.append(" 		<DTPREVENT>" + prevEntrega + "</DTPREVENT>");
		xmlRequestBody.append(" 		<SERIENOTA>" + serieNota + "</SERIENOTA>");

		xmlRequestBody.append(" 		<QTDVOL>" + qtdVol + "</QTDVOL> ");
		xmlRequestBody.append(" 		<PESO>" + pesoLiq + "</PESO>");
		xmlRequestBody.append(" 		<PESOBRUTO>" + pesoBruto + "</PESOBRUTO>");

		if (idiproc != null) {
			xmlRequestBody.append(" 	<IDIPROC>" + idiproc + "</IDIPROC> ");
		} else {
			xmlRequestBody.append(" 	<IDIPROC/> ");
		}

		if (observacao != null) {
			xmlRequestBody.append(" 		<OBSERVACAO>" + observacao
					+ "</OBSERVACAO>");
		} else {
			xmlRequestBody.append(" 	<OBSERVACAO/> ");
		}

		if (observacaoAdicional != null) {
			xmlRequestBody.append(" 	<AD_OBSERVACAOPEDIDO>"
					+ observacaoAdicional + "</AD_OBSERVACAOPEDIDO> ");
		} else {
			xmlRequestBody.append(" 	<AD_OBSERVACAOPEDIDO/> ");
		}

		if (codVend != null) {
			xmlRequestBody.append(" 	<CODVEND>" + codVend + "</CODVEND> ");
		} else {
			xmlRequestBody.append(" 	<CODVEND/> ");
		}

		if (nuNota != null) {
			xmlRequestBody.append(" 	<NUNOTA>" + nuNota + "</NUNOTA> ");
		} else {
			xmlRequestBody.append(" 	<NUNOTA></NUNOTA> ");
			xmlRequestBody.append(" 	<CODMOEDA/> ");

			xmlRequestBody.append(" 	<VLRMOEDA/> ");
			xmlRequestBody.append(" 	<CODUSUCOMPRADOR/> ");
			xmlRequestBody.append(" 	<STATUSCTE/> ");
			xmlRequestBody.append(" 	<CODHISTAC/> ");
			xmlRequestBody.append(" 	<CODCIDENTREGA/> ");
			xmlRequestBody.append(" 	<CODVENDTEC/> ");
			xmlRequestBody.append(" 	<DTENVIOPMB/> ");
			xmlRequestBody.append(" 	<CODUSU/> ");
			xmlRequestBody.append(" 	<HRENTSAI/> ");
			xmlRequestBody.append(" 	<VLRCOMPENSACAO/> ");
			xmlRequestBody.append(" 	<BASEPIS/> ");
			xmlRequestBody.append("	    <VLRICMS/> ");
			xmlRequestBody.append(" 	<CODOBSPADRAO/> ");
			xmlRequestBody.append(" 	<REGESPTRIBUT/> ");
			xmlRequestBody.append(" 	<BASESUBSTIT/> ");
			xmlRequestBody.append(" 	<VOLUME/> ");
			xmlRequestBody.append(" 	<VLRCOFINS/> ");
			xmlRequestBody.append(" 	<VLRSUBST/> ");
			xmlRequestBody.append(" 	<M3AENTREGAR/> ");
			xmlRequestBody.append(" 	<NUMCF/> ");
			xmlRequestBody.append(" 	<LOCEMBARQ/> ");
			xmlRequestBody.append(" 	<UFEMBARQ/> ");
			xmlRequestBody.append(" 	<VLRSTEXTRANOTATOT/> ");
			xmlRequestBody.append(" 	<REBOQUE1/> ");
			xmlRequestBody.append(" 	<VLRAFRMM/> ");
			xmlRequestBody.append(" 	<CNPJADQUIRENTE/> ");
			xmlRequestBody.append(" 	<PESOBRUTOMANUAL/> ");
			xmlRequestBody.append(" 	<ORDEMCARGA/> ");
			xmlRequestBody.append(" 	<DTCONTAB/> ");
			xmlRequestBody.append(" 	<NUPEDFRETE/> ");
			xmlRequestBody.append(" 	<CODMODDOCNOTA/> ");
			xmlRequestBody.append(" 	<NUPCA/> ");
			xmlRequestBody.append(" 	<TIPOPTAGJNFE/> ");
			xmlRequestBody.append(" 	<RETORNADOAC/> ");
			xmlRequestBody.append(" 	<NURD8/> ");
			xmlRequestBody.append(" 	<CODPARCCONSIGNATARIO/> ");
			xmlRequestBody.append(" 	<NULOTENFSE/> ");
			xmlRequestBody.append(" 	<VLRICMSFCP/> ");
			xmlRequestBody.append(" 	<DTREF3/> ");
			xmlRequestBody.append(" 	<DHPROTOC/> ");
			xmlRequestBody.append(" 	<STATUSCONFERENCIA/> ");
			xmlRequestBody.append(" 	<NOTAEMPENHO/> ");
			xmlRequestBody.append(" 	<PERMALTERCENTRAL/> ");
			xmlRequestBody.append(" 	<PEDIDOIMPRESSO/> ");
			xmlRequestBody.append(" 	<VLRICMSFCPINT/> ");
			xmlRequestBody.append(" 	<BASEISS/> ");
			xmlRequestBody.append(" 	<DTREF2/> ");
			xmlRequestBody.append(" 	<NUFOP/> ");
			xmlRequestBody.append(" 	<VLRTOTLIQITEMMOE/> ");
			xmlRequestBody.append(" 	<VLRDESCTOTITEMMOE/> ");
			xmlRequestBody.append(" 	<BASEICMSFRETE/> ");
			xmlRequestBody.append(" 	<PESOAENTREGAR/> ");
			xmlRequestBody.append(" 	<TERMACORDNOTA/> ");
			xmlRequestBody.append(" 	<VLRIPI/> ");
			xmlRequestBody.append(" 	<KMVEICULO/> ");
			xmlRequestBody.append(" 	<NUMNFSE/> ");
			xmlRequestBody.append(" 	<CLASSIFICMS/> ");
			xmlRequestBody.append(" 	<VLRISS/> ");
			xmlRequestBody.append(" 	<NULOTECTE/> ");
			xmlRequestBody.append(" 	<DANFE/> ");
			xmlRequestBody.append(" 	<TIPNOTAPMB/> ");
			xmlRequestBody.append(" 	<NUCFR/> ");
			xmlRequestBody.append(" 	<CODPARCREDESPACHO/> ");
			xmlRequestBody.append(" 	<IDNAVIO/> ");
			xmlRequestBody.append(" 	<CODTPD/> ");
			xmlRequestBody.append(" 	<VLRCARGAAVERB/> ");
			xmlRequestBody.append(" 	<CODCIDFIMCTE/> ");
			xmlRequestBody.append(" 	<DTENTSAIINFO/> ");
			xmlRequestBody.append(" 	<VLRINDENIZ/> ");
			xmlRequestBody.append(" 	<TPEMISNFE/> ");
			xmlRequestBody.append(" 	<DHREGDPEC/> ");
			xmlRequestBody.append(" 	<VLRPISST/> ");
			xmlRequestBody.append(" 	<IPIEMB/> ");
			xmlRequestBody.append(" 	<BASECOFINSST/> ");
			xmlRequestBody.append(" 	<VLRLIQITEMNFE/> ");
			xmlRequestBody.append(" 	<CODLOCALORIG/> ");
			xmlRequestBody.append(" 	<CONFIRMNOTAFAT/> ");
			xmlRequestBody.append(" 	<TPAMBNFE/> ");
			xmlRequestBody.append(" 	<VLRICMSEMB/> ");
			xmlRequestBody.append(" 	<PESOBRUTOITENS/> ");
			xmlRequestBody.append(" 	<ICMSFRETE/> ");
			xmlRequestBody.append(" 	<CODPARCREMETENTE/> ");
			xmlRequestBody.append(" 	<VENCFRETE/> ");
			xmlRequestBody.append(" 	<DTMOV/> ");
			xmlRequestBody.append(" 	<DTALTER/> ");
			xmlRequestBody.append(" 	<CODCIDINICTE/> ");
			xmlRequestBody.append(" 	<OBSERVACAOAC/> ");
			xmlRequestBody.append(" 	<STATUSWMS/> ");
			xmlRequestBody.append(" 	<TIPPROCIMP/> ");
			xmlRequestBody.append(" 	<STATUSNOTA/> ");
			xmlRequestBody.append(" 	<DTENVSUF/> ");
			xmlRequestBody.append(" 	<NUODP/> ");
			xmlRequestBody.append(" 	<NUMERACAOVOLUMES/> ");
			xmlRequestBody.append(" 	<STATUSNFE/> ");
			xmlRequestBody.append(" 	<CPFCNPJADQUIRENTE/> ");
			xmlRequestBody.append(" 	<NUMCOTACAO/> ");
			xmlRequestBody.append(" 	<BASEIPI/> ");
			xmlRequestBody.append(" 	<DTREMRET/> ");
			xmlRequestBody.append(" 	<CODVTP/> ");
			xmlRequestBody.append(" 	<COMGER/> ");
			xmlRequestBody.append(" 	<TIPLIBERACAO/> ");
			xmlRequestBody.append(" 	<VLRINDENIZDIST/> ");
			xmlRequestBody.append(" 	<VLRICMSDIFALDEST/> ");
			xmlRequestBody.append(" 	<NUNOTAPEDFRET/> ");
			xmlRequestBody.append(" 	<IDBALSA03/> ");
			xmlRequestBody.append(" 	<TPEMISNFSE/> ");
			xmlRequestBody.append(" 	<CTELOTACAO/> ");
			xmlRequestBody.append(" 	<LACRES/> ");
			xmlRequestBody.append(" 	<VLRSTFCPINTANT/> ");
			xmlRequestBody.append(" 	<MODENTREGA/> ");
			xmlRequestBody.append(" 	<LIBCONF/> ");
			xmlRequestBody.append(" 	<DTVAL/> ");
			xmlRequestBody.append(" 	<CTEGLOBAL/> ");
			xmlRequestBody.append(" 	<IDBALSA02/> ");
			xmlRequestBody.append(" 	<FRETEVLRPAGO/> ");
			xmlRequestBody.append(" 	<VENCIPI/> ");
			xmlRequestBody.append(" 	<DTADIAM/> ");
			xmlRequestBody.append(" 	<CODCC/> ");
			xmlRequestBody.append(" 	<PRODPRED/> ");
			xmlRequestBody.append(" 	<BASEIRF/> ");
			xmlRequestBody.append(" 	<REBOQUE2/> ");
			xmlRequestBody.append(" 	<TPEMISCTE/> ");
			xmlRequestBody.append(" 	<CODEMPNEGOC/> ");
			xmlRequestBody.append(" 	<NULOTENFE/> ");
			xmlRequestBody.append(" 	<CODMAQ/> ");
			xmlRequestBody.append(" 	<VLRICMSSEG/> ");
			xmlRequestBody.append(" 	<TPAMBCTE/> ");
			xmlRequestBody.append(" 	<VLRFRETECALC/> ");
			xmlRequestBody.append(" 	<TPASSINANTE/> ");
			xmlRequestBody.append(" 	<CODRASTREAMENTOECT/> ");
			xmlRequestBody.append(" 	<RATEADO/> ");
			xmlRequestBody.append(" 	<CHAVENFSE/> ");
			xmlRequestBody.append(" 	<VLRFRETETOTAL/> ");
			xmlRequestBody.append(" 	<AGRUPFINNOTA/> ");
			xmlRequestBody.append(" 	<ALIQIRF/> ");
			xmlRequestBody.append(" 	<CODUSUINC/> ");
			xmlRequestBody.append(" 	<VLRVENDOR/> ");
			xmlRequestBody.append(" 	<VLRJURODIST/> ");
			xmlRequestBody.append(" 	<CODCONTATOENTREGA/> ");
			xmlRequestBody.append(" 	<CODCIDDESTINO/> ");
			xmlRequestBody.append(" 	<LOTACAO/> ");
			xmlRequestBody.append(" 	<BASEINSS/> ");
			xmlRequestBody.append(" 	<SITUACAOWMS/> ");
			xmlRequestBody.append(" 	<CODCID/> ");
			xmlRequestBody.append(" 	<ANTT/> ");
			xmlRequestBody.append(" 	<BASESUBSTSEMRED/> ");
			xmlRequestBody.append(" 	<TIPOCTE/> ");
			xmlRequestBody.append(" 	<VIATRANSP/> ");
			xmlRequestBody.append(" 	<DTENTSAI/> ");
			xmlRequestBody.append(" 	<VLREMB/> ");
			xmlRequestBody.append(" 	<LOCALCOLETA/> ");
			xmlRequestBody.append(" 	<VLRPIS/> ");
			xmlRequestBody.append(" 	<VLRNOTA/> ");
			xmlRequestBody.append(" 	<VLRDESTAQUE/> ");
			xmlRequestBody.append(" 	<PERCDESCFOB/> ");
			xmlRequestBody.append(" 	<CIOT/> ");
			xmlRequestBody.append(" 	<CODUFDESTINO/> ");
			xmlRequestBody.append(" 	<DTREF/> ");
			xmlRequestBody.append(" 	<CODDOCA/> ");
			xmlRequestBody.append(" 	<VLROUTROS/> ");
			xmlRequestBody.append(" 	<PESOLIQUIMANUAL/> ");
			xmlRequestBody.append(" 	<VLRJURO/> ");
			xmlRequestBody.append(" 	<PERCDESC/> ");
			xmlRequestBody.append(" 	<VLRDESCSERV/> ");
			xmlRequestBody.append(" 	<CODPARCTRANSPFINAL/> ");
			xmlRequestBody.append(" 	<PLACA/> ");
			xmlRequestBody.append(" 	<LIBPENDENTE/> ");
			xmlRequestBody.append(" 	<CODOBRA/> ");
			xmlRequestBody.append(" 	<TOTALCUSTOSERV/> ");
			xmlRequestBody.append(" 	<CODGRUPOTENSAO/> ");
			xmlRequestBody.append(" 	<NUMCONTRATO/> ");
			xmlRequestBody.append(" 	<NUREM/> ");
			xmlRequestBody.append(" 	<LOCALENTREGA/> ");
			xmlRequestBody.append(" 	<DESCRHISTAC/> ");
			xmlRequestBody.append(" 	<NROREDZ/> ");
			xmlRequestBody.append(" 	<CODART/> ");
			xmlRequestBody.append(" 	<PRODUETLOC/> ");
			xmlRequestBody.append(" 	<UFADQUIRENTE/> ");
			xmlRequestBody.append(" 	<VLRFRETECPL/> ");
			xmlRequestBody.append(" 	<CLASCONS/> ");
			xmlRequestBody.append(" 	<COMISSAO/> ");
			xmlRequestBody.append(" 	<VLRFRETE/> ");
			xmlRequestBody.append(" 	<VLRREPREDTOTSEMDESC/> ");
			xmlRequestBody.append(" 	<REBOQUE3/> ");
			xmlRequestBody.append(" 	<BASECOFINS/> ");
			xmlRequestBody.append(" 	<VLRDESCTOTITEM/> ");
			xmlRequestBody.append(" 	<EXIGEISSQN/> ");
			xmlRequestBody.append(" 	<VLRIRF/> ");
			xmlRequestBody.append(" 	<VLRCOFINSST/> ");
			xmlRequestBody.append(" 	<IDBALSA01/> ");
			xmlRequestBody.append(" 	<SITUACAOCTE/> ");
			xmlRequestBody.append(" 	<DIGITAL/> ");
			xmlRequestBody.append(" 	<HRMOV/> ");
			xmlRequestBody.append(" 	<CODVEICULO/> ");
			xmlRequestBody.append(" 	<TIPSERVCTE/> ");
			xmlRequestBody.append(" 	<VLRSEG/> ");
			xmlRequestBody.append(" 	<NUMPROTOCCTE/> ");
			xmlRequestBody.append(" 	<DIRECAOVIAG/> ");
			xmlRequestBody.append(" 	<CODCIDORIGEM/> ");
			xmlRequestBody.append(" 	<CODGUF/> ");
			xmlRequestBody.append(" 	<VLRINSS/> ");
			xmlRequestBody.append(" 	<VLRDESCTOT/> ");
			xmlRequestBody.append(" 	<NUTRANSF/> ");
			xmlRequestBody.append(" 	<CODPARCTRANSP/> ");
			xmlRequestBody.append(" 	<HRADIAM/> ");
			xmlRequestBody.append(" 	<SERIENFDES/> ");
			xmlRequestBody.append(" 	<FUSOEMISSEPEC/> ");
			xmlRequestBody.append(" 	<MODELONFDES/> ");
			xmlRequestBody.append(" 	<TROCO/> ");
			xmlRequestBody.append(" 	<SITESPECIALRESP/> ");
			xmlRequestBody.append(" 	<NUMALEATORIO/> ");
			xmlRequestBody.append(" 	<VLRMERCADORIA/> ");
			xmlRequestBody.append(" 	<NUMREGDPEC/> ");
			xmlRequestBody.append(" 	<NROCAIXA/> ");
			xmlRequestBody.append(" 	<TOTDISPDESC/> ");
			xmlRequestBody.append(" 	<DHEMISSEPEC/> ");
			xmlRequestBody.append(" 	<NUCONFATUAL/> ");
			xmlRequestBody.append(" 	<NUNOTASUB/> ");
			xmlRequestBody.append(" 	<DESCTERMACORD/> ");
			xmlRequestBody.append(" 	<TPLIGACAO/> ");
			xmlRequestBody.append(" 	<MOTNAORETERISSQN/> ");
			xmlRequestBody.append(" 	<VLRICMSDIFALREM/> ");
			xmlRequestBody.append(" 	<TOTALCUSTOPROD/> ");
			xmlRequestBody.append(" 	<APROVADO/> ");
			xmlRequestBody.append(" 	<CHAVECTE/> ");
			xmlRequestBody.append(" 	<DTDECLARA/> ");
			xmlRequestBody.append(" 	<CODFUNC/> ");
			xmlRequestBody.append(" 	<IRINNAVIO/> ");
			xmlRequestBody.append(" 	<PENDENTE/> ");
			xmlRequestBody.append(" 	<NUMOS/> ");
			xmlRequestBody.append(" 	<VLRPRESTAFRMM/> ");
			xmlRequestBody.append(" 	<CODUFENTREGA/> ");
			xmlRequestBody.append(" 	<CODCIDPREST/> ");
			xmlRequestBody.append(" 	<TPAMBNFSE/> ");
			xmlRequestBody.append(" 	<NATUREZAOPERDES/> ");
			xmlRequestBody.append(" 	<UFVEICULO/> ");
			xmlRequestBody.append(" 	<CODCONTATO/> ");
			xmlRequestBody.append(" 	<SEQCARGA/> ");
			xmlRequestBody.append(" 	<DHPROTOCCTE/> ");
			xmlRequestBody.append(" 	<MARCA/> ");
			xmlRequestBody.append(" 	<PESOLIQITENS/> ");
			xmlRequestBody.append(" 	<NUMALEATORIOCTE/> ");
			xmlRequestBody.append(" 	<M3/> ");
			xmlRequestBody.append(" 	<NOTASCF/> ");
			xmlRequestBody.append(" 	<FORMPGTCTE/> ");
			xmlRequestBody.append(" 	<STATUSNFSE/> ");
			xmlRequestBody.append(" 	<CODUFORIGEM/> ");
			xmlRequestBody.append(" 	<NUMPROTOC/> ");
			xmlRequestBody.append(" 	<OCCN48/> ");
			xmlRequestBody.append(" 	<VLRREPREDTOT/> ");
			xmlRequestBody.append(" 	<CODSITE/> ");
			xmlRequestBody.append(" 	<CODPROJ/> ");
			xmlRequestBody.append(" 	<NFEDEVRECUSA/> ");
			xmlRequestBody.append(" 	<VLRSTFCPINT/> ");
			xmlRequestBody.append(" 	<BASEICMS/> ");
			xmlRequestBody.append(" 	<VLRROYALT/> ");
			xmlRequestBody.append(" 	<CODMOTORISTA/> ");
			xmlRequestBody.append(" 	<CONFIRMADA/> ");
			xmlRequestBody.append(" 	<BASEPISST/> ");
			xmlRequestBody.append(" 	<NOMEADQUIRENTE/> ");
		}
		xmlRequestBody.append("		</cabecalho> ");
		xmlRequestBody.append("		<txProperties>");
		xmlRequestBody
				.append("         		<prop name='br.com.sankhya.mgecom.gradeItens.pedidoWeb' value='false'/> ");
		xmlRequestBody
				.append(" 	    		<prop name='br.com.sankhya.mgefin.recalculo.custopreco.Automatico' value='false'/> ");
		xmlRequestBody
				.append(" 	    		<prop name='br.com.sankhya.mgefin.checarfinanceiro.VlrEntrada' value='0'/> ");
		xmlRequestBody
				.append(" 	    		<prop name='br.com.sankhya.mgefin.checarfinanceiro.RecalcularVencimento' value='false'/> ");
		xmlRequestBody
				.append(" 	    		<prop name='cabecalhoNota.inserindo.pedidoWeb' value='false'/> ");
		xmlRequestBody.append("		</txProperties> ");
		xmlRequestBody.append("	</nota> ");
		xmlRequestBody.append("	<clientEventList> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.central.itens.VendaCasada</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.parcelas.financeiro</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.central.itens.KitRevenda.msgValidaFormula</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecomercial.event.compensacao.credito.debito</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecomercial.event.baixaPortal</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecomercial.event.cadastrarDistancia</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgefin.event.fixa.vencimento</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.imobilizado</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgefin.solicitacao.liberacao.orcamento</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.exibir.variacao.valor.item</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.cancelamento.notas.remessa</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecomercial.event.estoque.componentes</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.nota.adicional.SolicitarUsuarioGerente</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.event.troca.item.por.produto.alternativo</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.central.itens.KitRevenda</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.utiliza.dtneg.servidor</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>central.save.grade.itens.mostrar.popup.info.lote</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.expedicao.SolicitarUsuarioConferente</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.valida.ChaveNFeCompraTerceiros</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.actionbutton.clientconfirm</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.compra.SolicitacaoComprador</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.checkout.obter.peso</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecomercial.event.estoque.insuficiente.produto</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.importacaoxml.cfi.para.produto</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.event.troca.item.por.produto.substituto</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecomercial.event.faturamento.confirmacao</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.exibe.msg.variacao.preco</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>br.com.sankhya.mgecom.coleta.entrega.recalculado</clientEvent> ");
		xmlRequestBody
				.append("			<clientEvent>central.save.grade.itens.mostrar.popup.serie</clientEvent> ");
		xmlRequestBody.append("	</clientEventList> ");

		// Chamada ao servi�o da Sankhya
		Document document = serviceInvoker.call(
				"CACSP.incluirAlterarCabecalhoNota", "mgecom",
				xmlRequestBody.toString());

		Node item = document.getChildNodes().item(0);

		if ("1".equals(item.getAttributes().getNamedItem("status")
				.getTextContent())) {
			return document.getElementsByTagName("NUNOTA").item(0)
					.getTextContent();

		} else {
			return XMLUtil.xmlToString(document);
		}
	}

}

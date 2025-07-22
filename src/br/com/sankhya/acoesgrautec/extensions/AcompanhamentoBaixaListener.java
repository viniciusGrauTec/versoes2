package br.com.sankhya.acoesgrautec.extensions;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.modelcore.financeiro.helper.BaixaHelper.BaixaHelperEvent;
import br.com.sankhya.modelcore.financeiro.helper.BaixaHelper.BaixaHelperListenerAdapter;
import br.com.sankhya.modelcore.util.SWRepositoryUtils;


public class AcompanhamentoBaixaListener extends BaixaHelperListenerAdapter {

	static {
		LogConfiguration.setPath(SWRepositoryUtils.getBaseFolder() + "/logAcao/logs");
	}

    private String formatarDetalhesFinanceiro(DynamicVO finVO) {
        if (finVO == null) {
            return "[DETALHES]: Informações do título não disponíveis.";
        }

        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
        
        BigDecimal codParc = finVO.asBigDecimalOrZero("CODPARC");
        String nomeParc = finVO.asString("Parceiro.NOMEPARC");
        Date dtVenc = finVO.asTimestamp("DTVENC");
        
        BigDecimal vlrBaixa = finVO.asBigDecimalOrZero("VLRBAIXA");
        BigDecimal vlrJuros = finVO.asBigDecimalOrZero("VLRJURO");
        BigDecimal vlrMulta = finVO.asBigDecimalOrZero("VLRMULTA");
        BigDecimal vlrDesc = finVO.asBigDecimalOrZero("VLRDESC");
       

        return String.format(
            "[DETALHES]: Parc: %s-%s | Venc: %s | Valor Baixado: R$ %.2f | Juros: R$ %.2f | Multa: R$ %.2f | Desconto: R$ %.2f",
            codParc,
            nomeParc != null ? nomeParc : "N/A",
            dtVenc != null ? sdf.format(dtVenc) : "N/A",
            vlrBaixa, vlrJuros, vlrMulta, vlrDesc
        );
    }

    @Override
    public void beforeBaixa(BaixaHelperEvent event) throws Exception {
        DynamicVO finVO = event.getFinVO();
        BigDecimal nufin = finVO.asBigDecimalOrZero("NUFIN");
        
        if (nufin.compareTo(BigDecimal.ZERO) == 0) {
             LogCatcher.logInfo(String.format("[ACOMPANHAMENTO-BAIXA] => INICIANDO baixa para um novo título (forma de pgto) com CODTIPTIT: %s", finVO.asBigDecimalOrZero("CODTIPTIT")));
        } else {
             LogCatcher.logInfo(String.format("[ACOMPANHAMENTO-BAIXA] => INICIANDO baixa para o NUFIN: %s", nufin));
        }
        
        LogCatcher.logInfo("     " + formatarDetalhesFinanceiro(finVO));
    }

    @Override
    public void afterBaixa(BaixaHelperEvent event) throws Exception {
        DynamicVO finVO = event.getFinVO();
        BigDecimal nufin = finVO.asBigDecimalOrZero("NUFIN");
        BigDecimal nuBco = finVO.asBigDecimalOrZero("NUBCO");

        LogCatcher.logInfo(String.format("[ACOMPANHAMENTO-BAIXA] <= SUCESSO na baixa do NUFIN: %s. Mov. Bancário NUBCO: %s", nufin, nuBco));
        
        LogCatcher.logInfo("     " + formatarDetalhesFinanceiro(finVO));
    }

    @Override
    public void afterPendencia(BaixaHelperEvent event) throws Exception {
        DynamicVO pendenciaVO = event.getFinVO();
        BigDecimal nufinPendente = pendenciaVO.asBigDecimalOrZero("NUFIN");
        BigDecimal vlrPendente = pendenciaVO.asBigDecimalOrZero("VLRDESDOB");

        LogCatcher.logInfo(String.format("[ACOMPANHAMENTO-BAIXA] ## PENDÊNCIA GERADA com NUFIN %s no valor de R$ %.2f.", nufinPendente, vlrPendente));

        LogCatcher.logInfo("     " + formatarDetalhesFinanceiro(pendenciaVO));
    }

    @Override
    public void afterCreditoBaixado(BaixaHelperEvent event) throws Exception {
        DynamicVO creditoVO = event.getFinVO();
        BigDecimal nufinCredito = creditoVO.asBigDecimalOrZero("NUFIN");
        BigDecimal vlrCredito = creditoVO.asBigDecimalOrZero("VLRDESDOB");
        
        LogCatcher.logInfo(String.format("[ACOMPANHAMENTO-BAIXA] ## CRÉDITO GERADO com NUFIN %s no valor de R$ %.2f.", nufinCredito, vlrCredito));
  
        LogCatcher.logInfo("     " + formatarDetalhesFinanceiro(creditoVO));
    }
}
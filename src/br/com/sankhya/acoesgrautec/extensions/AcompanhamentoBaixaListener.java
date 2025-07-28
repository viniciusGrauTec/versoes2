package br.com.sankhya.acoesgrautec.extensions;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import com.ibm.icu.text.NumberFormat;

import br.com.sankhya.jape.core.JapeSession;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.modelcore.financeiro.helper.BaixaHelper.BaixaHelperEvent;
import br.com.sankhya.modelcore.financeiro.helper.BaixaHelper.BaixaHelperListenerAdapter;
import br.com.sankhya.modelcore.util.SWRepositoryUtils;


public class AcompanhamentoBaixaListener extends BaixaHelperListenerAdapter {
	 private final SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
	 private final NumberFormat nf = NumberFormat.getCurrencyInstance(new Locale("pt", "BR"));
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
        String detalhes = montarDetalhes(finVO);
        LogCatcher.logInfo(String.format("[ACOMPANHAMENTO-BAIXA] => INICIANDO baixa para o NUFIN: %s", finVO.asBigDecimal("NUFIN")));
        LogCatcher.logInfo(detalhes);
    }

    @Override
    public void afterBaixa(BaixaHelperEvent event) throws Exception {
        DynamicVO finVO = event.getFinVO();
        BigDecimal nufin = finVO.asBigDecimal("NUFIN");
        BigDecimal nufinOriginal = (BigDecimal) JapeSession.getProperty("NUFIN_ORIGINAL_BAIXA");
        
        String tipoOperacao = (nufinOriginal != null && nufin.equals(nufinOriginal)) 
            ? "ATUALIZAÇÃO do título original" 
            : "CRIAÇÃO de nova parcela";

        LogCatcher.logInfo(String.format("[ACOMPANHAMENTO-BAIXA] <= SUCESSO na %s NUFIN: %s. Mov. Bancário NUBCO: %s", 
            tipoOperacao, nufin, finVO.asBigDecimalOrZero("NUBCO")));
            
        String detalhes = montarDetalhes(finVO);
        LogCatcher.logInfo(detalhes);
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
    
    private String montarDetalhes(DynamicVO finVO) {
        String nomeParc = finVO.asString("Parceiro.NOMEPARC");
        BigDecimal codParc = finVO.asBigDecimal("CODPARC");
        String vencimento = finVO.asTimestamp("DTVENC") != null ? sdf.format(finVO.asTimestamp("DTVENC")) : "N/A";
        
        return String.format("       [DETALHES]: Parc: %s-%s | Venc: %s | Valor Baixado: %s | Juros: %s | Multa: %s | Desconto: %s",
            codParc,
            nomeParc,
            vencimento,
            nf.format(finVO.asBigDecimalOrZero("VLRBAIXA")),
            nf.format(finVO.asBigDecimalOrZero("VLRJURO")),
            nf.format(finVO.asBigDecimalOrZero("VLRMULTA")),
            nf.format(finVO.asBigDecimalOrZero("VLRDESC"))
        );
    }
}
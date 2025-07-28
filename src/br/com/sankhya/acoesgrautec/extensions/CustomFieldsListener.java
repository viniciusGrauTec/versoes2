package br.com.sankhya.acoesgrautec.extensions;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.bmp.PersistentLocalEntity;
import br.com.sankhya.jape.core.JapeSession;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.vo.EntityVO;
import br.com.sankhya.modelcore.financeiro.helper.BaixaHelper.BaixaHelperEvent;
import br.com.sankhya.modelcore.financeiro.helper.BaixaHelper.BaixaHelperListener;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;


public class CustomFieldsListener implements BaixaHelperListener {

    @Override
    public void beforeBaixa(BaixaHelperEvent event) throws Exception {
        DynamicVO finVO = event.getFinVO(); 


        finVO.setAceptTransientProperties(true);

        if (finVO.containsProperty("AD_VLRDESCINT")) {
            finVO.setProperty("AD_VLRDESCINT", finVO.getProperty("AD_VLRDESCINT"));
        }
        if (finVO.containsProperty("AD_VLRMULTAINT")) {
            finVO.setProperty("AD_VLRMULTAINT", finVO.getProperty("AD_VLRMULTAINT"));
        }
        if (finVO.containsProperty("AD_VLRJUROSINT")) {
            finVO.setProperty("AD_VLRJUROSINT", finVO.getProperty("AD_VLRJUROSINT"));
        }
        if (finVO.containsProperty("AD_OUTACRESCIMOS")) {
            finVO.setProperty("AD_OUTACRESCIMOS", finVO.getProperty("AD_OUTACRESCIMOS"));
        }
        if (finVO.containsProperty("AD_BAIXAID")) {
            finVO.setProperty("AD_BAIXAID", finVO.getProperty("AD_BAIXAID"));
        }
        if (finVO.containsProperty("AD_BAIXA_CARTAO")) {
            finVO.setProperty("AD_BAIXA_CARTAO", finVO.getProperty("AD_BAIXA_CARTAO"));
        }
         if (finVO.containsProperty("AD_NSU_CART")) {
            finVO.setProperty("AD_NSU_CART", finVO.getProperty("AD_NSU_CART"));
        }
         if (finVO.containsProperty("AD_AUTORIZACAO_CART")) {
            finVO.setProperty("AD_AUTORIZACAO_CART", finVO.getProperty("AD_AUTORIZACAO_CART"));
        }
    }

    @Override
    public void afterBaixa(BaixaHelperEvent event) throws Exception { 
        DynamicVO finVOBaixado = event.getFinVO(); 
        if (finVOBaixado == null) {
            return;
        }
        String idExterno = (String) JapeSession.getProperty("IDEXTERNO_PARA_BAIXA");
        if (idExterno != null && !idExterno.trim().isEmpty()) {       
            EntityFacade dwfFacade = EntityFacadeFactory.getDWFFacade();          
            PersistentLocalEntity finEntity = dwfFacade.findEntityByPrimaryKey("Financeiro", finVOBaixado.asBigDecimal("NUFIN"));
            DynamicVO voToUpdate = (DynamicVO) finEntity.getValueObject();
            voToUpdate.setProperty("AD_IDEXTERNO", idExterno);           
            finEntity.setValueObject((EntityVO) voToUpdate);
        }
    }
    @Override
    public void beforeDespesaPendente(BaixaHelperEvent event) throws Exception {}
    @Override
    public void afterDespesaPendente(BaixaHelperEvent event) throws Exception {}
    @Override
    public void beforeCreditoBaixado(BaixaHelperEvent event) throws Exception {}
    @Override
    public void afterCreditoBaixado(BaixaHelperEvent event) throws Exception {}
    @Override
    public void beforePendencia(BaixaHelperEvent event) throws Exception {}
    @Override
    public void afterPendencia(BaixaHelperEvent event) throws Exception {}
}

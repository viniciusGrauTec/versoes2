package br.com.sankhya.acoesgrautec.eventos;

import br.com.sankhya.extensions.eventoprogramavel.EventoProgramavelJava;
import br.com.sankhya.jape.core.JapeSession;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.event.PersistenceEvent;
import br.com.sankhya.jape.event.TransactionContext;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.jape.wrapper.fluid.FluidUpdateVO;
import br.com.sankhya.modelcore.comercial.AtributosRegras;

import java.math.BigDecimal;
import java.sql.ResultSet;

public class EventoLocalDev implements EventoProgramavelJava {
    @Override
    public void beforeInsert(PersistenceEvent event) throws Exception {

    }

    @Override
    public void beforeUpdate(PersistenceEvent event) throws Exception {

    }

    @Override
    public void beforeDelete(PersistenceEvent event) throws Exception {

    }

    @Override
    public void afterInsert(PersistenceEvent event) throws Exception {}

    @Override
    public void afterUpdate(PersistenceEvent event) throws Exception {
        DynamicVO dadosVO = (DynamicVO) event.getVo();
        DynamicVO dadosOldVO = (DynamicVO) event.getOldVO();
        
        JapeWrapper cabDAO = JapeFactory.dao("CabecalhoNota");

        JapeWrapper iteDAO = JapeFactory.dao("ItemNota");

        JdbcWrapper jdbc = null;
        /*SQL PARA BUSCAR ITENS DO PEDIDO */

        NativeSql sql = null;


        /*SQL para buscar financeiro do pedido */

        NativeSql sqlfin = null;

        ResultSet rset = null;
        ResultSet rsetfin = null;

        jdbc = event.getJdbcWrapper();

        jdbc.openSession();

        String vstatusnota = dadosVO.asString("STATUSNOTA");
        String testeStatusNota = dadosOldVO.asString("STATUSNOTA");
        BigDecimal vcodtipoper = dadosVO.asBigDecimal("CODTIPOPER");
        BigDecimal vnunota = dadosVO.asBigDecimal("NUNOTA");
        BigDecimal vcodemp = dadosVO.asBigDecimal("CODEMP");
        
        System.out.println("Status: " + vstatusnota);
        System.out.println("Status OldVO: " + testeStatusNota);
        
        if(vstatusnota.equals("L")){
        	System.out.println("entrou no if");
            /*sql = new NativeSql(jdbc);

            sql.setNamedParameter("PNUNOTA", vnunota);

            sql.appendSql("SELECT CODPROD,SEQUENCIA FROM TGFITE WHERE NUNOTA = {PNUNOTA}");

            rset = sql.executeQuery();


            while(rset.next()) {

                FluidUpdateVO iteFVO = iteDAO.prepareToUpdateByPK(rset.getBigDecimal("NUNOTA"), rset.getBigDecimal("SEQUENCIA"));
                iteFVO.set("CODLOCALORIG", 2001).update();

            }*/


        }

        /* atualizar o finaceiro do pedido 1210 para recdesp = 0 */

        /*if(vcodtipoper.equals(BigDecimal.valueOf(1210))){


            if (dadosVO.asString("STATUSNOTA").equals("L")) {

                sqlfin = new NativeSql(jdbc);

                sqlfin.setNamedParameter("PNUNOTA", dadosVO.asBigDecimal("NUNOTA"));

                sqlfin.appendSql("SELECT * FROM TGFFIN WHERE NUNOTA = :PNUNOTA");

                rsetfin = sql.executeQuery();

                System.out.println("##### UPDATE NO FINANCEIRO 01 #####" + dadosVO.asString("STATUSNOTA").equals("L"));

                while (rsetfin.next()) {

                    JapeSession.SessionHandle hnd = null;
                    try {
                        hnd = JapeSession.open();
                        JapeWrapper finDAO = JapeFactory.dao("Financeiro");

                        System.out.println("##### UPDATE NO FINANCEIRO #####" + rsetfin.getBigDecimal("NUFIN"));

                        FluidUpdateVO finFVO = finDAO.prepareToUpdateByPK(rsetfin.getBigDecimal("NUFIN"));
                        finFVO.set("RECDESP", 0).update();


                    } finally {
                        JapeSession.close(hnd);
                    }

                }
            }

        }*/

        jdbc.closeSession();

    }

    @Override
    public void afterDelete(PersistenceEvent event) throws Exception {

    }

    @Override
    public void beforeCommit(TransactionContext ctx) throws Exception {

    }
}

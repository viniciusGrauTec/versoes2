package br.com.sankhya.acoesgrautec.jobs;

import java.sql.PreparedStatement;

import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.extensions.actionbutton.QueryExecutor;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

public class AcaoComando implements AcaoRotinaJava{

	@Override
	public void doAction(ContextoAcao contexto) throws Exception {
		
		System.out.println("Entrou no botão");
		
		//Registro[] linhas = contexto.getLinhas();
		//Registro registro = linhas[0];
		
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		
		//String q = "";
		
		try {
			
			jdbc.openSession();
			
			String sqlUpdate = "UPDATE AD_IDFORNACAD SET INTEGRADOFIN = 'N' WHERE CODEMP = 4";
			//String sqlUpdate = "DELETE FROM TCSPRJ WHERE CODPROJPAI = 410000000 AND CODPROJ > 410020000";
			
			pstmt = jdbc.getPreparedStatement(sqlUpdate);
			pstmt.executeUpdate();
			
		} catch (Exception se) {
			se.printStackTrace();
		}finally{
			if (pstmt != null) {
				pstmt.close();
			}
			if (jdbc != null) {
				jdbc.closeSession();
			}
		}
		
		contexto.setMensagemRetorno("Comando Executado Com Sucesso");
		
		
	}

}

package br.com.sankhya.acoesgrautec.jobs;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.cuckoo.core.ScheduledAction;
import org.cuckoo.core.ScheduledActionContext;

import br.com.sankhya.acoesgrautec.util.EnviromentUtils;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

public class JobBaixaCartao implements ScheduledAction{

	@Override
	public void onTime(ScheduledActionContext arg0) {
		
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		boolean movBanc = false;
		
		BigDecimal nufin = BigDecimal.ZERO;
		BigDecimal vlrBaixa = BigDecimal.ZERO;
		BigDecimal codConta = BigDecimal.ZERO;
		BigDecimal codemp = BigDecimal.ZERO;
		BigDecimal nubco = BigDecimal.ZERO;
		
		try{
			
			jdbc.openSession();
			
			String sql = "SELECT NUFIN, VLRDESDOB, CODCTABCOINT, CODEMP "
					+ "FROM TGFFIN "
					+ "WHERE AD_BAIXA_CARTAO = 'S' AND DHBAIXA IS NULL AND AD_IDALUNO IS NOT NULL";
			
			pstmt = jdbc.getPreparedStatement(sql);
			rs = pstmt.executeQuery();
			
			while(rs.next()){
				
				nufin = rs.getBigDecimal("NUFIN");
				vlrBaixa = rs.getBigDecimal("VLRDESDOB");
				codConta = rs.getBigDecimal("CODCTABCOINT");
				codemp = rs.getBigDecimal("CODEMP");
				
				nubco = insertMovBancaria(codConta,
						vlrBaixa, nufin, codemp);
				
				movBanc = true;
				
				updateBaixa(nufin, nubco, vlrBaixa, codemp);
				
			}
			
		}catch(Exception e){
			e.printStackTrace();
			
			if (movBanc) {
				try {
					updateFinExtorno(nufin, codemp);
				} catch (Exception e1) {
					e1.printStackTrace();
				}
				try {
					deleteTgfMbc(nubco, codemp);
				} catch (Exception e1) {
					e1.printStackTrace();
				}
				System.out.println("Apagou mov bank");
			}
			
		}finally{
			if(rs != null){
				try {
					rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if(pstmt != null){
				try {
					pstmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			
			jdbc.closeSession();
		}
		
	}
	
	public BigDecimal insertMovBancaria(BigDecimal contaBancaria,
			BigDecimal vlrDesdob, BigDecimal nufin, BigDecimal codemp)
			throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		
		EnviromentUtils util = new EnviromentUtils();

		BigDecimal nubco = util.getMaxNumMbc();

		try {

			jdbc.openSession();

			String sqlUpdate = "INSERT INTO TGFMBC " + "(NUBCO, " + "CODLANC, "
					+ "DTLANC, " + "CODTIPOPER, " + "DHTIPOPER, "
					+ "DTCONTAB, " + "HISTORICO, " + "CODCTABCOINT, "
					+ "NUMDOC, " + "VLRLANC, " + "TALAO, " + "PREDATA, "
					+ "CONCILIADO, " + "DHCONCILIACAO, " + "ORIGMOV, "
					+ "NUMTRANSF, " + "RECDESP, " + "DTALTER, "
					+ "DTINCLUSAO, " + "CODUSU, " + "VLRMOEDA, " + "SALDO, "
					+ "CODCTABCOCONTRA, " + "NUBCOCP, " + "CODPDV) "
					+ " VALUES ("
					+ nubco
					+ ", " // pk
					+ "1, "
					+ "SYSDATE, " // dtneg
					+ "1400, " // top baixa
					+ "(SELECT MAX(DHALTER) FROM TGFTOP WHERE CODTIPOPER = 1400), " // dhtop
					+ "NULL, "
					+ "(SELECT HISTORICO FROM TGFFIN WHERE NUFIN = "
					+ nufin
					+ "), " // historico
					+ ""
					+ contaBancaria
					+ ", " // conta bancaria (CODCTABCOINT)
					+ "0, " // NUMNOTA
					+ ""
					+ vlrDesdob
					+ ", " // vlrdesdob
					+ "NULL, "
					+ "SYSDATE, " // dtneg2
					+ "'N', "
					+ "NULL, "
					+ "'F', "
					+ "NULL, "
					+ "1, "
					+ "SYSDATE, " + "SYSDATE, " + "0, " // Usuario
					+ "0, " + "" + vlrDesdob + ", " // vlrDesdob
					+ "NULL,  " + "NULL, " + "NULL) ";

			pstmt = jdbc.getPreparedStatement(sqlUpdate);
			pstmt.executeUpdate();

		} catch (Exception se) {
			se.printStackTrace();
			throw se;
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
	

	public void updateBaixa(BigDecimal nufin, BigDecimal nubco,
			BigDecimal vlrDesdob, BigDecimal codemp) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		try {

			jdbc.openSession();

			String sqlNota = "UPDATE TGFFIN SET "
					+ "VLRBAIXA = "
					+ vlrDesdob
					+ ", "
					+ "DHBAIXA = SYSDATE , "
					+ "NUBCO = "
					+ nubco
					+ ", "
					+ "CODTIPOPERBAIXA = 1400, "
					+ "DHTIPOPERBAIXA = (SELECT MAX(DHALTER) FROM TGFTOP WHERE CODTIPOPER = 1400), "
					+ "CODUSUBAIXA = 0 " + "WHERE NUFIN = " + nufin;

			pstmt = jdbc.getPreparedStatement(sqlNota);

			pstmt.executeUpdate();

			System.out.println("Passou do update");
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}
	
	public void updateFinExtorno(BigDecimal nufin, BigDecimal codemp) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		try {

			jdbc.openSession();

			String sqlNota = "UPDATE TGFFIN SET "
					+ "VLRBAIXA = 0, "
					+ "DHBAIXA = NULL, "
					+ "NUBCO = NULL, "
					+ "CODTIPOPERBAIXA = 0, "
					+ "DHTIPOPERBAIXA = (SELECT MAX(DHALTER) FROM TGFTOP WHERE CODTIPOPER = 0), "
					+ "CODUSUBAIXA = NULL  " + "WHERE NUFIN = " + nufin;

			pstmt = jdbc.getPreparedStatement(sqlNota);

			pstmt.executeUpdate();

			System.out.println("Passou do update");
		} catch (SQLException e) {
			e.printStackTrace();
			
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}
	
	public void deleteTgfMbc(BigDecimal nubco, BigDecimal codemp) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		try {

			jdbc.openSession();

			String sqlNota = "DELETE FROM TGFMBC WHERE NUBCO = " + nubco;

			pstmt = jdbc.getPreparedStatement(sqlNota);

			pstmt.executeUpdate();

			System.out.println("Passou do update");
		} catch (SQLException e) {
			e.printStackTrace();
			
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}

}

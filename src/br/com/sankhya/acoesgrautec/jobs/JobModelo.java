package br.com.sankhya.acoesgrautec.jobs;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.cuckoo.core.ScheduledAction;
import org.cuckoo.core.ScheduledActionContext;

import br.com.sankhya.acoesgrautec.services.SkwServicoFinanceiro;
import br.com.sankhya.acoesgrautec.util.EnviromentUtils;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.core.JapeSession;
import br.com.sankhya.jape.core.JapeSession.SessionHandle;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.modelcore.MGEModelException;
import br.com.sankhya.modelcore.util.DynamicEntityNames;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

public class JobModelo implements ScheduledAction {

	final boolean INSERIR_LOG = false;

	@Override
	public void onTime(ScheduledActionContext ctx) {

		SkwServicoFinanceiro sf = null;
		StringBuffer mensagem = new StringBuffer();
		try {
			Connection connection;
			connection = EnviromentUtils.conectarSankhya();

			String nuFin;
			String codCtaBcoInt;
			String codUsuBaixa;
			String codTipOperBaixa;
			String codEmp;
			String numNota;
			Long codLancamento;
			String vlrDesdob;

			deletarLog();

			String sqlPendentes = "	SELECT 	f.numnota, ";
			sqlPendentes += "   			c.codemp,  ";
			sqlPendentes += "   			c.nunota, ";
			sqlPendentes += "   	   		f.nufin, ";
			sqlPendentes += "   	   		b.codctabcoint, ";
			sqlPendentes += "   	   		b.codusubaixa, ";
			sqlPendentes += "   	  		b.codtipoperbaixa, ";
			sqlPendentes += "   	   		f.vlrdesdob, ";
			sqlPendentes += "   	   		(CASE ";
			sqlPendentes += "   	    		 WHEN f.recdesp = 1 THEN ";
			sqlPendentes += "   	    		  1 ";
			sqlPendentes += "   	    		 ELSE ";
			sqlPendentes += "   	   		    2 ";
			sqlPendentes += "   	  		 END) codLan ";
			sqlPendentes += "   	FROM 	ad_parambaixatbm b ";
			sqlPendentes += "  		INNER JOIN tgfcab c ";
			sqlPendentes += "     		ON 	b.codemp = c.codemp ";
			sqlPendentes += "    		AND b.codparc = c.codparc ";
			sqlPendentes += "    		AND b.codtipopernota = c.codtipoper ";
			sqlPendentes += "  		INNER JOIN tgffin f ";
			sqlPendentes += "     		ON 	c.nunota = f.nunota ";
			sqlPendentes += "  		WHERE 	c.statusnota = 'L' ";
			sqlPendentes += "    		AND f.dhbaixa IS NULL ";

			PreparedStatement preparedStatement = connection
					.prepareStatement(sqlPendentes);
			ResultSet resultSet = preparedStatement.executeQuery();

			inserirLog("resultSet");

			while (resultSet.next()) {
				nuFin = resultSet.getString("nufin");
				codCtaBcoInt = resultSet.getString("codctabcoint");
				codUsuBaixa = resultSet.getString("codusubaixa");
				codTipOperBaixa = resultSet.getString("codtipoperbaixa");
				codEmp = resultSet.getString("codemp");
				numNota = resultSet.getString("numnota");
				codLancamento = resultSet.getLong("codLan");
				vlrDesdob = resultSet.getString("vlrdesdob");

				SessionHandle hnd = JapeSession.open();

				JapeWrapper parametrosExtDAO = JapeFactory
						.dao("ParametrosExtensoes");
				DynamicVO parametroVO = parametrosExtDAO
						.findByPK("DOMAIN_SERVICE");
				String domain = parametroVO.asString("VALOR");

				JapeWrapper usuarioDAO = JapeFactory
						.dao(DynamicEntityNames.USUARIO);
				DynamicVO usuarioVO = usuarioDAO.findByPK(new BigDecimal(
						codUsuBaixa));
				String md5 = usuarioVO.getProperty("INTERNO").toString();
				String nomeUsu = usuarioVO.getProperty("NOMEUSU").toString();

				inserirLog("SkwServicoFinanceiro");

				sf = new SkwServicoFinanceiro(domain, nomeUsu, md5, true);

				inserirLog("baixarTitulo - Inicio");

				// Realizar a baixa do titulo do financeiro
				sf.baixarTitulo(codUsuBaixa, nuFin, codEmp, vlrDesdob,
						codCtaBcoInt, codLancamento.intValue(), numNota,
						codTipOperBaixa, mensagem);

				inserirLog("baixarTitulo - Fim");

			}

			connection.close();
			resultSet.close();
			preparedStatement.close();
		} catch (Exception e) {
			RuntimeException re = new RuntimeException(e);
			throw re;
		}
	}

	private void deletarLog() throws MGEModelException, SQLException {
		if (!this.INSERIR_LOG)
			return;

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		try {
			jdbc.openSession();
			String sql = " delete from ad_logchedule ";
			pstmt = jdbc.getPreparedStatement(sql);
			pstmt.executeUpdate();

		} catch (Exception e) {
			e.printStackTrace();
			throw new MGEModelException(e.getMessage());
		} finally {
			pstmt.close();
			jdbc.closeSession();
		}
	}

	private void inserirLog(String log) throws MGEModelException, SQLException {

		if (!this.INSERIR_LOG)
			return;

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		try {
			jdbc.openSession();
			String sql = "insert into AD_LOGCHEDULE (id, log) values ((select NVL(max(id+1), 1) from ad_logchedule) , ?)";
			pstmt = jdbc.getPreparedStatement(sql);
			pstmt.setString(1, log);
			pstmt.executeUpdate();

		} catch (Exception e) {
			e.printStackTrace();
			throw new MGEModelException(e.getMessage());
		} finally {
			pstmt.close();
			jdbc.closeSession();
		}
	}
}
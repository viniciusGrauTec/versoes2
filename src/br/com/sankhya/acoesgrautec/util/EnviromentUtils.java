package br.com.sankhya.acoesgrautec.util;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.extensions.actionbutton.QueryExecutor;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.metadata.EntityMetaData;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.modelcore.util.DynamicEntityNames;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

public class EnviromentUtils {

	private static ContextoAcao contexto;
	private static String usuarioConectado = "";
	private static Map<String, String> mapPorts = new HashMap<String, String>() {
		{
			put("HOMOLOGA", "");
			put("SANKHYA", "");
			put("TESTE", "");
			put("TREINA", "");
		}
	};

	public static String getPort(ContextoAcao ctx) throws Exception {
		contexto = ctx;
		return mapPorts.get(getUserDB());
	}

	private static Map<String, String> mapSenhas = new HashMap<String, String>() {
		{
			put("TESTE", "");
			put("TREINA", "");
			put("SANKHYA", "");
			put("HOMOLOGA", "");
		}
	};

	public static String getSenhas(ContextoAcao ctx) throws Exception {
		contexto = ctx;
		return mapSenhas.get(getUserDB());
	}

	public static Object[] getDadosUser(String usuLogado) throws Exception {
		return findDadosUserDBJape(usuLogado);
	}

	private static Object[] findDadosUserDB(String usuLogado) throws Exception {
		QueryExecutor query = contexto.getQuery();
		query.setParam("CODUSU", usuLogado);
		query.nativeSelect("SELECT NOMEUSU, INTERNO FROM TSIUSU WHERE CODUSU = {CODUSU}");

		Object[] retorno = new Object[2];

		if (query.next()) {
			retorno[0] = query.getString("NOMEUSU");
			retorno[1] = query.getString("INTERNO");
		}

		query.close();

		return retorno;
	}

	private static Object[] findDadosUserDBJape(String usuLogado)
			throws Exception {
		Object[] retorno = new Object[2];

		JapeWrapper usuarioDAO = JapeFactory.dao(DynamicEntityNames.USUARIO);
		DynamicVO usuarioVO = usuarioDAO.findByPK(new BigDecimal(usuLogado));

		retorno[0] = usuarioVO.getProperty("NOMEUSU").toString();
		retorno[1] = usuarioVO.getProperty("INTERNO").toString();

		return retorno;

	}

	private static String getUserDB() throws SQLException, Exception {
		EntityFacade ef = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = null;

		try {
			jdbc = ef.getJdbcWrapper();
			jdbc.openSession();

			// forma A (Oracle e SQL Server)
			if (jdbc.getDialect() == EntityMetaData.ORACLE_DIALECT) {
				
				//System.out.println(jdbc.getUserName());
				//return findUserDBOracle();
				return jdbc.getUserName();
			} else if (jdbc.getDialect() == EntityMetaData.MSSQL_DIALECT) {
				return findUserDBSQLServer();
			} else {
				throw new RuntimeException("Banco de dados desconhecido");
			}
		} finally {
			JdbcWrapper.closeSession(jdbc);
		}
	}

	private static String findUserDBSQLServer() throws Exception {
		QueryExecutor query = contexto.getQuery();
		query.nativeSelect("SELECT DB_NAME() AS 'USER'");

		String user = null;

		if (query.next()) {
			usuarioConectado = query.getString("USER");
			System.out.println(usuarioConectado);
			return query.getString("USER");
		}

		query.close();

		return user;
	}

	private static String findUserDBOracle() throws Exception {
		QueryExecutor query = contexto.getQuery();
		query.nativeSelect("SELECT USER FROM DUAL");

		String user = null;

		if (query.next()) {
			usuarioConectado = query.getString("USER");
			System.out.println(usuarioConectado);
			return query.getString("USER");
		}

		query.close();

		return user;
	}

	public static Connection conectarSankhya() throws Exception {
		Connection conn = null;
		usuarioConectado = getUserDB();
		System.out.println("UserDb: " + usuarioConectado);
	//	findUserDBOracle();
		if ("TREINA".equals(usuarioConectado)) {
			conn = conectarSankhyaTreina();
		} else if ("GRAUTECNICOTST".equals(usuarioConectado)) {
			conn = conectarSankhyaTeste();
		} else {
			conn = conectarSankhyaProducao();
		}

		return conn;
	}

	public static Connection conectarSankhyaProducao() throws Exception {
		Connection retorno = null;
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			retorno = DriverManager.getConnection(
					"jdbc:oracle:thin:@10.40.0.48:1521/prd", "GRAUTECNICOPRD",
					"t86qxm7u");
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e);
		}
		return retorno;
	}

	public static Connection conectarSankhyaTreina() throws Exception {
		Connection retorno = null;
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			retorno = DriverManager.getConnection(
					"jdbc:oracle:thin:@10.40.0.48:1521/prd", "GRAUTECNICOTST",
					"t86qxm7u");
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e);
		}
		return retorno;
	}

	public static Connection conectarSankhyaTeste() throws Exception {
		Connection retorno = null;
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			retorno = DriverManager
					.getConnection("jdbc:oracle:thin:@10.40.0.48:1521/prd", 
							"GRAUTECNICOTST",
							"t86qxm7u");
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e);
		}
		return retorno;
	}

	public static void updateQueryConnection(String querySql) throws Exception {
		Connection connection = EnviromentUtils.conectarSankhya();
		PreparedStatement preparedStatement = null;
		try {
			System.out.println("Try");
			preparedStatement = connection.prepareStatement(querySql);
			System.out.println("Prepared");
			preparedStatement.executeUpdate();
			System.out.println("Executou");
		} catch (Exception e) {
			System.out.println("Entrou catch");
			e.printStackTrace();
			throw e;
		} finally {
			System.out.println("Finally");
			if (preparedStatement != null) {
				preparedStatement.close();
			}
			connection.close();
			System.out.println("Fechou");
		}
		System.out.println("Finalizou");
	}
	
	public static ResultSet queryConnection(String querySql) throws Exception {
		Connection connection = EnviromentUtils.conectarSankhya();
		ResultSet rs = null;
		try {
			PreparedStatement preparedStatement = connection
					.prepareStatement(querySql);
			rs = preparedStatement.executeQuery();
			preparedStatement.close();
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			connection.close();
		}
		
		return rs;
	}

	public static void criarEmailFilaEnvio(int seqFila, String corpoMensagem,
			Integer numUnico, String email, String usuLogado, String assunto)
			throws Exception {

		String sqlInsertEmail = " INSERT INTO tmdfmg (codfila, dtentrada, status, codcon, tentenvio, mensagem, tipoenvio, maxtentenvio, assunto, email, codusu, dhulttenta, dbhashcode, mimetype, tipodoc, nuchave) VALUES ("
				+ seqFila
				+ ", sysdate, 'Pendente', 0, 1, to_char('"
				+ corpoMensagem
				+ "'), 'E', 3, '"
				+ assunto
				+ "', '"
				+ email
				+ "', "
				+ usuLogado
				+ ", sysdate, 'ebeb9eea17452b275e38d0f76f0eb5e22d37537f', 'text/html', 'N', "
				+ numUnico + " ) ";
		// System.out.println(sqlInsertEmail);
		EnviromentUtils.updateQueryConnection(sqlInsertEmail);
	}
	
	public static Clob converterBlobParaChar(String querySql) throws Exception {
		Connection connection = EnviromentUtils.conectarSankhya();

		Clob arquivo;
		try {
			Statement stmt = connection.createStatement();
		//	ResultSet rs = stmt.executeQuery(" Select FNC_BLOB_TO_CHAR(a.arquivos) as arquivos From AD_EMBIMPANEXOS a Where a.codembimpanexos = 1 ");
			ResultSet rs = stmt.executeQuery(querySql);
			
			if (rs.next()) {
				arquivo = rs.getClob("arquivos");
			} else {
				throw new RuntimeException("N„o foi possÌvel obter o manipulador do " + "CLOB do banco de dados"); 
			}
			
			stmt.close();
			rs.close();
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			connection.close();
		}
		return arquivo;
	}
	
	public BigDecimal getMaxNumFin(boolean attNum) throws Exception{
		
		if(attNum){
			updateQueryConnection("UPDATE TGFNUM SET ULTCOD = (NVL(ULTCOD, 0) + 1) WHERE ARQUIVO = 'TGFFIN'");
		}
		
		BigDecimal maxNufin = BigDecimal.ZERO;
		Connection connection = EnviromentUtils.conectarSankhya();
		ResultSet rs = null;
		
		try {
			
			PreparedStatement preparedStatement = connection
					.prepareStatement("SELECT MAX(ULTCOD) AS ULTCOD FROM TGFNUM WHERE ARQUIVO = 'TGFFIN'");
			rs = preparedStatement.executeQuery();
			
			while(rs.next()){
				maxNufin = rs.getBigDecimal("ULTCOD");
			}
			
			preparedStatement.close();
			
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			connection.close();
			if(rs != null){
				rs.close();
			}
			
		}
		
		
		return maxNufin;
		
	}
	
	public void updateNumFinByQtd(int qtdAdd) throws Exception {
		
		updateQueryConnection("UPDATE TGFNUM SET ULTCOD = (NVL(ULTCOD, 0) + "
					+ qtdAdd + ")  WHERE ARQUIVO = 'TGFFIN'");
		
	}
	
	
	public void insertLogList(String listInsert) throws Exception {
		
		updateQueryConnection("INSERT INTO AD_LOGINTEGRACAO (NUMUNICO, DESCRICAO, DTHORA, "
				+ "										STATUS, CODEMP, MATRICULA_IDFORN) " + listInsert);
		
	}
	
	public BigDecimal getMaxNumParc() throws Exception{
		
		updateNumParc();
		
		Connection connection = EnviromentUtils.conectarSankhya();
		ResultSet rs = null;
		
		BigDecimal maxNumParc = BigDecimal.ZERO;
		
		try {
			
			PreparedStatement preparedStatement = connection
					.prepareStatement("SELECT MAX(ULTCOD) AS ULTCOD FROM TGFNUM WHERE ARQUIVO = 'TGFPAR'");
			rs = preparedStatement.executeQuery();
			
			while(rs.next()){
				maxNumParc = rs.getBigDecimal("ULTCOD");
			}
			
			preparedStatement.close();
			
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			connection.close();
			if(rs != null){
				rs.close();
			}
			
		}
		
		return maxNumParc;
		
	}
	
	public int getMaxNumLog() throws Exception{
		
		Connection connection = EnviromentUtils.conectarSankhya();
		ResultSet rs = null;
		PreparedStatement preparedStatement = null;
		
		int maxNumLog = 0;
		
		try {
			
			preparedStatement = connection
					.prepareStatement("SELECT AD_LOGINTEGRACAO_SEQUENCE.NEXTVAL AS ULTCOD FROM dual");
			rs = preparedStatement.executeQuery();
			
			while(rs.next()){
				maxNumLog = rs.getInt("ULTCOD");
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			connection.close();
			
			if(preparedStatement != null){
				preparedStatement.close();
			}
			
			if(rs != null){
				rs.close();
			}
			
		}
		
		return maxNumLog;
		
	}
	
	public void updateNumParc() throws Exception {
		
		updateQueryConnection("UPDATE TGFNUM SET ULTCOD = (NVL(ULTCOD, 0) + 1) WHERE ARQUIVO = 'TGFPAR'");
		
	}
	
	public BigDecimal getMaxNumMbc() throws Exception{
		
		updateNumMbc();
		
		Connection connection = EnviromentUtils.conectarSankhya();
		ResultSet rs = null;
		
		BigDecimal maxNufin = BigDecimal.ZERO;
		
		try {
			
			PreparedStatement preparedStatement = connection
					.prepareStatement("SELECT MAX(ULTCOD) AS ULTCOD FROM TGFNUM WHERE ARQUIVO = 'TGFMBC'");
			rs = preparedStatement.executeQuery();
			
			while(rs.next()){
				maxNufin = rs.getBigDecimal("ULTCOD");
			}
			
			preparedStatement.close();
			
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			connection.close();
			if(rs != null){
				rs.close();
			}
			
		}
		
		return maxNufin;
		
	}
	
	public void updateNumMbc() throws Exception {
		
		updateQueryConnection("UPDATE TGFNUM SET ULTCOD = (NVL(ULTCOD, 0) + 1) WHERE ARQUIVO = 'TGFMBC'");
		
	}
	
	public void insertProjeto(BigDecimal codProjPai, String cursoDescricao, String cursoAbrev, String ativo, String analitico, int grau, String cursoId) throws Exception {
		
		updateQueryConnection("INSERT INTO TCSPRJ(CODPROJ, CODPROJPAI ,IDENTIFICACAO, ABREVIATURA ,ATIVO, ANALITICO, GRAU, AD_CURSOID)VALUES ((SELECT NVL(MAX(CODPROJ), "+codProjPai+") + 1000 AS CURSONOVO FROM TCSPRJ WHERE CODPROJPAI = "
					+
					codProjPai
					+ ")"
					+ ","
					+ codProjPai
					+ " , '"
					+ cursoDescricao.toUpperCase()
					+ "', '"
					+ cursoAbrev.toUpperCase()
					+ "', '"
					+ ativo
					+ "', '"
					+ analitico + "','" + grau + "', '" + cursoId + "')");
		
	}
	public void insertCurso(String cursoDescr, BigDecimal codEmp, String turmaId, 
			String ativo, String analitico, int grau) throws Exception {
		
		updateQueryConnection("INSERT INTO TCSPRJ(CODPROJ, CODPROJPAI ,IDENTIFICACAO, ABREVIATURA, ATIVO, ANALITICO, GRAU)VALUES ((SELECT NVL((SELECT MAX(CODPROJ) FROM TCSPRJ WHERE CODPROJPAI = (SELECT CODPROJ FROM TCSPRJ "
				+ "WHERE TRANSLATE(UPPER (IDENTIFICACAO), '·ÈÌÛ˙‚ÍÓÙ˚‡ËÏÚ˘„ıÁ¡…Õ”⁄¬ Œ‘€¿»Ã“Ÿ√’«', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC') LIKE TRANSLATE (UPPER ('"
					+
					cursoDescr.toUpperCase()
					+ "'), "
					+ "'·ÈÌÛ˙‚ÍÓÙ˚‡ËÏÚ˘„ıÁ¡…Õ”⁄¬ Œ‘€¿»Ã“Ÿ√’«', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC') AND CODPROJPAI = (SELECT CODPROJ FROM TCSPRJ WHERE CODEMP = "
				+ codEmp
				+ "))), (SELECT CODPROJ FROM TCSPRJ WHERE TRANSLATE(UPPER (IDENTIFICACAO), '·ÈÌÛ˙‚ÍÓÙ˚‡ËÏÚ˘„ıÁ¡…Õ”⁄¬ Œ‘€¿»Ã“Ÿ√’«', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC') LIKE TRANSLATE (UPPER ('"
					+
					cursoDescr.toUpperCase()
					+ "'), "
					+ "'·ÈÌÛ˙‚ÍÓÙ˚‡ËÏÚ˘„ıÁ¡…Õ”⁄¬ Œ‘€¿»Ã“Ÿ√’«', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC') AND CODPROJPAI = (SELECT CODPROJ FROM TCSPRJ WHERE CODEMP = "
				+ codEmp
				+ "))) + 1 FROM DUAL),"
				+ " (SELECT CODPROJ FROM TCSPRJ WHERE TRANSLATE(UPPER (IDENTIFICACAO), '·ÈÌÛ˙‚ÍÓÙ˚‡ËÏÚ˘„ıÁ¡…Õ”⁄¬ Œ‘€¿»Ã“Ÿ√’«', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC') LIKE TRANSLATE (UPPER ('"
					+
					cursoDescr.toUpperCase()
					+ "'), "
					+ "'·ÈÌÛ˙‚ÍÓÙ˚‡ËÏÚ˘„ıÁ¡…Õ”⁄¬ Œ‘€¿»Ã“Ÿ√’«', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC') AND CODPROJPAI = (SELECT CODPROJ FROM TCSPRJ WHERE CODEMP = "
				+ codEmp
				+ ")), "
				+ "'"
				+ turmaId.toUpperCase()
				+ "' , "
				+ "'"
				+ turmaId.toUpperCase()
				+ "', '"
				+ ativo
				+ "', '"
				+ analitico + "', '" + grau + "')");
		
	}
	
	public void inserirLog(String descricao, String status, 
			String idMatricula, BigDecimal codemp) throws Exception {
		
		updateQueryConnection("INSERT INTO AD_LOGINTEGRACAO (NUMUNICO, DESCRICAO, DTHORA, "
				+ "										STATUS, CODEMP, MATRICULA_IDFORN)"
				+ " VALUES ((SELECT NVL(MAX(NUMUNICO), 0) + 1 FROM AD_LOGINTEGRACAO), '"+descricao+"', "
						+ "SYSDATE, '"+status+"', "+codemp+", '"+idMatricula+"')");
		
	}
	
	public void readBLOBToFileStream(BigDecimal idAnexos, BigDecimal idEmbarque, String vArq) {
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		FileOutputStream file = null;
		try {
			conn = EnviromentUtils.conectarSankhya();
			String sqlArq = " Select a.ARQUIVOS From AD_EMBIMPANEXOS a Where a.CODEMBIMP  = " + idEmbarque + " and a.CODEMBIMPANEXOS = " + idAnexos +  " ";
			pstmt = conn.prepareStatement(sqlArq);
			System.out.println(sqlArq);
			rs = pstmt.executeQuery();
			if (rs.next()) {
				InputStream data = rs.getBinaryStream("ARQUIVOS");
				try {
					file = new FileOutputStream(vArq);
					int chunk;
					while ((chunk = data.read()) != -1)
						file.write(chunk);	

				} catch (Exception e) {
					String err = e.toString();
					System.out.println(err);
				} finally {
					if (file != null) {
						file.close();
					}
				}
			}			
		} catch (Exception e) {
			e.printStackTrace();			
		} finally {
			try {
				pstmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			try {
				rs.close();
			} catch (SQLException e) {	
				e.printStackTrace();
			}
		}		
	}
}

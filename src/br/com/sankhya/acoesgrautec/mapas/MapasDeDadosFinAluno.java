package br.com.sankhya.acoesgrautec.mapas;


import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;



	
	public class MapasDeDadosFinAluno {
	    // Mapas para armazenar todas as informações necessárias
	    private Map<String, BigDecimal> mapaInfAlunos;
	    private Map<String, BigDecimal> mapaInfFinanceiro;
	    private Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco;
	    private Map<BigDecimal, String> mapaInfFinanceiroBaixado;
	    private Map<String, BigDecimal> mapaInfParceiros;
	    private Map<String, BigDecimal> mapaInfCenCus;
	    private Map<String, BigDecimal> mapaInfCenCusEmp;
	    private Map<String, BigDecimal> mapaInfCenCusAluno;
	    private Map<String, BigDecimal> mapaInfNatureza;
	    private Map<String, BigDecimal> mapaInfNaturezaEmp;
	    private Map<String, String> mapaInfRecDesp;
	    private Map<String, BigDecimal> mapaInfBanco;
	    private Map<String, BigDecimal> mapaInfConta;
	    
	    public MapasDeDadosFinAluno() throws Exception {
	        inicializarMapas();
	        carregarDados();
	    }
	    
	    private void inicializarMapas() {
	        // Inicialização dos mapas
	        mapaInfAlunos = new HashMap<>();
	        mapaInfFinanceiro = new HashMap<>();
	        mapaInfFinanceiroBanco = new HashMap<>();
	        mapaInfFinanceiroBaixado = new HashMap<>();
	        mapaInfParceiros = new HashMap<>();
	        mapaInfCenCus = new HashMap<>();
	        mapaInfCenCusEmp = new HashMap<>();
	        mapaInfCenCusAluno = new HashMap<>();
	        mapaInfNatureza = new HashMap<>();
	        mapaInfNaturezaEmp = new HashMap<>();
	        mapaInfRecDesp = new HashMap<>();
	        mapaInfBanco = new HashMap<>();
	        mapaInfConta = new HashMap<>();
	    }
	    
	    private void carregarDados() throws Exception {
	        // Alunos
	        List<Object[]> listInfAlunos = retornarInformacoesAlunos();
	        for (Object[] obj : listInfAlunos) {
	            BigDecimal codParc = (BigDecimal) obj[0];
	            String idExternoObj = (String) obj[1];
	            BigDecimal codCenCus = (BigDecimal) obj[2];
	            BigDecimal codemp = (BigDecimal) obj[3];

	            if (mapaInfAlunos.get(idExternoObj + "###" + codemp) == null) {
	                mapaInfAlunos.put(idExternoObj + "###" + codemp, codParc);
	            }
	            
	            if (mapaInfCenCusAluno.get(idExternoObj) == null) {
	                mapaInfCenCusAluno.put(idExternoObj, codCenCus);
	            }
	        }

	        // Financeiro
	        List<Object[]> listInfFinanceiro = retornarInformacoesFinanceiro();
	        for (Object[] obj : listInfFinanceiro) {
	            BigDecimal nuFin = (BigDecimal) obj[0];
	            BigDecimal codEmpObj = (BigDecimal) obj[1];
	            String idExternoObj = (String) obj[2];
	            String baixado = (String) obj[3];
	            BigDecimal nuBco = (BigDecimal) obj[5];

	            if (mapaInfFinanceiro.get(codEmpObj + "###" + idExternoObj) == null) {
	                mapaInfFinanceiro.put(codEmpObj + "###" + idExternoObj, nuFin);
	            }
	            
	            if (mapaInfFinanceiroBanco.get(nuFin) == null) {
	                mapaInfFinanceiroBanco.put(nuFin, nuBco);
	            }
	            
	            if (mapaInfFinanceiroBaixado.get(nuFin) == null) {
	                mapaInfFinanceiroBaixado.put(nuFin, baixado);
	            }
	        }

	        // Parceiros
	        List<Object[]> listInfParceiro = retornarInformacoesParceiros();
	        for (Object[] obj : listInfParceiro) {
	            BigDecimal codParc = (BigDecimal) obj[0];
	            String cpf_cnpj = (String) obj[1];

	            if (mapaInfParceiros.get(cpf_cnpj) == null) {
	                mapaInfParceiros.put(cpf_cnpj, codParc);
	            }
	        }

	        // CenCus
	        List<Object[]> listInfCenCus = retornarInformacoesCenCus();
	        for (Object[] obj : listInfCenCus) {
	            BigDecimal codCenCus = (BigDecimal) obj[0];
	            String idExterno = (String) obj[1];
	            String flag = (String) obj[2];

	            if (mapaInfCenCus.get(idExterno + "###" + flag) == null) {
	                mapaInfCenCus.put(idExterno + "###" + flag, codCenCus);
	            }
	        }
	        
	        // CenCus por empresa
	        List<Object[]> listInfCenCusEmpresa = retornarInformacoesCenCusEmpresa();
	        for (Object[] obj : listInfCenCusEmpresa) {
	            BigDecimal codCenCus = (BigDecimal) obj[0];
	            String idExterno = (String) obj[1];
	            BigDecimal codemp = (BigDecimal) obj[2];
	            
	            if (mapaInfCenCusEmp.get(idExterno + "###" + codemp) == null) {
	                mapaInfCenCusEmp.put(idExterno + "###" + codemp, codCenCus);
	            }
	        }

	        // Natureza
	        List<Object[]> listInfNatureza = retornarInformacoesNatureza();
	        for (Object[] obj : listInfNatureza) {
	            BigDecimal natureza = (BigDecimal) obj[0];
	            String idExternoObj = (String) obj[1];
	            String flag = (String) obj[2];

	            if (mapaInfNatureza.get(idExternoObj + "###" + flag) == null) {
	                mapaInfNatureza.put(idExternoObj + "###" + flag, natureza);
	            }
	        }
	        
	        // Natureza por empresa
	        List<Object[]> listInfNaturezaEmpresa = retornarInformacoesNaturezaEmpresa();
	        for (Object[] obj : listInfNaturezaEmpresa) {
	            BigDecimal natureza = (BigDecimal) obj[0];
	            String idExternoObj = (String) obj[1];
	            BigDecimal codemp = (BigDecimal) obj[2];
	            
	            if (mapaInfNaturezaEmp.get(idExternoObj + "###" + codemp) == null) {
	                mapaInfNaturezaEmp.put(idExternoObj + "###" + codemp, natureza);
	            }
	        }

	        // RecDesp
	        List<Object[]> listInfRecDesp = retornarInformacoesRecDesp();
	        for (Object[] obj : listInfRecDesp) {
	            String recDesp = (String) obj[0];
	            String idExternoObj = (String) obj[1];

	            if (mapaInfRecDesp.get(idExternoObj) == null) {
	                mapaInfRecDesp.put(idExternoObj, recDesp);
	            }
	        }

	        // Banco e Conta
	        List<Object[]> listInfBancoConta = retornarInformacoesBancoConta();
	        for (Object[] obj : listInfBancoConta) {
	            BigDecimal codCtabCointObj = (BigDecimal) obj[0];
	            Long codEmpObj = (Long) obj[1];
	            BigDecimal codBcoObj = (BigDecimal) obj[3];

	            if (mapaInfBanco.get(codEmpObj.toString()) == null) {
	                mapaInfBanco.put(codEmpObj.toString(), codBcoObj);
	            }
	            
	            if (mapaInfConta.get(codEmpObj.toString()) == null) {
	                mapaInfConta.put(codEmpObj.toString(), codCtabCointObj);
	            }
	        }
	    }
	    
	    // Métodos auxiliares que devem ser implementados para funcionar com a classe
	    private List<Object[]> retornarInformacoesAlunos() throws Exception {
			EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
			JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
			PreparedStatement pstmt = null;
			ResultSet rs = null;
			List<Object[]> listRet = new ArrayList<>();
			try { 
				jdbc.openSession();
				String sql = "	SELECT 	CODPARC, ID_EXTERNO, CODCENCUS, CODEMP ";
				sql += "		FROM  	AD_ALUNOS ";
				pstmt = jdbc.getPreparedStatement(sql);
				rs = pstmt.executeQuery();
				while (rs.next()) {
					Object[] ret = new Object[4];
					ret[0] = rs.getBigDecimal("CODPARC");
					ret[1] = rs.getString("ID_EXTERNO");
					ret[2] = rs.getBigDecimal("CODCENCUS");
					ret[3] = rs.getBigDecimal("CODEMP");

					listRet.add(ret);
				}

			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				if (rs != null) {
					rs.close();
				}
				if (pstmt != null) {
					pstmt.close();
				}
				jdbc.closeSession();
			}

			return listRet;
	    }
	    
	    private List<Object[]> retornarInformacoesFinanceiro() throws Exception {
	    	EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
			JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
			PreparedStatement pstmt = null;
			ResultSet rs = null;
			List<Object[]> listRet = new ArrayList<>();
			try {
				jdbc.openSession();
				String sql = "	SELECT 	CODEMP, NUFIN, AD_IDEXTERNO, (CASE WHEN DHBAIXA IS NOT NULL THEN 'S' ELSE 'N' END) BAIXADO, VLRDESDOB, NUBCO ";
				sql += "		FROM  	TGFFIN ";
				sql += "		WHERE  	RECDESP = 1 ";
				sql += "		    AND PROVISAO = 'N' ";
				pstmt = jdbc.getPreparedStatement(sql);
				rs = pstmt.executeQuery();
				while (rs.next()) {
					Object[] ret = new Object[6];
					ret[0] = rs.getBigDecimal("NUFIN");
					ret[1] = rs.getBigDecimal("CODEMP");
					ret[2] = rs.getString("AD_IDEXTERNO");
					ret[3] = rs.getString("BAIXADO");
					ret[4] = rs.getBigDecimal("VLRDESDOB");
					ret[5] = rs.getBigDecimal("NUBCO");

					listRet.add(ret);
				}

			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				if (rs != null) {
					rs.close();
				}
				if (pstmt != null) {
					pstmt.close();
				}
				jdbc.closeSession();
			}

			return listRet;
	    }
	    
	    private List<Object[]> retornarInformacoesParceiros() throws Exception {
	    	EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
			JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
			PreparedStatement pstmt = null;
			ResultSet rs = null;
			List<Object[]> listRet = new ArrayList<>();

			try {
				jdbc.openSession();
				String sql = "	SELECT 	CODPARC, CGC_CPF";
				sql += "		FROM  	TGFPAR ";

				pstmt = jdbc.getPreparedStatement(sql);
				rs = pstmt.executeQuery();

				while (rs.next()) {
					Object[] ret = new Object[2];
					ret[0] = rs.getBigDecimal("CODPARC");
					ret[1] = rs.getString("CGC_CPF");

					listRet.add(ret);
				}

			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				if (rs != null) {
					rs.close();
				}
				if (pstmt != null) {
					pstmt.close();
				}
				jdbc.closeSession();
			}

			return listRet;
	    }
	    
	    private List<Object[]> retornarInformacoesCenCus() throws Exception {
	    	EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
			JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
			PreparedStatement pstmt = null;
			ResultSet rs = null;
			List<Object[]> listRet = new ArrayList<>();
			try {
				jdbc.openSession();
				String sql = "SELECT CODCENCUS, IDEXTERNO, CASE WHEN PROFISSIONAL = 'S' THEN 'P' WHEN TECNICO = 'S' THEN 'T' ELSE 'N' END AS FLAG "
						+ " FROM AD_NATACAD";

				pstmt = jdbc.getPreparedStatement(sql);
				rs = pstmt.executeQuery();
				while (rs.next()) {
					Object[] ret = new Object[3];
					ret[0] = rs.getBigDecimal("CODCENCUS");
					ret[1] = rs.getString("IDEXTERNO");
					ret[2] = rs.getString("FLAG");

					listRet.add(ret);
				}

			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				if (rs != null) {
					rs.close();
				}
				if (pstmt != null) {
					pstmt.close();
				}
				jdbc.closeSession();
			}

			return listRet;
	    }
	    
	    private List<Object[]> retornarInformacoesCenCusEmpresa() throws Exception {
	    	EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
			JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
			PreparedStatement pstmt = null;
			ResultSet rs = null;
			List<Object[]> listRet = new ArrayList<>();
			try {
				jdbc.openSession();
				String sql = "SELECT CODCENCUS, IDEXTERNO, CODEMP FROM AD_NATACAD WHERE CODEMP IS NOT NULL";
				
				pstmt = jdbc.getPreparedStatement(sql);
				rs = pstmt.executeQuery();
				while (rs.next()) {
					Object[] ret = new Object[3];
					ret[0] = rs.getBigDecimal("CODCENCUS");
					ret[1] = rs.getString("IDEXTERNO");
					ret[2] = rs.getBigDecimal("CODEMP");
					
					listRet.add(ret);
				}
				
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				if (rs != null) {
					rs.close();
				}
				if (pstmt != null) {
					pstmt.close();
				}
				jdbc.closeSession();
			}
			
			return listRet;
	    }
	    
	    private List<Object[]> retornarInformacoesNatureza() throws Exception {
	    	EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
			JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
			PreparedStatement pstmt = null;
			ResultSet rs = null;
			List<Object[]> listRet = new ArrayList<>();
			try {
				jdbc.openSession();
				String sql = "SELECT CODNAT, IDEXTERNO, CASE WHEN PROFISSIONAL = 'S' THEN 'P' WHEN TECNICO = 'S' THEN 'T' ELSE 'N' END AS FLAG FROM AD_NATACAD WHERE CODEMP IS NULL";

				pstmt = jdbc.getPreparedStatement(sql);
				rs = pstmt.executeQuery();
				while (rs.next()) {
					Object[] ret = new Object[3];
					ret[0] = rs.getBigDecimal("CODNAT");
					ret[1] = rs.getString("IDEXTERNO");
					ret[2] = rs.getString("FLAG");

					listRet.add(ret);
				}

			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				if (rs != null) {
					rs.close();
				}
				if (pstmt != null) {
					pstmt.close();
				}
				jdbc.closeSession();
			}

			return listRet;
	    }
	    
	    private List<Object[]> retornarInformacoesNaturezaEmpresa() throws Exception {
		    JapeWrapper natAcadDAO = JapeFactory.dao("AD_NATACAD");
		    List<Object[]> listRet = new ArrayList<>();
		    
		    try {
		        // Utiliza o JapeWrapper para consultar os registros
		        Collection<DynamicVO> naturezas = natAcadDAO.find("CODEMP IS NOT NULL");
		        
		        // Processa cada registro retornado
		        for (DynamicVO vo : naturezas) {
		            Object[] ret = new Object[3];
		            ret[0] = vo.asBigDecimal("CODNAT");
		            ret[1] = vo.asString("IDEXTERNO");
		            ret[2] = vo.asBigDecimal("CODEMP");
		            
		            listRet.add(ret);
		        }
		    } catch (Exception e) {
		        e.printStackTrace();
		        throw e;
		    }
		    
		    return listRet;
	    }
	    
	    private List<Object[]> retornarInformacoesRecDesp() throws Exception {
	    	EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
			JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
			PreparedStatement pstmt = null;
			ResultSet rs = null;
			List<Object[]> listRet = new ArrayList<>();
			try {
				jdbc.openSession();
				String sql = "SELECT CASE WHEN SUBSTR(CODNAT, 0, 1) > 1 THEN '-1' ELSE '1' END AS NAT, idexterno "
						+ "FROM AD_NATACAD where idexterno IS NOT NULL "
						+ " union SELECT '0', '0' FROM DUAL "
						+ " WHERE NOT EXISTS (SELECT SUBSTR(CODNAT, 0, 1) NAT FROM AD_NATACAD where idexterno IS NOT NULL)";

				pstmt = jdbc.getPreparedStatement(sql);
				rs = pstmt.executeQuery();
				while (rs.next()) {
					Object[] ret = new Object[2];
					ret[0] = rs.getString("NAT");
					ret[1] = rs.getString("idexterno");

					listRet.add(ret);
				}

			} catch (SQLException e) {
				e.printStackTrace();
				throw new Exception(
						"Erro Ao Executar Metodo retornarInformacoesBancoConta");
			} finally {
				if (rs != null) {
					rs.close();
				}
				if (pstmt != null) {
					pstmt.close();
				}
				jdbc.closeSession();
			}

			return listRet;
	    }
	    
	    private List<Object[]> retornarInformacoesBancoConta() throws Exception {
	    	EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
			JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
			PreparedStatement pstmt = null;
			ResultSet rs = null;
			List<Object[]> listRet = new ArrayList<>();
			try {
				jdbc.openSession();
				String sql = "	SELECT 	CODCTABCOINT, CODEMP, IDEXTERNO, CODBCO ";
				sql += "		FROM  	ad_infobankbaixa WHERE IDEXTERNO IS NULL";
				pstmt = jdbc.getPreparedStatement(sql);
				rs = pstmt.executeQuery();
				while (rs.next()) {
					Object[] ret = new Object[4];
					ret[0] = rs.getBigDecimal("CODCTABCOINT");
					ret[1] = rs.getLong("CODEMP");
					ret[2] = rs.getLong("IDEXTERNO");
					ret[3] = rs.getBigDecimal("CODBCO");

					listRet.add(ret);
				}

			} catch (SQLException e) {
				e.printStackTrace();
				throw new Exception(
						"Erro Ao Executar Metodo retornarInformacoesBancoConta");
			} finally {
				if (rs != null) {
					rs.close();
				}
				if (pstmt != null) {
					pstmt.close();
				}
				jdbc.closeSession();
			}

			return listRet;
	    }
	    
	    // Getters para acessar os mapas
	    public Map<String, BigDecimal> getMapaInfAlunos() {
	        return mapaInfAlunos;
	    }
	    
	    public Map<String, BigDecimal> getMapaInfFinanceiro() {
	        return mapaInfFinanceiro;
	    }
	    
	    public Map<BigDecimal, BigDecimal> getMapaInfFinanceiroBanco() {
	        return mapaInfFinanceiroBanco;
	    }
	    
	    public Map<BigDecimal, String> getMapaInfFinanceiroBaixado() {
	        return mapaInfFinanceiroBaixado;
	    }
	    
	    public Map<String, BigDecimal> getMapaInfParceiros() {
	        return mapaInfParceiros;
	    }
	    
	    public Map<String, BigDecimal> getMapaInfCenCus() {
	        return mapaInfCenCus;
	    }
	    
	    public Map<String, BigDecimal> getMapaInfCenCusEmp() {
	        return mapaInfCenCusEmp;
	    }
	    
	    public Map<String, BigDecimal> getMapaInfCenCusAluno() {
	        return mapaInfCenCusAluno;
	    }
	    
	    public Map<String, BigDecimal> getMapaInfNatureza() {
	        return mapaInfNatureza;
	    }
	    
	    public Map<String, BigDecimal> getMapaInfNaturezaEmp() {
	        return mapaInfNaturezaEmp;
	    }
	    
	    public Map<String, String> getMapaInfRecDesp() {
	        return mapaInfRecDesp;
	    }
	    
	    public Map<String, BigDecimal> getMapaInfBanco() {
	        return mapaInfBanco;
	    }
	    
	    public Map<String, BigDecimal> getMapaInfConta() {
	        return mapaInfConta;
	    }
	}


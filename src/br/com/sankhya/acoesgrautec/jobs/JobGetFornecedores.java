package br.com.sankhya.acoesgrautec.jobs;

import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cuckoo.core.ScheduledAction;
import org.cuckoo.core.ScheduledActionContext;

public class JobGetFornecedores
  implements ScheduledAction
 {
	public void onTime(ScheduledActionContext arg0) {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal codEmp = BigDecimal.ZERO;

		String url = "";
		String token = "";

		System.out.println("Iniciou o cadastro dos fornecedores empresa 4");
		try {

			// Parceiros
			List<Object[]> listInfParceiro = retornarInformacoesParceiros();
			Map<String, BigDecimal> mapaInfParceiros = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfParceiro) {

				BigDecimal codParc = (BigDecimal) obj[0];
				String cpf_cnpj = (String) obj[1];
				String idExterno = (String) obj[2];
				BigDecimal codemp = (BigDecimal) obj[3];

				if (mapaInfParceiros.get(cpf_cnpj + "###" + codemp) == null) {
					mapaInfParceiros.put(cpf_cnpj + "###" + codemp, codParc);
				}
			}

			// Id Forncedor
			Map<String, BigDecimal> mapaInfIdParceiros = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfParceiro) {

				BigDecimal codParc = (BigDecimal) obj[0];
				String cpf_cnpj = (String) obj[1];
				String idExterno = (String) obj[2];
				BigDecimal codemp = (BigDecimal) obj[3];

				if (mapaInfIdParceiros.get(idExterno + "###" + cpf_cnpj + "###" + codemp) == null) {
					mapaInfIdParceiros.put(idExterno + "###" + cpf_cnpj + "###" + codemp, codParc);
				}
			}

			jdbc.openSession();

			String query = "SELECT CODEMP, URL, TOKEN FROM AD_LINKSINTEGRACAO";
			//String query3 = "SELECT CODEMP, URL, TOKEN FROM AD_LINKSINTEGRACAO WHERE CODEMP = 3";
			//String query4 = "SELECT CODEMP, URL, TOKEN FROM AD_LINKSINTEGRACAO WHERE CODEMP = 4";

			pstmt = jdbc.getPreparedStatement(query);

			rs = pstmt.executeQuery();
			while (rs.next()) {
				System.out.println("While principal");

				codEmp = rs.getBigDecimal("CODEMP");

				url = rs.getString("URL");
				token = rs.getString("TOKEN");

				iterarEndpoint(mapaInfIdParceiros, mapaInfParceiros, url,
						token, codEmp);
			}
			System.out
					.println("Finalizou o cadastro dos fornecedores empresa 4");
		} catch (Exception e) {
			e.printStackTrace();
			try {
				insertLogIntegracao(
						"Erro ao integrar Fornecedores, Mensagem de erro: "
								+ e.getMessage(), "Erro", "");
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		} finally {
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			jdbc.closeSession();
		}
	}

	public void iterarEndpoint(Map<String, BigDecimal> mapaInfIdParceiros,
			Map<String, BigDecimal> mapaInfParceiros, String url, String token,
			BigDecimal codEmp) throws Exception {
		int pagina = 1;

		Date dataAtual = new Date();
		SimpleDateFormat formato = new SimpleDateFormat("yyyy-MM-dd");

		Calendar calendar = Calendar.getInstance();
		calendar.setTime(dataAtual);
		calendar.add(Calendar.DAY_OF_MONTH, -1);

		Date dataUmDiaAtras = calendar.getTime();

		String dataUmDiaFormatada = formato.format(dataUmDiaAtras);
		String dataAtualFormatada = formato.format(dataAtual);
		//dataAtualFormatada = "2024-10-14";

		System.out.println("data um dia atras forn: " + dataUmDiaFormatada);
		System.out.println("data normal forn: " + dataAtualFormatada);

		try {
			// for (;;) {
			System.out.println("While de iteração");

			String[] response = apiGet(url
					+ "/financeiro/clientes/fornecedores?" + "quantidade=0"
					+ "&dataInicial=" + dataAtualFormatada
					+ " 00:00:00&dataFinal=" + dataAtualFormatada + " 23:59:59"

			, token);

			int status = Integer.parseInt(response[0]);
			System.out.println("Status teste: " + status);
			System.out.println("pagina: " + pagina);

			String responseString = response[1];
			System.out.println("response string: " + responseString);
			// if ((responseString.equals("[]")) || (pagina == 30)) {
			// System.out.println("Entrou no if da quebra");
			// break;
			// }
			cadastrarFornecedor(mapaInfIdParceiros, mapaInfParceiros,
					responseString, codEmp);
			// pagina++;
			// }
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void cadastrarFornecedor(Map<String, BigDecimal> mapaInfIdParceiros,
			Map<String, BigDecimal> mapaInfParceiros, String responseString,
			BigDecimal codEmp) {
		System.out.println("Cadastro principal");
		try {
			JsonParser parser = new JsonParser();
			JsonArray jsonArray = parser.parse(responseString).getAsJsonArray();
			int count = 0;
			System.out.println("contagem: " + count);
			for (JsonElement jsonElement : jsonArray) {
				System.out.println("contagem2: " + count);
				JsonObject jsonObject = jsonElement.getAsJsonObject();

				String fornecedorTipo = jsonObject.get("fornecedor_tipo")
						.isJsonNull() ? null : jsonObject
						.get("fornecedor_tipo").getAsString();

				String fornecedorId = jsonObject.get("fornecedor_id")
						.isJsonNull() ? null : jsonObject.get("fornecedor_id")
						.getAsString();

				String fornecedorNome = jsonObject.get("fornecedor_nome")
						.isJsonNull() ? null : jsonObject
						.get("fornecedor_nome").getAsString();

				String fornecedorNomeFantasia = jsonObject.get(
						"fornecedor_nomefantasia").isJsonNull() ? null
						: jsonObject.get("fornecedor_nomefantasia")
								.getAsString();
				if (fornecedorNomeFantasia == null) {
					fornecedorNomeFantasia = fornecedorNome;
				}
				String fornecedorEndereco = jsonObject.get(
						"fornecedor_endereco").isJsonNull() ? null : jsonObject
						.get("fornecedor_endereco").getAsString();
				if (fornecedorEndereco == null) {
					fornecedorEndereco = " ";
					/*
					 * insertLogIntegracao(
					 * "Fornecedor com variável Endereço nula, Endereço cadastrado como vazio: "
					 * , "Aviso", fornecedorNome);
					 */
				}
				String fornecedorBairro = jsonObject.get("fornecedor_bairro")
						.isJsonNull() ? null : jsonObject.get(
						"fornecedor_bairro").getAsString();
				if (fornecedorBairro == null) {
					fornecedorBairro = " ";
					/*
					 * insertLogIntegracao(
					 * "Fornecedor com variável Bairro nulo, Bairro cadastrado como vazio: "
					 * , "Aviso", fornecedorNome);
					 */
				}
				String fornecedorCidade = jsonObject.get("fornecedor_cidade")
						.isJsonNull() ? null : jsonObject.get(
						"fornecedor_cidade").getAsString();
				if (fornecedorCidade == null) {
					/*
					 * insertLogIntegracao(
					 * "Fornecedor com variável Cidade nulo, não será cadastrado: "
					 * , "Aviso", fornecedorNome);
					 */
				}
				String fornecedorUf = jsonObject.get("fornecedor_uf")
						.isJsonNull() ? null : jsonObject.get("fornecedor_uf")
						.getAsString();

				String fornecedorCep = jsonObject.get("fornecedor_cep")
						.isJsonNull() ? null : jsonObject.get("fornecedor_cep")
						.getAsString();

				String fornecedorInscMunicipal = jsonObject.get(
						"fornecedor_isncmunicipal").isJsonNull() ? null
						: jsonObject.get("fornecedor_isncmunicipal")
								.getAsString();

				String fornecedorInscestadual = jsonObject.get(
						"fornecedor_inscestadual").isJsonNull() ? null
						: jsonObject.get("fornecedor_inscestadual")
								.getAsString();

				String fornecedorFone1 = jsonObject.get("fornecedor_fone1")
						.isJsonNull() ? null : jsonObject.get(
						"fornecedor_fone1").getAsString();

				String fornecedorFone2 = jsonObject.get("fornecedor_fone2")
						.isJsonNull() ? null : jsonObject.get(
						"fornecedor_fone2").getAsString();

				String fornecedorFax = jsonObject.get("fornecedor_fax")
						.isJsonNull() ? null : jsonObject.get("fornecedor_fax")
						.getAsString();

				String fornecedorCelular = jsonObject.get("fornecedor_celular")
						.isJsonNull() ? null : jsonObject.get(
						"fornecedor_celular").getAsString();

				String fornecedorContato = jsonObject.get("fornecedor_contato")
						.isJsonNull() ? null : jsonObject.get(
						"fornecedor_contato").getAsString();

				String fornecedorCpfcnpj = jsonObject.get("fornecedor_cpfcnpj")
						.isJsonNull() ? null : jsonObject.get(
						"fornecedor_cpfcnpj").getAsString();
				if (fornecedorCpfcnpj == null) {
					/*
					 * insertLogIntegracao(
					 * "Fornecedor com variável CpfCnpj nulo, não será cadastrado: "
					 * , "Aviso", fornecedorNome);
					 */
				}
				String fornecedorEmail = jsonObject.get("fornecedor_email")
						.isJsonNull() ? null : jsonObject.get(
						"fornecedor_email").getAsString();

				String fornecedorHomepage = jsonObject.get(
						"fornecedor_homepage").isJsonNull() ? null : jsonObject
						.get("fornecedor_homepage").getAsString();

				String fornecedorAtivo = jsonObject.get("fornecedor_ativo")
						.isJsonNull() ? null : jsonObject.get(
						"fornecedor_ativo").getAsString();
				if (fornecedorAtivo == null) {
					/*
					 * insertLogIntegracao(
					 * "Fornecedor com variável Ativo nulo, não será cadastrado: "
					 * , "Aviso", fornecedorNome);
					 */
				}
				String dataAtualizacao = jsonObject.get("data_atualizacao")
						.isJsonNull() ? null : jsonObject.get(
						"data_atualizacao").getAsString();
				if ((fornecedorCpfcnpj != null) && (fornecedorAtivo != null)
						&& (fornecedorCidade != null)) {
					
					boolean fornecedor = mapaInfParceiros.get(codEmp + "###"
							+ fornecedorCpfcnpj) == null ? false : true;
					
					if (fornecedor) {
						BigDecimal credotAtual = insertFornecedor(
								fornecedorTipo, fornecedorId, fornecedorNome,
								fornecedorNomeFantasia, fornecedorEndereco,
								fornecedorBairro, fornecedorCidade,
								fornecedorUf, fornecedorCep,
								fornecedorInscMunicipal, fornecedorCpfcnpj,
								fornecedorHomepage, fornecedorAtivo,
								dataAtualizacao, fornecedorInscestadual,
								fornecedorFone1, fornecedorFone2,
								fornecedorFax, fornecedorCelular,
								fornecedorContato, fornecedorNome,
								fornecedorEmail, codEmp);

						insertLogIntegracao("Fornecedor Cadastrado: ",
								"Sucesso", fornecedorNome);
					} else {
						boolean IdFornecedor = mapaInfIdParceiros.get(fornecedorId + "###" + fornecedorCpfcnpj + "###" + codEmp)
								== null ? false : true;
						//getIfIdFornecedorExist(fornecedorCpfcnpj, fornecedorId, codEmp);

						if (IdFornecedor) {
							insertIdForn(fornecedorId, fornecedorCpfcnpj,
									codEmp);
						}

						// updateIdForn(fornecedorId.trim(),
						// fornecedorCpfcnpj.trim());

						/*
						 * insertLogIntegracao(
						 * "Fornecedor já Cadastrado no Sistema: ", "Aviso",
						 * fornecedorNome);
						 */
					}
					count++;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			try {
				insertLogIntegracao(
						"Erro no chamado do endpoint: " + e.getMessage(),
						"Erro", "");
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
	}

	public boolean getIfFornecedorExist(String fornecedorCpfcnpj)
			throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		int fornecedor = 0;
		try {

			jdbc.openSession();

			String sqlSlt = "SELECT COUNT(0) AS FORNECEDOR FROM TGFPAR WHERE CGC_CPF = ?";

			pstmt = jdbc.getPreparedStatement(sqlSlt);
			pstmt.setString(1, fornecedorCpfcnpj);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				fornecedor = rs.getInt("FORNECEDOR");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			if (rs != null) {
				rs.close();
			}
			jdbc.closeSession();
		}
		if (fornecedor > 0) {
			return false;
		}
		return true;
	}

	public boolean getIfIdFornecedorExist(String fornecedorCpfcnpj,
			String idAcad, BigDecimal codemp) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		int fornecedor = 0;
		try {

			jdbc.openSession();

			String sqlSlt = "SELECT COUNT(0) AS C FROM AD_IDFORNACAD WHERE CODPARC = (SELECT CODPARC FROM TGFPAR WHERE CGC_CPF = '"
					+ fornecedorCpfcnpj
					+ "') AND IDACADWEB = '"
					+ idAcad
					+ "' AND CODEMP = " + codemp;

			pstmt = jdbc.getPreparedStatement(sqlSlt);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				fornecedor = rs.getInt("C");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			if (rs != null) {
				rs.close();
			}
			jdbc.closeSession();
		}
		if (fornecedor > 0) {
			return false;
		}
		return true;
	}

	public void updateTgfNumParc() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		try {
			jdbc.openSession();

			String sqlUpd = "UPDATE TGFNUM SET ULTCOD = ULTCOD + 1  WHERE ARQUIVO = 'TGFPAR'";

			pstmt = jdbc.getPreparedStatement(sqlUpd);
			pstmt.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}

	public BigDecimal getMaxNumParc() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		BigDecimal bd = BigDecimal.ZERO;
		try {
			updateTgfNumParc();

			jdbc.openSession();

			String sqlUpd = "SELECT MAX (ULTCOD) AS ULTCOD FROM TGFNUM WHERE ARQUIVO = 'TGFPAR'";

			pstmt = jdbc.getPreparedStatement(sqlUpd);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				bd = rs.getBigDecimal("ULTCOD");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			if (rs != null) {
				rs.close();
			}
			jdbc.closeSession();
		}
		return bd;
	}

	public BigDecimal insertFornecedor(String fornecedorTipo,
			String fornecedorId, String fornecedorNome,
			String fornecedorNomeFantasia, String fornecedorEndereco,
			String fornecedorBairro, String fornecedorCidade,
			String fornecedorUf, String fornecedorCep,
			String fornecedorInscMunicipal, String fornecedorCpfcnpj,
			String fornecedorHomepage, String fornecedorAtivo,
			String dataAtualizacao, String fornecedorInscestadual,
			String fornecedorFone1, String fornecedorFone2,
			String fornecedorFax, String fornecedorCelular,
			String fornecedorContato, String fornecedorNome2,
			String fornecedorEmail, BigDecimal codEmp) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		BigDecimal atualCodparc = getMaxNumParc();

		String tipPessoa = "";
		if (fornecedorCpfcnpj.length() == 11) {
			tipPessoa = "F";
		} else if (fornecedorCpfcnpj.length() == 14) {
			tipPessoa = "J";
		}
		try {
			jdbc.openSession();

			String sqlP = "INSERT INTO TGFPAR(CODPARC, AD_ID_EXTERNO_FORN, AD_IDENTINSCMUNIC, AD_TIPOFORNECEDOR, FORNECEDOR, IDENTINSCESTAD, HOMEPAGE, ATIVO, NOMEPARC, RAZAOSOCIAL ,TIPPESSOA, AD_ENDCREDOR, CODBAI, CODCID, CEP, CGC_CPF, DTCAD, DTALTER, CODEMP) \t\tVALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NVL((select max(codbai) from tsibai where TRANSLATE( \t\t\t    upper(nomebai), \t\t\t    'áéíóúâêîôûàèìòùãõçÁÉÍÓÚÂÊÎÔÛÀÈÌÒÙÃÕÇ', \t\t\t    'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC' \t\t\t  ) like TRANSLATE( \t\t\t    upper(?), \t\t\t    'áéíóúâêîôûàèìòùãõçÁÉÍÓÚÂÊÎÔÛÀÈÌÒÙÃÕÇ', \t\t\t    'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC' \t\t\t  )), 0), NVL((SELECT max(codcid) FROM tsicid WHERE TRANSLATE(              UPPER(descricaocorreio),               'áéíóúâêîôûàèìòùãõçÁÉÍÓÚÂÊÎÔÛÀÈÌÒÙÃÕÇ',               'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC')               LIKE TRANSLATE(UPPER(?),               'áéíóúâêîôûàèìòùãõçÁÉÍÓÚÂÊÎÔÛÀÈÌÒÙÃÕÇ',               'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC')               OR SUBSTR(UPPER(descricaocorreio),               1, INSTR(UPPER(descricaocorreio), ' ') - 1)               LIKE TRANSLATE(UPPER(?),               'áéíóúâêîôûàèìòùãõçÁÉÍÓÚÂÊÎÔÛÀÈÌÒÙÃÕÇ',               'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC')),0),  ?, ?, SYSDATE, SYSDATE, ?)";

			pstmt = jdbc.getPreparedStatement(sqlP);
			pstmt.setBigDecimal(1, atualCodparc);
			pstmt.setString(2, fornecedorId);
			pstmt.setString(3, fornecedorInscMunicipal);
			pstmt.setString(4, fornecedorTipo);
			pstmt.setString(5, "S");
			pstmt.setString(6, fornecedorInscestadual);
			pstmt.setString(7, fornecedorHomepage);
			pstmt.setString(8, fornecedorAtivo);
			pstmt.setString(9, fornecedorNome.toUpperCase());
			pstmt.setString(10, fornecedorNomeFantasia.toUpperCase());
			pstmt.setString(11, tipPessoa);
			pstmt.setString(12, fornecedorEndereco);

			pstmt.setString(13, fornecedorBairro);

			pstmt.setString(14, fornecedorCidade.trim());
			pstmt.setString(15, fornecedorCidade.trim());

			pstmt.setString(16, fornecedorCep);

			pstmt.setString(17, fornecedorCpfcnpj);
			pstmt.setBigDecimal(18, codEmp);

			pstmt.executeUpdate();
			if ((fornecedorFone1 != null) && (fornecedorFone2 != null)
					&& (fornecedorFax != null) && (fornecedorCelular != null)
					&& (fornecedorContato != null) && (fornecedorNome != null)
					&& (fornecedorEmail != null) && (atualCodparc != null)) {
				insertContatoFornecedor(fornecedorFone1, fornecedorFone2,
						fornecedorFax, fornecedorCelular, fornecedorContato,
						fornecedorNome, fornecedorEmail, atualCodparc);
			} else {
				insertLogIntegracao(
						"Uma ou mais variáveis são nulas, a função de cadastrar contato não será chamada.",
						"Aviso", fornecedorNome);
			}

			insertIdForn(fornecedorId, fornecedorCpfcnpj, codEmp);

		} catch (SQLException e) {
			insertLogIntegracao(
					"Erro ao cadastrar fornecedor: " + e.getMessage(), "Erro",
					fornecedorNome);
			e.printStackTrace();
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
		return atualCodparc;
	}

	public void updateIdForn(String idForn, String cgc) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		try {
			jdbc.openSession();

			String sqlP = "UPDATE TGFPAR SET AD_ID_EXTERNO_FORN = '"
					+ idForn
					+ "' WHERE CODPARC = (SELECT CODPARC FROM TGFPAR WHERE CGC_CPF = '"
					+ cgc + "')";

			pstmt = jdbc.getPreparedStatement(sqlP);

			pstmt.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}

	public void insertIdForn(String idForn, String cgc, BigDecimal codemp)
			throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		try {

			jdbc.openSession();

			String sqlP = "INSERT INTO AD_IDFORNACAD (CODPARC, ID, IDACADWEB, CODEMP) VALUES ((SELECT CODPARC FROM TGFPAR WHERE CGC_CPF = '"
					+ cgc
					+ "'), (SELECT NVL(MAX(ID), 0) + 1 FROM AD_IDFORNACAD), '"
					+ idForn + "', " + codemp + ")";

			pstmt = jdbc.getPreparedStatement(sqlP);

			pstmt.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}

			jdbc.closeSession();
		}
	}

	private void insertContatoFornecedor(String fornecedorFone1,
			String fornecedorFone2, String fornecedorFax,
			String fornecedorCelular, String fornecedorContato,
			String fornecedorNome, String fornecedorEmail,
			BigDecimal credotAtual) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		try {
			jdbc.openSession();

			String sqlP = "INSERT INTO TGFCTT (CODCONTATO, CODPARC, NOMECONTATO, CELULAR, TELEFONE, TELRESID, FAX, EMAIL) VALUES ((SELECT MAX(NVL(CODCONTATO, 0)) + 1 FROM TGFCTT WHERE CODPARC = ?), ?, ?, ?, ?, ?, ?, ?)";

			pstmt = jdbc.getPreparedStatement(sqlP);
			pstmt.setBigDecimal(1, credotAtual);
			pstmt.setBigDecimal(2, credotAtual);
			pstmt.setString(3, fornecedorNome);
			pstmt.setString(4, fornecedorCelular);
			pstmt.setString(5, fornecedorFone1);
			pstmt.setString(6, fornecedorFone2);
			pstmt.setString(7, fornecedorFax);
			pstmt.setString(8, fornecedorEmail);
			pstmt.executeUpdate();
		} catch (SQLException e) {
			insertLogIntegracao("Erro ao cadastrar contatos do fornecedor: "
					+ e.getMessage(), "Erro", fornecedorNome);
			e.printStackTrace();
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}

	public void insertLogIntegracao(String descricao, String status,
			String fornecedorNome) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		try {
			jdbc.openSession();

			String descricaoCompleta = null;
			if (fornecedorNome.equals("")) {
				descricaoCompleta = descricao;
			} else if (!fornecedorNome.isEmpty()) {
				descricaoCompleta = descricao + " " + " Fornecedor:"
						+ fornecedorNome;
			} else if (!fornecedorNome.isEmpty()) {
				descricaoCompleta = descricao + " " + " Fornecedor:"
						+ fornecedorNome;
			}
			String sqlUpdate = "INSERT INTO AD_LOGINTEGRACAO (NUMUNICO, DESCRICAO, DTHORA, STATUS)VALUES (((SELECT NVL(MAX(NUMUNICO), 0) + 1 FROM AD_LOGINTEGRACAO)), ?, SYSDATE, ?)";

			pstmt = jdbc.getPreparedStatement(sqlUpdate);
			pstmt.setString(1, descricaoCompleta);
			pstmt.setString(2, status);
			pstmt.executeUpdate();
		} catch (Exception se) {
			se.printStackTrace();
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}

	public String[] apiGet(String ur, String token) throws Exception {

		BufferedReader reader;
		String line;
		StringBuilder responseContent = new StringBuilder();
		// String key = preferenciaSenha();

		// Preparando a requisição
		URL obj = new URL(ur);
		HttpURLConnection https = (HttpURLConnection) obj.openConnection();

		System.out.println("Entrou na API");
		System.out.println("URL: " + ur);
		System.out.println("https: " + https);
		System.out.println("token: " + token);

		https.setRequestMethod("GET");
		// https.setConnectTimeout(50000);
		https.setRequestProperty("User-Agent",
				"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0)");
		https.setRequestProperty("Content-Type",
				"application/json; charset=UTF-8");
		https.setRequestProperty("Authorization", "Bearer " + token);
		https.setDoOutput(true);
		https.setDoInput(true);

		int status = https.getResponseCode();

		System.out.println("Status: " + status);

		if (status >= 300) {
			reader = new BufferedReader(new InputStreamReader(
					https.getErrorStream()));
			while ((line = reader.readLine()) != null) {
				responseContent.append(line);
			}
			reader.close();
		} else {
			reader = new BufferedReader(new InputStreamReader(
					https.getInputStream()));
			while ((line = reader.readLine()) != null) {
				responseContent.append(line);
			}
			reader.close();
		}
		System.out.println("Output from Server .... \n" + status);
		String response = responseContent.toString();

		https.disconnect();

		return new String[] { Integer.toString(status), response };

	}

	private List<Object[]> retornarInformacoesParceiros() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		List<Object[]> listRet = new ArrayList<>();

		try {
			jdbc.openSession();
			String sql = "SELECT p.CODPARC, p.CGC_CPF, "
					+ "a.IDACADWEB, a.codemp " + "FROM TGFPAR p "
					+ "LEFT join AD_IDFORNACAD a "
					+ "on a.codparc = p.codparc " + "where fornecedor = 'S'";

			pstmt = jdbc.getPreparedStatement(sql);
			rs = pstmt.executeQuery();

			while (rs.next()) {
				Object[] ret = new Object[4];
				ret[0] = rs.getBigDecimal("CODPARC");
				ret[1] = rs.getString("CGC_CPF");
				ret[2] = rs.getString("IDACADWEB");
				ret[3] = rs.getBigDecimal("codemp");

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
}

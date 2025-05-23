package br.com.sankhya.acoesgrautec.extensions;

import br.com.sankhya.acoesgrautec.util.EnviromentUtils;
import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.extensions.actionbutton.Registro;
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
import java.net.URLEncoder;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.activiti.engine.impl.util.json.JSONArray;
import org.cuckoo.core.ScheduledAction;
import org.cuckoo.core.ScheduledActionContext;

public class AcaoGetCredorAlunoTurmaCursoCarga
		implements AcaoRotinaJava, ScheduledAction
{

	private List<String> selectsParaInsert = new ArrayList<String>();
	private EnviromentUtils util = new EnviromentUtils();

	@Override
	public void doAction(ContextoAcao contexto) throws Exception {

		Registro[] linhas = contexto.getLinhas();
		Registro registro = linhas[0];

		String url = (String) registro.getCampo("URL");
		String token = (String) registro.getCampo("TOKEN");
		BigDecimal codEmp = (BigDecimal) registro.getCampo("CODEMP");

		String dataInicio = contexto.getParam("DTINICIO").toString().substring(0, 10);
		String dataFim = contexto.getParam("DTFIM").toString().substring(0, 10);

		String matricula = (String) contexto.getParam("Matricula");

		try {

			// Alunos Popula o mapa com alunos existentes, evitando consultas repetidas ao banco.
			List<Object[]> listInfAlunos = retornarInformacoesAlunos();
			Map<String, BigDecimal> mapaInfAlunos = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfAlunos) {
				BigDecimal codParc = (BigDecimal) obj[0];
				String idExternoObj = (String) obj[1];
				BigDecimal codemp = (BigDecimal) obj[2];

				if (mapaInfAlunos.get(idExternoObj + "###" + codemp) == null) {
					mapaInfAlunos.put(idExternoObj + "###" + codemp, codParc);
				}
			}

			// Parceiros
			List<Object[]> listInfParceiro = retornarInformacoesParceiros();
			Map<String, BigDecimal> mapaInfParceiros = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfParceiro) {
				BigDecimal codParc = (BigDecimal) obj[0];
				String cpf_cnpj = (String) obj[1];

				if (mapaInfParceiros.get(cpf_cnpj) == null) {
					mapaInfParceiros.put(cpf_cnpj, codParc);
				}
			}

			processDateRangeByMonths(mapaInfAlunos, mapaInfParceiros, url, token,
					codEmp, dataInicio, dataFim, matricula);

			contexto.setMensagemRetorno("Periodo Processado!");

		}catch(Exception e){
			e.printStackTrace();
			contexto.mostraErro(e.getMessage());
		}finally{
			if(selectsParaInsert.size() > 0){

				StringBuilder msgError = new StringBuilder();

				System.out.println("Entrou na lista do finally: " + selectsParaInsert.size());

				//BigDecimal idInicial = util.getMaxNumLog();

				int qtdInsert = selectsParaInsert.size();

				int i = 1;
				for (String sqlInsert : selectsParaInsert) {
					String sql = sqlInsert;
					int nuFin = util.getMaxNumLog();
					sql = sql.replace("<#NUMUNICO#>", String.valueOf(nuFin));
					msgError.append(sql);

					if (i < qtdInsert) {
						msgError.append(" \nUNION ALL ");
					}
					i++;
				}

				System.out.println("Consulta de log: \n" + msgError);
				insertLogList(msgError.toString());

			}
		}

	}

	@Override
	public void onTime(ScheduledActionContext arg0) {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal codEmp = BigDecimal.ZERO;
		BigDecimal idCarga = BigDecimal.ZERO;

		String url = "";
		String token = "";
		String matricula = "";

		System.out.println("Iniciou cadastro dos alunos");

		String threadName = Thread.currentThread().getName();
		System.out.println("=== INÍCIO DO JOB === Thread: " + threadName + " - Hora: " + new Date());

		try {

			System.out.println("Iniciando carregamento de informações - Thread: " + threadName);

			// Alunos
			List<Object[]> listInfAlunos = retornarInformacoesAlunos();
			Map<String, BigDecimal> mapaInfAlunos = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfAlunos) {
				BigDecimal codParc = (BigDecimal) obj[0];
				String idExternoObj = (String) obj[1];
				BigDecimal codemp = (BigDecimal) obj[2];

				if (mapaInfAlunos.get(idExternoObj + "###" + codemp) == null) {
					mapaInfAlunos.put(idExternoObj + "###" + codemp, codParc);
				}
			}

			// Parceiros
			List<Object[]> listInfParceiro = retornarInformacoesParceiros();
			Map<String, BigDecimal> mapaInfParceiros = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfParceiro) {
				BigDecimal codParc = (BigDecimal) obj[0];
				String cpf_cnpj = (String) obj[1];

				if (mapaInfParceiros.get(cpf_cnpj) == null) {
					mapaInfParceiros.put(cpf_cnpj, codParc);
				}
			}

			jdbc.openSession();

			// Modificado para incluir a verificação da flag INTEGRACAO
			String queryEmp = "SELECT CODEMP, URL, TOKEN, INTEGRACAO FROM AD_LINKSINTEGRACAO";

			pstmt = jdbc.getPreparedStatement(queryEmp);

			rs = pstmt.executeQuery();


			System.out.println("Iniciando processamento das empresas - Thread: " + threadName);


			while (rs.next()) {
				System.out.println("While principal");

				codEmp = rs.getBigDecimal("CODEMP");
				url = rs.getString("URL");
				token = rs.getString("TOKEN");
				String statusIntegracao = rs.getString("INTEGRACAO");

				// Verifica se a integração está ativa para esta empresa
				if (!"S".equals(statusIntegracao)) {
					System.out.println("Integração desativada para a empresa " + codEmp + " - pulando processamento");
					continue; // Pula para a próxima iteração do loop
				}

				System.out.println("Processando empresa " + codEmp + " na thread " + threadName);

				iterarEndpoint(mapaInfAlunos, mapaInfParceiros, url, token,
						codEmp);

				System.out.println("Finalizou processamento da empresa " + codEmp + " na thread " + threadName);

			}

			System.out.println("Finalizou o cadastro dos alunos");

		} catch (Exception e) {
			System.out.println("Erro no job - Thread: " + threadName);
			e.printStackTrace();
			try {
				insertLogIntegracao(
						"Erro ao integrar Alunos e/ou Credor, Mensagem de erro: "
								+ e.getMessage(), "Erro", "", "");
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		} finally {
			System.out.println("=== FIM DO JOB === Thread: " + threadName + " - Hora: " + new Date());
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


			if(selectsParaInsert.size() > 0){

				StringBuilder msgError = new StringBuilder();

				System.out.println("Entrou na lista do finally: " + selectsParaInsert.size());

				//BigDecimal idInicial = util.getMaxNumLog();

				int qtdInsert = selectsParaInsert.size();

				int i = 1;
				for (String sqlInsert : selectsParaInsert) {
					String sql = sqlInsert;
					int nuFin = 0;

					try {
						nuFin = util.getMaxNumLog();
					} catch (Exception e) {
						e.printStackTrace();
					}

					sql = sql.replace("<#NUMUNICO#>", String.valueOf(nuFin));
					msgError.append(sql);

					if (i < qtdInsert) {
						msgError.append(" \nUNION ALL ");
					}
					i++;
				}

				System.out.println("Consulta de log: \n" + msgError);

				try {
					insertLogList(msgError.toString());
				} catch (Exception e) {
					e.printStackTrace();
				}

				msgError = null;
				this.selectsParaInsert = new ArrayList<String>();

			}
		}
	}




	//Retorna informações dos parceiros (credores) do banco de dados.
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


	//Retorna informações dos alunos do banco de dados.
	public List<Object[]> retornarInformacoesAlunos() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		List<Object[]> listRet = new ArrayList<>();
		try {
			jdbc.openSession();
			String sql = "	SELECT 	CODPARC, ID_EXTERNO, CODEMP ";
			sql += "		FROM  	AD_ALUNOS ";
			pstmt = jdbc.getPreparedStatement(sql);
			rs = pstmt.executeQuery();
			while (rs.next()) {  //erro aqui 313
				Object[] ret = new Object[3];
				ret[0] = rs.getBigDecimal("CODPARC");
				ret[1] = rs.getString("ID_EXTERNO");
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


	public void processDateRangeByMonths(Map<String, BigDecimal> mapaInfAlunos,
										 Map<String, BigDecimal> mapaInfParceiros, String url, String token, BigDecimal codEmp, String dataInicio,
										 String dataFim, String matricula) throws Exception {

// Converter strings de data para objetos LocalDate
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
		LocalDate inicio = LocalDate.parse(dataInicio, formatter);
		LocalDate fim = LocalDate.parse(dataFim, formatter);

// Dividir o intervalo em períodos mensais
		LocalDate periodoInicio = inicio;
		while (periodoInicio.isBefore(fim) || periodoInicio.isEqual(fim)) {
// Definir o fim do período atual (fim do mês ou a data final)
			LocalDate periodoFim = periodoInicio.plusMonths(1).withDayOfMonth(1).minusDays(1);
			if (periodoFim.isAfter(fim)) {
				periodoFim = fim;
			}

// Processar este período
			System.out.println("Processando período: " + periodoInicio + " até " + periodoFim);
			processDateRange(mapaInfAlunos, mapaInfParceiros, url, token, codEmp, periodoInicio.format(formatter),
					periodoFim.format(formatter), matricula);

// Avançar para o próximo mês
			periodoInicio = periodoInicio.plusMonths(1).withDayOfMonth(1);
		}
	}


	//iterarendpoint das requisicoes
	//novo método iterarendpoint com URL encoder
	//Processa um intervalo de datas, buscando e cadastrando alunos e credores.
	//correção do processDateRange  onde : Se todas as páginas completas retornam exatamente 100 registros (inclusive a última), o sistema nunca irá parar de fazer requisições porque a condição paginaAtual.length() < 100 nunca se tornará verdadeira.
	public void processDateRange(
			Map<String, BigDecimal> mapaInfAlunos,
			Map<String, BigDecimal> mapaInfParceiros,
			String url,
			String token,
			BigDecimal codEmp,
			String dataInicio,
			String dataFim,
			String matricula) throws Exception {

		try {
			// Preparar as datas
			String dataInicialCompleta = dataInicio + " 00:00:00";
			String dataFinalCompleta = dataFim + " 23:59:59";

			// Codificar os parâmetros
			String dataInicialEncoded = URLEncoder.encode(dataInicialCompleta, "UTF-8");
			String dataFinalEncoded = URLEncoder.encode(dataFinalCompleta, "UTF-8");

			// Lista para armazenar todos os registros
			JSONArray todosRegistros = new JSONArray();
			int pagina = 1;
			boolean temMaisRegistros = true;

			while (temMaisRegistros) {
				// Construir a URL para a página atual
				StringBuilder urlBuilder = new StringBuilder();
				urlBuilder.append(url.trim())
						.append("/alunos")
						.append("?pagina=").append(pagina)
						.append("&quantidade=100")
						.append("&dataInicial=").append(dataInicialEncoded)
						.append("&dataFinal=").append(dataFinalEncoded);

				// Adicionar parâmetro de matrícula se estiver presente
				if (matricula != null && !matricula.isEmpty()) {
					String matriculaEncoded = URLEncoder.encode(matricula, "UTF-8");
					urlBuilder.append("&matricula=").append(matriculaEncoded);
				}

				String urlCompleta = urlBuilder.toString();
				System.out.println("URL para alunos (página " + pagina + "): " + urlCompleta);

				// Fazer a requisição
				String[] response = apiGet2(urlCompleta, token);
				int status = Integer.parseInt(response[0]);

				if (status == 200) {
					JSONArray paginaAtual = new JSONArray(response[1]);

					// Verificar se há registros nesta página
					if (paginaAtual.length() == 0) {
						// Página vazia, parar o processamento
						temMaisRegistros = false;
						System.out.println("Página " + pagina + " vazia. Finalizando coleta de dados.");
					} else {
						// Adicionar registros ao array acumulado
						for (int i = 0; i < paginaAtual.length(); i++) {
							todosRegistros.put(paginaAtual.getJSONObject(i));
						}

						// Verificar se é a última página (menos de 100 registros OU exatamente 100 mas a próxima página não existe)
						if (paginaAtual.length() < 100) {
							temMaisRegistros = false;
							System.out.println("Última página encontrada com " + paginaAtual.length() + " registros.");
						} else {
							// Considerar solicitar a próxima página para verificar se ela existe
							pagina++;
							System.out.println("Página " + (pagina-1) + " completa com 100 registros. Avançando para página " + pagina);
						}

						System.out.println("Página " + (pagina-1) + ": " + paginaAtual.length() +
								" registros. Total acumulado: " + todosRegistros.length());
					}
				} else if (status == 404) {
					// Assumindo que a API retorna 404 quando a página não existe
					temMaisRegistros = false;
					System.out.println("Página " + pagina + " >:( não encontrada (404). Finalizando coleta de dados.");
				} else {
					throw new Exception(String.format(
							"Erro na requisição de alunos. Status: %d. Resposta: %s. URL: %s",
							status, response[1], urlCompleta
					));
				}
			}

			// Verificar se foram encontrados registros
			if (todosRegistros.length() == 0) {
				System.out.println("Nenhum registro de aluno encontrado para o período especificado. ;(");
				return;
			}

			// Criar uma resposta combinada com todos os registros
			String dadosCombinados = todosRegistros.toString();
			System.out.println("Total de registros de alunos acumulados: " + todosRegistros.length());

			// Processar todos os registros acumulados
			cadastrarCredorAlunoCursoTurma(
					mapaInfAlunos,
					mapaInfParceiros,
					dadosCombinados,
					codEmp
			);

		} catch (Exception e) {
			System.err.println("Erro ao processar período " + dataInicio +
					" até " + dataFim + ": " + e.getMessage());
			throw e;
		}
	}


	// Itera sobre o endpoint de alunos para processar e cadastrar informações.
	public void iterarEndpoint(Map<String, BigDecimal> mapaInfAlunos,
							   Map<String, BigDecimal> mapaInfParceiros, String url, String token,
							   BigDecimal codEmp) throws Exception {

		Date dataAtual = new Date();

		SimpleDateFormat formato = new SimpleDateFormat("yyyy-MM-dd");

		String dataFormatada = formato.format(dataAtual);

		try {

			System.out.println("While de iteração");

			String[] response = apiGet2(url + "/alunos" + "?dataInicial="
					+ dataFormatada + " 00:00:00&dataFinal=" + dataFormatada
					+ " 23:59:59" + "&quantidade=0", token);

			int status = Integer.parseInt(response[0]);
			System.out.println("Status teste: " + status);

			String responseString = response[1];
			System.out.println("response string: " + responseString);

			cadastrarCredorAlunoCursoTurma(mapaInfAlunos, mapaInfParceiros,
					responseString, codEmp);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	//Atualiza o status de uma ação agendada no banco de dados.
	private void updateAcaoAgendada() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		try {
			jdbc.openSession();

			String sqlUpd = "UPDATE TSIAAG SET ATIVO = 'N' WHERE NUAAG = 35";

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


	//Atualiza o status de uma carga de alunos no banco de dados.
	private void updateCarga(BigDecimal idCarga) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		try {
			jdbc.openSession();

			String sqlUpd = "UPDATE AD_CARGAALUNOS SET INTEGRADO = 'S' WHERE IDCARGA = "
					+ idCarga;

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

	// Insere a última página processada no banco de dados.
	private void insertUltPagina(int pagina) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		try {
			jdbc.openSession();

			String sqlUpd = "INSERT INTO AD_BLOCOPAGINACAO (IDPAGINA, PAGINAATUAL) VALUES ((SELECT NVL(MAX(IDPAGINA), 0) + 1 FROM AD_BLOCOPAGINACAO), "
					+ pagina + ")";

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


	//Retorna a última página processada do banco de dados.
	public int getPagina() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		int pagina = 0;
		try {

			jdbc.openSession();

			String sqlSlt = "SELECT NVL((SELECT PAGINAATUAL FROM AD_BLOCOPAGINACAO WHERE IDPAGINA = (SELECT MAX(IDPAGINA) FROM AD_BLOCOPAGINACAO)), 0) AS PAGINA FROM DUAL";

			pstmt = jdbc.getPreparedStatement(sqlSlt);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				pagina = rs.getInt("PAGINA");
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

		return pagina;
	}



	public void cadastrarCredorAlunoCursoTurma(
			Map<String, BigDecimal> mapaInfAlunos,
			Map<String, BigDecimal> mapaInfParceiros,
			String dadosCombinados,
			BigDecimal codEmp) {

		System.out.println("Cadastro principal");
		EnviromentUtils util = new EnviromentUtils();

		try {
			JsonParser parser = new JsonParser();
			JsonArray jsonArray = parser.parse(dadosCombinados).getAsJsonArray();
			for (JsonElement jsonElement : jsonArray) {
				JsonObject jsonObject = jsonElement.getAsJsonObject();

				// Extração dos dados do credor
				String credorNome       = getJsonString(jsonObject, "credor_nome");
				String credorCpf        = getJsonString(jsonObject, "credor_cpf");
				String credorEndereco   = getJsonString(jsonObject, "credor_endereco");
				String credorCep        = getJsonString(jsonObject, "credor_endereco_cep");
				String credorBairro     = getJsonString(jsonObject, "credor_endereco_bairro");
				String credorCidade     = getJsonString(jsonObject, "credor_endereco_cidade");
				String credorUf         = getJsonString(jsonObject, "credor_endereco_uf");
				String credorResidencial= getJsonString(jsonObject, "credor_telefone_residencial");
				String credorCelular    = getJsonString(jsonObject, "credor_telefone_celular");
				String credorComercial  = getJsonString(jsonObject, "credor_telefone_comercial");

				// Extração dos dados do aluno
				String alunoId          = getJsonString(jsonObject, "aluno_id");
				String alunoNome        = getJsonString(jsonObject, "aluno_nome");
				String alunoNomeSocial  = getJsonString(jsonObject, "aluno_nome_social");
				String alunoEndereco    = getJsonString(jsonObject, "aluno_endereco");
				String alunoCep         = getJsonString(jsonObject, "aluno_endereco_cep");
				String alunoBairro      = getJsonString(jsonObject, "aluno_endereco_bairro");
				String alunoCidade      = getJsonString(jsonObject, "aluno_endereco_cidade");
				String alunoUf          = getJsonString(jsonObject, "aluno_endereco_uf");
				String alunoSexo        = getJsonString(jsonObject, "aluno_sexo");
				String alunoDataNascimento = getJsonString(jsonObject, "aluno_data_nascimento");
				String alunoRg          = getJsonString(jsonObject, "aluno_rg");
				String alunoCpf         = getJsonString(jsonObject, "aluno_cpf");
				String alunoCelular     = getJsonString(jsonObject, "aluno_telefone_celular");
				String alunoResidencial = getJsonString(jsonObject, "aluno_telefone_residencial");
				String alunoEmail       = getJsonString(jsonObject, "aluno_email");

				// Extração dos dados do curso (primeiro item do array)
				JsonObject cursoObj = jsonObject.getAsJsonArray("cursos")
						.get(0)
						.getAsJsonObject();
				String cursoDescricao   = getJsonString(cursoObj, "curso_descricao");
				String cursoId          = getJsonString(cursoObj, "curso_id");
				String turmaId          = getJsonString(cursoObj, "turma_id");
				String alunoSituacao    = getJsonString(cursoObj, "situacao_descricao");
				String alunoSituacaoId  = getJsonString(cursoObj, "situacao_id");

				// Validação dos campos obrigatórios
				if (credorCpf == null || credorCidade == null) {
					selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Credor com informacoes invalidas ou nulas', SYSDATE, 'Aviso', "
							+ codEmp + ", '" + alunoId + "' FROM DUAL");
					continue;
				}

				String credorCpfTrim = credorCpf.trim();
				boolean credorExiste = mapaInfParceiros.containsKey(credorCpfTrim);
				boolean alunoExiste = alunoId != null && mapaInfAlunos.containsKey(alunoId.trim() + "###" + codEmp);

				if (!credorExiste) {
					System.out.println("Cadastrando novo credor: " + credorCpfTrim);
					BigDecimal credorAtual = insertCredor(credorNome, credorCpf, credorEndereco, credorCep,
							credorBairro, credorCidade, credorUf, credorResidencial,
							credorCelular, credorComercial, alunoNome, codEmp);
					if (credorAtual != null) {
						mapaInfParceiros.put(credorCpfTrim, credorAtual);
						insertCursoTurma(cursoDescricao, cursoId, turmaId, credorNome, alunoNome, codEmp);

						if (!alunoExiste && alunoId != null) {
							insertAluno(credorAtual, alunoId, alunoNome, alunoNomeSocial, alunoEndereco,
									alunoCep, alunoBairro, alunoCidade, alunoUf, alunoSexo,
									alunoDataNascimento, alunoRg, alunoCpf, alunoCelular,
									alunoResidencial, alunoEmail, alunoSituacao, alunoSituacaoId,
									credorNome, codEmp, cursoDescricao, turmaId);
							mapaInfAlunos.put(alunoId.trim() + "###" + codEmp, BigDecimal.ONE);
						}
					}
				} else {
					System.out.println("Credor já cadastrado: " + credorCpfTrim);
					BigDecimal credorCadastrado = mapaInfParceiros.get(credorCpfTrim);
					insertCursoTurma(cursoDescricao, cursoId, turmaId, credorNome, alunoNome, codEmp);
					if (!alunoExiste && alunoId != null) {
						insertAluno(credorCadastrado, alunoId, alunoNome, alunoNomeSocial, alunoEndereco,
								alunoCep, alunoBairro, alunoCidade, alunoUf, alunoSexo,
								alunoDataNascimento, alunoRg, alunoCpf, alunoCelular,
								alunoResidencial, alunoEmail, alunoSituacao, alunoSituacaoId,
								credorNome, codEmp, cursoDescricao, turmaId);
						mapaInfAlunos.put(alunoId.trim() + "###" + codEmp, BigDecimal.ONE);
					} else if (alunoExiste) {
						updateAluno(alunoSituacaoId, alunoSituacao, alunoId, alunoEndereco,
								alunoCep, alunoBairro, alunoCidade, alunoUf);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro no chamado do endpoint: "
					+ e.getMessage() + "', SYSDATE, 'Erro', " + codEmp + ", NULL FROM DUAL");
		}
	}

	// Método auxiliar para extrair valores String de um JsonObject de forma segura
	private String getJsonString(JsonObject obj, String key) {
		return (obj.has(key) && !obj.get(key).isJsonNull()) ? obj.get(key).getAsString() : null;
	}





	//Verifica se um credor já existe no banco de dados.
	public boolean getIfCredorExist(String credorCpf) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		int credor = 0;
		try {
			// updateTgfNumParc();

			jdbc.openSession();

			String sqlSlt = "SELECT COUNT(0) AS CREDOR FROM TGFPAR WHERE CGC_CPF = ?";

			pstmt = jdbc.getPreparedStatement(sqlSlt);
			pstmt.setString(1, credorCpf);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				credor = rs.getInt("CREDOR");
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
		if (credor > 0) {
			return true;
		}
		return false;
	}

	//Verifica se um aluno já existe no banco de dados.
	public boolean getIfAlunoExist(String alunoCpf) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		int aluno = 0;
		try {
			// updateTgfNumParc();

			jdbc.openSession();

			String sqlSlt = "SELECT COUNT(0) AS ALUNO FROM AD_ALUNOS WHERE ID_EXTERNO = ?";

			pstmt = jdbc.getPreparedStatement(sqlSlt);
			pstmt.setString(1, alunoCpf);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				aluno = rs.getInt("ALUNO");
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
		if (aluno > 0) {
			return true;
		}
		return false;
	}


	//Retorna o código do credor cadastrado no banco de dados.
	public BigDecimal getCredorCadastrado(String credorCpf) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal credorCadastrado = BigDecimal.ZERO;
		try {
			// updateTgfNumParc();

			jdbc.openSession();

			String sqlSlt = "SELECT CODPARC FROM TGFPAR WHERE CGC_CPF = ?";

			pstmt = jdbc.getPreparedStatement(sqlSlt);
			pstmt.setString(1, credorCpf);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				credorCadastrado = rs.getBigDecimal("CODPARC");
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
		return credorCadastrado;
	}


	//Atualiza o número de parceiros no banco de dados.
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


	//Atualiza as informações de um aluno no banco de dados.
	public void updateAluno(String idSituacao, String situacao, String idAluno, String endereco, String cep,
							String bairro, String cidade, String uf) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		try {
			jdbc.openSession();

			String sqlUpd = "UPDATE AD_ALUNOS SET SITUACAO_ID = ?, SITUACAO = ?, "
					+ "ENDERECO = ?, CEP = ?, BAIRRO = ?, CIDADE = ?, UF = ? " + "WHERE ID_EXTERNO = ?";

			pstmt = jdbc.getPreparedStatement(sqlUpd);

// Definindo os parâmetros de forma segura usando prepared statement
			pstmt.setString(1, idSituacao);
			pstmt.setString(2, situacao);
			pstmt.setString(3, endereco);
			pstmt.setString(4, cep);
			pstmt.setString(5, bairro);
			pstmt.setString(6, cidade);
			pstmt.setString(7, uf);
			pstmt.setString(8, idAluno);

			pstmt.executeUpdate();

			System.out.println("[DEBUG] Aluno atualizado com sucesso. ID_EXTERNO: " + idAluno);
		} catch (SQLException e) {
			System.err.println("[DEBUG] Erro ao atualizar aluno. ID_EXTERNO: " + idAluno);
			System.err.println("[DEBUG] Mensagem de erro: " + e.getMessage());
			e.printStackTrace();
			throw e;
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}


	//Retorna o número máximo de parceiros cadastrados no banco de dados.
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



	//codusu alterado para 426
//	public BigDecimal insertCredor(String credorNome, String credorCpf,
//	        String credorEndereco, String credorCep, String credorBairro,
//	        String credorCidade, String credorUf, String credorResidencial,
//	        String credorCelular, String credorComercial, String alunoNome,
//	        BigDecimal codemp)
//	        throws Exception {
//	    EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
//	    JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
//	    PreparedStatement pstmt = null;
//	    PreparedStatement pstmtCheck = null;
//	    PreparedStatement pstmtCredor = null;
//	    ResultSet rs = null;
//	    ResultSet rsCredor = null;
//
//	    EnviromentUtils util = new EnviromentUtils();
//
//	    BigDecimal atualCodparc = null;
//	    BigDecimal codCid = null;
//
//	    String tipPessoa = "";
//
//	    BigDecimal countBai = BigDecimal.ZERO;
//	    BigDecimal codEnd = BigDecimal.ZERO;
//	    BigDecimal codBai = BigDecimal.ZERO;
//
//	    // Log inicial de entrada no método
//	    System.out.println("[TRACE] Iniciando inserção de credor");
//	    System.out.println("[TRACE] Dados de entrada:");
//	    System.out.println("[TRACE] Nome: " + credorNome);
//	    System.out.println("[TRACE] CPF/CNPJ: " + credorCpf);
//	    System.out.println("[TRACE] Cidade: " + credorCidade);
//	    System.out.println("[TRACE] UF: " + credorUf);
//
//	    // Validação de entrada
//	    if (credorNome == null || credorNome.trim().isEmpty()) {
//	        System.err.println("[ERROR] Nome do credor não pode ser nulo ou vazio");
//	        return null;
//	    }
//
//	    if (credorCpf == null || credorCpf.trim().isEmpty()) {
//	        System.err.println("[ERROR] CPF/CNPJ do credor não pode ser nulo ou vazio");
//	        return null;
//	    }
//
//	    if (credorCpf.length() == 11) {
//	        tipPessoa = "F";
//	        System.out.println("[TRACE] Tipo de Pessoa: Física (CPF)");
//	    } else if (credorCpf.length() == 14) {
//	        tipPessoa = "J";
//	        System.out.println("[TRACE] Tipo de Pessoa: Jurídica (CNPJ)");
//	    } else {
//	        System.err.println("[ERROR] CPF/CNPJ inválido: tamanho incorreto");
//	        return null;
//	    }
//
//	    if ((credorBairro != null)
//	            && (validarCadastroBairro(credorBairro, credorNome, alunoNome))) {
//	        try {
//	            codBai = insertBairro(credorBairro, credorNome, alunoNome);
//	            countBai = countBai.add(BigDecimal.ONE);
//	            System.out.println("[TRACE] Bairro inserido com sucesso. Código: " + codBai);
//	        } catch (Exception e) {
//	            System.err.println("[ERROR] Falha ao inserir bairro: " + e.getMessage());
//	            e.printStackTrace();
//	        }
//	    }
//
//	    try {
//	        jdbc.openSession();
//
//	        // Log antes de verificar a existência do credor
//	        System.out.println("[TRACE] Verificando se credor já existe");
//	        String checkCredorSQL = "SELECT CODPARC FROM TGFPAR WHERE CGC_CPF = ?";
//	        pstmtCredor = jdbc.getPreparedStatement(checkCredorSQL);
//	        pstmtCredor.setString(1, credorCpf);
//	        rsCredor = pstmtCredor.executeQuery();
//
//	        if (rsCredor.next()) {
//	            atualCodparc = rsCredor.getBigDecimal("CODPARC");
//	            System.out.println("[WARN] Credor já existe. CODPARC: " + atualCodparc);
//	            return atualCodparc;
//	        }
//
//	        // Log antes de gerar novo código de parceiro
//	        atualCodparc = util.getMaxNumParc();
//	        System.out.println("[TRACE] Novo CODPARC gerado: " + atualCodparc);
//
//	        // Log de busca de cidade
//	        System.out.println("[TRACE] Buscando código da cidade");
//	        String checkCidadeSQL = "SELECT max(codcid) as codcid FROM tsicid WHERE " +
//	            "TRANSLATE(UPPER(descricaocorreio), 'áéíóúâêîôûàèìòùãõçÁÉÍÓÚÂÊÎÔÛÀÈÌÒÙÃÕÇ', " +
//	            "'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC') LIKE TRANSLATE(UPPER(?), " +
//	            "'áéíóúâêîôûàèìòùãõçÁÉÍÓÚÂÊÎÔÛÀÈÌÒÙÃÕÇ', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC') " +
//	            "OR SUBSTR(UPPER(descricaocorreio), 1, INSTR(UPPER(descricaocorreio), ' ') - 1) " +
//	            "LIKE TRANSLATE(UPPER(?), 'áéíóúâêîôûàèìòùãõçÁÉÍÓÚÂÊÎÔÛÀÈÌÒÙÃÕÇ', " +
//	            "'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC')";
//
//	        pstmtCheck = jdbc.getPreparedStatement(checkCidadeSQL);
//	        pstmtCheck.setString(1, credorCidade.trim());
//	        pstmtCheck.setString(2, credorCidade.trim());
//	        rs = pstmtCheck.executeQuery();
//
//	        if (rs.next()) {
//	            codCid = rs.getBigDecimal("codcid");
//	            System.out.println("[TRACE] Código da cidade encontrado: " + codCid);
//	        }
//
//	        // Se a cidade não for encontrada, insere a cidade primeiro
//	        if (codCid == null) {
//	            System.out.println("[WARN] Cidade não encontrada. Tentando inserir cidade.");
//	            codCid = insertCidade(credorCidade, credorUf, jdbc);
//
//	            if (codCid == null) {
//	                System.err.println("[ERROR] Falha ao cadastrar cidade: " + credorCidade);
//	                selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro ao cadastrar credor: Cidade não encontrada ou não pode ser criada: "
//	                    + credorCidade + "', SYSDATE, 'Erro', " + codemp + ", '" + credorNome + "' FROM DUAL");
//	                return null;
//	            }
//	        }
//
//	        String sqlP = "INSERT INTO TGFPAR(CODPARC, NOMEPARC, RAZAOSOCIAL, TIPPESSOA, AD_ENDCREDOR, " +
//		            "CODBAI, CODCID, CEP, TELEFONE, CGC_CPF, DTCAD, DTALTER, AD_FLAGALUNO, CODUSU)  " +
//		            "VALUES(?, ?, ?, ?, ?, NVL((select max(codbai) from tsibai where TRANSLATE(upper(nomebai), " +
//		            "'áéíóúâêîôûàèìòùãõçÁÉÍÓÚÂÊÎÔÛÀÈÌÒÙÃÕÇ', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC') " +
//		            "like TRANSLATE(upper(?), 'áéíóúâêîôûàèìòùãõçÁÉÍÓÚÂÊÎÔÛÀÈÌÒÙÃÕÇ', " +
//		            "'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC')), 0), ?, ?, ?, ?, SYSDATE, SYSDATE, 'S', ?)";
//
//	        pstmt = jdbc.getPreparedStatement(sqlP);
//	        pstmt.setBigDecimal(1, atualCodparc);
//	        pstmt.setString(2, credorNome.toUpperCase());
//	        pstmt.setString(3, credorNome.toUpperCase());
//	        pstmt.setString(4, tipPessoa);
//	        pstmt.setString(5, credorEndereco);
//	        pstmt.setString(6, credorBairro);
//	        pstmt.setBigDecimal(7, codCid);
//	        pstmt.setString(8, credorCep);
//	        pstmt.setString(9, credorCelular);
//	        pstmt.setString(10, credorCpf);
//	        pstmt.setInt(11, 426);
//
//	        // Log antes da inserção
//	        System.out.println("[TRACE] Preparando para inserir novo credor");
//	        System.out.println("[TRACE] Dados para inserção:");
//	        System.out.println("[TRACE] CODPARC: " + atualCodparc);
//	        System.out.println("[TRACE] Nome: " + credorNome.toUpperCase());
//	        System.out.println("[TRACE] Tipo Pessoa: " + tipPessoa);
//	        System.out.println("[TRACE] Cidade Código: " + codCid);
//	        System.out.println("[TRACE] CEP: " + credorCep);
//	        System.out.println("[TRACE] Celular: " + credorCelular);
//	        System.out.println("[TRACE] CPF/CNPJ: " + credorCpf);
//
//	        int rowsAffected = pstmt.executeUpdate();
//
//	        if (rowsAffected > 0) {
//	            System.out.println("[INFO] Credor inserido com sucesso!");
//	            System.out.println("[INFO] Linhas afetadas: " + rowsAffected);
//	            System.out.println("[INFO] CODPARC gerado: " + atualCodparc);
//	        } else {
//	            System.err.println("[ERROR] Nenhuma linha foi inserida. Possível problema na inserção.");
//	            atualCodparc = null;
//	        }
//	    } catch (SQLException e) {
//	        System.err.println("[FATAL] Erro completo durante inserção de credor:");
//	        System.err.println("[FATAL] Mensagem: " + e.getMessage());
//	        System.err.println("[FATAL] Código de Erro SQL: " + e.getErrorCode());
//	        System.err.println("[FATAL] Estado SQL: " + e.getSQLState());
//
//	        selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro ao cadastrar credor: " + e.getMessage()
//	            + "', SYSDATE, 'Erro', " + codemp + ", '" + credorNome + "' FROM DUAL");
//
//	        e.printStackTrace();
//	        atualCodparc = null;
//	    } finally {
//	        // Logs de fechamento de recursos
//	        System.out.println("[TRACE] Fechando recursos de banco de dados");
//
//	        try {
//	            if (rsCredor != null) rsCredor.close();
//	            if (rs != null) rs.close();
//	            if (pstmtCredor != null) pstmtCredor.close();
//	            if (pstmtCheck != null) pstmtCheck.close();
//	            if (pstmt != null) pstmt.close();
//	            jdbc.closeSession();
//
//	            System.out.println("[TRACE] Todos os recursos fechados com sucesso");
//	        } catch (SQLException e) {
//	            System.err.println("[ERROR] Erro ao fechar recursos: " + e.getMessage());
//	            e.printStackTrace();
//	        }
//	    }
//	    return atualCodparc;
//	}
	public BigDecimal insertCredor(String credorNome, String credorCpf,
								   String credorEndereco, String credorCep, String credorBairro,
								   String credorCidade, String credorUf, String credorResidencial,
								   String credorCelular, String credorComercial, String alunoNome,
								   BigDecimal codemp)
			throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		PreparedStatement pstmtCheck = null;
		PreparedStatement pstmtCredor = null;
		ResultSet rs = null;
		ResultSet rsCredor = null;

		EnviromentUtils util = new EnviromentUtils();

		BigDecimal atualCodparc = null;
		BigDecimal codCid = null;

		String tipPessoa = "";

		BigDecimal countBai = BigDecimal.ZERO;
		BigDecimal codEnd = BigDecimal.ZERO;
		BigDecimal codBai = BigDecimal.ZERO;
		if (credorCpf.length() == 11) {
			tipPessoa = "F";
		} else if (credorCpf.length() == 14) {
			tipPessoa = "J";
		}
		if ((credorBairro != null)
				&& (validarCadastroBairro(credorBairro, credorNome, alunoNome))) {
			codBai = insertBairro(credorBairro, credorNome, alunoNome);
			countBai = countBai.add(BigDecimal.ONE);
		}
		try {
			jdbc.openSession();

			// Verificar se o credor já existe pelo CPF/CNPJ
			String checkCredorSQL = "SELECT CODPARC FROM TGFPAR WHERE CGC_CPF = ?";
			pstmtCredor = jdbc.getPreparedStatement(checkCredorSQL);
			pstmtCredor.setString(1, credorCpf);
			rsCredor = pstmtCredor.executeQuery();

			// Se o credor já existe, retorna o CODPARC existente
			if (rsCredor.next()) {
				atualCodparc = rsCredor.getBigDecimal("CODPARC");
				System.out.println("[DEBUG] Credor já existe com CPF/CNPJ: " + credorCpf + ", CODPARC: " + atualCodparc);

				// Opcionalmente, você pode atualizar os dados do credor aqui se necessário
				// updateCredor(atualCodparc, credorNome, credorEndereco, ...);

				return atualCodparc;
			}

			// Se chegou aqui, o credor não existe e precisa ser inserido
			atualCodparc = util.getMaxNumParc();

			// Verificar se a cidade existe e obter seu código
			String checkCidadeSQL = "SELECT max(codcid) as codcid FROM tsicid WHERE " +
					"TRANSLATE(UPPER(descricaocorreio), 'áéíóúâêîôûàèìòùãõçÁÉÍÓÚÂÊÎÔÛÀÈÌÒÙÃÕÇ', " +
					"'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC') LIKE TRANSLATE(UPPER(?), " +
					"'áéíóúâêîôûàèìòùãõçÁÉÍÓÚÂÊÎÔÛÀÈÌÒÙÃÕÇ', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC') " +
					"OR SUBSTR(UPPER(descricaocorreio), 1, INSTR(UPPER(descricaocorreio), ' ') - 1) " +
					"LIKE TRANSLATE(UPPER(?), 'áéíóúâêîôûàèìòùãõçÁÉÍÓÚÂÊÎÔÛÀÈÌÒÙÃÕÇ', " +
					"'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC')";

			pstmtCheck = jdbc.getPreparedStatement(checkCidadeSQL);
			pstmtCheck.setString(1, credorCidade.trim());
			pstmtCheck.setString(2, credorCidade.trim());
			rs = pstmtCheck.executeQuery();

			if (rs.next()) {
				codCid = rs.getBigDecimal("codcid");
			}

			// Se a cidade não for encontrada, insere a cidade primeiro
			if (codCid == null) {

				codCid = insertCidade(credorCidade, credorUf, jdbc);

				// Se ainda não conseguiu um código de cidade, registrar o erro e abortar
				if (codCid == null) {
					selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro ao cadastrar credor: Cidade não encontrada ou não pode ser criada: "
							+ credorCidade + "', SYSDATE, 'Erro', " + codemp + ", '" + credorNome + "' FROM DUAL");
					return null;
				}
			}

			String sqlP = "INSERT INTO TGFPAR(CODPARC, NOMEPARC, RAZAOSOCIAL, TIPPESSOA, AD_ENDCREDOR, " +
					"CODBAI, CODCID, CEP, TELEFONE, CGC_CPF, DTCAD, DTALTER, AD_FLAGALUNO) " +
					"VALUES(?, ?, ?, ?, ?, NVL((select max(codbai) from tsibai where TRANSLATE(upper(nomebai), " +
					"'áéíóúâêîôûàèìòùãõçÁÉÍÓÚÂÊÎÔÛÀÈÌÒÙÃÕÇ', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC') " +
					"like TRANSLATE(upper(?), 'áéíóúâêîôûàèìòùãõçÁÉÍÓÚÂÊÎÔÛÀÈÌÒÙÃÕÇ', " +
					"'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC')), 0), ?, ?, ?, ?, SYSDATE, SYSDATE, 'S')";

			pstmt = jdbc.getPreparedStatement(sqlP);
			pstmt.setBigDecimal(1, atualCodparc);
			pstmt.setString(2, credorNome.toUpperCase());
			pstmt.setString(3, credorNome.toUpperCase());
			pstmt.setString(4, tipPessoa);
			pstmt.setString(5, credorEndereco);
			pstmt.setString(6, credorBairro);
			pstmt.setBigDecimal(7, codCid);  // Usar o código da cidade que foi verificado ou criado
			pstmt.setString(8, credorCep);
			pstmt.setString(9, credorCelular);
			pstmt.setString(10, credorCpf);

			pstmt.executeUpdate();
			System.out.println("[DEBUG] Novo credor inserido com CPF/CNPJ: " + credorCpf + ", CODPARC: " + atualCodparc);
		} catch (SQLException e) {
			selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro ao cadastrar credor: " + e.getMessage()
					+ "', SYSDATE, 'Erro', " + codemp + ", '" + credorNome + "' FROM DUAL");
			System.err.println("[DEBUG] Erro ao inserir credor: " + e.getMessage());
			e.printStackTrace();
			atualCodparc = null;
		} finally {
			if (rsCredor != null) {
				rsCredor.close();
			}
			if (rs != null) {
				rs.close();
			}
			if (pstmtCredor != null) {
				pstmtCredor.close();
			}
			if (pstmtCheck != null) {
				pstmtCheck.close();
			}
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
		return atualCodparc;
	}



	private BigDecimal insertCidade(String nomeCidade, String uf, JdbcWrapper jdbc) throws Exception {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		BigDecimal codCid = null;

		try {
			System.out.println("DEBUG: Iniciando inserção da cidade: " + nomeCidade + ", UF: " + uf);

			// Verificar se a cidade já existe
			String sqlVerifica = "SELECT CODCID FROM TSICID WHERE UPPER(NOMECID) = ? AND UPPER(UF) = ?";
			System.out.println("DEBUG: Verificando se cidade já existe: " + sqlVerifica);
			pstmt = jdbc.getPreparedStatement(sqlVerifica);
			pstmt.setString(1, nomeCidade.toUpperCase());
			pstmt.setString(2, uf.toUpperCase());
			rs = pstmt.executeQuery();

			if (rs.next()) {
				codCid = rs.getBigDecimal("CODCID");
				System.out.println("DEBUG: Cidade já existe com código: " + codCid);
				return codCid;
			}

			rs.close();
			pstmt.close();

			// Obter o próximo código usando consulta mais simples (evitando NVL e funções complexas)
			String sqlSeq = "SELECT MAX(CODCID) + 1 AS NEXT_CODCID FROM TSICID";
			System.out.println("DEBUG: Executando SQL simplificado para obter próximo código: " + sqlSeq);
			pstmt = jdbc.getPreparedStatement(sqlSeq);
			rs = pstmt.executeQuery();

			if (rs.next()) {
				codCid = rs.getBigDecimal("NEXT_CODCID");
				// Se o resultado for NULL (tabela vazia), começar com 1
				if (codCid == null) {
					codCid = new BigDecimal(1);
				}
				System.out.println("DEBUG: Código gerado para a nova cidade: " + codCid);
			} else {
				System.out.println("DEBUG: Não foi possível obter o próximo código, definindo como 1");
				codCid = new BigDecimal(1);
			}

			rs.close();
			pstmt.close();

			// Tentar inserção usando JDBC padrão
			String sqlInsert = "INSERT INTO TSICID (CODCID, NOMECID, UF, DESCRICAOCORREIO) VALUES (?, ?, ?, ?)";


			System.out.println("DEBUG: Preparando SQL de inserção: " + sqlInsert);
			pstmt = jdbc.getPreparedStatement(sqlInsert);

			int codCidInt = codCid.intValue(); // Converter para inteiro (já que CODCID é inteiro)
			System.out.println("DEBUG: Usando CODCID como inteiro: " + codCidInt);

			// Definir parâmetros como tipos primitivos para evitar conversões automatizadas
			pstmt.setInt(1, codCidInt);
			pstmt.setString(2, nomeCidade.toUpperCase());
			pstmt.setString(3, uf.toUpperCase());
			pstmt.setString(4, nomeCidade.toUpperCase());

			int rowsAffected = pstmt.executeUpdate();
			System.out.println("DEBUG: Inserção realizada. Linhas afetadas: " + rowsAffected);

			return new BigDecimal(codCidInt);
		} catch (SQLException e) {
			System.out.println("DEBUG: ERRO na inserção da cidade: " + e.getMessage());
			System.out.println("DEBUG: SQL State: " + e.getSQLState() + ", Error Code: " + e.getErrorCode());

			// Tentar inserção direta com valores literais como último recurso
			try {
				if (codCid != null) {
					String directSql = "INSERT INTO TSICID (CODCID, NOMECID, UF, DESCRICAOCORREIO) VALUES (" +
							codCid.intValue() + ", '" +
							nomeCidade.toUpperCase().replace("'", "''") + "', '" +
							uf.toUpperCase() + "', '" +
							nomeCidade.toUpperCase().replace("'", "''") + "')";
					System.out.println("DEBUG: Tentando inserção direta: " + directSql);

					// Usando Statement em vez de PreparedStatement para evitar parâmetros
					Statement stmt = jdbc.getConnection().createStatement();
					int rowsAffected = stmt.executeUpdate(directSql);
					stmt.close();

					System.out.println("DEBUG: Inserção direta realizada. Linhas afetadas: " + rowsAffected);
					return codCid;
				}
			} catch (Exception ex) {
				System.out.println("DEBUG: Falha na inserção direta: " + ex.getMessage());
			}

			e.printStackTrace();
			return null;
		} finally {
			System.out.println("DEBUG: Finalizando método insertCidade");
			if (rs != null) {
				try { rs.close(); } catch (Exception e) { /* ignore */ }
			}
			if (pstmt != null) {
				try { pstmt.close(); } catch (Exception e) { /* ignore */ }
			}
		}
	}




	//Insere um novo curso e turma no banco de dados.
	public void insertCursoTurma(String cursoDescricao, String cursoId,
								 String turmaId, String credorNome, String alunoNome,
								 BigDecimal codEmp) throws Exception {

		if ((cursoDescricao != null)
				&& (validarCadastroCurso(cursoDescricao, credorNome, alunoNome))) {
			// insertCurso(cursoDescricao, cursoId, credorNome, alunoNome);
		} else {
			//updateCurso(cursoDescricao, cursoId, credorNome, alunoNome);
		}

		if ((cursoDescricao != null)
				&& (validarCadastroCursoProj(cursoDescricao, credorNome,
				alunoNome, codEmp))) {
			insertCursoProj(cursoDescricao, credorNome, alunoNome, codEmp,
					cursoId);
		}

		if ((turmaId != null)
				&& (validarCadastroTurma(turmaId, credorNome, alunoNome,
				cursoDescricao, codEmp))) {
			insertCadastroTurma(turmaId, codEmp, cursoDescricao);
		}
	}


	//Valida se um curso já está cadastrado no banco de dados.
	public boolean validarCadastroCurso(String curso, String credorNome,
										String alunoNome) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal count = BigDecimal.ZERO;
		try {
			jdbc.openSession();

			String sqlNota = "SELECT COUNT(0) AS COUNT FROM TSICUS  WHERE TRANSLATE(UPPER (DESCRCENCUS), '������������������������������������', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC') LIKE TRANSLATE (UPPER ('%"
					+ curso.trim()
					+ "%'), "
					+ "'������������������������������������', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC')";

			pstmt = jdbc.getPreparedStatement(sqlNota);
			rs = pstmt.executeQuery();

			if (rs.next()) {
				count = rs.getBigDecimal("COUNT");
			}

		} catch (SQLException e) {
			e.printStackTrace();
			insertLogIntegracao(
					"Erro ao validar se curso j� cadastrado: " + e.getMessage(),
					"Erro", credorNome, alunoNome);
			throw e;
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
		if (count.compareTo(BigDecimal.ZERO) == 0) {
			return true;
		}
		return false;
	}



	//Valida se um curso já está cadastrado como projeto no banco de dados.
	public boolean validarCadastroCursoProj(String curso, String credorNome,
											String alunoNome, BigDecimal codEmp) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal count = BigDecimal.ZERO;
		try {
			jdbc.openSession();

			String sqlNota = "SELECT COUNT(0) AS COUNT FROM TCSPRJ  WHERE TRANSLATE(UPPER (IDENTIFICACAO), '������������������������������������', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC') LIKE TRANSLATE (UPPER ('"
					+ curso.trim()
					+ "'), "
					+ "'������������������������������������', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC') AND CODPROJPAI = (SELECT MAX(CODPROJ) FROM TCSPRJ WHERE CODEMP = "
					+ codEmp + ")";

			pstmt = jdbc.getPreparedStatement(sqlNota);
			rs = pstmt.executeQuery();
			if (rs.next()) {
				count = rs.getBigDecimal("COUNT");
			}
		} catch (SQLException e) {
			e.printStackTrace();
			insertLogIntegracao(
					"Erro ao validar se curso j� cadastrado como projeto: "
							+ e.getMessage(), "Erro", credorNome, alunoNome);
			throw e;
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
		if (count.compareTo(BigDecimal.ZERO) == 0) {
			return true;
		}
		return false;
	}


	//Valida se uma turma já está cadastrada no banco de dados.
	public boolean validarCadastroTurma(String turma, String credorNome,
										String alunoNome, String cursoNome, BigDecimal codEmp)
			throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal count = BigDecimal.ZERO;


		try {
			jdbc.openSession();

			String sqlNota = "SELECT COUNT(0) AS COUNT FROM TCSPRJ  WHERE TRANSLATE(UPPER (IDENTIFICACAO), '������������������������������������', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC') LIKE TRANSLATE (UPPER ('%"
					+
					turma.trim()
					+ "%'), "
					+ "'������������������������������������', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC') AND CODPROJPAI = (SELECT CODPROJ FROM TCSPRJ WHERE CODPROJPAI = (SELECT MAX(CODPROJ) FROM TCSPRJ WHERE CODEMP = "
					+ codEmp + ") AND TRANSLATE(UPPER (IDENTIFICACAO), '������������������������������������', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC') LIKE TRANSLATE (UPPER ('"
					+
					cursoNome.trim()
					+ "'), "
					+ "'������������������������������������', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC'))";

			pstmt = jdbc.getPreparedStatement(sqlNota);
			rs = pstmt.executeQuery();
			if (rs.next()) {
				count = rs.getBigDecimal("COUNT");
			}
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
		if (count.compareTo(BigDecimal.ZERO) == 0) {
			return true;
		}
		return false;
	}


	//Insere uma nova turma no banco de dados.
	public void insertCadastroTurma(String turmaId, BigDecimal codEmp,
									String cursoDescricao) throws Exception {

		EnviromentUtils util = new EnviromentUtils();

		String ativo = "S";
		String analitico = "N";

		if (turmaId.length() >= 20) {
			turmaId = turmaId.substring(0, 20).toUpperCase();
		} else {
			turmaId = turmaId.toUpperCase();
		}

		int grau = 3;

		try {

			util.insertCurso(cursoDescricao.trim(), codEmp, turmaId.trim(), ativo, analitico, grau);

		} catch (Exception se) {
			selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro cadastrando turma: " + se.getMessage() +" Nome Turma: "+turmaId+"', SYSDATE, 'Erro', "+codEmp+", null FROM DUAL");

			/*util.inserirLog("Erro cadastrando turma: " + se.getMessage() + " Nome Turma: " + turmaId,
					"Erro", "", codEmp);*/
			se.printStackTrace();
		} finally {

		}
	}

	//Insere um novo curso no banco de dados.

	public void insertCurso(String cursoDescricao, String cursoId,
							String credorNome, String alunoNome) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		String ativo = "S";
		String analitico = "S";

		int codCenCusPai = 10101000;
		int grau = 4;

		System.out.println("ID_EXTERNO: " + cursoId);
		try {
			jdbc.openSession();

			String sqlUpdate = "INSERT INTO TSICUS(CODCENCUS, CODCENCUSPAI, DESCRCENCUS, ATIVO, ANALITICO, GRAU, AD_IDEXTERNO)VALUES ((SELECT MAX(CODCENCUS) + 1 FROM TSICUS WHERE CODCENCUSPAI = '10101000'), ? ,? , ?, ?, ?, ?)";

			pstmt = jdbc.getPreparedStatement(sqlUpdate);

			pstmt.setInt(1, codCenCusPai);
			pstmt.setString(2, cursoDescricao);
			pstmt.setString(3, ativo);
			pstmt.setString(4, analitico);
			pstmt.setInt(5, grau);
			pstmt.setString(6, cursoId);
			pstmt.executeUpdate();
		} catch (Exception se) {
			insertLogIntegracao("Erro cadastrando curso: " + se.getMessage(),
					"Erro", credorNome, alunoNome);
			se.printStackTrace();
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}


	//Atualiza as informações de um curso no banco de dados.
	public void updateCurso(String cursoDescricao, String cursoId,
							String credorNome, String alunoNome) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		System.out.println("ID_EXTERNO: " + cursoId);
		try {
			jdbc.openSession();

			String sqlUpdate = "UPDATE TSICUS SET AD_IDEXTERNO = '"
					+ cursoId
					+ "' WHERE CODCENCUS = (SELECT CODCENCUS FROM TSICUS  WHERE TRANSLATE(UPPER (DESCRCENCUS), '������������������������������������', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC') LIKE TRANSLATE (UPPER ('%"
					+ cursoDescricao.trim()
					+ "'),  '������������������������������������', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC'))";

			pstmt = jdbc.getPreparedStatement(sqlUpdate);

			pstmt.executeUpdate();
		} catch (Exception se) {
			insertLogIntegracao("Erro cadastrando curso: " + se.getMessage(),
					"Erro", credorNome, alunoNome);
			se.printStackTrace();
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}


	// Retorna o código máximo de projetos no banco de dados.
	public BigDecimal getMaxCodProjPaiProj(BigDecimal codEmp) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal codProjPai = BigDecimal.ZERO;
		try {
			jdbc.openSession();

			String sqlSelect = "select codproj from TCSPRJ where codemp = "
					+ codEmp;

			pstmt = jdbc.getPreparedStatement(sqlSelect);

			rs = pstmt.executeQuery();
			if (rs.next()) {
				codProjPai = rs.getBigDecimal("codproj");
			}
		} catch (Exception se) {
			insertLogIntegracao(
					"Erro cadastrando curso como projeto: " + se.getMessage(),
					"Erro", "", "");
			se.printStackTrace();
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
		return codProjPai;
	}


	// Insere um novo curso como projeto no banco de dados.
	public BigDecimal insertCursoProj(String cursoDescricao, String credorNome,
									  String alunoNome, BigDecimal codEmp, String cursoId)
			throws Exception {


		EnviromentUtils util = new EnviromentUtils();

		BigDecimal codProjPai = getMaxCodProjPaiProj(codEmp);

		String ativo = "S";
		String analitico = "S";
		int grau = 2;

		String cursoAbrev = "";
		if (cursoDescricao.length() >= 20) {
			cursoAbrev = cursoDescricao.substring(0, 20).toUpperCase();
		} else {
			cursoAbrev = cursoDescricao.toUpperCase();
		}
		try {

			util.insertProjeto(codProjPai, cursoDescricao.toUpperCase().trim(),
					cursoAbrev.toUpperCase().trim(), ativo, analitico, grau, cursoId);

		} catch (Exception se) {
			selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro cadastrando curso como projeto: " + se.getMessage() +" Nome Curso: "+cursoDescricao+"', SYSDATE, 'Erro', "+codEmp+", null FROM DUAL");

			/*util.inserirLog(
					"Erro cadastrando curso como projeto: " + se.getMessage() +" Nome Curso: " + cursoDescricao,
					"Erro", "", codEmp);*/
			se.printStackTrace();
		} finally {

		}
		return null;
	}



	//Insere um novo aluno no banco de dados.

	//ERRO DA DESCRICAO DO CURSO
	public void insertAluno(BigDecimal credotAtual, String alunoId,
							String alunoNome, String alunoNomeSocial, String alunoEndereco,
							String alunoCep, String alunoBairro, String alunoCidade,
							String alunoUf, String alunoSexo, String alunoDataNascimento,
							String alunoRg, String alunoCpf, String alunoCelular,
							String alunoResidencial, String alunoEmail, String alunoSituacao,
							String alunoSituacaoId, String credorNome, BigDecimal codEmp,
							String descrCurso, String turmaId) throws Exception {

		// System.out.println("\n=== DEBUG INSERT ALUNO ===");
		//  System.out.println("turmaId: " + turmaId);
		//    System.out.println("descrCurso original: " + descrCurso);


		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		PreparedStatement verifyStmt = null;
		ResultSet rs = null;
		BigDecimal codCenCus = null;

		EnviromentUtils util = new EnviromentUtils();

		if (alunoNomeSocial == null) {
			alunoNomeSocial = " ";
		}

		try {
			jdbc.openSession();

			// Abordagem mais simples usando UPPER e removendo acentos
			String verifyQuery = "SELECT codcencus, descrcencus FROM tsicus";
			verifyStmt = jdbc.getPreparedStatement(verifyQuery);
			rs = verifyStmt.executeQuery();

			// Normaliza o nome do curso (remove acentos, maiúsculas e espaços extras)
			String normalizedDescrCurso = descrCurso
					.toUpperCase()
					.replaceAll("\\s+", " ")
					.trim()
					.replace("Á", "A")
					.replace("À", "A")
					.replace("Ã", "A")
					.replace("Â", "A")
					.replace("É", "E")
					.replace("Ê", "E")
					.replace("Í", "I")
					.replace("Ó", "O")
					.replace("Ô", "O")
					.replace("Õ", "O")
					.replace("Ú", "U")
					.replace("Ç", "C");

			//  System.out.println("[DEBUG] descrCurso normalizado: " + normalizedDescrCurso);

			boolean cursoEncontrado = false;
			//    System.out.println("[DEBUG] Buscando curso no banco. Cursos disponíveis:");

			while (rs.next()) {
				String descricaoBanco = rs.getString("descrcencus");
				String descricaoBancoNormalizada = descricaoBanco
						.toUpperCase()
						.replaceAll("\\s+", " ")
						.trim()
						.replace("Á", "A")
						.replace("À", "A")
						.replace("Ã", "A")
						.replace("Â", "A")
						.replace("É", "E")
						.replace("Ê", "E")
						.replace("Í", "I")
						.replace("Ó", "O")
						.replace("Ô", "O")
						.replace("Õ", "O")
						.replace("Ú", "U")
						.replace("Ç", "C");

				//    System.out.println(" - Original: [" + descricaoBanco + "] Normalizado: [" + descricaoBancoNormalizada + "]");

				if (normalizedDescrCurso.equals(descricaoBancoNormalizada)) {
					codCenCus = rs.getBigDecimal("codcencus");
					System.out.println("[DEBUG] Curso encontrado! Código: " + codCenCus + " para: [" + descricaoBanco + "]");
					cursoEncontrado = true;
					break;
				}
			}

			if (!cursoEncontrado) {
				// Tentar uma busca menos restritiva
				rs.close();
				verifyStmt.close();

				verifyQuery = "SELECT codcencus, descrcencus FROM tsicus WHERE UPPER(descrcencus) LIKE ?";
				verifyStmt = jdbc.getPreparedStatement(verifyQuery);
				verifyStmt.setString(1, "%" + normalizedDescrCurso.replace("CURSO TECNICO EM", "%") + "%");
				rs = verifyStmt.executeQuery();

				System.out.println("[DEBUG] Fazendo busca menos restritiva com: " +
						normalizedDescrCurso.replace("CURSO TECNICO EM", "%"));

				if (rs.next()) {
					codCenCus = rs.getBigDecimal("codcencus");
					System.out.println("[DEBUG] Curso encontrado na busca menos restritiva! Código: " +
							codCenCus + " para: [" + rs.getString("descrcencus") + "]");
					cursoEncontrado = true;
				} else {
					throw new SQLException("Nenhum curso encontrado para a descrição: " + descrCurso);
				}
			}

			String sqlP = "INSERT INTO AD_ALUNOS (CODPARC, ID_EXTERNO, NOME, NOME_SOCIAL, ENDERECO, CEP, BAIRRO, CIDADE, UF, SEXO, DATA_NASCIMENTO, RG, CPF, TELEFONE_CELULAR, TELEFONE_RESIDENCIAL, EMAIL, SITUACAO, SITUACAO_ID, CODEMP, CODCENCUS, TURMA) "
					+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, (SELECT TO_CHAR(TO_DATE(?, 'yyyy-MM-dd'), 'dd/MM/yyyy') FROM dual), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

			System.out.println("[DEBUG] Query SQL: " + sqlP);
			System.out.println("[DEBUG] Valor de turmaId antes do INSERT: " + turmaId);

			pstmt = jdbc.getPreparedStatement(sqlP);

			pstmt.setBigDecimal(1, credotAtual);
			pstmt.setString(2, alunoId);
			pstmt.setString(3, alunoNome);
			pstmt.setString(4, alunoNomeSocial);
			pstmt.setString(5, alunoEndereco);
			pstmt.setString(6, alunoCep);
			pstmt.setString(7, alunoBairro);
			pstmt.setString(8, alunoCidade);
			pstmt.setString(9, alunoUf);
			pstmt.setString(10, alunoSexo);
			pstmt.setString(11, alunoDataNascimento);
			pstmt.setString(12, alunoRg);
			pstmt.setString(13, alunoCpf);
			pstmt.setString(14, alunoCelular);
			pstmt.setString(15, alunoResidencial);
			pstmt.setString(16, alunoEmail);
			pstmt.setString(17, alunoSituacao);
			pstmt.setString(18, alunoSituacaoId);
			pstmt.setBigDecimal(19, codEmp);
			pstmt.setBigDecimal(20, codCenCus); // Usa o código do curso obtido na verificação
			pstmt.setString(21, turmaId);

			int rowsAffected = pstmt.executeUpdate();

			System.out.println("[DEBUG] Número de linhas afetadas pelo INSERT: " + rowsAffected);
			System.out.println("[DEBUG] Aluno inserido com sucesso na turma: " + turmaId);

		} catch (SQLException e) {
			String errorDetails = String.format(
					"Erro ao inserir aluno. CODPARC: %s, ID_EXTERNO: %s, NOME: %s, Curso: %s, Erro: %s",
					credotAtual, alunoId, alunoNome, descrCurso, e.getMessage()
			);
			selectsParaInsert.add("SELECT <#NUMUNICO#>, '" + errorDetails + "', SYSDATE, 'Erro', " + codEmp + ", '" + alunoId + "' FROM DUAL");

			System.err.println("[DEBUG] Erro ao inserir aluno na turma: " + turmaId);
			System.err.println("[DEBUG] Mensagem de erro: " + e.getMessage());
			e.printStackTrace();
			throw e;
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (verifyStmt != null) {
				verifyStmt.close();
			}
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}


	//Valida se um endereço já está cadastrado no banco de dados.
	public boolean validarCadastroEndereco(String endereco, String credorNome,
										   String alunoNome) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal count = BigDecimal.ZERO;
		try {
			jdbc.openSession();

			String sqlNota = "SELECT COUNT(0) AS COUNT FROM TSIEND WHERE TRANSLATE(  \t    upper(NOMEEND),   \t    '������������������������������������',   \t    'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC'   \t  ) like TRANSLATE(    \t    upper((SELECT LTRIM(SUBSTR(?, INSTR(?, ' ') + 1))  FROM DUAL)),   \t    '������������������������������������',   \t    'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC')";

			pstmt = jdbc.getPreparedStatement(sqlNota);
			pstmt.setString(1, endereco);
			pstmt.setString(2, endereco);

			rs = pstmt.executeQuery();
			if (rs.next()) {
				count = rs.getBigDecimal("COUNT");
			}
		} catch (SQLException e) {
			e.printStackTrace();
			insertLogIntegracao("Erro ao validar se endere�o j� cadastrado: "
					+ e.getMessage(), "Erro", credorNome, alunoNome);
			throw e;
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
		if (count.compareTo(BigDecimal.ZERO) == 0) {
			return true;
		}
		return false;
	}


	//Valida se um bairro já está cadastrado no banco de dados.
	public boolean validarCadastroBairro(String bairro, String credorNome,
										 String alunoNome) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal count = BigDecimal.ZERO;
		try {
			jdbc.openSession();

			String sqlNota = "SELECT COUNT(0) AS COUNT FROM TSIBAI WHERE TRANSLATE(  \t    upper(NOMEBAI),   \t    '������������������������������������',   \t    'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC'   \t  ) like TRANSLATE(    \t    upper(?),   \t    '������������������������������������',   \t    'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC')";

			pstmt = jdbc.getPreparedStatement(sqlNota);
			pstmt.setString(1, bairro);

			rs = pstmt.executeQuery();
			if (rs.next()) {
				count = rs.getBigDecimal("COUNT");
			}
		} catch (SQLException e) {
			e.printStackTrace();
			insertLogIntegracao(
					"Erro ao validar se bairro j� cadastrado: "
							+ e.getMessage(), "Erro", credorNome, alunoNome);
			throw e;
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
		if (count.compareTo(BigDecimal.ZERO) == 0) {
			return true;
		}
		return false;
	}


	//Insere um novo bairro no banco de dados.
	public BigDecimal insertBairro(String bairro, String credorNome,
								   String alunoNome) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		BigDecimal codbai = getMaxNumBai();

		jdbc.openSession();

		String sqlUpdate = "INSERT INTO TSIBAI (CODBAI, NOMEBAI, DTALTER)VALUES (?, ?, SYSDATE) ";

		pstmt = jdbc.getPreparedStatement(sqlUpdate);
		pstmt.setBigDecimal(1, codbai);
		pstmt.setString(2, bairro.toUpperCase());
		pstmt.executeUpdate();
		try {
			if (pstmt != null) {
				pstmt.close();
			}
			if (jdbc != null) {
				jdbc.closeSession();
			}
		} catch (Exception se) {
			se.printStackTrace();
			insertLogIntegracao("Erro ao bairro endere�o: " + se.getMessage(),
					"Erro", credorNome, alunoNome);
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
		return codbai;
	}



	public BigDecimal getMaxNumBai() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal id = BigDecimal.ZERO;
		try {
			updateNumBai();

			jdbc.openSession();

			String sqlNota = "SELECT MAX(ULTCOD) AS ID FROM TGFNUM WHERE ARQUIVO = 'TSIBAI'";

			pstmt = jdbc.getPreparedStatement(sqlNota);

			rs = pstmt.executeQuery();
			if (rs.next()) {
				id = rs.getBigDecimal("ID");
			}
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
		return id;
	}

	public void updateNumBai() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		try {
			jdbc.openSession();

			String sqlUpdate = "UPDATE TGFNUM SET ULTCOD = ULTCOD + 1  WHERE ARQUIVO = 'TSIBAI'";

			pstmt = jdbc.getPreparedStatement(sqlUpdate);
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

	public void insertLogIntegracao(String descricao, String status,
									String credorNome, String alunoNome) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		try {
			jdbc.openSession();

			String descricaoCompleta = null;
			if ((credorNome.equals("")) && (alunoNome.equals(""))) {
				descricaoCompleta = descricao;
			} else if ((credorNome.equals("")) && (!alunoNome.isEmpty())) {
				descricaoCompleta = descricao + " " + " Aluno:" + alunoNome;
			} else if ((!credorNome.isEmpty()) && (alunoNome.equals(""))) {
				descricaoCompleta = descricao + " " + " Credor:" + credorNome;
			} else if ((!credorNome.isEmpty()) && (!alunoNome.isEmpty())) {
				descricaoCompleta = descricao + " " + " Credor:" + credorNome
						+ " " + " Aluno:" + alunoNome;
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

	public void insertLogList(String listInsert) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		try {
			jdbc.openSession();

			String sqlUpdate = "INSERT INTO AD_LOGINTEGRACAO (NUMUNICO, DESCRICAO, DTHORA, "
					+ "	STATUS, CODEMP, MATRICULA_IDFORN) " + listInsert;

			pstmt = jdbc.getPreparedStatement(sqlUpdate);
			//pstmt.setString(1, listInsert);
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


	/**
	 * Versão otimizada do método apiGet com melhor tratamento de erros e recursos
	 */
//	 public String[] apiGet2(String ur, String token) throws Exception {
//		    BufferedReader reader;
//		    StringBuilder responseContent = new StringBuilder();
//		    String encodedUrl = ur.replace(" ", "%20");
//		    URL obj = new URL(encodedUrl);
//		    HttpURLConnection https = (HttpURLConnection)obj.openConnection();
//		    System.out.println("Entrou na API");
//		    System.out.println("URL: " + encodedUrl);
//		    System.out.println("Token Enviado: [" + token + "]");
//		    https.setRequestMethod("GET");
//		    https.setRequestProperty("User-Agent",
//		        "Mozilla/5.0 (Windows NT 10.0; Win64; x64)");
//		    https.setRequestProperty("Content-Type",
//		        "application/json; charset=UTF-8");
//		    https.setRequestProperty("Accept", "application/json");
//		    https.setRequestProperty("Authorization", "Bearer " + token);
//		    https.setDoInput(true);
//		    int status = https.getResponseCode();
//		    if (status >= 300) {
//		      reader = new BufferedReader(new InputStreamReader(
//		            https.getErrorStream()));
//		    } else {
//		      reader = new BufferedReader(new InputStreamReader(
//		            https.getInputStream()));
//		    }
//		    String line;
//		    while ((line = reader.readLine()) != null)
//		      responseContent.append(line);
//		    reader.close();
//		    System.out.println("Output from Server .... \n" + status);
//		    String response = responseContent.toString();
//		    https.disconnect();
//		    return new String[] { Integer.toString(status), response };
//		  }


	//ainda em fase de teste
	// Variáveis estáticas para controle de requisições
	private static final int MAX_REQUESTS_PER_MINUTE = 60;
	private static final long ONE_MINUTE_IN_MS = 60 * 1000;
	private static final Queue<Long> requestTimestamps = new LinkedList<>();

	public synchronized String[] apiGet2(String ur, String token) throws Exception {
		// Remover timestamps antigos (mais de 1 minuto)
		long currentTime = System.currentTimeMillis();
		requestTimestamps.removeIf(timestamp ->
				currentTime - timestamp > ONE_MINUTE_IN_MS);

		// Verificar se atingiu o limite
		if (requestTimestamps.size() >= MAX_REQUESTS_PER_MINUTE) {
			// Calcula tempo de espera até que possa fazer nova requisição
			long oldestRequestTime = requestTimestamps.peek();
			long waitTime = ONE_MINUTE_IN_MS - (currentTime - oldestRequestTime);

			System.out.println("Limite de 60 requisições por minuto atingido. " +
					"Aguardando " + waitTime + "ms");

			Thread.sleep(waitTime);

			// Limpa timestamps antigos novamente após espera
			requestTimestamps.removeIf(timestamp ->
					currentTime - timestamp > ONE_MINUTE_IN_MS);
		}

		// Adiciona timestamp da requisição atual
		requestTimestamps.offer(System.currentTimeMillis());

		// Código original de requisição
		BufferedReader reader;
		StringBuilder responseContent = new StringBuilder();
		String encodedUrl = ur.replace(" ", "%20");

		URL obj = new URL(encodedUrl);
		HttpURLConnection https = (HttpURLConnection)obj.openConnection();

		System.out.println("Entrou na API");
		System.out.println("URL: " + encodedUrl);
		System.out.println("Token Enviado: [" + token + "]");

		https.setRequestMethod("GET");
		https.setRequestProperty("User-Agent",
				"Mozilla/5.0 (Windows NT 10.0; Win64; x64)");
		https.setRequestProperty("Content-Type",
				"application/json; charset=UTF-8");
		https.setRequestProperty("Accept", "application/json");
		https.setRequestProperty("Authorization", "Bearer " + token);
		https.setDoInput(true);

		int status = https.getResponseCode();

		// Resto do código original de leitura da resposta
		reader = (status >= 300)
				? new BufferedReader(new InputStreamReader(https.getErrorStream()))
				: new BufferedReader(new InputStreamReader(https.getInputStream()));

		String line;
		while ((line = reader.readLine()) != null)
			responseContent.append(line);

		reader.close();

		System.out.println("Output from Server .... \n" + status);

		String response = responseContent.toString();
		https.disconnect();

		return new String[] { Integer.toString(status), response };
	}


}
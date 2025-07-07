package br.com.sankhya.acoesgrautec.jobs;

import br.com.sankhya.acoesgrautec.util.EnviromentUtils;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import org.cuckoo.core.ScheduledAction;
import org.cuckoo.core.ScheduledActionContext;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VerificaCredorSemAlunoJob implements ScheduledAction {

    private List<String> selectsParaInsertLog = new ArrayList<>();
    private EnviromentUtils util = new EnviromentUtils();

    @Override
    public void onTime(ScheduledActionContext contexto) {
        System.out.println("=== INÍCIO DO JOB VerificaCredorSemAlunoJob ===");
        System.out.println("Iniciando verificação de credores sem alunos cadastrados...");

        EntityFacade entityFacade = null;
        JdbcWrapper jdbc = null;

        try {
            entityFacade = EntityFacadeFactory.getDWFFacade();
            jdbc = entityFacade.getJdbcWrapper();
            jdbc.openSession();

            // 1. Obter lista de credores (TGFPAR) que não possuem cadastro em AD_ALUNOS
            List<Map<String, Object>> credoresSemAluno = obterCredoresSemAluno(jdbc);

            // 2. Carregar mapeamento de alunos existentes
            Map<String, BigDecimal> mapaAlunos = carregarAlunosCadastrados(jdbc);

            // 3. Para cada credor sem aluno, verificar e cadastrar
            for (Map<String, Object> credor : credoresSemAluno) {
                processarCredor(credor, mapaAlunos, jdbc);
            }

            System.out.println("Processamento concluído. Total de registros verificados: " + credoresSemAluno.size());

        } catch (Exception e) {
            System.err.println("Erro durante a execução do job: " + e.getMessage());
            e.printStackTrace();
            registrarLogErro("Erro geral no job: " + e.getMessage(), BigDecimal.ZERO);
        } finally {
            if (jdbc != null) {
                jdbc.closeSession();
            }
            processarLogs();
            System.out.println("=== FIM DO JOB VerificaCredorSemAlunoJob ===");
        }
    }
    
 // Adicione este método no início do seu job
    private String descobrirColunaCPF(JdbcWrapper jdbc) throws Exception {
        NativeSql sql = new NativeSql(jdbc);
        sql.appendSql("SELECT column_name FROM user_tab_columns ");
        sql.appendSql("WHERE table_name = 'TGFPAR' ");
        sql.appendSql("AND (column_name LIKE '%CPF%' OR column_name LIKE '%CNPJ%' OR column_name LIKE '%CGC%' OR column_name = 'DOCUMENTO')");
        
        try (ResultSet rs = sql.executeQuery()) {
            if (rs.next()) {
                String coluna = rs.getString("column_name");
                System.out.println("Coluna de CPF/CNPJ encontrada: " + coluna);
                return coluna;
            }
        }
        
        return null;
    }

    private List<Map<String, Object>> obterCredoresSemAluno(JdbcWrapper jdbc) throws Exception {
        // Descobrir o nome da coluna primeiro
        String colunaCPF = descobrirColunaCPF(jdbc);
        
        if (colunaCPF == null) {
            throw new Exception("Não foi possível identificar a coluna de CPF/CNPJ na tabela TGFPAR");
        }
        
        List<Map<String, Object>> credores = new ArrayList<>();
        NativeSql sql = new NativeSql(jdbc);
        
        // Usar o nome da coluna encontrado dinâmicamente
        sql.appendSql("SELECT P.CODPARC, P." + colunaCPF + " AS CGC_CPF, P.NOMEPARC, P.EMAIL "); 
        sql.appendSql("FROM TGFPAR P ");
        sql.appendSql("WHERE P.AD_FLAGALUNO = 'S' ");
        sql.appendSql("AND NOT EXISTS ( ");
        sql.appendSql("  SELECT 1 FROM AD_ALUNOS A WHERE A.CODPARC = P.CODPARC ");
        sql.appendSql(")");

        try (ResultSet rs = sql.executeQuery()) {
            while (rs.next()) {
                Map<String, Object> credor = new HashMap<>();
                credor.put("CODPARC", rs.getBigDecimal("CODPARC"));
                credor.put("CGC_CPF", rs.getString("CGC_CPF")); // Usa o alias
                credor.put("NOMEPARC", rs.getString("NOMEPARC"));
                credor.put("EMAIL", rs.getString("EMAIL"));
                credores.add(credor);
            }
        }
        return credores;
    }

    private Map<String, BigDecimal> carregarAlunosCadastrados(JdbcWrapper jdbc) throws Exception {
        Map<String, BigDecimal> mapaAlunos = new HashMap<>();
        NativeSql sql = new NativeSql(jdbc);
        // Use o nome correto: CPF em vez de CGC_CPF
        sql.appendSql("SELECT CODPARC, CPF FROM AD_ALUNOS");

        try (ResultSet rs = sql.executeQuery()) {
            while (rs.next()) {
                String cpf = rs.getString("CPF");
                BigDecimal codParc = rs.getBigDecimal("CODPARC");
                mapaAlunos.put(cpf, codParc);
            }
        }
        return mapaAlunos;
    }

    private void processarCredor(Map<String, Object> credor, Map<String, BigDecimal> mapaAlunos, JdbcWrapper jdbc) {
        try {
            String cpf = (String) credor.get("CGC_CPF");
            BigDecimal codParc = (BigDecimal) credor.get("CODPARC");

            // Verificar se o CPF já existe em outro aluno
            if (mapaAlunos.containsKey(cpf)) {
                System.out.println("CPF já cadastrado em outro aluno: " + cpf);
                registrarLogAviso("CPF duplicado: " + cpf, codParc);
                return;
            }

            // Cadastrar novo aluno
            cadastrarNovoAluno(credor, jdbc);
            registrarLogSucesso("Aluno cadastrado para credor: " + codParc, codParc);

        } catch (Exception e) {
            System.err.println("Erro ao processar credor " + credor.get("CODPARC") + ": " + e.getMessage());
            registrarLogErro("Erro ao processar credor: " + e.getMessage(), (BigDecimal) credor.get("CODPARC"));
        }
    }

    private void cadastrarNovoAluno(Map<String, Object> credor, JdbcWrapper jdbc) throws Exception {
        NativeSql sql = new NativeSql(jdbc);
        sql.appendSql("INSERT INTO AD_ALUNOS (");
        sql.appendSql("  CODPARC, CPF, NOME, EMAIL, DT_CADASTRO, ");
        sql.appendSql("  CODEMP, ID_EXTERNO, SITUACAO");
        sql.appendSql(") VALUES (");
        sql.appendSql("  :CODPARC, :CPF, :NOME, :EMAIL, SYSDATE, ");
        sql.appendSql("  1, :ID_EXTERNO, 'ATIVO'");
        sql.appendSql(")");

        sql.setNamedParameter("CODPARC", credor.get("CODPARC"));
        sql.setNamedParameter("CPF", credor.get("CGC_CPF"));
        sql.setNamedParameter("NOME", credor.get("NOMEPARC"));
        sql.setNamedParameter("EMAIL", credor.get("EMAIL"));
        sql.setNamedParameter("ID_EXTERNO", "ALN_" + credor.get("CGC_CPF"));

        sql.executeUpdate();
    }

    private void registrarLogSucesso(String mensagem, BigDecimal codParc) {
        String log = String.format("SELECT <#NUMUNICO#>, '%s', SYSDATE, 'Sucesso', %s, '%s' FROM DUAL",
                mensagem, codParc, "ALUNO_CRIADO");
        selectsParaInsertLog.add(log);
    }

    private void registrarLogAviso(String mensagem, BigDecimal codParc) {
        String log = String.format("SELECT <#NUMUNICO#>, '%s', SYSDATE, 'Aviso', %s, '%s' FROM DUAL",
                mensagem, codParc, "ALUNO_DUPLICADO");
        selectsParaInsertLog.add(log);
    }

    private void registrarLogErro(String mensagem, BigDecimal codParc) {
        String log = String.format("SELECT <#NUMUNICO#>, '%s', SYSDATE, 'Erro', %s, '%s' FROM DUAL",
                mensagem, codParc, "ERRO_PROCESSAMENTO");
        selectsParaInsertLog.add(log);
    }

    private void processarLogs() {
        if (selectsParaInsertLog.isEmpty()) return;

        EntityFacade entityFacade = null;
        JdbcWrapper jdbc = null;
        PreparedStatement pstmt = null;

        try {
            entityFacade = EntityFacadeFactory.getDWFFacade();
            jdbc = entityFacade.getJdbcWrapper();
            jdbc.openSession();

            // 1. Get max log number
            int nuFin = util.getMaxNumLog();
            
            // 2. Build final SQL
            StringBuilder sql = new StringBuilder();
            sql.append("INSERT INTO AD_LOGINTEGRACAO (NUMUNICO, DESCRICAO, DTHORA, STATUS, CODEMP, MATRICULA_IDFORN) ");
            
            for(int i=0; i<selectsParaInsertLog.size(); i++) {
                String logEntry = selectsParaInsertLog.get(i)
                    .replace("<#NUMUNICO#>", String.valueOf(nuFin + i));
                
                if(i > 0) sql.append(" UNION ALL ");
                sql.append(logEntry);
            }

            // 3. Execute using PreparedStatement
            pstmt = jdbc.getPreparedStatement(sql.toString());
            pstmt.executeUpdate();

        } catch (Exception e) {
            System.err.println("Erro ao registrar logs: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                if(pstmt != null) pstmt.close();
                if(jdbc != null) jdbc.closeSession();
            } catch (Exception e) {
                e.printStackTrace();
            }
            selectsParaInsertLog.clear();
        }
    }
}
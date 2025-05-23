package br.com.sankhya.acoesgrautec.extensions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class Teste {

    public static void main(String[] args) {
        try {
            // Caminho do arquivo JSON no seu computador
            File arquivoJson = new File("C:\\Users\\bmcode\\Documents\\Java SK\\novo 158.json");

            // Criar o ObjectMapper para ler o arquivo JSON
            ObjectMapper objectMapper = new ObjectMapper();

            // Ler o conte�do do arquivo e mapear para uma �rvore de JsonNodes
            JsonNode rootNode = objectMapper.readTree(arquivoJson);

            // Conjunto para armazenar as combina��es �nicas de taxa_id e taxa_descricao
            Set<String> taxasUnicas = new HashSet<>();

            // Percorrer o array de objetos JSON
            for (JsonNode tituloNode : rootNode) {
                String taxaId = tituloNode.get("taxa_id").asText();
                String taxaDescricao = tituloNode.get("taxa_descricao").asText();

                // Armazenar a combina��o �nica de taxa_id e taxa_descricao
                taxasUnicas.add("ID da Taxa: " + taxaId + ", Descri��o: " + taxaDescricao);
            }

            // Exibir todas as combina��es �nicas
            System.out.println("Taxas �nicas encontradas:");
            for (String taxa : taxasUnicas) {
                System.out.println(taxa);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}


# 📑 Premissas de Negócio e Governança do Pipeline — Fase 2

## 1. Contexto do Problema (Compromiso Nacional Criança Alfabetizada)

O objetivo deste pipeline é estruturar dados públicos em larga escala para apoiar a análise dos índices de alfabetização infantil no Brasil, identificando gargalos e insights por recortes demográficos e geográficos.

## 2. Parâmetro Técnico de Corte (Regra de Ouro)

Conforme estabelecido pela Pesquisa Alfabetiza Brasil (Inep/Saeb 2023):

* **Ponto de Corte:** **743 pontos** na escala de proficiência.
* **Variável Target Derivada:** * $\ge 743$: Criança considerada alfabetizada.
  * $< 743$: Criança em estágio de atenção (Não alfabetizada).

## 3. Arquitetura Medallion Aplicada (Mapeamento de Sprints)

* [ ] **Bronze (Raw):** Dados educacionais em formato bruto extraídos das fontes do governo.
* [ ] **Silver (Trusted):** Dados limpos por PySpark, aplicando tipagem e remoção de nulos críticos.
* [ ] **Gold (Refined):** Agregações por Município/Estado prontas para consumo analítico.

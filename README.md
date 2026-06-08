# 📊 Tech Challenge - Fase 2 | AI Scientist FIAP

## 🏫 Pipeline Híbrido: Indicador Criança Alfabetizada

### 📝 Contexto de Negócio e Desafio Educacional

Este projeto implementa uma pipeline de engenharia de dados em larga escala para analisar os índices de alfabetização infantil no Brasil - Tech Challenge - Fase 2.pdf]. Alinhado ao **Compromisso Nacional Criança Alfabetizada**, o sistema adota o critério técnico da Pesquisa Alfabetiza Brasil (Inep/Saeb), estabelecendo o ponto de corte de **743 pontos** de proficiência para mapear o percentual de sucesso por município e estado - Tech Challenge - Fase 2.pdf].

---

### 🏗️ Arquitetura da Solução (Medallion & NoSQL)

A engenharia de dados do repositório está estruturada sob as melhores práticas de MLOps e isolamento de ambientes:

* **`data/`**: Simulação das camadas físicas do Data Lakehouse (Bronze, Silver e Gold) - Tech Challenge - Fase 2.pdf].
* **`notebooks/`**: Motores de processamento distribuído PySpark (Tratamentos Silver/Gold conduzidos pelo Leonardo) e documentação de premissas (Winny) - Tech Challenge - Fase 2.pdf].
* **`src/`**: Códigos modulares de produção, incluindo orquestradores de pipeline e conectores NoSQL (Construídos pelo Caio) - Tech Challenge - Fase 2.pdf].
* **`app/`**: Camada analítica de consumo e storytelling visual (Dashboard Streamlit desenvolvido pelo Reinaldo) - Tech Challenge - Fase 2.pdf].

---

### 👥 Divisão Operacional do Squad

* **Engenharia de Dados (PySpark - Silver/Gold):** Leonardo
* **Infraestrutura & Persistência NoSQL:** Caio
* **Governança, Negócio & Documentação:** Winny
* **Business Intelligence & Storytelling (Streamlit):** Reinaldo

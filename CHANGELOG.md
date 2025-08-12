# 📜 CHANGELOG - Sistema de Gestão de Contratos

Este changelog documenta as versões e alterações aplicadas ao projeto desde sua criação.

---

## ✅ v1.0 - Base inicial
- Estrutura inicial do projeto com FastAPI e SQLAlchemy.
- Modelo `Contrato` criado.
- Upload de planilhas `.csv` e processamento de dados.
- Cálculo de campos derivados: `meses_restantes`, `valor_global_contrato`, `valor_presente_contrato`.

---

## ✅ v1.1 - Cadastro manual de contratos (cabeçalho)
- Criação do modelo `CabecalhoContrato`.
- Adicionado formulário HTML (`cadastrar.html`) para cadastro de cabeçalho de contrato.
- Rota `GET` e `POST` para `/cadastrar`.

---

## ✅ v1.2 - Listagem visual com tabela HTML
- Criação de rota `/contratos_html`.
- Template `contratos.html` para exibição tabulada de contratos importados.

---

## ✅ v1.3 - Filtros avançados de listagem
- Rota `/contratos` aceita filtros:
  - Por nome de cliente.
  - Por data de envio (intervalo).
  - Por faixa de meses restantes.

---

## ✅ v1.4 - Dashboard inicial
- Página `/dashboard` com template `dashboard.html`.
- Rota `/dashboard_data` com API que retorna:
  - Total de contratos.
  - Total global somado.
  - Distribuição de contratos por cliente.
- Gráficos adicionados com Chart.js.

---

## ✅ v1.5 - Correção de contagem de contratos
- Corrigido cálculo de total de contratos (considerando agrupamento por `contrato_n` e não por linha).
- Melhoria na visualização tabular.

---

## ✅ v1.5.1 - Correção agregação SQLite
- Substituída chamada incorreta de `sum(max(...))` por subquery separada (`subquery + sum`).

---

## ✅ v1.5.2 - Serialização robusta para API
- Corrigido erro de serialização do `jsonable_encoder` na resposta de `/dashboard_data`.

---

## ✅ v1.5.3 - Refatoração geral do dashboard
- Reescrita de agregações para garantir compatibilidade com SQLite.
- Dashboard renderizando todos gráficos corretamente.

---

## ✅ v1.5.4 - Suporte a `now()` no Jinja2
- Adicionado `templates.env.globals['now'] = datetime.now`.
- Permite uso de `{{ now().year }}` no footer do `base.html`.

---

## ✅ v1.6 - Melhorias visuais e usabilidade
- Adicionado Bootstrap 5 com layout mais moderno.
- Inclusão do logo da empresa.
- Padronização de botões e navegação.

---

## 🔜 Próximas versões (planejadas)
- v1.7: Exportação para Excel.
- v1.8: Sistema de autenticação de usuários.
- v1.9: Permissão por nível de acesso.
- v2.0: Integração com banco de dados PostgreSQL.


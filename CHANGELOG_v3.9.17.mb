# CHANGELOG v3.9.17 — 12/08/2025

**Status:** baseline congelado (tag recomendada: `app-contratos-v3.9.17-baseline-2025-08-12`)

## Destaques
- **Baseline estável** do app com autenticação por sessão, importação de movimentação (ENVIO/RETORNO/TROCA) com anti‑duplicação por hash e dashboard funcional.
- **Auditoria e rastreabilidade:** logs por item e por cabeçalho de contrato; visualização da última importação em HTML e API.
- **Estrutura pronta** para evoluir UI (Bootstrap 5) e gráficos do dashboard sem quebrar rotas existentes.

## Novidades
- **Autenticação baseada em sessão** (SessionMiddleware) com middleware obrigatório e whitelist de rotas públicas (ex.: `/login`, `/logout`, `/static/*`). Templates de login em `templates/login.html` e link no `base.html`.
- **Dashboard** disponível em `/dashboard` e API em `/api/dashboard/` (agregados de carteira e gráficos, com filtro por cliente).
- **Fluxo de importação de movimentação (CSV)** em `/importar_movimentacao`: upload → preview → pré‑import (salva lote) → commit. Regras de negócio:
  - `tp_transacao`: **ENVIO** (insere/atualiza), **RETORNO** (remove 1 item do contrato), **TROCA** (remove e reinsere).
  - **Anti‑duplicação** por hash composto (`contrato+ativo+cod_cli+tipo+data_mov`).
  - **Valida cabeçalho** do contrato antes de gravar.
  - **Recalcula** `meses_restantes`, `valor_global_contrato`, `valor_presente_contrato` após commit.
- **Logs e auditoria**
  - `ContratoLog` por item (helper tolerante a schema).
  - Logs de cabeçalho em `runtime/logs_cabecalhos.jsonl` (expostos em rotas HTML/API).
- **Última importação**
  - JSON: `/api/ultima_importacao`
  - HTML: `/ultima_importacao`

## Melhorias
- Organização modular do projeto: `routers/`, `utils/`, `models.py`, `database.py` e versão do app em `utils/versioning.py`.
- Tratamento robusto de importação com *preview* e *commit* separados, minimizando risco de corrupção de dados.
- Ponto único para cálculos agregados usados no dashboard (reuso entre HTML e API).

## Correções
- Ajustes no fluxo para garantir recomputo de campos calculados durante o commit da importação.
- Normalização de whitelist do middleware para evitar bloqueio indevido de rotas públicas.

## Compatibilidade
- **Banco:** SQLAlchemy (migrador sugerido: Alembic). Nenhuma migração obrigatória incluída no baseline.
- **Python:** compatível com a versão em uso local do projeto. Recomenda-se registrar a versão atual em `.python-version.txt`.
- **Ambiente:** sugere-se `.env` com `SECRET_KEY`, `RUNTIME_DIR` e `ENABLE_DEBUG_AUTH` (exemplo em `.env.example`).

## Observações e Próximos Passos (fora do baseline)
- Adicionar endpoints de *health check* (`/healthz`, `/api/health`) conforme já previsto pela whitelist.
- Evoluir UI com Bootstrap 5 (layout moderno no `index.html`) e incluir logotipo.
- Gráficos adicionais no dashboard e exportação para Excel/CSV (relatórios por carteira e por cliente).

## Checksum e metadados do baseline
- `main.py` — **SHA‑256**: `65344b9e80c6bf1f9dd93bb070f593e4608c232352bbcf5376eaf567f90c9c98`  
  **Tamanho:** 74 834 bytes • **Linhas:** 1 737

## Como aplicar este baseline
1. Garanta que está na raiz do projeto e o repositório está inicializado (`git init`).
2. Commit do estado atual:
   ```powershell
   git add .
   git commit -m "baseline: app-contratos v3.9.17 (12/08/2025)"
   ```
3. Crie o branch e a tag do baseline:
   ```powershell
   git checkout -b baseline/v3.9.17-2025-08-12
   git tag -a app-contratos-v3.9.17-baseline-2025-08-12 -m "Baseline estável do app-contratos v3.9.17 (12/08/2025)" -m "SHA256 main.py: 65344b9e80c6bf1f9dd93bb070f593e4608c232352bbcf5376eaf567f90c9c98"
   ```
4. (Opcional) Congele dependências e versão do Python:
   ```powershell
   pip freeze --local > requirements.lock
   python -V > .python-version.txt
   git add requirements.lock .python-version.txt
   git commit -m "baseline: freeze de dependências e versão do Python"
   ```
5. (Opcional) Publique no remoto:
   ```powershell
   git push -u origin baseline/v3.9.17-2025-08-12
   git push origin app-contratos-v3.9.17-baseline-2025-08-12
   ```

---

> Este arquivo documenta o estado **congelado** do app-contratos na data indicada. Mudanças futuras devem ocorrer em branches de trabalho (ex.: `develop`) para preservar o baseline.

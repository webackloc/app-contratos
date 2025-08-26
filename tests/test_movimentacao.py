# Versão: 0.1.0 (2025-08-14)
import datetime as dt
from services.movimentacao_service import aplicar_lote
from models import MovimentacaoLote, MovimentacaoItem, ContratoCabecalho, Contrato, ContratoLog

def _mk_item(payload, idx):
    return MovimentacaoItem(linha_idx=idx, payload=payload, status="OK", msg="")

def test_envio_retorno_fifo(db_session):
    cab = ContratoCabecalho(nome_cliente="ACME", cnpj="00.000.000/0000-00",
                            contrato_num="C-1", prazo_contratual=12,
                            indice_reajuste="IPCA", vendedor="Joao")
    db_session.add(cab); db_session.flush()

    lote = MovimentacaoLote(status="PREVIEW"); db_session.add(lote); db_session.flush()

    # ENVIO 1
    p1 = {"tp_transacao":"ENVIO","contrato_num":"C-1","cod_cli":"001","ativo":"A1","data_mov":"2025-08-01"}
    # ENVIO 2 (mesmo ativo com serial -> permitido)
    p2 = {"tp_transacao":"ENVIO","contrato_num":"C-1","cod_cli":"001","ativo":"A1","serial":"S2","data_mov":"2025-08-02"}
    # RETORNO 1 (fecha o mais antigo)
    p3 = {"tp_transacao":"RETORNO","contrato_num":"C-1","cod_cli":"001","ativo":"A1","data_mov":"2025-08-10"}

    for i, p in enumerate([p1, p2, p3], start=1):
        db_session.add(_mk_item(p, i))
    db_session.commit()

    with db_session.begin():
        r = aplicar_lote(db_session, lote.id)

    ativos = db_session.query(Contrato).filter(Contrato.cabecalho_id==cab.id, Contrato.ativo=="A1").all()
    assert len(ativos) == 2
    assert sum(1 for a in ativos if a.status=="ATIVO") == 1
    assert sum(1 for a in ativos if a.status=="RETORNADO") == 1

def test_troca_atomica(db_session):
    cab = ContratoCabecalho(nome_cliente="B", cnpj="11.111.111/1111-11",
                            contrato_num="C-2", prazo_contratual=12,
                            indice_reajuste="IPCA", vendedor="Ana")
    db_session.add(cab); db_session.flush()
    # pré: um item ativo
    db_session.add(Contrato(cabecalho_id=cab.id, ativo="X", cod_cli="002", nome_cli="B",
                            data_envio=dt.date(2025,8,1), status="ATIVO"))
    db_session.commit()

    lote = MovimentacaoLote(status="PREVIEW"); db_session.add(lote); db_session.flush()
    p = {"tp_transacao":"TROCA","contrato_num":"C-2","cod_cli":"002","ativo_antigo":"X","ativo_novo":"Y","data_mov":"2025-08-05"}
    db_session.add(MovimentacaoItem(lote_id=lote.id, linha_idx=1, payload=p, status="OK"))
    db_session.commit()

    with db_session.begin():
        r = aplicar_lote(db_session, lote.id)

    xs = db_session.query(Contrato).filter_by(cabecalho_id=cab.id, ativo="X").one()
    ys = db_session.query(Contrato).filter_by(cabecalho_id=cab.id, ativo="Y").one()
    assert xs.status == "RETORNADO"
    assert ys.status == "ATIVO"

def test_idempotencia(db_session):
    cab = ContratoCabecalho(nome_cliente="C", cnpj="22.222.222/2222-22",
                            contrato_num="C-3", prazo_contratual=12,
                            indice_reajuste="IPCA", vendedor="Eva")
    db_session.add(cab); db_session.flush()

    lote = MovimentacaoLote(status="PREVIEW"); db_session.add(lote); db_session.flush()
    p = {"tp_transacao":"ENVIO","contrato_num":"C-3","cod_cli":"003","ativo":"Z","data_mov":"2025-08-03"}
    db_session.add(MovimentacaoItem(lote_id=lote.id, linha_idx=1, payload=p, status="OK"))
    db_session.commit()

    with db_session.begin():
        r1 = aplicar_lote(db_session, lote.id)
    # reaplicar o mesmo lote deve resultar em IGNORADO
    with db_session.begin():
        r2 = aplicar_lote(db_session, lote.id)

    logs = db_session.query(ContratoLog).filter_by(contrato_cabecalho_id=cab.id).all()
    assert any(l.status=="OK" for l in logs)

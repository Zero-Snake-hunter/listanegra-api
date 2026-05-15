from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from motor.motor_asyncio import AsyncIOMotorClient
import httpx, pandas as pd, io, os, asyncio
from datetime import datetime

app = FastAPI(title="Lista Negra 69-B API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET"])

MONGO_URI = os.environ.get("MONGO_URI", "")
SAT_CSV = "https://wu1agsprosta001.blob.core.windows.net/agsc-publicaciones/Datos_abiertos/Documents_AGAFF/Listado_completo_69-B.csv"
SAT_PAGE = "https://www.gob.mx/sat/acciones-y-programas/notificacion-a-contribuyentes-con-operaciones-presuntamente-inexistentes-y-listados-definitivos-333336"
BASE_PDF = "http://omawww.sat.gob.mx/informacionfiscal/Documents/"

client = AsyncIOMotorClient(MONGO_URI)
db = client["listanegra"]

async def actualizar_listado():
    print(f"[{datetime.now()}] Descargando listado SAT...")
    try:
        async with httpx.AsyncClient(timeout=120) as h:
            r = await h.get(SAT_CSV, headers={"User-Agent": "Mozilla/5.0"})
        df = pd.read_csv(io.StringIO(r.content.decode("latin-1")), header=2)
        df = df.iloc[1:]
        df.columns = [
            "no","rfc","nombre","situacion",
            "of_presuncion_sat","pub_sat_presuntos","of_presuncion_dof","pub_dof_presuntos",
            "of_desvirtuaron_sat","pub_sat_desvirtuados","of_desvirtuaron_dof","pub_dof_desvirtuados",
            "of_definitivos_sat","pub_sat_definitivos","of_definitivos_dof","pub_dof_definitivos",
            "of_sentencia_sat","pub_sat_sentencia","of_sentencia_dof","pub_dof_sentencia"
        ]
        df = df.dropna(subset=["rfc"]).fillna("")
        docs = df.to_dict(orient="records")
        col = db["contribuyentes"]
        await col.drop()
        await col.insert_many(docs)
        await col.create_index("rfc")
        total = len(docs)
        await db["meta"].find_one_and_replace(
            {"_id": "info"},
            {"_id": "info", "total": total, "updated_at": datetime.now().isoformat()},
            upsert=True
        )
        print(f"[{datetime.now()}] Actualizado: {total} registros")
    except Exception as e:
        print(f"[{datetime.now()}] Error: {e}")

@app.on_event("startup")
async def startup():
    if not MONGO_URI:
        print("ADVERTENCIA: MONGO_URI no configurado")
        return
    scheduler = AsyncIOScheduler()
    scheduler.add_job(actualizar_listado, "cron", day_of_week="mon", hour=2)
    scheduler.start()
    total = await db["contribuyentes"].count_documents({})
    if total == 0:
        await actualizar_listado()
    else:
        print(f"Base existente: {total} registros")

@app.get("/")
async def root():
    meta = await db["meta"].find_one({"_id": "info"})
    total = await db["contribuyentes"].count_documents({})
    return {"app": "Lista Negra 69-B API", "total": total, "updated_at": meta["updated_at"] if meta else None}

@app.get("/status")
async def status():
    total = await db["contribuyentes"].count_documents({})
    meta = await db["meta"].find_one({"_id": "info"})
    return {"total": total, "updated_at": meta["updated_at"] if meta else None}

@app.get("/buscar/{rfc}")
async def buscar(rfc: str):
    rfc = rfc.strip().upper()
    docs = await db["contribuyentes"].find({"rfc": rfc}, {"_id": 0}).to_list(10)
    if not docs:
        return {"encontrado": False, "rfc": rfc}
    registros = []
    for d in docs:
        registros.append([
            d.get("nombre",""), d.get("situacion",""),
            d.get("of_presuncion_sat",""), d.get("pub_sat_presuntos",""),
            d.get("of_presuncion_dof",""), d.get("pub_dof_presuntos",""),
            d.get("of_desvirtuaron_sat",""), d.get("pub_sat_desvirtuados",""),
            d.get("of_desvirtuaron_dof",""), d.get("pub_dof_desvirtuados",""),
            d.get("of_definitivos_sat",""), d.get("pub_sat_definitivos",""),
            d.get("of_definitivos_dof",""), d.get("pub_dof_definitivos",""),
            d.get("of_sentencia_sat",""), d.get("pub_sat_sentencia",""),
            d.get("of_sentencia_dof",""), d.get("pub_dof_sentencia",""),
        ])
    return {"encontrado": True, "rfc": rfc, "registros": registros}

@app.get("/oficio/{num}")
async def resolver_oficio(num: str):
    """Resuelve la URL del PDF del oficio SAT probando todos los patrones conocidos"""
    if not num.isdigit():
        return {"url": SAT_PAGE, "fallback": True}

    # Lista completa de patrones extraídos de la página oficial del SAT
    patrones = [
        # Prefijos clásicos
        f"O_{num}.pdf",
        f"Oficio_{num}.pdf",
        f"oficio_{num}.pdf",
        # Presunción
        f"{num}_OPE.pdf", f"{num}_OPF.pdf", f"{num}_OPM.pdf",
        f"{num}_OPA.pdf", f"{num}_OPJ.pdf", f"{num}_OPS.pdf",
        f"{num}_OPO.pdf", f"{num}_OPN.pdf", f"{num}_OPD.pdf",
        f"{num}_OPJ_SIFEN.pdf",
        # Definitivos - DS (sin pruebas)
        f"{num}_ODSE.pdf", f"{num}_ODSF.pdf", f"{num}_ODSM.pdf",
        f"{num}_ODSA.pdf", f"{num}_ODSJ.pdf", f"{num}_ODSS.pdf",
        f"{num}_ODSO.pdf", f"{num}_ODSN.pdf", f"{num}_ODSD.pdf",
        # Definitivos - DC (con pruebas)
        f"{num}_ODCE.pdf", f"{num}_ODCF.pdf", f"{num}_ODCM.pdf",
        f"{num}_ODCA.pdf", f"{num}_ODCJ.pdf", f"{num}_ODCS.pdf",
        f"{num}_ODCO.pdf", f"{num}_ODCN.pdf", f"{num}_ODCD.pdf",
        # Definitivos - otros
        f"{num}_ODVA.pdf", f"{num}_ODVD.pdf", f"{num}_ODVE.pdf",
        f"{num}_ODVF.pdf", f"{num}_ODVJ.pdf", f"{num}_ODVM.pdf",
        f"{num}_ODVN.pdf", f"{num}_ODVS.pdf", f"{num}_ODVO.pdf",
        # Desvirtuados
        f"{num}_ADVD.pdf", f"{num}_ADVF.pdf", f"{num}_ADVJ.pdf",
        f"{num}_ADVM.pdf", f"{num}_ADVN.pdf", f"{num}_ADVO.pdf",
        f"{num}_ADVS.pdf",
        # Sentencia favorable
        f"{num}_OSFM.pdf", f"{num}_OSFJ.pdf", f"{num}_OSFA.pdf",
        f"{num}_LSFE.pdf", f"{num}_LSFF.pdf", f"{num}_LSFM.pdf",
        f"{num}_LSFN.pdf", f"{num}_LSFA.pdf", f"{num}_LSFD.pdf",
        f"{num}_LSFJ.pdf", f"{num}_LSFS.pdf", f"{num}_LSF.pdf",
        f"{num}_LMDA.pdf", f"{num}_LMDE.pdf", f"{num}_LMDF.pdf",
        f"{num}_LMDJ.pdf", f"{num}_LMDM.pdf", f"{num}_LMDN.pdf",
        # Sin sufijo
        f"{num}.pdf",
    ]

    async with httpx.AsyncClient(timeout=20) as client:
        tasks = [client.head(BASE_PDF + p) for p in patrones]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    for i, r in enumerate(results):
        if not isinstance(r, Exception) and r.status_code in (200, 302):
            return {"url": BASE_PDF + patrones[i]}

    return {"url": SAT_PAGE, "fallback": True}

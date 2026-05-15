from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from motor.motor_asyncio import AsyncIOMotorClient
import httpx, pandas as pd, io, os, asyncio
from datetime import datetime

app = FastAPI(title="Lista Negra 69-B API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET"])

MONGO_URI = os.environ.get("MONGO_URI", "")
SAT_URL = "https://wu1agsprosta001.blob.core.windows.net/agsc-publicaciones/Datos_abiertos/Documents_AGAFF/Listado_completo_69-B.csv"

client = AsyncIOMotorClient(MONGO_URI)
db = client["listanegra"]

async def actualizar_listado():
    print(f"[{datetime.now()}] Descargando listado SAT...")
    try:
        async with httpx.AsyncClient(timeout=120) as h:
            r = await h.get(SAT_URL, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            })
        df = pd.read_csv(io.StringIO(r.content.decode("latin-1")), header=2, skiprows=0)
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
        print(f"[{datetime.now()}] Error actualizando: {e}")

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
        print("Base vacía — cargando datos iniciales...")
        await actualizar_listado()
    else:
        print(f"Base existente: {total} registros")

@app.get("/")
async def root():
    meta = await db["meta"].find_one({"_id": "info"})
    total = await db["contribuyentes"].count_documents({})
    return {
        "app": "Lista Negra 69-B API",
        "total": total,
        "updated_at": meta["updated_at"] if meta else None
    }

@app.get("/buscar/{rfc}")
async def buscar(rfc: str):
    rfc = rfc.strip().upper()
    docs = await db["contribuyentes"].find(
        {"rfc": rfc}, {"_id": 0}
    ).to_list(10)
    if not docs:
        return {"encontrado": False, "rfc": rfc}
    # Convertir a formato array compacto igual que el HTML actual
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

@app.get("/status")
async def status():
    total = await db["contribuyentes"].count_documents({})
    meta = await db["meta"].find_one({"_id": "info"})
    return {"total": total, "updated_at": meta["updated_at"] if meta else None}

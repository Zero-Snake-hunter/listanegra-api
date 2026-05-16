from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from motor.motor_asyncio import AsyncIOMotorClient
import httpx, pandas as pd, io, os, asyncio, re
from datetime import datetime

app = FastAPI(title="Lista Negra 69-B API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET"])

MONGO_URI = os.environ.get("MONGO_URI", "")
SAT_CSV = "https://wu1agsprosta001.blob.core.windows.net/agsc-publicaciones/Datos_abiertos/Documents_AGAFF/Listado_completo_69-B.csv"
SAT_PAGE = "https://www.gob.mx/sat/acciones-y-programas/notificacion-a-contribuyentes-con-operaciones-presuntamente-inexistentes-y-listados-definitivos-333336"
BASE_PDF = "http://omawww.sat.gob.mx/informacionfiscal/Documents/"

client = AsyncIOMotorClient(MONGO_URI)
db = client["listanegra"]

# Mapa en memoria: número de oficio → nombre de archivo PDF
oficios_map: dict = {}

async def actualizar_mapa_oficios():
    """Descarga la página del SAT y extrae todos los links de PDFs de oficios"""
    global oficios_map
    print(f"[{datetime.now()}] Actualizando mapa de oficios...")
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "es-MX,es;q=0.9",
        }
        async with httpx.AsyncClient(timeout=30) as h:
            r = await h.get(SAT_PAGE, headers=headers)
        
        # Extraer todos los links a PDFs en omawww.sat.gob.mx
        urls = re.findall(
            r'href=["\'](?:http://omawww\.sat\.gob\.mx/(?:informacionfiscal|cifras_sat|documentossat)/Documents/([^"\']+\.pdf))["\']',
            r.text, re.IGNORECASE
        )
        
        nuevo_mapa = {}
        for fname in urls:
            # Extraer número del oficio del nombre del archivo
            m = re.search(r'(?:^|[_/])(\d{4,6})(?:[_.]|$)', fname)
            if m:
                num = m.group(1)
                if num not in nuevo_mapa:
                    nuevo_mapa[num] = fname

        if nuevo_mapa:
            oficios_map = nuevo_mapa
            print(f"[{datetime.now()}] Mapa actualizado: {len(oficios_map)} oficios")
        else:
            print(f"[{datetime.now()}] ADVERTENCIA: mapa vacío, manteniendo anterior")

    except Exception as e:
        print(f"[{datetime.now()}] Error actualizando mapa: {e}")

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
        print(f"[{datetime.now()}] Listado actualizado: {total} registros")
    except Exception as e:
        print(f"[{datetime.now()}] Error actualizando listado: {e}")

@app.on_event("startup")
async def startup():
    # Cargar mapa de oficios al arrancar
    await actualizar_mapa_oficios()

    if not MONGO_URI:
        print("ADVERTENCIA: MONGO_URI no configurado")
        return

    scheduler = AsyncIOScheduler()
    # CSV cada lunes a las 2am
    scheduler.add_job(actualizar_listado, "cron", day_of_week="mon", hour=2)
    # Mapa de oficios cada lunes a las 3am (después del CSV)
    scheduler.add_job(actualizar_mapa_oficios, "cron", day_of_week="mon", hour=3)
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
    return {
        "app": "Lista Negra 69-B API",
        "total": total,
        "updated_at": meta["updated_at"] if meta else None,
        "oficios_mapeados": len(oficios_map)
    }

@app.get("/status")
async def status():
    total = await db["contribuyentes"].count_documents({})
    meta = await db["meta"].find_one({"_id": "info"})
    return {
        "total": total,
        "updated_at": meta["updated_at"] if meta else None,
        "oficios_mapeados": len(oficios_map)
    }

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
    """Resuelve URL del PDF usando el mapa extraído de la página del SAT"""
    if not num.isdigit():
        return {"url": SAT_PAGE, "fallback": True}

    fname = oficios_map.get(num)
    if fname:
        return {"url": BASE_PDF + fname}

    return {"url": SAT_PAGE, "fallback": True}

@app.get("/actualizar-oficios")
async def forzar_actualizacion_oficios():
    """Fuerza actualización del mapa de oficios desde la página del SAT"""
    await actualizar_mapa_oficios()
    return {"ok": True, "oficios_mapeados": len(oficios_map)}

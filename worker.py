import os, json, asyncio, importlib

BASE_DIR = os.path.expanduser("D:/shared")
INPUT_DIR = os.path.join(BASE_DIR, "input")
QUEUE_DIR = os.path.join(BASE_DIR, "queue")

print("INPUT_DIR:", INPUT_DIR)
#print("OUTPUT_DIR:", OUTPUT_DIR)
print("QUEUE_DIR:", QUEUE_DIR)


os.makedirs(INPUT_DIR, exist_ok=True)
os.makedirs(QUEUE_DIR, exist_ok=True)

def update_task_file(task_file, progress=None, status=None, estado_validacion=None, mensaje=None):
    """
    Actualiza un archivo JSON de tarea con los valores que se pasen.
    """
    if not os.path.exists(task_file):
        return
    try:
        with open(task_file, "r") as f:
            data = json.load(f)
    except:
        data = {}

    if progress is not None:
        data["progress"] = progress
    if status is not None:
        data["status"] = status
    if estado_validacion is not None:
        data["estado_validacion"] = estado_validacion
    if mensaje is not None:
        data["mensaje"] = mensaje

    with open(task_file, "w") as f:
        json.dump(data, f)

async def process_task(seccion: str, job_id: str, task_file: str):
    """
    Procesa una tarea según el tipo (app_bd o app_bd2) y actualiza JSON de progreso y validación.
    """
    try:
        input_dir = os.path.join(INPUT_DIR, job_id)
        base_file = os.path.join(input_dir, "base.txt")
        diccionario_file = os.path.join(input_dir, "diccionario.xlsx")

        # Inicia el progreso
        update_task_file(task_file, progress=10, status=2, estado_validacion=1)

        # Ejecuta la app correspondiente
        module_name = f"apps.{seccion}"
        mod = importlib.import_module(module_name)

        # Todas las apps deben exponer run_app()
        func = getattr(mod, "run_app")

        # Ejecuta
        func(base_file, diccionario_file, seccion=seccion, task_file=task_file, job_id=job_id)

        # Si llegó hasta aquí, todo bien
        update_task_file(task_file, progress=100, status=3, estado_validacion=1)
        print(f"✅ Tarea completada: {job_id} ({seccion})")

    except Exception as e:
        # Actualiza JSON con error
        update_task_file(task_file, progress=100, status=2, estado_validacion=2, mensaje=str(e))
        print(f"❌ Error en tarea {job_id}: {e}")

async def worker():
    print("👂 Batch worker escuchando en queue...", flush=True)
    while True:
        for seccion in os.listdir(QUEUE_DIR):
            seccion_path = os.path.join(QUEUE_DIR, seccion)
            if not os.path.isdir(seccion_path):
                continue

            for file in os.listdir(seccion_path):
                if file.endswith(".json"):
                    job_id = os.path.splitext(file)[0]
                    task_file = os.path.join(seccion_path, file)

                    with open(task_file) as f:
                        data = json.load(f)

                    if data.get("progress", 0) < 100:
                        print(f"🔎 Nueva tarea detectada -> job_id={job_id}, tipo={seccion}", flush=True)
                        await process_task(seccion, job_id, task_file)

                        # Eliminar JSON de la cola solo si terminó correctamente
                        with open(task_file) as f:
                            final_data = json.load(f)
                        if final_data.get("estado_validacion") == 1:
                            os.remove(task_file)
                            print(f"🗑️ Tarea {job_id} ({seccion}) eliminada de la cola.", flush=True)

        await asyncio.sleep(2)

def main():
    asyncio.run(worker())

if __name__ == "__main__":
    main()

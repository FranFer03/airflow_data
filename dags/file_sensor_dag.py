from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

def process_csv_file(**context):
    """Función para procesar el archivo CSV encontrado"""
    file_path = "/tmp/data.csv"
    
    if os.path.exists(file_path):
        # Leer el archivo y contar líneas
        with open(file_path, 'r') as file:
            lines = file.readlines()
            line_count = len(lines)
        
        print(f"Archivo encontrado: {file_path}")
        print(f"Número de líneas: {line_count}")
        print("Contenido del archivo:")
        
        # Mostrar las primeras 5 líneas
        for i, line in enumerate(lines[:5]):
            print(f"Línea {i+1}: {line.strip()}")
        
        return f"Procesado exitosamente - {line_count} líneas"
    else:
        raise FileNotFoundError(f"Archivo no encontrado: {file_path}")

def cleanup_file(**context):
    """Función para limpiar/mover el archivo procesado"""
    file_path = "/tmp/data.csv"
    backup_path = "/tmp/data_processed.csv"
    
    if os.path.exists(file_path):
        # Mover archivo a backup
        os.rename(file_path, backup_path)
        print(f"Archivo movido de {file_path} a {backup_path}")
        return "Limpieza completada"
    else:
        print("No hay archivo para limpiar")
        return "No hay archivo para limpiar"

# Configuración del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="file_sensor_example",
    default_args=default_args,
    description='DAG de ejemplo usando sensor personalizado',
    schedule=timedelta(minutes=30),
    catchup=False,
    tags=['sensor', 'file', 'example'],
) as dag:

    # Tarea 1: Crear archivo de ejemplo (para pruebas)
    create_test_file = BashOperator(
        task_id="create_test_file",
        bash_command="""
        echo "timestamp,value,description" > /tmp/data.csv
        echo "$(date),100,Test data 1" >> /tmp/data.csv
        echo "$(date),200,Test data 2" >> /tmp/data.csv
        echo "$(date),300,Test data 3" >> /tmp/data.csv
        echo "Archivo de prueba creado en /tmp/data.csv"
        """,
    )

    # Tarea 2: Sensor personalizado usando Bash
    wait_for_file = BashOperator(
        task_id="wait_for_file",
        bash_command="""
        echo "🔍 Esperando archivo /tmp/data.csv..."
        
        # Contador para timeout
        counter=0
        max_attempts=10
        
        while [ $counter -lt $max_attempts ]; do
            if [ -f /tmp/data.csv ]; then
                echo "✅ Archivo encontrado: /tmp/data.csv"
                ls -la /tmp/data.csv
                exit 0
            else
                echo "⏳ Intento $((counter + 1))/$max_attempts - Archivo no encontrado, esperando 10 segundos..."
                sleep 10
                counter=$((counter + 1))
            fi
        done
        
        echo "❌ Timeout: Archivo no encontrado después de $max_attempts intentos"
        exit 1
        """,
    )

    # Tarea 3: Verificar que el archivo existe
    verify_file = BashOperator(
        task_id="verify_file",
        bash_command="""
        if [ -f /tmp/data.csv ]; then
            echo "✅ Archivo encontrado: /tmp/data.csv"
            echo "📊 Tamaño del archivo: $(du -h /tmp/data.csv | cut -f1)"
            echo "📅 Fecha de modificación: $(stat -c %y /tmp/data.csv)"
            echo "📄 Contenido:"
            cat /tmp/data.csv
        else
            echo "❌ Archivo no encontrado"
            exit 1
        fi
        """,
    )

    # Tarea 4: Procesar el archivo con Python
    process_file = PythonOperator(
        task_id="process_file",
        python_callable=process_csv_file,
    )

    # Tarea 5: Crear reporte
    create_report = BashOperator(
        task_id="create_report",
        bash_command="""
        echo "=== REPORTE DE PROCESAMIENTO ===" > /tmp/processing_report.txt
        echo "Fecha: $(date)" >> /tmp/processing_report.txt
        echo "Archivo procesado: /tmp/data.csv" >> /tmp/processing_report.txt
        echo "Estado: COMPLETADO" >> /tmp/processing_report.txt
        echo "Reporte creado en /tmp/processing_report.txt"
        cat /tmp/processing_report.txt
        """,
    )

    # Tarea 6: Limpiar archivos
    cleanup = PythonOperator(
        task_id="cleanup",
        python_callable=cleanup_file,
    )

    # Definir el flujo de tareas
    create_test_file >> wait_for_file >> verify_file >> process_file >> create_report >> cleanup

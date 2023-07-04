import json
from flask import Flask, jsonify, render_template, abort
import psycopg2
import psycopg2.extras
import os
import sys
import pprint
from dotenv import load_dotenv
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

def get_connection():
    conn = psycopg2.connect(database="proyecto", user=os.environ.get("USERNAME"), password=os.environ.get("PASSWORD"), host="localhost")
    conn.autocommit = True
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return conn, cursor

@app.route("/consulta1", methods=["GET"])
def get_consulta1():
    conn,cursor = get_connection()
    cursor.execute("""
SELECT SUM(des.desap)::FLOAT /
       SUM(des.total_al)::FLOAT * 100 as porc_desap,
       des.anio, des.grado
    FROM (
        SELECT COUNT(at.nota) FILTER (WHERE at.nota < 11) as desap,
               COUNT(at.nota) as total_al,
               s.grado, at.anio
        FROM (
            SELECT es2.alumno_dni, cur_codigo, anio, nota FROM (
                SELECT alumno_dni, curso_codigo, anioescolar_anio as anio FROM tiene t
                    JOIN curso cs ON t.curso_codigo = cs.codigo
                WHERE cs.tipo = 'Matemáticas'
                  AND cs.nivel = 'Secundaria'
                ) tiene_mat
            INNER JOIN (
                SELECT ev.curso_codigo as cur_codigo, ev.fecha, ev.alumno_dni, ev.nota FROM evaluacion ev
                    JOIN curso cs ON ev.curso_codigo = cs.codigo
                 WHERE ev.nombre = 'Examen Final'
                   AND cs.nivel = 'Secundaria'
                   AND cs.tipo = 'Matemáticas'
                ) es2
            ON tiene_mat.alumno_dni = es2.alumno_dni AND tiene_mat.curso_codigo = es2.cur_codigo
            AND tiene_mat.anio = EXTRACT(YEAR FROM es2.fecha)
            WHERE (SELECT MAX(anioescolar_anio) FROM tiene) - anio BETWEEN 0 AND 5
        ) at
        INNER JOIN ensenia es ON at.cur_codigo = es.curso_codigo
               AND at.anio = es.anioescolar_anio
        INNER JOIN salon s ON es.salon_codigo = s.codigo
        GROUP BY s.grado, at.anio
    ) des
GROUP BY des.anio, des.grado
ORDER BY grado ASC, anio ASC;

                   """)
    records = cursor.fetchall()
    return json.dumps({
        'success': True,
        "resultado":[dict(i) for i in records]
    }), 200



@app.route("/consulta2", methods=["GET"])
def get_consulta2():
    conn,cursor = get_connection()
    cursor.execute("""
SELECT DISTINCT p.nombre, p.apellido
        FROM persona p
        JOIN alumno a ON p.dni = a.dni
        JOIN tiene t ON a.dni = t.alumno_dni
        JOIN ensenia e ON t.curso_codigo = e.curso_codigo AND t.anioescolar_anio = e.anioescolar_anio
        JOIN profesor pro ON e.profesor_dni = pro.dni
        WHERE t.alumno_dni IN (
            SELECT ev.alumno_dni
            FROM evaluacion ev
            WHERE ev.fecha >= current_date - INTERVAL '1 year' AND ev.fecha < current_date
            GROUP BY ev.alumno_dni
            HAVING AVG(ev.nota) > 13
        )
        AND t.alumno_dni IN (
            SELECT a.alumno_dni
            FROM asistencia a
            WHERE a.fecha >= current_date - INTERVAL '1 year' AND a.fecha < current_date
            GROUP BY a.alumno_dni
            HAVING COUNT(CASE WHEN a.asistio THEN 1 ELSE 0 END) >= (
                SELECT AVG(asistencias)
                FROM (
                    SELECT COUNT(*) as asistencias
                    FROM asistencia
                    WHERE fecha >= current_date - INTERVAL '1 year' AND fecha < current_date AND asistio = TRUE
                    GROUP BY alumno_dni
                ) as promedio_asistencia
            )
        );
                   """)
    records = cursor.fetchall()
    return json.dumps({
        'success': True,
        "resultado":[dict(i) for i in records]
    }), 200



@app.route("/consulta3", methods=["GET"])
def get_consulta3():
    conn,cursor = get_connection()
    cursor.execute("""
WITH c3 AS (
            SELECT c1.alumno_dni, COUNT(*) AS faltas
            FROM (
                SELECT fecha, alumno_dni, salon_codigo
                FROM asistencia
                WHERE asistio = false
                AND EXTRACT(YEAR FROM fecha) = (SELECT MAX(anio) FROM anioescolar)
            ) AS c1
            JOIN (
                SELECT fecha, curso_codigo, salon_codigo
                FROM horario
                WHERE curso_codigo = (SELECT codigo FROM curso WHERE nombre = 'Razonamiento Matemático')
                AND EXTRACT(YEAR FROM fecha) = (SELECT MAX(anio) FROM anioescolar)
            ) AS c2 ON (c1.fecha = c2.fecha AND c1.salon_codigo = c2.salon_codigo)
            GROUP BY c1.alumno_dni
            ORDER BY faltas DESC
            LIMIT 10
        )
        SELECT c3.alumno_dni, promedio, faltas
        FROM (
            SELECT alumno_dni, AVG(nota) AS promedio
            FROM evaluacion
            WHERE EXTRACT(YEAR FROM fecha) = (SELECT MAX(anio) FROM anioescolar)
            AND curso_codigo = (SELECT codigo FROM curso WHERE nombre = 'Razonamiento Matemático')
            AND alumno_dni IN (SELECT alumno_dni FROM c3)
            GROUP BY alumno_dni
        ) AS c4
        right JOIN c3 ON c4.alumno_dni = c3.alumno_dni
        ORDER BY faltas DESC;
                   """)
    records = cursor.fetchall()
    return json.dumps({
        'success': True,
        "resultado":[dict(i) for i in records]
    }), 200


if __name__ == "__main__":
     app.run(debug=True)

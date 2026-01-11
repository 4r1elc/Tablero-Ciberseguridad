import sqlite3
import logging
from pathlib import Path
from datetime import datetime
import math

import pandas as pd
import streamlit as st

DATA_EXCEL = "Indicadores_estrategia2.xlsx"
DB_PATH = "cs_dashboard.db"
LOG_FILE = "cs_dashboard.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ],
)
logger = logging.getLogger(__name__)


# ------------------- CONEXIÓN Y ESTRUCTURA DB -------------------

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS indicators (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            indicador TEXT NOT NULL,
            descripcion TEXT,
            formula TEXT,
            verde TEXT,
            amarillo TEXT,
            rojo TEXT,
            categoria TEXT,
            referencia TEXT,
            tipo_indicador TEXT,
            nivel_esfuerzo TEXT,
            funcion_nist TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS measurements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            indicator_id INTEGER NOT NULL,
            periodo TEXT NOT NULL,
            valor REAL NOT NULL,
            comentario TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(indicator_id, periodo),
            FOREIGN KEY(indicator_id) REFERENCES indicators(id)
        )
        """
    )
    conn.commit()
    conn.close()
    logger.info("Base de datos inicializada o verificada.")


def extract_nist_function_code(ref):
    if not isinstance(ref, str):
        return None
    if "." in ref:
        return ref.split(".")[0].strip().upper()
    return None


NIST_FUNC_LABELS = {
    "GV": "Gobernar",
    "ID": "Identificar",
    "PR": "Proteger",
    "DE": "Detectar",
    "RS": "Responder",
    "RC": "Recuperar",
}


# ------------------- LÓGICA DE FÓRMULAS Y SEMÁFORO -------------------

def parse_ratio_formula(formula_text: str):
    if not isinstance(formula_text, str):
        return None

    text = formula_text.strip()

    if "(" not in text or ")" not in text or "/" not in text:
        return None

    try:
        inside = text[text.index("(") + 1 : text.index(")")]
    except ValueError:
        return None

    parts = inside.split("/")
    if len(parts) != 2:
        return None

    num_label = parts[0].strip()
    den_label = parts[1].strip()

    multiplier = 1.0
    if "100" in text:
        multiplier = 100.0

    return {
        "num_label": num_label,
        "den_label": den_label,
        "multiplier": multiplier,
    }


def parse_threshold_expr(expr):
    if not isinstance(expr, str):
        return None

    s = expr.strip()
    if not s:
        return None

    s = s.replace("–", "-")
    s = s.replace("%", "")
    s = s.replace(" ", "")

    if s in {"—", "-"}:
        return None

    if s.startswith((">=", "≥")):
        if s.startswith(">="):
            num = float(s[2:])
        else:
            num = float(s[1:])
        return lambda v, n=num: v >= n

    if s.startswith(("<=", "≤")):
        if s.startswith("<="):
            num = float(s[2:])
        else:
            num = float(s[1:])
        return lambda v, n=num: v <= n

    if s.startswith(">"):
        num = float(s[1:])
        return lambda v, n=num: v > n

    if s.startswith("<"):
        num = float(s[1:])
        return lambda v, n=num: v < n

    if "-" in s:
        left, right = s.split("-", 1)
        try:
            a = float(left)
            b = float(right)
            return lambda v, a=a, b=b: a <= v <= b
        except ValueError:
            return None

    try:
        num = float(s)
        return lambda v, n=num: abs(v - n) < 1e-9
    except ValueError:
        return None


def classify_indicator_value(value, verde_expr, amarillo_expr, rojo_expr):
    for color, expr in (("Verde", verde_expr), ("Amarillo", amarillo_expr), ("Rojo", rojo_expr)):
        cond = parse_threshold_expr(expr)
        if cond is not None:
            try:
                if cond(value):
                    return color
            except Exception:
                pass

    if value >= 80:
        return "Verde"
    elif value >= 50:
        return "Amarillo"
    else:
        return "Rojo"


def classify_score(score):
    if score >= 80:
        return "Verde"
    elif score >= 50:
        return "Amarillo"
    else:
        return "Rojo"


# ------------------- CARGA DESDE EXCEL -------------------

def load_indicators_from_excel_if_needed():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) AS c FROM indicators")
    count = cur.fetchone()["c"]

    if count == 0:
        if not Path(DATA_EXCEL).exists():
            logger.error("No se encontró el archivo Excel de indicadores.")
            conn.close()
            return

        df = pd.read_excel(DATA_EXCEL, sheet_name="Hoja1")

        df = df.rename(
            columns={
                "Indicador": "indicador",
                "Descripción": "descripcion",
                "Fórmula / Métrica": "formula",
                "Verde": "verde",
                "Amarillo": "amarillo",
                "Rojo": "rojo",
                "Categoría": "categoria",
                "Referencia": "referencia",
                "Tipo Indicador": "tipo_indicador",
                "Nivel de Esfuerzo": "nivel_esfuerzo",
            }
        )

        rows = []
        for _, row in df.iterrows():
            fn_code = extract_nist_function_code(row.get("referencia"))
            rows.append(
                (
                    row.get("indicador"),
                    row.get("descripcion"),
                    row.get("formula"),
                    row.get("verde"),
                    row.get("amarillo"),
                    row.get("rojo"),
                    row.get("categoria"),
                    row.get("referencia"),
                    row.get("tipo_indicador"),
                    row.get("nivel_esfuerzo"),
                    fn_code,
                )
            )

        cur.executemany(
            """
            INSERT INTO indicators (
                indicador, descripcion, formula, verde, amarillo, rojo,
                categoria, referencia, tipo_indicador, nivel_esfuerzo, funcion_nist
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
        logger.info("Indicadores cargados desde Excel en la base de datos.")

    conn.close()


# Correcciones puntuales de referencias NIST
def fix_indicator_references():
    conn = get_db_connection()
    cur = conn.cursor()

    changes = [
        ("Cumplimiento de RTO", "RC.RP-05"),
        ("Tasa de verificación exitosa de respaldos", "RC.RP-03"),
        ("Cuentas con privilegios excesivos", "PR.AA-05"),
    ]
    for indicador_name, new_ref in changes:
        fn_code = extract_nist_function_code(new_ref)
        cur.execute(
            "UPDATE indicators SET referencia = ?, funcion_nist = ? WHERE indicador = ?",
            (new_ref, fn_code, indicador_name),
        )

    cur.execute(
        "DELETE FROM indicators WHERE indicador = ? OR referencia = ?",
        ("Vulnerabilidades explotables en activos expuestos", "PR-PS-02"),
    )

    conn.commit()
    conn.close()
    logger.info("Correcciones de referencias NIST aplicadas.")


# ------------------- OPERACIONES DB -------------------

def upsert_measurement(indicator_id, periodo, valor, comentario):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO measurements (indicator_id, periodo, valor, comentario, created_at, updated_at)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT(indicator_id, periodo)
        DO UPDATE SET
            valor = excluded.valor,
            comentario = excluded.comentario,
            updated_at = CURRENT_TIMESTAMP
        """,
        (indicator_id, periodo, valor, comentario),
    )
    conn.commit()
    conn.close()
    logger.info(
        "Upsert de medición: indicator_id=%s, periodo=%s, valor=%s",
        indicator_id,
        periodo,
        valor,
    )


def update_indicator_reference(indicator_id, referencia, funcion_nist):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "UPDATE indicators SET referencia = ?, funcion_nist = ? WHERE id = ?",
        (referencia, funcion_nist, indicator_id),
    )
    conn.commit()
    conn.close()
    logger.info(
        "Referencia actualizada manualmente: id=%s, referencia=%s, funcion_nist=%s",
        indicator_id,
        referencia,
        funcion_nist,
    )


def get_indicators():
    conn = get_db_connection()
    df = pd.read_sql_query("SELECT * FROM indicators ORDER BY indicador", conn)
    conn.close()
    return df


def get_measurements(periodo=None):
    conn = get_db_connection()
    if periodo:
        query = """
            SELECT m.*,
                   i.indicador, i.categoria, i.referencia, i.funcion_nist,
                   i.verde, i.amarillo, i.rojo
            FROM measurements m
            JOIN indicators i ON m.indicator_id = i.id
            WHERE m.periodo = ?
        """
        df = pd.read_sql_query(query, conn, params=(periodo,))
    else:
        query = """
            SELECT m.*,
                   i.indicador, i.categoria, i.referencia, i.funcion_nist,
                   i.verde, i.amarillo, i.rojo
            FROM measurements m
            JOIN indicators i ON m.indicator_id = i.id
        """
        df = pd.read_sql_query(query, conn)
    conn.close()
    return df


def get_distinct_periods():
    conn = get_db_connection()
    df = pd.read_sql_query(
        "SELECT DISTINCT periodo FROM measurements ORDER BY periodo", conn
    )
    conn.close()
    return df["periodo"].tolist()


def get_last_measurement_for_indicator(indicator_id):
    conn = get_db_connection()
    df = pd.read_sql_query(
        """
        SELECT periodo, valor, created_at
        FROM measurements
        WHERE indicator_id = ?
        ORDER BY datetime(created_at) DESC
        LIMIT 1
        """,
        conn,
        params=(indicator_id,),
    )
    conn.close()
    if df.empty:
        return None
    return df.iloc[0]


# ------------------- INTERFAZ STREAMLIT -------------------

def main():
    st.set_page_config(
        page_title="Tablero de Ciberseguridad",
        layout="wide",
    )

    st.title("Tablero interactivo de indicadores de Ciberseguridad")

    init_db()
    load_indicators_from_excel_if_needed()
    fix_indicator_references()

    menu = st.sidebar.radio(
        "Secciones",
        ["Carga de datos", "Tablero", "Configuración"],
    )

    if menu == "Carga de datos":
        seccion_carga_datos()
    elif menu == "Tablero":
        seccion_tablero()
    elif menu == "Configuración":
        seccion_configuracion()


def seccion_carga_datos():
    st.header("Carga y modificación de valores de indicadores")

    indicadores_df = get_indicators()
    if indicadores_df.empty:
        st.warning("No hay indicadores cargados en la base de datos.")
        return

    indicadores_df["label"] = (
        indicadores_df["indicador"] + " (" + indicadores_df["referencia"].fillna("") + ")"
    )

    seleccion = st.selectbox(
        "Selecciona un indicador",
        options=indicadores_df.index,
        format_func=lambda idx: indicadores_df.loc[idx, "label"],
    )

    indicador = indicadores_df.loc[seleccion]

    col_info, col_form = st.columns([2, 3])

    with col_info:
        st.subheader("Detalle del indicador")
        st.text(f"Nombre: {indicador['indicador']}")
        st.text(f"Descripción: {indicador['descripcion']}")
        st.text(f"Fórmula/Métrica: {indicador['formula']}")
        st.text(f"Categoría: {indicador['categoria']}")
        st.text(f"Referencia: {indicador['referencia']}")
        fn_code = indicador.get("funcion_nist")
        if fn_code:
            st.text(
                f"Función NIST: {fn_code} - {NIST_FUNC_LABELS.get(fn_code, 'Desconocida')}"
            )
        else:
            st.text("Función NIST: No definida")
        st.text(f"Tipo de indicador: {indicador['tipo_indicador']}")

        st.text(f"Rango verde: {indicador['verde']}")
        st.text(f"Rango amarillo: {indicador['amarillo']}")
        st.text(f"Rango rojo: {indicador['rojo']}")

        ultimo = get_last_measurement_for_indicator(int(indicador["id"]))
        if ultimo is not None:
            color = classify_indicator_value(
                ultimo["valor"],
                indicador.get("verde"),
                indicador.get("amarillo"),
                indicador.get("rojo"),
            )
            emoji_map = {"Verde": "🟢", "Amarillo": "🟡", "Rojo": "🔴"}
            semaforo_txt = f"{emoji_map.get(color, '⚪')} {color}"
            st.text(
                f"Último valor registrado: {ultimo['valor']:.2f} "
                f"(Período {ultimo['periodo']}) - Semáforo: {semaforo_txt}"
            )
        else:
            st.text("Aún no hay mediciones registradas para este indicador.")

    with col_form:
        st.subheader("Registrar o actualizar medición")

        default_periodo = datetime.now().strftime("%Y-%m")
        periodo = st.text_input(
            "Período de la medición (ejemplo: 2025-Q4, 2025-11)",
            value=default_periodo,
        )

        formula_info = parse_ratio_formula(indicador.get("formula"))

        valor_calculado = None
        num_val = None
        den_val = None

        if formula_info:
            st.caption(
                "Este indicador se calculará automáticamente como "
                f"({formula_info['num_label']} / {formula_info['den_label']}) "
                f"x {int(formula_info['multiplier']) if formula_info['multiplier'] != 1 else ''}".strip()
            )

            num_val = st.number_input(
                f"Valor para '{formula_info['num_label']}'",
                min_value=0.0,
                value=0.0,
                step=1.0,
            )
            den_val = st.number_input(
                f"Valor para '{formula_info['den_label']}'",
                min_value=0.0,
                value=0.0,
                step=1.0,
            )

            if den_val > 0:
                valor_calculado = (num_val / den_val) * formula_info["multiplier"]
                st.metric("Valor calculado del indicador", f"{valor_calculado:.2f}")
            else:
                st.info("Para calcular el indicador, el denominador debe ser mayor que cero.")
        else:
            valor_calculado = st.number_input(
                "Valor del indicador",
                min_value=0.0,
                max_value=100.0,
                value=0.0,
                step=0.1,
            )

        comentario = st.text_area("Comentario (opcional)", "")

        if st.button("Guardar / Actualizar medición"):
            if not periodo.strip():
                st.error("El período no puede estar vacío.")
            elif formula_info and (den_val is None or den_val == 0):
                st.error("No se puede calcular el indicador porque el denominador es cero.")
            else:
                valor_guardar = float(valor_calculado) if valor_calculado is not None else 0.0
                upsert_measurement(
                    indicator_id=int(indicador["id"]),
                    periodo=periodo.strip(),
                    valor=valor_guardar,
                    comentario=comentario.strip(),
                )
                st.success("Medición guardada o actualizada correctamente.")

                conn = get_db_connection()
                df = pd.read_sql_query(
                    """
                    SELECT periodo, valor, comentario, created_at, updated_at
                    FROM measurements
                    WHERE indicator_id = ?
                    ORDER BY periodo
                    """,
                    conn,
                    params=(int(indicador["id"]),),
                )
                conn.close()
                st.subheader("Histórico de mediciones para este indicador")
                st.dataframe(df)


def seccion_tablero():
    st.header("Tablero dinámico de indicadores")

    periodos = get_distinct_periods()
    if not periodos:
        st.info("Aún no hay mediciones registradas. Primero carga datos en la sección correspondiente.")
        return

    col_filtros, col_score = st.columns([2, 1])

    with col_filtros:
        periodo_seleccionado = st.selectbox(
            "Selecciona el período a visualizar",
            options=periodos,
            index=len(periodos) - 1,
        )

    df = get_measurements(periodo=periodo_seleccionado)

    if df.empty:
        st.warning("No hay mediciones para el período seleccionado.")
        return

    df["nivel_color"] = df.apply(
        lambda r: classify_indicator_value(
            r["valor"], r.get("verde"), r.get("amarillo"), r.get("rojo")
        ),
        axis=1,
    )

    emoji_map = {"Verde": "🟢", "Amarillo": "🟡", "Rojo": "🔴"}
    df["Semáforo"] = df["nivel_color"].map(
        lambda c: f"{emoji_map.get(c, '⚪')} {c}" if isinstance(c, str) else "⚪ Sin criterio"
    )

    with col_score:
        score_general = df["valor"].mean()
        st.subheader("Score de Ciberseguridad")
        st.metric(
            label=f"Período {periodo_seleccionado}",
            value=f"{score_general:.1f}",
        )
        estado_score = classify_score(score_general)
        st.markdown(f"{emoji_map.get(estado_score, '⚪')} Estado del score general: {estado_score}")

    st.subheader("Promedio por función NIST")

    df_func = df.copy()
    df_func = df_func[df_func["funcion_nist"].notnull()]

    if df_func.empty:
        st.info("No se encontró información de función NIST asociada a las mediciones.")
    else:
        resumen = (
            df_func.groupby("funcion_nist")["valor"]
            .agg(["mean", "count"])
            .reset_index()
        )
        resumen["nombre_funcion"] = resumen["funcion_nist"].map(
            lambda c: NIST_FUNC_LABELS.get(c, "Desconocida")
        )
        resumen = resumen[["funcion_nist", "nombre_funcion", "mean", "count"]]
        resumen = resumen.rename(
            columns={
                "funcion_nist": "Función NIST",
                "nombre_funcion": "Nombre",
                "mean": "Promedio",
                "count": "Cantidad indicadores",
            }
        )

        st.dataframe(resumen.style.format({"Promedio": "{:.1f}"}))

        chart_df = resumen.set_index("Nombre")[["Promedio"]]
        st.bar_chart(chart_df)

    st.subheader("Detalle de mediciones del período seleccionado")

    detalle = df[
        [
            "indicador",
            "categoria",
            "referencia",
            "funcion_nist",
            "periodo",
            "valor",
            "Semáforo",
            "comentario",
        ]
    ].copy()
    detalle = detalle.rename(
        columns={
            "indicador": "Indicador",
            "categoria": "Categoría",
            "referencia": "Referencia",
            "funcion_nist": "Función NIST",
            "periodo": "Período",
            "valor": "Valor",
            "Semáforo": "Semáforo",
            "comentario": "Comentario",
        }
    )

    def estilo_semaforo(val):
        if isinstance(val, str):
            if "Verde" in val:
                return "background-color: #d4edda"
            if "Amarillo" in val:
                return "background-color: #fff3cd"
            if "Rojo" in val:
                return "background-color: #f8d7da"
        return ""

    st.dataframe(
        detalle.style.format({"Valor": "{:.1f}"}).applymap(
            estilo_semaforo, subset=["Semáforo"]
        )
    )


def seccion_configuracion():
    st.header("Configuración y diagnóstico")

    st.subheader("Resumen de indicadores")

    df_ind = get_indicators()
    if df_ind.empty:
        st.info("No hay indicadores en la base de datos.")
        return

    df_ind_vista = df_ind[
        [
            "indicador",
            "categoria",
            "referencia",
            "funcion_nist",
            "tipo_indicador",
        ]
    ].copy()
    df_ind_vista = df_ind_vista.rename(
        columns={
            "indicador": "Indicador",
            "categoria": "Categoría",
            "referencia": "Referencia",
            "funcion_nist": "Función NIST",
            "tipo_indicador": "Tipo indicador",
        }
    )

    st.dataframe(df_ind_vista)

    st.subheader("Editar códigos de referencia NIST (avanzado)")

    indicadores_df = get_indicators()
    indicadores_df["label"] = (
        indicadores_df["indicador"] + " (" + indicadores_df["referencia"].fillna("") + ")"
    )

    sel = st.selectbox(
        "Selecciona un indicador para editar su referencia",
        options=indicadores_df.index,
        format_func=lambda idx: indicadores_df.loc[idx, "label"],
    )

    ind_sel = indicadores_df.loc[sel]

    nueva_ref = st.text_input("Referencia NIST", value=ind_sel["referencia"] or "")
    fn_sugerida = extract_nist_function_code(nueva_ref) or (ind_sel["funcion_nist"] or "")
    nueva_fn = st.text_input("Función NIST (código)", value=fn_sugerida)

    if st.button("Guardar cambios de referencia"):
        update_indicator_reference(int(ind_sel["id"]), nueva_ref.strip(), nueva_fn.strip() or None)
        st.success("Referencia y función NIST actualizadas para el indicador seleccionado.")

    st.subheader("Información de funciones NIST")

    fn_info_rows = []
    for code, name in NIST_FUNC_LABELS.items():
        total_ind = df_ind[df_ind["funcion_nist"] == code].shape[0]
        fn_info_rows.append({"Código": code, "Función": name, "Cantidad indicadores": total_ind})
    fn_info_df = pd.DataFrame(fn_info_rows)
    st.dataframe(fn_info_df)

    st.subheader("Mediciones registradas (vista rápida)")

    df_med = get_measurements()
    if df_med.empty:
        st.info("No hay mediciones registradas todavía.")
    else:
        vista_med = df_med[
            [
                "indicador",
                "periodo",
                "valor",
                "funcion_nist",
                "categoria",
                "created_at",
                "updated_at",
            ]
        ].copy()
        vista_med = vista_med.rename(
            columns={
                "indicador": "Indicador",
                "periodo": "Período",
                "valor": "Valor",
                "funcion_nist": "Función NIST",
                "categoria": "Categoría",
                "created_at": "Creado",
                "updated_at": "Actualizado",
            }
        )
        st.dataframe(vista_med.style.format({"Valor": "{:.1f}"}))


if __name__ == "__main__":
    main()

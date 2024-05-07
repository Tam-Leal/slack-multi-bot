import psycopg2
import pandas as pd
import time
from dotenv import load_dotenv
import os

# Função para conectar ao Redshift
load_dotenv()  # Carrega as variáveis de ambiente do arquivo .env

host = os.getenv('REDSHIFT_HOST')
port = os.getenv('REDSHIFT_PORT')
dbname = os.getenv('REDSHIFT_DBNAME')
user = os.getenv('REDSHIFT_USER')
password = os.getenv('REDSHIFT_PASSWORD')


# Função para conectar ao Redshift usando variáveis de ambiente
def connect_to_redshift(host, port, dbname, user, password):
    conn_string = f"dbname='{dbname}' user='{user}' host='{host}' password='{password}' port='{port}'"
    try:
        conn = psycopg2.connect(conn_string)
        print("Connected to Redshift")
        return conn
    except Exception as e:
        print(f"Error: {e}")
        return None


# Função para converter uma query em DataFrame
def query_to_dataframe(conn, query):
    try:
        return pd.read_sql(query, conn)
    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame()


# Função para fechar a conexão
def close_connection(conn):
    if conn is not None:
        conn.close()
        print("Connection closed")


# Função para salvar DataFrame em um arquivo Excel
def save_dataframe_to_excel(df, filename):
    try:
        # Utiliza 'openpyxl' como motor para escrita do arquivo Excel
        df.to_excel(filename, index=False, engine='openpyxl')
        print(f"DataFrame saved to {filename}")
    except Exception as e:
        raise RuntimeError(f"Failed to save DataFrame to Excel: {e}")


def save_dataframe_to_csv(df, filename):
    try:
        df.to_csv(filename, index=False)
        print(f"DataFrame saved to {filename}")
    except Exception as e:
        raise RuntimeError(f"Failed to save DataFrame to CSV: {e}")


def add_attribute_join(table, attribute, join_type="LEFT JOIN", custom_conditions=""):
    if table.endswith('_var'):
        prefix = 'pav'
    elif table.endswith('_int'):
        prefix = 'pai'
    elif table.endswith('_txt'):
        prefix = 'pat'
    else:
        raise ValueError(f"Unknown table type for {table}")

    join_conditions = f"AND {prefix}.store_id = 0 "
    if custom_conditions:
        join_conditions += custom_conditions

    return f"""
    {join_type} (
        SELECT {prefix}.value, {prefix}.product_id
        FROM magento.{table} {prefix}
        JOIN magento.attribute_product ap ON {prefix}.attribute_id = ap.id
        AND ap.name = '{attribute}'
        {join_conditions}
    ) {attribute} ON {attribute}.product_id = p.row_id
    """


def rename_dataframe_columns(df, conn):
    # Busca o mapeamento de nomes de colunas
    mapping_query = "SELECT name, label FROM magento.attribute_product"
    df_mapping = query_to_dataframe(conn, mapping_query)

    # Cria um dicionário para mapear nomes antigos para novos
    name_mapping = {row['name']: row['label'] for index, row in df_mapping.iterrows() if row['label']}

    # Renomeia as colunas no DataFrame
    df = df.rename(columns=name_mapping)
    return df


def main():
    conn = connect_to_redshift(host, port, dbname, user, password)
    start_time = time.time()

    if conn is not None:
        # Construa sua consulta SQL base
        base_query = """
          SELECT
            p.id,
            p.row_id,
            p.sku,
            p.attribute_set_id,
            p.type_id,
            p.created_at,
            p.updated_at,
            ats.name AS attribute_set_name,
            COALESCE(visibility.value, 0) AS visibility,  -- Ajustado para inteiro
            COALESCE(status.value, 0) AS status,  -- Ajustado para inteiro
            COALESCE(an.value, '') AS name,
            COALESCE(pm.manufacturer_id, 0) AS manufacturer_id,  -- Ajustado para inteiro
            COALESCE(pm.name, '') AS manufacturer_name,
            COALESCE(url.value, '') AS url_key,
            convert_timezone('US/Eastern', p.created_at) AS created_at_local,
            convert_timezone('US/Eastern', p.updated_at) AS updated_at_local,
            COALESCE(manufacturer_sku.value, '') AS manufacturer_sku,  -- Presumido como texto
            COALESCE(hide_from_product_view.value, 0) AS hide_from_product_view,
            COALESCE(applications.value, '') AS applications,
            COALESCE(abrasion_wear_resistance.value, '') AS abrasion_wear_resistance,
            COALESCE(acoustics.value, '') AS acoustics,
            COALESCE(ada.value, '') AS ada,
            COALESCE(antimicrobial_testing.value, '') AS antimicrobial_testing,
            COALESCE(backing_input.value, '') AS backing_input,
            COALESCE(thickness_pile_height_carpet.value, '') AS thickness_pile_height_carpet,
            COALESCE(classification.value, '') AS classification,
            COALESCE(collection_name.value, '') AS collection_name,
            COALESCE(construction.value, '') AS construction,
            COALESCE(content_test_attribute.value, '') AS content_test_attribute,
            COALESCE(custom_capabilities.value, '') AS custom_capabilities,
            COALESCE(description.value, '') AS description,
            COALESCE(designer_name.value, '') AS designer_name,
            COALESCE(edge_end_detail.value, '') AS edge_end_detail,
            COALESCE(emissions.value, '') AS emissions,
            COALESCE(finish_treatment.value, '') AS finish_treatment,
            COALESCE(flammability.value, '') AS flammability,
            COALESCE(grout_width.value, '') AS grout_width,
            COALESCE(hanging_information.value, '') AS hanging_information,
            COALESCE(hardness_result.value, '') AS hardness_result,
            COALESCE(hide_configuration.value, '') AS hide_configuration,
            COALESCE(installation.value, '') AS installation,
            COALESCE(installation_direction.value, '') AS installation_direction,
            COALESCE(lead_time.value, '') AS lead_time,
            COALESCE(leed.value, '') AS leed,
            COALESCE(length.value, '') AS length,
            COALESCE(lightfastness_mdp.value, '') AS lightfastness_mdp,
            COALESCE(maintenance_maya.value, '') AS maintenance_maya,
            COALESCE(manufacturer_notes.value, '') AS manufacturer_notes,
            COALESCE(min_max_units.value, '') AS min_max_units,
            COALESCE(percentage_of_opacity.value, '') AS percentage_of_opacity,
            COALESCE(order_increments.value, '') AS order_increments,
            COALESCE(thickness.value, '') AS thickness,
            COALESCE(pattern_repeat_width.value, '') AS pattern_repeat_width,
            COALESCE(performance.value, '') AS performance,
            COALESCE(pile_face_weight_carpet.value, '') AS pile_face_weight_carpet,
            COALESCE(product_details.value, '') AS product_details,
            COALESCE(recommended_grout.value, '') AS recommended_grout,
            COALESCE(series_name.value, '') AS series_name,
            COALESCE(size_notes.value, '') AS size_notes,
            COALESCE(slip_resistance.value, '') AS slip_resistance,
            COALESCE(stain_resistance.value, '') AS stain_resistance,
            COALESCE(state_of_origin.value, 0) AS state_of_origin,
            COALESCE(surface_pile_density.value, '') AS surface_pile_density,
            COALESCE(surface_texture_mdp.value, '') AS surface_texture_mdp,
            COALESCE(tile_sheet_dimensions.value, '') AS tile_sheet_dimensions,
            COALESCE(total_weight.value, '') AS total_weight,
            COALESCE(warranty.value, '') AS warranty,
            COALESCE(water_absorption_result.value, '') AS water_absorption_result,
            COALESCE(wear_layer_thickness.value, '') AS wear_layer_thickness,
            COALESCE(weather_resistance_result.value, '') AS weather_resistance_result,
            COALESCE(width.value, '') AS width,
            COALESCE(wood_grade.value, '') AS wood_grade,
            COALESCE(yarn_tuft_details.value, '') AS yarn_tuft_details,
            COALESCE(year_of_introduction.value, '') AS year_of_introduction,
            COALESCE(category_ids.value, '') AS category_ids
        FROM magento.product_basic p
        JOIN magento.attribute_set ats ON ats.id = p.attribute_set_id
        LEFT JOIN (
            SELECT pav.value, pav.product_id
            FROM magento.product_attribute_var pav
            JOIN magento.attribute_product ap ON pav.attribute_id = ap.id
            AND ap.name = 'name'
            AND pav.store_id = 0
        ) an ON an.product_id = p.row_id
        LEFT JOIN (
            SELECT m.manufacturer_id, m.name, cpei.row_id
            FROM mbdw.catalog_product_entity_int cpei
            JOIN magento.manufacturer_listing m ON m.manufacturer_id = cpei.value
            AND cpei.store_id = 0
            AND cpei._sdc_deleted_at__string is null
            AND cpei._sdc_deleted_at__inst is null
        ) pm ON pm.row_id = p.row_id
        LEFT JOIN (
            SELECT pav.value, pav.product_id
            FROM magento.product_attribute_var pav
            JOIN magento.attribute_product ap ON pav.attribute_id = ap.id
            AND ap.name = 'url_key'
            AND pav.store_id = 0
        ) url ON url.product_id = p.row_id
        LEFT JOIN (
            SELECT pai.value, pai.product_id
            FROM magento.product_attribute_int pai
            JOIN magento.attribute_product ap ON pai.attribute_id = ap.id
            AND ap.name = 'status'
            AND pai.store_id = 0
        ) status ON status.product_id = p.row_id
        LEFT JOIN (
            SELECT pai.value, pai.product_id
            FROM magento.product_attribute_int pai
            JOIN magento.attribute_product ap ON pai.attribute_id = ap.id
            AND ap.name = 'visibility'
            AND pai.store_id = 0
        ) visibility ON visibility.product_id = p.row_id
        """

        # Adicione JOINS adicionais conforme necessário
        additional_joins = [
            add_attribute_join("product_attribute_int", "hide_from_product_view"),
            add_attribute_join("product_attribute_var", "manufacturer_sku"),
            add_attribute_join("product_attribute_txt", "category_ids"),
            add_attribute_join("product_attribute_txt", "applications"),
            add_attribute_join("product_attribute_txt", "abrasion_wear_resistance"),
            add_attribute_join("product_attribute_var", "acoustics"),
            add_attribute_join("product_attribute_var", "ada"),
            add_attribute_join("product_attribute_var", "antimicrobial_testing"),
            add_attribute_join("product_attribute_var", "backing_input"),
            add_attribute_join("product_attribute_var", "thickness_pile_height_carpet"),
            add_attribute_join("product_attribute_txt", "classification"),
            add_attribute_join("product_attribute_txt", "collection_name"),
            add_attribute_join("product_attribute_txt", "construction"),
            add_attribute_join("product_attribute_var", "content_test_attribute"),
            add_attribute_join("product_attribute_txt", "custom_capabilities"),
            add_attribute_join("product_attribute_txt", "description"),
            # Note: Appears twice in Excel, adjust if needed
            add_attribute_join("product_attribute_txt", "designer_name"),
            add_attribute_join("product_attribute_var", "edge_end_detail"),
            add_attribute_join("product_attribute_var", "emissions"),
            add_attribute_join("product_attribute_var", "finish_treatment"),
            add_attribute_join("product_attribute_txt", "flammability"),
            add_attribute_join("product_attribute_var", "grout_width"),
            add_attribute_join("product_attribute_var", "hanging_information"),
            add_attribute_join("product_attribute_var", "hardness_result"),
            add_attribute_join("product_attribute_var", "hide_configuration"),
            add_attribute_join("product_attribute_var", "installation"),
            add_attribute_join("product_attribute_var", "installation_direction"),
            add_attribute_join("product_attribute_var", "lead_time"),
            add_attribute_join("product_attribute_txt", "leed"),
            add_attribute_join("product_attribute_var", "length"),
            add_attribute_join("product_attribute_var", "lightfastness_mdp"),
            add_attribute_join("product_attribute_txt", "maintenance_maya"),
            add_attribute_join("product_attribute_txt", "manufacturer_notes"),
            add_attribute_join("product_attribute_var", "min_max_units"),
            add_attribute_join("product_attribute_var", "percentage_of_opacity"),
            add_attribute_join("product_attribute_var", "order_increments"),
            add_attribute_join("product_attribute_var", "thickness"),
            add_attribute_join("product_attribute_var", "pattern_repeat_width"),
            add_attribute_join("product_attribute_var", "performance"),
            add_attribute_join("product_attribute_var", "pile_face_weight_carpet"),
            add_attribute_join("product_attribute_txt", "product_details"),
            add_attribute_join("product_attribute_var", "recommended_grout"),
            add_attribute_join("product_attribute_var", "series_name"),
            add_attribute_join("product_attribute_txt", "size_notes"),
            add_attribute_join("product_attribute_var", "slip_resistance"),
            add_attribute_join("product_attribute_var", "stain_resistance"),
            add_attribute_join("product_attribute_int", "state_of_origin"),
            add_attribute_join("product_attribute_var", "surface_pile_density"),
            add_attribute_join("product_attribute_var", "surface_texture_mdp"),
            add_attribute_join("product_attribute_var", "tile_sheet_dimensions"),
            add_attribute_join("product_attribute_var", "total_weight"),
            add_attribute_join("product_attribute_var", "warranty"),
            add_attribute_join("product_attribute_var", "water_absorption_result"),
            add_attribute_join("product_attribute_var", "wear_layer_thickness"),
            add_attribute_join("product_attribute_var", "weather_resistance_result"),
            add_attribute_join("product_attribute_var", "width"),
            add_attribute_join("product_attribute_var", "wood_grade"),
            add_attribute_join("product_attribute_var", "yarn_tuft_details"),
            add_attribute_join("product_attribute_var", "year_of_introduction")
        ]

        # Combine base_query with additional_joins
        full_query = base_query + ' ' + ' '.join(additional_joins) + """
            WHERE hide_from_product_view.value = 0
            AND visibility.value = 2
            AND pm.manufacturer_id NOT IN (50461, 50462, 50459, 50460, 38379)
            LIMIT 50;
        """

        # Execute a query
        df = query_to_dataframe(conn, full_query)

        # Renomeia as colunas do DataFrame
        df = rename_dataframe_columns(df, conn)

        # Fecha a conexão
        close_connection(conn)

        end_time = time.time()
        duration = end_time - start_time
        print(f"O processo todo levou {duration} segundos.")

        return df  # Retorna o DataFrame diretamente

    return None  # Retorna None se a conexão falhar


if __name__ == '__main__':
    main()

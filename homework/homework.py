"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """

    import glob
    import pandas as pd
    import os

    zip_files = glob.glob("files/input/**/*.zip", recursive=True)

    columns = ["client_id", "age", "job", "marital", "education", "credit_default", "mortgage", "number_contacts", "contact_duration",
               "previous_campaign_contacts", "previous_outcome", "campaign_outcome", "last_contact_date", "cons_price_idx", "euribor_three_months"]
    
    client_data = []
    campaign_data = []
    economics_data = []

    for zip_file in zip_files:
        df = pd.read_csv(zip_file)
        df["job"] = df["job"].str.replace(".", "").str.replace("-", "_")
        df["education"] = df["education"].str.replace(".", "_")
        df["education"] = df["education"].apply(lambda x: pd.NA if x == "unknown" else x)
        df["credit_default"] = df["credit_default"].apply(lambda x: 1 if x == "yes" else 0)
        df["mortgage"] = df["mortgage"].apply(lambda x: 1 if x == "yes" else 0)
        df["previous_outcome"] = df["previous_outcome"].apply(lambda x: 1 if x == "success" else 0)
        df["campaign_outcome"] = df["campaign_outcome"].apply(lambda x: 1 if x == "yes" else 0)
        df["last_contact_date"] = pd.to_datetime(df["day"].astype(str) + '-' + df["month"] + "-2022", format="%d-%b-%Y")
        client_data.extend(df[columns[:7]].values.tolist())
        campaign_data.extend(df[columns[0:1]].join(df[columns[7:13]]).values.tolist())
        economics_data.extend(df[columns[0:1]].join(df[columns[13:]]).values.tolist())

    client_pd = pd.DataFrame(client_data,columns=columns[:7])
    campaign_pd = pd.DataFrame(campaign_data,columns=[columns[0]] + columns[7:13])
    economics_pd = pd.DataFrame(economics_data,columns=[columns[0]] + columns[13:])
    
    route = "files/output"
    os.makedirs(route, exist_ok=True)

    client_pd.to_csv(os.path.join(route, "client.csv"), index=False)
    campaign_pd.to_csv(os.path.join(route, "campaign.csv"), index=False)
    economics_pd.to_csv(os.path.join(route, "economics.csv"), index=False)

    return

if __name__ == "__main__":
    clean_campaign_data()

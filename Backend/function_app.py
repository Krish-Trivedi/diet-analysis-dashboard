import azure.functions as func
import time
import json
import logging
import pandas as pd
import os
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp()

# Helper function: read CSV from Blob
def read_csv_from_blob():
    try:
        connect_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)

        container_client = blob_service_client.get_container_client("datasets")
        blob_client = container_client.get_blob_client("All_Diets.csv")

        download_stream = blob_client.download_blob().readall()

        df = pd.read_csv(pd.io.common.BytesIO(download_stream))

        # Clean data
        df["Protein(g)"] = pd.to_numeric(df["Protein(g)"], errors="coerce")
        df["Carbs(g)"]   = pd.to_numeric(df["Carbs(g)"],   errors="coerce")
        df["Fat(g)"]     = pd.to_numeric(df["Fat(g)"],     errors="coerce")

        df.fillna(df.mean(numeric_only=True), inplace=True)

        return df

    except Exception as e:
        logging.error(f"Error reading CSV from blob: {e}")
        return pd.DataFrame()


# API 1: Nutritional Insights (Average macros per diet type)
@app.route(route="nutritionalInsights", auth_level=func.AuthLevel.ANONYMOUS)
def nutritionalInsights(req: func.HttpRequest) -> func.HttpResponse:

    logging.info("Nutritional Insights API triggered.")
    start = time.time()

    df = read_csv_from_blob()

    avg_macros = (
        df.groupby("Diet_type")[["Protein(g)", "Carbs(g)", "Fat(g)"]]
        .mean()
        .round(2)
        .reset_index()
    )

    data = avg_macros.to_dict(orient="records")

    response = {
        "execution_time": round(time.time() - start, 3),
        "record_count": len(data),
        "data": data
    }

    return func.HttpResponse(json.dumps(response), mimetype="application/json")


# API 2: Recipes (Top 5 protein-rich recipes per diet type)
@app.route(route="recipes", auth_level=func.AuthLevel.ANONYMOUS)
def recipes(req: func.HttpRequest) -> func.HttpResponse:

    logging.info("Recipes API triggered.")
    start = time.time()

    df = read_csv_from_blob()

    top_recipes = (
        df.sort_values("Protein(g)", ascending=False)
        .groupby("Diet_type")
        .head(5)[["Diet_type", "Recipe_name", "Cuisine_type", "Protein(g)", "Carbs(g)", "Fat(g)"]]
    )

    data = top_recipes.to_dict(orient="records")

    response = {
        "execution_time": round(time.time() - start, 3),
        "record_count": len(data),
        "data": data
    }

    return func.HttpResponse(json.dumps(response), mimetype="application/json")


# API 3: Clusters (Nutritional ratios per diet type — averaged, no Infinity)
@app.route(route="clusters", auth_level=func.AuthLevel.ANONYMOUS)
def clusters(req: func.HttpRequest) -> func.HttpResponse:

    logging.info("Clusters API triggered.")
    start = time.time()

    df = read_csv_from_blob()

    # FIX: replace 0 with NaN before dividing to avoid Infinity, then fill with 0
    df["Protein_to_Carbs_ratio"] = (
        df["Protein(g)"] / df["Carbs(g)"].replace(0, float("nan"))
    ).fillna(0)

    df["Carbs_to_Fat_ratio"] = (
        df["Carbs(g)"] / df["Fat(g)"].replace(0, float("nan"))
    ).fillna(0)

    # Group by diet type and average the ratios (keeps response small and meaningful)
    cluster_summary = (
        df.groupby("Diet_type")[[
            "Protein(g)", "Carbs(g)", "Fat(g)",
            "Protein_to_Carbs_ratio", "Carbs_to_Fat_ratio"
        ]]
        .mean()
        .round(3)
        .reset_index()
    )

    data = cluster_summary.to_dict(orient="records")

    response = {
        "execution_time": round(time.time() - start, 3),
        "record_count": len(data),
        "data": data
    }

    return func.HttpResponse(json.dumps(response), mimetype="application/json")

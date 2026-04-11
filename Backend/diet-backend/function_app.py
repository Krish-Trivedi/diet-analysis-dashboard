import azure.functions as func
import time
import json
import logging
import pandas as pd
import os
import redis
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp()


def get_redis_client():
    return redis.Redis(
        host=os.environ["REDIS_HOST"],
        port=int(os.environ.get("REDIS_PORT", "10000")),
        username="default",
        password=os.environ["REDIS_PASSWORD"],
        ssl=True,
        ssl_cert_reqs=None,
        decode_responses=True,
        socket_connect_timeout=10,
        socket_timeout=10,
        retry_on_timeout=False
    )


def test_redis():
    client = get_redis_client()
    return client.ping()


def get_cached_json(key: str):
    client = get_redis_client()
    raw = client.get(key)
    return json.loads(raw) if raw else None


def set_cached_json(key: str, value):
    client = get_redis_client()
    client.set(key, json.dumps(value))
    logging.info(f"Redis write success for {key}")


def read_csv_from_blob() -> pd.DataFrame:
    try:
        connect_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)

        container_client = blob_service_client.get_container_client("datasets")
        blob_client = container_client.get_blob_client("All_Diets.csv")

        download_stream = blob_client.download_blob().readall()
        df = pd.read_csv(pd.io.common.BytesIO(download_stream))

        df["Protein(g)"] = pd.to_numeric(df["Protein(g)"], errors="coerce")
        df["Carbs(g)"] = pd.to_numeric(df["Carbs(g)"], errors="coerce")
        df["Fat(g)"] = pd.to_numeric(df["Fat(g)"], errors="coerce")
        df.fillna(df.mean(numeric_only=True), inplace=True)

        return df

    except Exception as e:
        logging.exception(f"Error reading CSV from blob: {e}")
        return pd.DataFrame()


def save_cleaned_csv_to_blob(df: pd.DataFrame) -> None:
    try:
        connect_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)

        container_client = blob_service_client.get_container_client("datasets")
        blob_client = container_client.get_blob_client("cleaned_diets.csv")

        blob_client.upload_blob(
            df.to_csv(index=False).encode("utf-8"),
            overwrite=True
        )

        logging.info("cleaned_diets.csv uploaded successfully")

    except Exception as e:
        logging.exception(f"Error saving cleaned CSV: {e}")
        raise


def build_nutritional_insights(df: pd.DataFrame) -> dict:
    avg_macros = (
        df.groupby("Diet_type")[["Protein(g)", "Carbs(g)", "Fat(g)"]]
        .mean()
        .round(2)
        .reset_index()
    )

    return {
        "diet_count": int(df["Diet_type"].nunique()),
        "record_count": int(len(avg_macros)),
        "data": avg_macros.to_dict(orient="records")
    }


def build_recipes(df: pd.DataFrame) -> dict:
    top_recipes = (
        df.sort_values(["Diet_type", "Protein(g)"], ascending=[True, False])
        .groupby("Diet_type")
        .head(5)[
            ["Diet_type", "Recipe_name", "Cuisine_type", "Protein(g)", "Carbs(g)", "Fat(g)"]
        ]
    )

    return {
        "record_count": int(len(top_recipes)),
        "data": top_recipes.to_dict(orient="records")
    }


def build_clusters(df: pd.DataFrame) -> dict:
    cluster_data = (
        df.groupby("Diet_type")[["Protein(g)", "Carbs(g)", "Fat(g)"]]
        .mean()
    )

    cluster_data["Protein_to_Carbs_ratio"] = (
        cluster_data["Protein(g)"] / cluster_data["Carbs(g)"].replace(0, pd.NA)
    )

    cluster_data["Carbs_to_Fat_ratio"] = (
        cluster_data["Carbs(g)"] / cluster_data["Fat(g)"].replace(0, pd.NA)
    )

    cluster_data.replace([float("inf"), -float("inf")], pd.NA, inplace=True)
    cluster_data = cluster_data.reset_index().fillna(0).round(3)

    return {
        "record_count": int(len(cluster_data)),
        "data": cluster_data.to_dict(orient="records")
    }


def load_cache_from_blob() -> dict:
    df = read_csv_from_blob()

    if df.empty:
        return {"ok": False, "message": "Dataset is empty or could not be read."}

    redis_ok = test_redis()
    logging.info(f"Redis ping success: {redis_ok}")

    save_cleaned_csv_to_blob(df)

    nutritional = build_nutritional_insights(df)
    recipes_data = build_recipes(df)
    clusters_data = build_clusters(df)

    set_cached_json("api:nutritionalInsights", nutritional)
    set_cached_json("api:recipes", recipes_data)
    set_cached_json("api:clusters", clusters_data)

    return {
        "ok": True,
        "message": "Cache loaded successfully from All_Diets.csv",
        "nutritional_count": nutritional["record_count"],
        "recipes_count": recipes_data["record_count"],
        "clusters_count": clusters_data["record_count"]
    }


@app.blob_trigger(
    arg_name="inputblob",
    path="datasets/{name}",
    connection="AzureWebJobsStorage",
    source="EventGrid"
)
def diets_blob_trigger(inputblob: func.InputStream):
    try:
        logging.info(f"Blob trigger fired for: {inputblob.name}")

        blob_name = inputblob.name.split("/")[-1]
        if blob_name != "All_Diets.csv":
            logging.info(
                f"Skipping blob '{blob_name}' because only All_Diets.csv should be processed."
            )
            return

        result = load_cache_from_blob()
        logging.info(json.dumps(result))

    except Exception as e:
        logging.exception(f"diets_blob_trigger failed: {e}")
        raise


@app.route(route="loadCache", auth_level=func.AuthLevel.ANONYMOUS)
def loadCache(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Manual cache load API triggered.")

    try:
        result = load_cache_from_blob()
        status_code = 200 if result["ok"] else 500
        return func.HttpResponse(
            json.dumps(result),
            status_code=status_code,
            mimetype="application/json"
        )

    except Exception as e:
        logging.exception(f"loadCache failed: {e}")
        return func.HttpResponse(
            json.dumps({"ok": False, "message": str(e)}),
            status_code=500,
            mimetype="application/json"
        )


@app.route(route="nutritionalInsights", auth_level=func.AuthLevel.ANONYMOUS)
def nutritionalInsights(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Nutritional Insights API triggered.")
    start = time.time()

    try:
        cached = get_cached_json("api:nutritionalInsights")

        if cached is None:
            return func.HttpResponse(
                json.dumps({"error": "Cached data not available. Trigger the blob update first."}),
                status_code=500,
                mimetype="application/json"
            )

        cached["execution_time"] = round(time.time() - start, 3)
        return func.HttpResponse(
            json.dumps(cached),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        logging.exception(f"nutritionalInsights failed: {e}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )


@app.route(route="recipes", auth_level=func.AuthLevel.ANONYMOUS)
def recipes(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Recipes API triggered.")
    start = time.time()

    try:
        cached = get_cached_json("api:recipes")

        if cached is None:
            return func.HttpResponse(
                json.dumps({"error": "Cached data not available. Trigger the blob update first."}),
                status_code=500,
                mimetype="application/json"
            )

        cached["execution_time"] = round(time.time() - start, 3)
        return func.HttpResponse(
            json.dumps(cached),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        logging.exception(f"recipes failed: {e}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )


@app.route(route="clusters", auth_level=func.AuthLevel.ANONYMOUS)
def clusters(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Clusters API triggered.")
    start = time.time()

    try:
        cached = get_cached_json("api:clusters")

        if cached is None:
            return func.HttpResponse(
                json.dumps({"error": "Cached data not available. Trigger the blob update first."}),
                status_code=500,
                mimetype="application/json"
            )

        cached["execution_time"] = round(time.time() - start, 3)
        return func.HttpResponse(
            json.dumps(cached),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        logging.exception(f"clusters failed: {e}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )
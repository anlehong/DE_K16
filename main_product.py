from product_enrichment.collects_product import collect_products_data
import time
import logging

if __name__ == "__main__":
    start_time = time.time()
    logging.info(f"Starting crawling product data to enrich data")

    try:
        collect_products_data("product_enrichment/data/add_to_cart_action.json")
    except Exception as e:
        logging.exception("Error when collecting product data:", e)

    end_time = time.time()
    logging.info(f"Completed product enrichment in {end_time - start_time:.2f} seconds")